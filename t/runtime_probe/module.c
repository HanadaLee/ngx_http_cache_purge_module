#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_http_cache_purge.h"

extern ngx_module_t ngx_http_cache_purge_runtime_probe_module;
typedef struct {
    ngx_flag_t                     enabled, wrong_uid;
    ngx_str_t                       rpc;
    ngx_http_cache_purge_client_t  *client;
} probe_conf_t;
typedef struct {
    ngx_flag_t   enable_agent;
    uid_t        owner_uid;
    ngx_array_t  clients;
} probe_main_t;
typedef struct {
    unsigned flags;
} probe_result_t;
typedef struct {
    ngx_http_request_t             *request;
    ngx_http_cache_purge_call_t    *call;
    ngx_http_cache_purge_client_t  *client;
    ngx_str_t                      prepare, temporary;
    ngx_msec_t  timeout;
    int         temporary_fd;
    unsigned    prepare_sent : 1;
    unsigned    finish_requested : 1;
} probe_rpc_t;

static ngx_int_t rpc_handler(ngx_http_request_t *r);

static ngx_int_t
rpc_header(ngx_http_request_t *r, const char *name, ngx_uint_t value)
{
    ngx_table_elt_t *h = ngx_list_push(&r->headers_out.headers);
    if (h == NULL) {
        return NGX_ERROR;
    }
    h->hash = 1;
    h->key.data = (u_char *) name;
    h->key.len = ngx_strlen(name);
    h->value.data = ngx_pnalloc(r->pool, NGX_INT_T_LEN);
    if (h->value.data == NULL) {
        return NGX_ERROR;
    }
    h->value.len = ngx_sprintf(h->value.data, "%ui", value) - h->value.data;
    return NGX_OK;
}

static ngx_int_t
rpc_send(ngx_http_request_t *r, ngx_uint_t status, const ngx_str_t *body)
{
    ngx_buf_t   *b;
    ngx_chain_t  out;
    ngx_int_t    rc;

    r->headers_out.status = status;
    r->headers_out.content_length_n = body->len;
    ngx_str_set(&r->headers_out.content_type, "application/octet-stream");
    if (rpc_header(r, "X-IPC-Worker", (ngx_uint_t) ngx_pid) != NGX_OK) {
        return NGX_ERROR;
    }
    rc = ngx_http_send_header(r);
    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }
    if (body->len == 0) {
        return ngx_http_send_special(r, NGX_HTTP_LAST);
    }
    b = ngx_create_temp_buf(r->pool, body->len);
    if (b == NULL) {
        return NGX_ERROR;
    }
    b->last = ngx_cpymem(b->pos, body->data, body->len);
    b->last_buf = r == r->main;
    b->last_in_chain = 1;
    out.buf = b;
    out.next = NULL;
    return ngx_http_output_filter(r, &out);
}

static void
rpc_cleanup(void *data)
{
    probe_rpc_t *ctx = data;
    if (ctx->call) {
        ngx_http_cache_purge_client_cancel(ctx->call);
        ctx->call = NULL;
    }
}

static void
rpc_done(ngx_http_cache_purge_call_t *call, void *data,
         const ngx_http_cache_purge_reply_t *reply)
{
    probe_rpc_t        *ctx = data;
    ngx_http_request_t *r = ctx->request;
    ngx_uint_t  status;
    ngx_int_t   rc;

    ctx->call = NULL;
    status = reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK      ? 200 :
             reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_TIMEOUT ? 504 :
                                                                       503;
    if (rpc_header(r, "X-IPC-Transport", reply->transport) != NGX_OK
        || rpc_header(r, "X-IPC-Lease", reply->lease_fd >= 0) != NGX_OK
        || rpc_header(r, "X-IPC-Result", reply->result) != NGX_OK
        || rpc_header(r, "X-IPC-Ambiguous", reply->may_have_executed) != NGX_OK)
    {
        ngx_http_finalize_request(r, NGX_ERROR);
        return;
    }
    rc = rpc_send(r, status, &reply->payload);
    ngx_http_finalize_request(r, rc);
}

static ngx_http_cache_purge_client_t *
rpc_client(ngx_http_request_t *r)
{
    probe_conf_t  *conf;
    probe_main_t  *main;
    ngx_http_cache_purge_client_t        *client, **slot;
    ngx_http_cache_purge_client_options_t options;

    conf = ngx_http_get_module_loc_conf(
        r, ngx_http_cache_purge_runtime_probe_module);
    if (conf->rpc.len == 7 && ngx_strncmp(conf->rpc.data, "runtime", 7) == 0) {
        if (ngx_http_cache_purge_runtime_client(r, &client) != 0) {
            return NULL;
        }
        return client;
    }
    if (conf->client) {
        return conf->client;
    }
    main = ngx_http_get_module_main_conf(
        r, ngx_http_cache_purge_runtime_probe_module);
    ngx_http_cache_purge_client_options(&options);
    options.server_uid = main->owner_uid + (conf->wrong_uid == 1 ? 1 : 0);
    options.max_pending = 4;
    options.max_payload = 524288;
    options.request_bytes = 524312;
    slot = ngx_array_push(&main->clients);
    if (slot == NULL) {
        return NULL;
    }
    *slot = ngx_http_cache_purge_client_create(ngx_cycle->pool, ngx_cycle->log,
                                               &conf->rpc, &options);
    conf->client = *slot;
    return *slot;
}

static u_char *
rpc_number(u_char *p, uint64_t value, ngx_uint_t width)
{
    ngx_uint_t i;
    for (i = width; i; i--) {
        *p++ = (u_char) (value >> ((i - 1) * 8));
    }
    return p;
}

static u_char *
rpc_string(u_char *p, ngx_str_t value)
{
    p = rpc_number(p, value.len, 4);
    return value.len ? ngx_cpymem(p, value.data, value.len) : p;
}

static void
rpc_prepare_done(ngx_http_cache_purge_call_t *call, void *data,
                 const ngx_http_cache_purge_reply_t *reply)
{
    probe_rpc_t        *ctx = data;
    ngx_http_request_t *r = ctx->request;
    ngx_table_elt_t  *header;
    ngx_int_t         rc;
    int               fds[2];
    ctx->call = NULL;
    if (!ctx->prepare_sent && reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK
        && reply->result == 0)
    {
        fds[0] = reply->lease_fd;
        fds[1] = ctx->temporary_fd;
        ctx->prepare_sent = 1;
        if (ctx->finish_requested) {
            rc = ngx_http_cache_purge_client_finish_file(
                ctx->client, &ctx->prepare, fds[0], ctx->timeout,
                rpc_prepare_done, ctx, &ctx->call);
        } else {
            rc = ngx_http_cache_purge_client_submit_files(
                ctx->client, &ctx->prepare, fds, ctx->timeout, rpc_prepare_done,
                ctx, &ctx->call);
        }
        if (rc != NGX_OK) {
            ngx_http_finalize_request(r, 503);
        }
        return;
    }
    header = ngx_list_push(&r->headers_out.headers);
    if (header == NULL) {
        ngx_http_finalize_request(r, NGX_ERROR);
        return;
    }
    header->hash = 1;
    ngx_str_set(&header->key, "X-IPC-Temp-Path");
    header->value = ctx->temporary;
    rpc_done(call, data, reply);
}

/* Test provider: use nginx's own request-body temporary FD, not synchronous
 * file creation in the request handler. This is not a cache-fill consumer. */
static void
rpc_prepare_body(ngx_http_request_t *r, ngx_http_cache_purge_client_t *client,
                 ngx_msec_t timeout)
{
    ngx_str_t root, zone = ngx_string("a"), value, target, address;
    ngx_str_t fields[] = { ngx_string("publication-probe"),
                           ngx_string("https://probe.test/publication"),
                           ngx_string("probe.test"),
                           ngx_string("team"),
                           ngx_null_string,
                           ngx_null_string,
                           ngx_null_string,
                           ngx_null_string,
                           ngx_null_string };
    ngx_str_t empty = ngx_null_string, metadata = ngx_string("fixture");
    ngx_http_cleanup_t  *cleanup;
    probe_rpc_t         *ctx;
    ngx_int_t           id, rc;
    ngx_uint_t  i;
    u_char     *p;
    ngx_fd_t    selected_fd;
    if (!r->request_body || !r->request_body->temp_file
        || ngx_http_arg(r, (u_char *) "root", 4, &root) != NGX_OK
        || ngx_http_arg(r, (u_char *) "id", 2, &value) != NGX_OK
        || (id = ngx_atoi(value.data, value.len)) <= 0)
    {
        ngx_http_finalize_request(r, 400);
        return;
    }
    (void) ngx_http_arg(r, (u_char *) "zone", 4, &zone);
    target.data = ngx_pnalloc(r->pool, 64);
    ctx = ngx_pcalloc(r->pool, sizeof(*ctx));
    cleanup = ngx_http_cleanup_add(r, 0);
    if (!target.data || !ctx || !cleanup) {
        ngx_http_finalize_request(r, 500);
        return;
    }
    target.len = ngx_sprintf(target.data, "prepared-%i", id) - target.data;
    ctx->request = r;
    ctx->client = client;
    ctx->timeout = timeout;
    ctx->finish_requested =
        ngx_http_arg(r, (u_char *) "finish", 6, &value) == NGX_OK;
    ctx->temporary = r->request_body->temp_file->file.name;
    ctx->temporary_fd = r->request_body->temp_file->file.fd;
    cleanup->handler = rpc_cleanup;
    cleanup->data = ctx;
    ctx->prepare.data =
        ngx_pnalloc(r->pool, 1024 + root.len + zone.len + ctx->temporary.len);
    address.data = ngx_pnalloc(r->pool, 4 + root.len + 1 + target.len);
    if (!ctx->prepare.data || !address.data) {
        ngx_http_finalize_request(r, 500);
        return;
    }
    if (ngx_http_arg(r, (u_char *) "inspect", 7, &value) == NGX_OK) {
        p = rpc_string(ctx->prepare.data, root);
        p = rpc_string(p, zone);
        p = rpc_string(p, target);
        ctx->prepare.len = p - ctx->prepare.data;
        p = ngx_cpymem(address.data, root.data, root.len);
        *p++ = '/';
        p = ngx_cpymem(p, target.data, target.len);
        *p = '\0';
        /* Test-only open; the production consumer supplies r->cache->file.fd. */
        selected_fd = ngx_open_file(address.data, NGX_FILE_RDONLY,
                                    NGX_FILE_OPEN, 0);
        if (selected_fd == NGX_INVALID_FILE) {
            ngx_http_finalize_request(r, 404);
            return;
        }
        rc = ngx_http_cache_purge_client_inspect_file(
            client, &ctx->prepare, selected_fd, timeout, rpc_done, ctx,
            &ctx->call);
        ngx_close_file(selected_fd);
        if (rc != NGX_OK) {
            ngx_http_finalize_request(r, 503);
            return;
        }
        r->read_event_handler = ngx_http_test_reading;
        if (ngx_handle_read_event(r->connection->read, 0) != NGX_OK) {
            ngx_http_finalize_request(r, NGX_ERROR);
        }
        return;
    }
    p = ctx->prepare.data;
    p = rpc_number(p, 0, 8);
    p = rpc_number(p, id, 8);
    fields[4] = root;
    fields[5] = zone;
    fields[6] = target;
    for (i = 0; i < 9; i++) {
        p = rpc_string(p, fields[i]);
    }
    p = rpc_string(p, metadata);
    *p++ = 0;
    *p++ = 0;
    *p++ = 0;
    for (i = 0; i < 6; i++) {
        p = rpc_string(p, empty);
    }
    p = rpc_number(p, 0, 8);
    p = rpc_number(p, 0, 8);
    p = rpc_string(p, ctx->temporary);
    ctx->prepare.len = p - ctx->prepare.data;
    if (ctx->finish_requested) {
        p = rpc_number(ctx->prepare.data, 0, 8);
        p = rpc_number(p, id, 8);
        p = rpc_number(p, 1, 8);
        ctx->prepare.len = p - ctx->prepare.data;
    }
    p = rpc_number(address.data, root.len + 1 + target.len, 4);
    p = ngx_cpymem(p, root.data, root.len);
    *p++ = '/';
    p = ngx_cpymem(p, target.data, target.len);
    address.len = p - address.data;
    rc = ngx_http_cache_purge_client_submit(
        client, NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_FILE_LEASE, &address, timeout,
        rpc_prepare_done, ctx, &ctx->call);
    if (rc != NGX_OK) {
        ngx_http_finalize_request(r, 503);
        return;
    }
    r->read_event_handler = ngx_http_test_reading;
    if (ngx_handle_read_event(r->connection->read, 0) != NGX_OK) {
        ngx_http_finalize_request(r, NGX_ERROR);
    }
}

static void
rpc_body(ngx_http_request_t *r)
{
    ngx_http_cache_purge_client_t  *client;
    ngx_http_cleanup_t             *cleanup;
    probe_rpc_t                    *ctx;
    ngx_chain_t                    *cl;
    ngx_str_t                      payload = ngx_null_string, value;
    ngx_int_t op = NGX_HTTP_CACHE_PURGE_OP_PING, timeout = 2000, rc;
    u_char   *p;

    client = rpc_client(r);
    if (client == NULL) {
        ngx_http_finalize_request(r, 503);
        return;
    }
    if (ngx_http_arg(r, (u_char *) "op", 2, &value) == NGX_OK) {
        op = ngx_atoi(value.data, value.len);
    }
    if (ngx_http_arg(r, (u_char *) "timeout", 7, &value) == NGX_OK) {
        timeout = ngx_atoi(value.data, value.len);
    }
    if (op < 1 || op > 65535 || timeout < 1 || timeout > 10000) {
        ngx_http_finalize_request(r, 400);
        return;
    }
    if (ngx_http_arg(r, (u_char *) "files", 5, &value) == NGX_OK) {
        rpc_prepare_body(r, client, timeout);
        return;
    }
    for (cl = r->request_body ? r->request_body->bufs : NULL; cl; cl = cl->next)
    {
        if (cl->buf->in_file
            || ngx_buf_size(cl->buf) > 524288 - (off_t) payload.len)
        {
            ngx_http_finalize_request(r, 413);
            return;
        }
        payload.len += ngx_buf_size(cl->buf);
    }
    if (payload.len) {
        payload.data = ngx_pnalloc(r->pool, payload.len);
        if (payload.data == NULL) {
            ngx_http_finalize_request(r, 500);
            return;
        }
        p = payload.data;
        for (cl = r->request_body->bufs; cl; cl = cl->next) {
            p = ngx_cpymem(p, cl->buf->pos, ngx_buf_size(cl->buf));
        }
    }
    ctx = ngx_pcalloc(r->pool, sizeof(*ctx));
    cleanup = ngx_http_cleanup_add(r, 0);
    if (ctx == NULL || cleanup == NULL) {
        ngx_http_finalize_request(r, 500);
        return;
    }
    ctx->request = r;
    cleanup->handler = rpc_cleanup;
    cleanup->data = ctx;
    rc = ngx_http_cache_purge_client_submit(client, (uint16_t) op, &payload,
                                            timeout, rpc_done, ctx, &ctx->call);
    if (rc != NGX_OK) {
        ngx_http_finalize_request(r, rc == NGX_BUSY ? 503 : 500);
    } else {
        r->read_event_handler = ngx_http_test_reading;
        if (ngx_handle_read_event(r->connection->read, 0) != NGX_OK) {
            ngx_http_finalize_request(r, NGX_ERROR);
        }
    }
}

static ngx_int_t
rpc_handler(ngx_http_request_t *r)
{
    ngx_int_t                           rc;
    ngx_str_t                           value, body;
    ngx_http_cache_purge_client_t       *client;
    ngx_http_cache_purge_client_stats_t  stats;
    u_char                               buffer[384];

    if (ngx_http_arg(r, (u_char *) "stop", 4, &value) == NGX_OK) {
        client = rpc_client(r);
        if (client == NULL) {
            return 503;
        }
        ngx_http_cache_purge_client_stop(client);
        ngx_str_set(&body, "stopped");
        return rpc_send(r, 200, &body);
    }
    if (ngx_http_arg(r, (u_char *) "stats", 5, &value) == NGX_OK) {
        client = rpc_client(r);
        if (client == NULL) {
            return 503;
        }
        ngx_http_cache_purge_client_stats(client, &stats);
        body.data = buffer;
        body.len =
            ngx_snprintf(buffer, sizeof(buffer),
                         "{\"accepted\":%uL,\"completed\":%uL,\"cancelled\":%"
                         "uL,\"rejected\":%uL,\"connections\":%uL,\"pending\":%"
                         "ui,\"request_bytes\":%uz,\"response_bytes\":%uz,"
                         "\"peak_bytes\":%uz}",
                         stats.accepted, stats.completed, stats.cancelled,
                         stats.rejected, stats.connections, stats.pending,
                         stats.request_bytes, stats.response_bytes,
                         stats.peak_bytes)
            - buffer;
        return rpc_send(r, 200, &body);
    }
    r->request_body_in_single_buf = 1;
    rc = ngx_http_read_client_request_body(r, rpc_body);
    return rc >= NGX_HTTP_SPECIAL_RESPONSE ? rc : NGX_DONE;
}

static char *
rpc_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    char                     *rc = ngx_conf_set_str_slot(cf, cmd, conf);
    ngx_http_core_loc_conf_t *core;
    if (rc != NGX_CONF_OK) {
        return rc;
    }
    core = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    core->handler = rpc_handler;
    return NGX_CONF_OK;
}

static int
visit(void *data, const ngx_http_cache_purge_control_match_t *match)
{
    probe_result_t *result = data;
    result->flags |= match->flags;
    return 0;
}

static ngx_int_t
guard(ngx_http_request_t *r)
{
    probe_conf_t                           *conf;
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_rocksdb_error_t          error;
    ngx_http_cache_purge_control_context_t  context;
    probe_result_t                         result = { 0 };
    uint64_t  publication;
    int       rc;

    conf = ngx_http_get_module_loc_conf(
        r, ngx_http_cache_purge_runtime_probe_module);
    if (!conf->enabled) {
        return NGX_DECLINED;
    }
    rc = ngx_http_cache_purge_runtime_reader(r, &reader, &error);
    if (rc != 0) {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    ngx_memzero(&context, sizeof(context));
    /* Deliberately test the barrier with URI bytes. This fixture is not the
     * production key/URL provider and does not attest native file hooks. */
    context.key.data = r->uri.data;
    context.key.len = r->uri.len;
    context.url = context.key;
    context.host.data = r->headers_in.server.data;
    context.host.len = r->headers_in.server.len;
    context.now_ms = (uint64_t) ngx_time() * 1000;
    context.proxy = 1;
    rc = ngx_http_cache_purge_control_match(reader, &context, 64, 65536, visit,
                                            &result, &publication, &error);
    if (rc != 0 || (result.flags & NGX_HTTP_CACHE_PURGE_CONTROL_FENCE)) {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    return NGX_DECLINED;
}

static void *
create(ngx_conf_t *cf)
{
    probe_conf_t *conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf) {
        conf->enabled = conf->wrong_uid = NGX_CONF_UNSET;
    }
    return conf;
}

static void *
create_main(ngx_conf_t *cf)
{
    probe_main_t *conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf) {
        conf->enable_agent = NGX_CONF_UNSET;
        conf->owner_uid = geteuid();
        if (ngx_array_init(&conf->clients, cf->pool, 2,
                           sizeof(ngx_http_cache_purge_client_t *))
            != NGX_OK)
        {
            return NULL;
        }
    }
    return conf;
}

static ngx_int_t
init_module(ngx_cycle_t *cycle)
{
#if (HAVE_PRIVILEGED_PROCESS_PATCH)
    probe_main_t     *conf;
    ngx_core_conf_t  *core;
    conf = ngx_http_cycle_get_module_main_conf(
        cycle, ngx_http_cache_purge_runtime_probe_module);
    if (conf && conf->enable_agent == 1) {
        core =
            (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
        core->privileged_agent = 1;
    }
#endif
    return NGX_OK;
}

static ngx_int_t
init_process(ngx_cycle_t *cycle)
{
    ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                  "probe HTTP init_process called");
    return NGX_OK;
}

static char *
merge(ngx_conf_t *cf, void *parent, void *child)
{
    probe_conf_t *p = parent, *c = child;
    ngx_conf_merge_value(c->enabled, p->enabled, 0);
    ngx_conf_merge_value(c->wrong_uid, p->wrong_uid, 0);
    ngx_conf_merge_str_value(c->rpc, p->rpc, "");
    return NGX_CONF_OK;
}

static ngx_int_t
post(ngx_conf_t *cf)
{
    ngx_http_core_main_conf_t *core;
    ngx_http_handler_pt       *handler;
    core = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    handler = ngx_array_push(&core->phases[NGX_HTTP_PRECONTENT_PHASE].handlers);
    if (handler == NULL) {
        return NGX_ERROR;
    }
    *handler = guard;
    return NGX_OK;
}

static ngx_command_t commands[] = {

    { ngx_string("cache_purge_test_rpc"), NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      rpc_conf, NGX_HTTP_LOC_CONF_OFFSET, offsetof(probe_conf_t, rpc), NULL },
    { ngx_string("cache_purge_test_rpc_wrong_uid"),
      NGX_HTTP_LOC_CONF | NGX_CONF_FLAG, ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET, offsetof(probe_conf_t, wrong_uid), NULL },
    { ngx_string("cache_purge_test_enable_agent"),
      NGX_HTTP_MAIN_CONF | NGX_CONF_FLAG, ngx_conf_set_flag_slot,
      NGX_HTTP_MAIN_CONF_OFFSET, offsetof(probe_main_t, enable_agent), NULL },
    { ngx_string("cache_purge_test_reader"), NGX_HTTP_LOC_CONF | NGX_CONF_FLAG,
      ngx_conf_set_flag_slot, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(probe_conf_t, enabled), NULL },
    ngx_null_command
};
static ngx_http_module_t context = { NULL, post, create_main, NULL,
                                     NULL, NULL, create,      merge };
static void
exit_process(ngx_cycle_t *cycle)
{
    probe_main_t                    *main;
    ngx_http_cache_purge_client_t  **clients;
    ngx_uint_t                       i;
    main = ngx_http_cycle_get_module_main_conf(
        cycle, ngx_http_cache_purge_runtime_probe_module);
    if (main == NULL) {
        return;
    }
    clients = main->clients.elts;
    for (i = 0; i < main->clients.nelts; i++) {
        ngx_http_cache_purge_client_stop(clients[i]);
    }
}
ngx_module_t ngx_http_cache_purge_runtime_probe_module = {
    NGX_MODULE_V1, &context,     commands,     NGX_HTTP_MODULE,
    NULL,          init_module,  init_process, NULL,
    NULL,          exit_process, NULL,         NGX_MODULE_V1_PADDING
};
