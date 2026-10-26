/* Copyright (C) Hanada */
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_cache_purge_runtime.h"
#include "ngx_cache_purge_fill.h"
#include <sys/random.h>
#include <fcntl.h>

#if (NGX_HTTP_CACHE_KEY)
#include <ngx_http_cache_key_module.h>
#endif

#define CAPTURE_BYTES   (2 * 1024 * 1024)
#define CAPTURE_COUNT   64
#define CAPTURE_RECORD  (128 * 1024)
#define CAPTURE_TIMEOUT 3000

enum { FIELD_KEY, FIELD_URL, FIELD_TAG, FIELD_NAMESPACE, FIELD_COUNT };
enum { RPC_ACQUIRE, RPC_PREPARE, RPC_FINISH };
typedef struct { ngx_http_complex_value_t *field[FIELD_COUNT]; } capture_conf_t;
typedef struct {
    ngx_array_t locations;
    ngx_int_t host_index;
#if (NGX_HTTP_CACHE_KEY)
    ngx_int_t (*snapshot)(ngx_http_request_t *, ngx_http_cache_key_snapshot_t **);
    ngx_int_t (*value)(const ngx_http_cache_key_snapshot_t *, ngx_http_cache_key_field_t, ngx_str_t *);
#endif
} capture_main_t;
typedef struct {
    ngx_http_request_t *request;
    ngx_http_cache_t *cache;
    ngx_http_cache_purge_fill_t *fill;
    ngx_cache_purge_client_t *client;
    ngx_cache_purge_call_t *call;
    ngx_pool_t *publication_pool;
    ngx_temp_file_t *temporary;
    ngx_str_t key, url, host, namespace, root, zone, path;
    ngx_str_t components[6], tags[64];
    ngx_uint_t tag_count, rpc;
    size_t reserved, input_bytes;
    ngx_msec_t deadline;
    int lease;
    u_char id[16], previous[16];
    uint64_t revision;
    unsigned admitted:1;
    unsigned failed:1;
    unsigned structured_key:1;
    unsigned structured_url:1;
    unsigned need_finish:1;
} capture_t;
typedef struct {
    ngx_http_script_code_pt code;
    ngx_array_t *predicates;
    uintptr_t end;
} capture_code_t;

extern ngx_module_t ngx_http_cache_purge_index_module;
static ngx_http_output_header_filter_pt next_header;
static size_t retained_bytes;
static ngx_uint_t retained_count;
static ngx_int_t capture_step(ngx_http_request_t *, ngx_http_cache_t *, ngx_temp_file_t *, ngx_uint_t, ngx_int_t, void *);
static void capture_reply(ngx_cache_purge_call_t *, void *, const ngx_cache_purge_reply_t *);

static ngx_int_t
equal(ngx_str_t value, const char *literal)
{
    size_t length = ngx_strlen(literal);
    return value.len == length && ngx_memcmp(value.data, literal, length) == 0;
}

static ngx_int_t
reserve(capture_t *ctx, size_t bytes)
{
    if (bytes > CAPTURE_BYTES - retained_bytes) return NGX_ERROR;
    retained_bytes += bytes; ctx->reserved += bytes;
    return NGX_OK;
}

static ngx_int_t
copy(capture_t *ctx, ngx_str_t *out, ngx_str_t value)
{
    if (value.len > 32768 || reserve(ctx, value.len) != NGX_OK) return NGX_ERROR;
    out->len = value.len;
    out->data = value.len ? ngx_pnalloc(ctx->publication_pool, value.len) : NULL;
    if (value.len && out->data == NULL) return NGX_ERROR;
    if (value.len) ngx_memcpy(out->data, value.data, value.len);
    return NGX_OK;
}

static void
close_lease(capture_t *ctx)
{
    if (ctx->lease >= 0) { close(ctx->lease); ctx->lease = -1; }
}

static ngx_int_t
borrow(capture_t *ctx, ngx_str_t *out, ngx_str_t value)
{
    if (value.len > 32768 || value.len > CAPTURE_RECORD - ctx->input_bytes) return NGX_ERROR;
    ctx->input_bytes += value.len;
    *out = value;
    return NGX_OK;
}

static void
release_publication(capture_t *ctx)
{
    close_lease(ctx);
    if (ctx->publication_pool) { ngx_destroy_pool(ctx->publication_pool); ctx->publication_pool = NULL; }
    retained_bytes -= ctx->reserved; ctx->reserved = 0;
    if (ctx->admitted) { retained_count--; ctx->admitted = 0; }
}

static void
cleanup(void *data)
{
    capture_t *ctx = data;
    if (ctx->call) ngx_cache_purge_client_cancel(ctx->call);
    release_publication(ctx);
}

static ngx_int_t
native_key(capture_t *ctx, ngx_str_t *value)
{
    ngx_str_t *keys = ctx->cache->keys.elts;
    ngx_uint_t i;
    u_char *p;
    if (ctx->cache->keys.nelts == 1) return borrow(ctx, value, keys[0]);
    value->len = 0;
    for (i = 0; i < ctx->cache->keys.nelts; i++) {
        if (keys[i].len > 32768 - value->len) return NGX_ERROR;
        value->len += keys[i].len;
    }
    if (value->len > CAPTURE_RECORD - ctx->input_bytes) return NGX_ERROR;
    ctx->input_bytes += value->len;
    value->data = ngx_pnalloc(ctx->request->pool, value->len);
    if (value->data == NULL) return NGX_ERROR;
    p = value->data;
    for (i = 0; i < ctx->cache->keys.nelts; i++) {
        if (keys[i].len) p = ngx_cpymem(p, keys[i].data, keys[i].len);
    }
    return NGX_OK;
}

static ngx_int_t
capture(ngx_http_request_t *r)
{
    capture_conf_t *conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_index_module);
    capture_main_t *main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_index_module);
    capture_t *ctx;
    ngx_pool_cleanup_t *cln;
    ngx_http_variable_value_t *host;
    ngx_str_t value;
    ngx_uint_t i;
#if (NGX_HTTP_CACHE_KEY)
    ngx_http_cache_key_snapshot_t *snapshot = NULL;
    ngx_int_t snapshot_result = NGX_DECLINED;
#endif

    if (conf->field[FIELD_KEY] == NULL || r->cache == NULL) return NGX_OK;
    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ctx && ctx->cache == r->cache) return ctx->failed ? NGX_ERROR : NGX_OK;
    cln = ngx_pool_cleanup_add(r->pool, sizeof(*ctx));
    if (cln == NULL) return NGX_ERROR;
    ctx = cln->data; ngx_memzero(ctx, sizeof(*ctx));
    ctx->request = r; ctx->cache = r->cache; ctx->lease = -1; ctx->failed = 1;
    cln->handler = cleanup;
    ngx_http_set_ctx(r, ctx, ngx_http_cache_purge_index_module);

#if (NGX_HTTP_CACHE_KEY)
    if (main->snapshot) snapshot_result = main->snapshot(r, &snapshot);
    if (snapshot_result == NGX_ERROR) return NGX_ERROR;
    if (snapshot_result == NGX_OK) {
        for (i = 0; i < 6; i++) {
            if (main->value(snapshot, i, &value) != NGX_OK
                || borrow(ctx, &ctx->components[i], value) != NGX_OK) return NGX_ERROR;
        }
    }
#endif

    for (i = FIELD_KEY; i < FIELD_COUNT; i++) {
        ngx_str_t *out;
        if (i == FIELD_TAG) continue;
        out = i == FIELD_KEY ? &ctx->key : i == FIELD_URL ? &ctx->url : &ctx->namespace;
        if (conf->field[i] == NULL) {
            if (native_key(ctx, out) != NGX_OK) return NGX_ERROR;
            continue;
        }
#if (NGX_HTTP_CACHE_KEY)
        if ((i == FIELD_KEY && equal(conf->field[i]->value, "$cache_key_base"))
            || (i == FIELD_URL && equal(conf->field[i]->value, "$cache_key_url"))
            || (i == FIELD_NAMESPACE && equal(conf->field[i]->value, "$cache_key_namespace")))
        {
            if (snapshot_result != NGX_OK
                || main->value(snapshot, i == FIELD_KEY ? NGX_HTTP_CACHE_KEY_BASE
                                      : i == FIELD_URL ? NGX_HTTP_CACHE_KEY_URL
                                                       : NGX_HTTP_CACHE_KEY_NAMESPACE, &value) != NGX_OK)
                return NGX_ERROR;
            if (i == FIELD_KEY) ctx->structured_key = 1;
            if (i == FIELD_URL) ctx->structured_url = 1;
        } else
#endif
        if (ngx_http_complex_value(r, conf->field[i], &value) != NGX_OK) return NGX_ERROR;
        if (borrow(ctx, out, value) != NGX_OK) return NGX_ERROR;
    }
    host = ngx_http_get_flushed_variable(r, main->host_index);
    if (host == NULL || host->not_found) return NGX_ERROR;
    value.data = host->data; value.len = host->len;
    if (ctx->structured_key || ctx->structured_url) value = ctx->components[2];
    if (borrow(ctx, &ctx->host, value) != NGX_OK || ctx->host.len == 0
        || ctx->key.len == 0 || ctx->url.len == 0
        || ctx->namespace.len == 0 || ctx->namespace.len > 255) return NGX_ERROR;
    for (i = 0; i < ctx->namespace.len; i++) {
        if (ctx->namespace.data[i] <= 0x20 || ctx->namespace.data[i] >= 0x7f
            || ctx->namespace.data[i] == ',') return NGX_ERROR;
    }
    ctx->failed = 0;
    return NGX_OK;
}

static size_t
guard_length(ngx_http_script_engine_t *engine)
{
    engine->ip += sizeof(ngx_http_script_len_code_pt);
    return 1;
}

static void
guard_value(ngx_http_script_engine_t *engine)
{
    capture_code_t *code = (capture_code_t *) engine->ip;
    ngx_int_t rc = ngx_http_test_predicates(engine->request, code->predicates);
    engine->ip += sizeof(*code) - sizeof(uintptr_t);
    if (rc == NGX_ERROR) { engine->status = NGX_HTTP_INTERNAL_SERVER_ERROR; return; }
    if (capture(engine->request) != NGX_OK) {
        engine->request->upstream->cacheable = 0;
        rc = NGX_DECLINED;
        ngx_log_error(NGX_LOG_WARN, engine->request->connection->log, 0,
                      "cache index snapshot unavailable or over budget; skipping cache");
    }
    *engine->pos++ = rc == NGX_DECLINED ? '1' : '0';
}

static ngx_int_t
tags(capture_t *ctx, ngx_str_t value)
{
    size_t at = 0, start, end, i;
    ngx_uint_t n;
    ngx_str_t tag;
    while (at < value.len) {
        start = at;
        while (at < value.len && value.data[at] != ',') at++;
        end = at;
        if (at < value.len) at++;
        while (start < end && (value.data[start] == ' ' || value.data[start] == '\t')) start++;
        while (end > start && (value.data[end - 1] == ' ' || value.data[end - 1] == '\t')) end--;
        if (start == end) continue;
        if (end - start > 1024) return NGX_ERROR;
        for (i = start; i < end; i++) if (value.data[i] < 0x20 || value.data[i] == 0x7f) return NGX_ERROR;
        tag.data = value.data + start; tag.len = end - start;
        for (n = 0; n < ctx->tag_count; n++) {
            if (ctx->tags[n].len == tag.len && ngx_memcmp(ctx->tags[n].data, tag.data, tag.len) == 0) break;
        }
        if (n < ctx->tag_count) continue;
        if (ctx->tag_count == 64 || borrow(ctx, &ctx->tags[ctx->tag_count], tag) != NGX_OK) return NGX_ERROR;
        ctx->tag_count++;
    }
    return NGX_OK;
}

static ngx_int_t
header(ngx_http_request_t *r)
{
    capture_t *ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    capture_conf_t *conf;
    ngx_str_t value;
    if (ctx && !ctx->failed && ctx->cache == r->cache && !r->cached) {
        conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_index_module);
        if ((conf->field[FIELD_TAG]
             && (ngx_http_complex_value(r, conf->field[FIELD_TAG], &value) != NGX_OK
                 || tags(ctx, value) != NGX_OK))
            || ngx_http_cache_purge_fill_arm(r, &ctx->fill, capture_step, ctx) == NGX_ERROR)
        {
            ctx->failed = 1;
            r->upstream->cacheable = 0;
            ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                          "cache index response metadata unavailable; skipping cache fill");
        }
    }
    return next_header(r);
}

static u_char *
number(u_char *p, uint64_t value, unsigned width)
{
    while (width) *p++ = (u_char) (value >> (--width * 8));
    return p;
}
static uint64_t
read_number(u_char *p, unsigned width)
{
    uint64_t value = 0;
    while (width--) value = (value << 8) | *p++;
    return value;
}
static u_char *
string(u_char *p, ngx_str_t value)
{
    p = number(p, value.len, 4);
    return value.len ? ngx_cpymem(p, value.data, value.len) : p;
}
static ngx_int_t
allocate(capture_t *ctx, ngx_str_t *payload, size_t bytes)
{
    if (bytes > CAPTURE_RECORD || reserve(ctx, bytes) != NGX_OK) return NGX_ERROR;
    payload->len = bytes; payload->data = ngx_pnalloc(ctx->publication_pool, bytes);
    return payload->data ? NGX_OK : NGX_ERROR;
}
static ngx_msec_t
remaining(capture_t *ctx)
{
    ngx_msec_int_t left = (ngx_msec_int_t) (ctx->deadline - ngx_current_msec);
    return left > 0 ? (ngx_msec_t) left : 0;
}

static ngx_int_t
submit_prepare(capture_t *ctx)
{
    ngx_str_t payload, empty = ngx_null_string;
    ngx_str_t fields[] = {ctx->key, ctx->url, ctx->host, ctx->namespace, ctx->root,
                         ctx->zone, ctx->path, ctx->components[5], ctx->cache->vary};
    size_t size = 16 + 9 * 4 + 4 + 1 + 2 + 6 * 4 + 16 + 4;
    ngx_uint_t i;
    ngx_msec_t timeout = remaining(ctx);
    ngx_int_t rc;
    u_char *p;
    int fds[2] = {ctx->lease, ctx->temporary->file.fd};
    if (!timeout) return NGX_ERROR;
    for (i = 0; i < 9; i++) { if (fields[i].len > 32768) return NGX_ERROR; size += fields[i].len; }
    for (i = 0; i < 6; i++) size += ctx->components[i].len;
    for (i = 0; i < ctx->tag_count; i++) size += 4 + ctx->tags[i].len;
    size += ctx->temporary->file.name.len;
    if (allocate(ctx, &payload, size) != NGX_OK) return NGX_ERROR;
    p = ngx_cpymem(payload.data, ctx->id, 16);
    for (i = 0; i < 9; i++) p = string(p, fields[i]);
    p = string(p, empty); *p++ = (u_char) ctx->tag_count;
    for (i = 0; i < ctx->tag_count; i++) p = string(p, ctx->tags[i]);
    *p++ = ctx->structured_key; *p++ = ctx->structured_url;
    for (i = 0; i < 6; i++) p = string(p, ctx->components[i]);
    p = ngx_cpymem(p, ctx->previous, 16);
    p = string(p, ctx->temporary->file.name);
    if ((size_t) (p - payload.data) != size) return NGX_ERROR;
    ctx->rpc = RPC_PREPARE;
    rc = ngx_cache_purge_client_submit_files(ctx->client, &payload, fds, timeout,
                                            capture_reply, ctx, &ctx->call);
    if (rc == NGX_OK) ctx->need_finish = 1;
    return rc;
}

static ngx_int_t
capture_step(ngx_http_request_t *r, ngx_http_cache_t *cache, ngx_temp_file_t *tf,
    ngx_uint_t phase, ngx_int_t result, void *data)
{
    capture_t *ctx = data;
    ngx_str_t payload, root, name;
    u_char *p;
    ngx_int_t rc;
    ngx_msec_t timeout;
    if (phase == NGX_CACHE_PURGE_FILL_FINISH) {
        if (!ctx->need_finish || ctx->lease < 0) { release_publication(ctx); return NGX_OK; }
        timeout = remaining(ctx);
        if (!timeout || allocate(ctx, &payload, 24) != NGX_OK) { release_publication(ctx); return NGX_ERROR; }
        p = ngx_cpymem(payload.data, ctx->id, 16);
        number(p, ctx->revision, 8);
        ctx->rpc = RPC_FINISH;
        rc = ngx_cache_purge_client_finish_file(ctx->client, &payload, ctx->lease,
                                               timeout, capture_reply, ctx, &ctx->call);
        if (rc == NGX_OK) return NGX_AGAIN;
        release_publication(ctx); return NGX_ERROR;
    }
    /* Publication limits apply after a complete body exists, never to HITs
     * or requests still receiving an origin response. Input values belong
     * to immutable key snapshots or complex-value request-pool buffers. */
    if (retained_count >= CAPTURE_COUNT
        || reserve(ctx, sizeof(*ctx) + ctx->input_bytes) != NGX_OK) return NGX_DECLINED;
    retained_count++; ctx->admitted = 1;
    ctx->publication_pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, r->connection->log);
    if (ctx->publication_pool == NULL) return NGX_DECLINED;
    ctx->temporary = tf;
    ctx->deadline = ngx_current_msec + CAPTURE_TIMEOUT;
    /* New FileIds start at revision 1. It also identifies a PREPARE whose
     * reply was lost; FINISH then observes NOT_FOUND or retires its intent. */
    ctx->revision = 1;
    if (ngx_http_cache_purge_runtime_client(r, &ctx->client) != 0
        || getrandom(ctx->id, sizeof(ctx->id), GRND_NONBLOCK) != (ssize_t) sizeof(ctx->id)) return NGX_DECLINED;
    root = cache->file_cache->path->name;
    name = cache->file.name;
    if (name.len <= root.len + 1 || ngx_memcmp(name.data, root.data, root.len)
        || name.data[root.len] != '/') return NGX_DECLINED;
    name.data += root.len + 1; name.len -= root.len + 1;
    ctx->path = name; ctx->zone = cache->file_cache->shm_zone->shm.name;
    if (ctx->zone.len > 32768 || root.len > 32768 || name.len > 32768
        || allocate(ctx, &payload, 12 + root.len + ctx->zone.len + name.len) != NGX_OK) return NGX_DECLINED;
    p = string(payload.data, root); p = string(p, ctx->zone); string(p, name);
    ctx->rpc = RPC_ACQUIRE;
    rc = ngx_cache_purge_client_submit(ctx->client, NGX_PURGE_OP_ACQUIRE_PUBLICATION,
                                     &payload, remaining(ctx), capture_reply, ctx, &ctx->call);
    return rc == NGX_OK ? NGX_AGAIN : NGX_DECLINED;
}

static void
capture_reply(ngx_cache_purge_call_t *call, void *data, const ngx_cache_purge_reply_t *reply)
{
    capture_t *ctx = data;
    ngx_uint_t stage = ctx->rpc;
    ngx_int_t rc = NGX_DECLINED;
    ngx_str_t root;
    size_t length;
    ctx->call = NULL;
    if (stage == RPC_PREPARE && !reply->may_have_executed) ctx->need_finish = 0;
    if (reply->transport == NGX_PURGE_CLIENT_OK && reply->result == 0) {
        if (stage == RPC_ACQUIRE && reply->lease_fd >= 0 && reply->payload.len >= 44) {
            length = read_number(reply->payload.data, 4);
            if (length && length <= 32768 && reply->payload.len == 44 + length) {
                root.data = reply->payload.data + 4; root.len = length;
                ctx->lease = fcntl(reply->lease_fd, F_DUPFD_CLOEXEC, 0);
                if (ctx->lease >= 0 && copy(ctx, &ctx->root, root) == NGX_OK) {
                    ngx_memcpy(ctx->previous, root.data + root.len, 16);
                    if (submit_prepare(ctx) == NGX_OK) return;
                }
            }
        } else if (stage != RPC_ACQUIRE && reply->payload.len == 25
                   && ngx_memcmp(reply->payload.data, ctx->id, 16) == 0)
        {
            if (stage == RPC_PREPARE && read_number(reply->payload.data + 16, 8) == 1
                && reply->payload.data[24] == 1) rc = NGX_OK;
            if (stage == RPC_FINISH && (reply->payload.data[24] == 2 || reply->payload.data[24] == 3)) rc = NGX_OK;
        }
    } else if (stage == RPC_FINISH && reply->transport == NGX_PURGE_CLIENT_OK
               && reply->result == NGX_PURGE_STORE_NOT_FOUND) rc = NGX_OK;
    if (rc != NGX_OK) {
        ngx_log_error(NGX_LOG_WARN, ctx->request->connection->log, 0,
                      "cache index publication stage %ui failed: transport=%ui result=%uD",
                      stage, (ngx_uint_t) reply->transport, reply->result);
    }
    if (stage == RPC_FINISH) release_publication(ctx);
    ngx_http_cache_purge_fill_complete(ctx->fill, rc);
}

static void *
create_main(ngx_conf_t *cf)
{
    capture_main_t *main = ngx_pcalloc(cf->pool, sizeof(*main));
    if (main == NULL || ngx_array_init(&main->locations, cf->pool, 4, sizeof(void **)) != NGX_OK) return NULL;
    return main;
}
static void *
create_conf(ngx_conf_t *cf)
{
    return ngx_pcalloc(cf->pool, sizeof(capture_conf_t));
}
static char *
merge(ngx_conf_t *cf, void *parent, void *child)
{
    capture_conf_t *p = parent, *c = child;
    capture_main_t *main;
    ngx_http_compile_complex_value_t compile;
    ngx_str_t host = ngx_string("$host");
    void ***location;
    ngx_uint_t i;
    for (i = 0; i < FIELD_COUNT; i++) if (c->field[i] == NULL) c->field[i] = p->field[i];
    if (c->field[FIELD_NAMESPACE] == NULL) {
        c->field[FIELD_NAMESPACE] = ngx_pcalloc(cf->pool, sizeof(ngx_http_complex_value_t));
        if (c->field[FIELD_NAMESPACE] == NULL) return NGX_CONF_ERROR;
        ngx_memzero(&compile, sizeof(compile)); compile.cf = cf;
        compile.value = &host; compile.complex_value = c->field[FIELD_NAMESPACE];
        if (ngx_http_compile_complex_value(&compile) != NGX_OK) return NGX_CONF_ERROR;
    }
    if (c->field[FIELD_KEY] && equal(c->field[FIELD_KEY]->value, "$cache_key_full")) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "cache_purge_key must exclude range; use $cache_key_base");
        return NGX_CONF_ERROR;
    }
    main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_index_module);
    location = ngx_array_push(&main->locations);
    if (location == NULL) return NGX_CONF_ERROR;
    *location = ((ngx_http_conf_ctx_t *) cf->ctx)->loc_conf;
    return NGX_CONF_OK;
}

static ngx_int_t
post(ngx_conf_t *cf)
{
    static struct { ngx_http_script_len_code_pt code; uintptr_t end; } lengths = {guard_length, 0};
    static const char *names[] = {"proxy_cache_bypass", "fastcgi_cache_bypass", "scgi_cache_bypass", "uwsgi_cache_bypass"};
    capture_main_t *main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_index_module);
    capture_conf_t *conf;
    ngx_module_t *module;
    ngx_command_t *cmd;
    ngx_array_t **slot, *array;
    ngx_http_complex_value_t *value;
    ngx_http_upstream_conf_t *upstream;
    capture_code_t *code;
    void ***locations = main->locations.elts;
    ngx_uint_t i, j, k, n;
    ngx_str_t host = ngx_string("host");
    main->host_index = ngx_http_get_variable_index(cf, &host);
    if (main->host_index == NGX_ERROR) return NGX_ERROR;
#if (NGX_HTTP_CACHE_KEY && NGX_HAVE_DLOPEN)
    {
        void *handle = ngx_dlopen(NULL);
        if (handle) {
            main->snapshot = (void *) ngx_dlsym(handle, "ngx_http_cache_key_cache_snapshot");
            main->value = (void *) ngx_dlsym(handle, "ngx_http_cache_key_snapshot_value");
            ngx_dlclose(handle);
        }
        if (main->value == NULL) main->snapshot = NULL;
    }
#endif
    for (k = 0; k < main->locations.nelts; k++) {
        conf = locations[k][ngx_http_cache_purge_index_module.ctx_index];
        if (conf->field[FIELD_KEY] == NULL) continue;
        if (!ngx_http_cache_purge_runtime_configured(cf)) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "cache_purge_key requires cache_purge_store and cache_purge_state_zone");
            return NGX_ERROR;
        }
        for (n = 0; n < FIELD_COUNT; n++) {
            if (n == FIELD_TAG || conf->field[n] == NULL) continue;
            if (equal(conf->field[n]->value, "$cache_key_base")
                || equal(conf->field[n]->value, "$cache_key_url")
                || equal(conf->field[n]->value, "$cache_key_namespace"))
            {
#if (NGX_HTTP_CACHE_KEY)
                if (main->snapshot && main->value) continue;
#endif
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "cache index requires the native cache key snapshot API");
                return NGX_ERROR;
            }
        }
    }
    for (i = 0; cf->cycle->modules[i]; i++) {
        module = cf->cycle->modules[i];
        if (module->type != NGX_HTTP_MODULE || module->commands == NULL) continue;
        for (cmd = module->commands; cmd->name.len; cmd++) {
            for (j = 0; j < 4 && !equal(cmd->name, names[j]); j++) { }
            if (j == 4) continue;
            if (cmd->set != ngx_http_set_predicate_slot || cmd->conf != NGX_HTTP_LOC_CONF_OFFSET
                || cmd->offset < offsetof(ngx_http_upstream_conf_t, cache_bypass)) return NGX_ERROR;
            for (k = 0; k < main->locations.nelts; k++) {
                conf = locations[k][ngx_http_cache_purge_index_module.ctx_index];
                if (conf->field[FIELD_KEY] == NULL || locations[k][module->ctx_index] == NULL) continue;
                slot = (ngx_array_t **) ((u_char *) locations[k][module->ctx_index] + cmd->offset);
                upstream = (ngx_http_upstream_conf_t *) ((u_char *) slot - offsetof(ngx_http_upstream_conf_t, cache_bypass));
                if (upstream->cache <= 0) continue;
                if (*slot && *slot != NGX_CONF_UNSET_PTR && (*slot)->nelts) {
                    value = (*slot)->elts;
                    if (value[0].lengths == (void *) &lengths) continue;
                }
                array = ngx_array_create(cf->pool, 1, sizeof(*value));
                code = ngx_pcalloc(cf->pool, sizeof(*code));
                if (array == NULL || code == NULL) return NGX_ERROR;
                value = ngx_array_push(array);
                if (value == NULL) return NGX_ERROR;
                ngx_memzero(value, sizeof(*value));
                code->code = guard_value;
                code->predicates = *slot == NGX_CONF_UNSET_PTR ? NULL : *slot;
                value->lengths = (void *) &lengths; value->values = (void *) code;
                *slot = array;
            }
        }
    }
    next_header = ngx_http_top_header_filter;
    ngx_http_top_header_filter = header;
    return NGX_OK;
}

#define CAPTURE_DIRECTIVE(name, field) \
    {ngx_string(name), NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1, \
     ngx_http_set_complex_value_slot, NGX_HTTP_LOC_CONF_OFFSET, \
     offsetof(capture_conf_t, field) , NULL}
/* Explicit offsets keep these four independent, inheritable inputs. */
static ngx_command_t commands[] = {
    CAPTURE_DIRECTIVE("cache_purge_key", field[FIELD_KEY]),
    CAPTURE_DIRECTIVE("cache_purge_url", field[FIELD_URL]),
    CAPTURE_DIRECTIVE("cache_purge_tag", field[FIELD_TAG]),
    CAPTURE_DIRECTIVE("cache_purge_tag_namespace", field[FIELD_NAMESPACE]),
    ngx_null_command
};
static ngx_http_module_t context = {NULL, post, create_main, NULL, NULL, NULL, create_conf, merge};
ngx_module_t ngx_http_cache_purge_index_module = {
    NGX_MODULE_V1, &context, commands, NGX_HTTP_MODULE,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NGX_MODULE_V1_PADDING
};
