/* Test-only native cache adapter. No persistent purge behavior is implied. */
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_cache_purge_fill.h"

#if !(NGX_RESTY_EXT && NGX_HTTP_CACHE)
#error "native cache adapter probe requires extended HTTP cache"
#endif

typedef struct {
    ngx_flag_t  enabled;
} probe_conf_t;

typedef struct {
    ngx_http_request_t  *request;
    ngx_http_cache_t    *cache;
    ngx_event_t          event;
    ngx_event_t          store_event;
    ngx_str_t            mode;
    ngx_uint_t           lookups;
    u_char              *last;
    u_char               digest[NGX_HTTP_CACHE_KEY_LEN];
    ngx_uint_t           store_phase;
    ngx_uint_t           store_calls;
    ngx_int_t            store_result;
    ngx_http_cache_purge_fill_t *fill;
} probe_ctx_t;

extern ngx_module_t ngx_http_cache_adapter_probe_module;

static ngx_int_t
mode(probe_ctx_t *ctx, const char *value)
{
    size_t n = ngx_strlen(value);
    return ctx->mode.len == n && ngx_strncmp(ctx->mode.data, value, n) == 0;
}

static ngx_int_t
decision(probe_ctx_t *ctx)
{
    if (mode(ctx, "expire") || mode(ctx, "deny304")) {
        ctx->cache->invalidated = 1;
    }
    if (mode(ctx, "miss")) {
        return NGX_DECLINED;
    }
    if (mode(ctx, "lookup_error")) {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    if (mode(ctx, "bad_return")) {
        return NGX_DONE;
    }
    return NGX_OK;
}

static void
complete(ngx_event_t *ev)
{
    probe_ctx_t *ctx = ev->data;
    ngx_int_t rc;

    /* A native AIO or adapter continuation must not parse the buffer twice. */
    rc = decision(ctx);
    if ((ctx->cache->buf && ctx->cache->buf->last != ctx->last)
        || ngx_memcmp(ctx->cache->key, ctx->digest, sizeof(ctx->digest)))
    {
        rc = NGX_ERROR;
    }
    ngx_http_file_cache_read_complete(ctx->request, ctx->cache, rc);
    /* Completion may destroy both request and ctx. */
}

static ngx_int_t
identity(ngx_http_request_t *r)
{
    probe_conf_t *conf;
    probe_ctx_t *ctx;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_adapter_probe_module);
    if (!conf->enabled) {
        r->cache->adapter = NULL;
        return NGX_OK;
    }
    if (r->cache->adapter_ctx || !r->cache->file_cache
        || !r->cache->keys.nelts)
    {
        return NGX_ERROR;
    }
    ctx = ngx_pcalloc(r->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }
    ctx->request = r;
    ctx->cache = r->cache;
    (void) ngx_http_arg(r, (u_char *) "mode", 4, &ctx->mode);
    r->cache->adapter_ctx = ctx;
    return NGX_OK;
}

static ngx_int_t
lookup(ngx_http_request_t *r)
{
    probe_ctx_t *ctx = r->cache->adapter_ctx;
    ngx_str_t value;
    ngx_int_t delay;

    if (ctx == NULL || ++ctx->lookups != 1) {
        return NGX_ERROR;
    }
    if (ngx_http_arg(r, (u_char *) "delay", 5, &value) != NGX_OK) {
        return decision(ctx);
    }
    delay = ngx_atoi(value.data, value.len);
    if (delay < 0 || delay > 1000) {
        return NGX_ERROR;
    }
    ctx->last = r->cache->buf ? r->cache->buf->last : NULL;
    ngx_memcpy(ctx->digest, r->cache->key, sizeof(ctx->digest));
    ctx->event.data = ctx;
    ctx->event.handler = complete;
    ctx->event.log = r->connection->log;
    ngx_add_timer(&ctx->event, delay);
    return NGX_AGAIN;
}

static ngx_int_t
send_response(ngx_http_request_t *r, ngx_http_handler_pt next)
{
    probe_ctx_t *ctx = r->cache->adapter_ctx;

    if (ctx == NULL) {
        return NGX_ERROR;
    }
    if (mode(ctx, "send_error")
        || (mode(ctx, "deny304")
            && r->upstream->cache_status == NGX_HTTP_CACHE_REVALIDATED))
    {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    return next(r);
}

static void
store_complete(ngx_event_t *ev)
{
    probe_ctx_t *ctx = ev->data;

    if ((mode(ctx, "store_cancel_prepare")
         && ctx->store_phase == NGX_CACHE_PURGE_FILL_PREPARE)
        || (mode(ctx, "store_cancel_finish")
            && ctx->store_phase == NGX_CACHE_PURGE_FILL_FINISH))
    {
        ngx_http_finalize_request(ctx->request, NGX_HTTP_CLIENT_CLOSED_REQUEST);
    }

    ngx_log_error(NGX_LOG_NOTICE, ctx->request->connection->log, 0,
                  "probe store complete %ui %i %V", ctx->store_phase,
                  ctx->store_result, &ctx->request->uri);
    ngx_http_cache_purge_fill_complete(ctx->fill, ctx->store_result);
}

static ngx_int_t
store_step(ngx_http_request_t *r, ngx_http_cache_t *c, ngx_temp_file_t *tf,
    ngx_uint_t phase, ngx_int_t result, void *data)
{
    probe_ctx_t *ctx = data;
    ngx_str_t value;
    ngx_int_t delay, answer;
    ngx_uint_t held;

    if (ctx == NULL || ++ctx->store_calls != phase || tf == NULL) {
        return NGX_ERROR;
    }
    ngx_shmtx_lock(&c->file_cache->shpool->mutex);
    held = c->node && c->node->count;
    ngx_shmtx_unlock(&c->file_cache->shpool->mutex);
    if (!held) {
        ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0,
                      "probe store lost cache node");
        return NGX_ERROR;
    }

    ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0,
                  "probe store phase %ui %i %V", phase, result, &r->uri);
    answer = NGX_OK;
    if (phase == NGX_CACHE_PURGE_FILL_PREPARE && mode(ctx, "store_reject")) {
        answer = NGX_DECLINED;
    }
    if (phase == NGX_CACHE_PURGE_FILL_FINISH && mode(ctx, "store_finish_error")) {
        answer = NGX_ERROR;
    }
    if (ngx_http_arg(r, (u_char *) (phase == NGX_CACHE_PURGE_FILL_PREPARE
                                  ? "prepare" : "finish"),
                     phase == NGX_CACHE_PURGE_FILL_PREPARE ? 7 : 6,
                     &value) != NGX_OK)
    {
        return answer;
    }
    delay = ngx_atoi(value.data, value.len);
    if (delay < 0 || delay > 1000) {
        return NGX_ERROR;
    }
    ctx->store_phase = phase;
    ctx->store_result = answer;
    ctx->store_event.data = ctx;
    ctx->store_event.handler = store_complete;
    ctx->store_event.log = r->connection->log;
    ngx_add_timer(&ctx->store_event, delay);
    return NGX_AGAIN;
}

static ngx_int_t
store(ngx_http_request_t *r, ngx_temp_file_t *tf)
{
    probe_ctx_t *ctx = r->cache->adapter_ctx;
    return ngx_http_cache_purge_fill_start(r, tf, &ctx->fill, store_step, ctx);
}

static ngx_int_t
state(ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data)
{
    probe_ctx_t *ctx;
    u_char *p;

    ctx = r->cache ? r->cache->adapter_ctx : NULL;
    if (ctx == NULL) {
        v->not_found = 1;
        return NGX_OK;
    }
    p = ngx_pnalloc(r->pool, 128);
    if (p == NULL) {
        return NGX_ERROR;
    }
    v->data = p;
    p = ngx_snprintf(p, 128, "%V:%ui:%d:%d:%d",
                     &r->cache->file_cache->shm_zone->shm.name, ctx->lookups,
                     r->cache->secondary, r->cache->invalidated,
                     r->cache->must_revalidate);
    v->len = p - v->data;
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}

static ngx_int_t
pre(ngx_conf_t *cf)
{
    ngx_http_variable_t *v;
    ngx_str_t name = ngx_string("cache_adapter_probe_state");
    v = ngx_http_add_variable(cf, &name, NGX_HTTP_VAR_NOCACHEABLE);
    if (v == NULL) {
        return NGX_ERROR;
    }
    v->get_handler = state;
    return NGX_OK;
}

static ngx_int_t
post(ngx_conf_t *cf)
{
    static ngx_http_cache_adapter_t adapter = {identity, lookup, send_response, store};
    ngx_http_core_main_conf_t *core;
    core = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    if (core->cache_adapter) {
        return NGX_ERROR;
    }
    core->cache_adapter = &adapter;
    return NGX_OK;
}

static void *
create(ngx_conf_t *cf)
{
    probe_conf_t *conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf) { conf->enabled = NGX_CONF_UNSET; }
    return conf;
}

static char *
merge(ngx_conf_t *cf, void *parent, void *child)
{
    probe_conf_t *p = parent, *c = child;
    ngx_conf_merge_value(c->enabled, p->enabled, 0);
    return NGX_CONF_OK;
}

static ngx_command_t commands[] = {
    { ngx_string("cache_adapter_probe"), NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|
      NGX_HTTP_LOC_CONF|NGX_CONF_FLAG, ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET, offsetof(probe_conf_t, enabled), NULL },
    ngx_null_command
};
static ngx_http_module_t context = {pre, post, NULL, NULL, NULL, NULL, create, merge};
ngx_module_t ngx_http_cache_adapter_probe_module = {
    NGX_MODULE_V1, &context, commands, NGX_HTTP_MODULE,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NGX_MODULE_V1_PADDING
};
