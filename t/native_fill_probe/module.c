/* Test consumer for the module-owned native fill lifecycle. */
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_cache_purge_fill.h"

typedef struct {
    ngx_http_request_t *r;
    ngx_http_cache_purge_fill_t *fill;
    ngx_event_t event;
    ngx_uint_t phase;
    ngx_int_t result;
} context_t;
static ngx_http_output_header_filter_pt next_header;

static void
complete(ngx_event_t *event)
{
    context_t *ctx = event->data;
    if (ctx->phase == NGX_CACHE_PURGE_FILL_FINISH) {
        ngx_log_error(NGX_LOG_NOTICE, ctx->r->connection->log, 0,
                      "native finalize FINISH %V", &ctx->r->uri);
    }
    ngx_http_cache_purge_fill_complete(ctx->fill, ctx->result);
}

static ngx_int_t
step(ngx_http_request_t *r, ngx_http_cache_t *c, ngx_temp_file_t *tf,
    ngx_uint_t phase, ngx_int_t result, void *data)
{
    context_t *ctx = data;
    ngx_str_t value;
    if (phase == NGX_CACHE_PURGE_FILL_PREPARE) {
        ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0,
                      "native finalize PREPARE %V %V", &c->file.name, &tf->file.name);
    }
    ctx->phase = phase;
    ctx->result = phase == NGX_CACHE_PURGE_FILL_PREPARE
                 && ngx_http_arg(r, (u_char *) "reject", 6, &value) == NGX_OK
                 ? NGX_DECLINED : NGX_OK;
    ctx->event.data = ctx;
    ctx->event.handler = complete;
    ctx->event.log = r->connection->log;
    ngx_add_timer(&ctx->event, phase == NGX_CACHE_PURGE_FILL_PREPARE ? 75 : 25);
    return NGX_AGAIN;
}

static ngx_int_t
header(ngx_http_request_t *r)
{
    context_t *ctx;
    if (r->cache && r->upstream && !r->cached) {
        ctx = ngx_pcalloc(r->pool, sizeof(*ctx));
        if (ctx == NULL) return NGX_ERROR;
        ctx->r = r;
        if (ngx_http_cache_purge_fill_arm(r, &ctx->fill, step, ctx) == NGX_ERROR) {
            return NGX_ERROR;
        }
    }
    return next_header(r);
}

static ngx_int_t post(ngx_conf_t *cf)
{
    next_header = ngx_http_top_header_filter;
    ngx_http_top_header_filter = header;
    return NGX_OK;
}
static ngx_http_module_t context = {NULL, post, NULL, NULL, NULL, NULL, NULL, NULL};
ngx_module_t ngx_http_native_fill_probe_module = {
    NGX_MODULE_V1, &context, NULL, NGX_HTTP_MODULE,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NGX_MODULE_V1_PADDING
};
