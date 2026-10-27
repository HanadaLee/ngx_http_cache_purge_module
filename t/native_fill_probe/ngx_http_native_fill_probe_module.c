/* Test consumer for the module-owned native fill lifecycle. */
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_http_cache_purge.h"

typedef struct {
    ngx_http_request_t          *r;
    ngx_http_cache_purge_fill_t *fill;
    ngx_event_t                  event;
    ngx_uint_t                   phase;
    ngx_int_t                    result;
} ngx_http_cache_purge_fill_probe_ctx_t;
static void ngx_http_cache_purge_fill_probe_complete(ngx_event_t *event);
static ngx_int_t
ngx_http_cache_purge_fill_probe_step(ngx_http_request_t *r, ngx_http_cache_t *c,
                                     ngx_temp_file_t *tf, ngx_uint_t phase,
                                     ngx_int_t result, void *data);
static ngx_int_t ngx_http_cache_purge_fill_probe_header(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_fill_probe_post(ngx_conf_t *cf);


static ngx_http_module_t  ngx_http_cache_purge_fill_probe_module_ctx = {
    NULL,                                         /* preconfiguration */
    ngx_http_cache_purge_fill_probe_post,         /* postconfiguration */
    NULL,                                         /* create main conf */
    NULL,                                         /* init main conf */
    NULL,                                         /* create server conf */
    NULL,                                         /* merge server conf */
    NULL,                                         /* create location conf */
    NULL                                          /* merge location conf */
};

ngx_module_t  ngx_http_native_fill_probe_module = {
    NGX_MODULE_V1,
    &ngx_http_cache_purge_fill_probe_module_ctx,
    NULL,                                         /* module directives */
    NGX_HTTP_MODULE,
    NULL,                                         /* init master */
    NULL,                                         /* init module */
    NULL,                                         /* init process */
    NULL,                                         /* init thread */
    NULL,                                         /* exit thread */
    NULL,                                         /* exit process */
    NULL,                                         /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_http_output_header_filter_pt
    ngx_http_cache_purge_fill_probe_next_header;

static void
ngx_http_cache_purge_fill_probe_complete(ngx_event_t *event)
{
    ngx_http_cache_purge_fill_probe_ctx_t *ctx = event->data;
    if (ctx->phase == NGX_HTTP_CACHE_PURGE_FILL_FINISH) {
        ngx_log_error(NGX_LOG_NOTICE, ctx->r->connection->log, 0,
                      "native finalize FINISH %V", &ctx->r->uri);
    }
    ngx_http_cache_purge_fill_complete(ctx->fill, ctx->result);
}

static ngx_int_t
ngx_http_cache_purge_fill_probe_step(ngx_http_request_t *r, ngx_http_cache_t *c,
                                     ngx_temp_file_t *tf, ngx_uint_t phase,
                                     ngx_int_t result, void *data)
{
    ngx_http_cache_purge_fill_probe_ctx_t *ctx = data;
    ngx_str_t                                     value;
    if (phase == NGX_HTTP_CACHE_PURGE_FILL_PREPARE) {
        ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0,
                      "native finalize PREPARE %V %V", &c->file.name,
                      &tf->file.name);
    }
    ctx->phase = phase;
    ctx->result =
        phase == NGX_HTTP_CACHE_PURGE_FILL_PREPARE
                && ngx_http_arg(r, (u_char *) "reject", 6, &value) == NGX_OK ?
            NGX_DECLINED :
            NGX_OK;
    ctx->event.data = ctx;
    ctx->event.handler = ngx_http_cache_purge_fill_probe_complete;
    ctx->event.log = r->connection->log;
    ngx_add_timer(&ctx->event,
                  phase == NGX_HTTP_CACHE_PURGE_FILL_PREPARE ? 75 : 25);
    return NGX_AGAIN;
}

static ngx_int_t
ngx_http_cache_purge_fill_probe_header(ngx_http_request_t *r)
{
    ngx_http_cache_purge_fill_probe_ctx_t *ctx;
    if (r->cache && r->upstream && !r->cached) {
        ctx = ngx_pcalloc(r->pool, sizeof(*ctx));
        if (ctx == NULL) {
            return NGX_ERROR;
        }
        ctx->r = r;
        if (ngx_http_cache_purge_fill_arm(
                r, &ctx->fill, ngx_http_cache_purge_fill_probe_step, ctx)
            == NGX_ERROR)
        {
            return NGX_ERROR;
        }
    }
    return ngx_http_cache_purge_fill_probe_next_header(r);
}

static ngx_int_t
ngx_http_cache_purge_fill_probe_post(ngx_conf_t *cf)
{
    ngx_http_cache_purge_fill_probe_next_header = ngx_http_top_header_filter;
    ngx_http_top_header_filter = ngx_http_cache_purge_fill_probe_header;
    return NGX_OK;
}
