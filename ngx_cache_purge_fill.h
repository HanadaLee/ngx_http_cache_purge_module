/* Copyright (C) Hanada */
#ifndef _NGX_CACHE_PURGE_FILL_H_INCLUDED_
#define _NGX_CACHE_PURGE_FILL_H_INCLUDED_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#if (NGX_HTTP_CACHE && NGX_RESTY_EXT && NGX_HTTP_CACHE_ADAPTER_VERSION >= 3)

#define NGX_CACHE_PURGE_FILL_PREPARE  1
#define NGX_CACHE_PURGE_FILL_FINISH   2

typedef struct ngx_http_cache_purge_fill_s ngx_http_cache_purge_fill_t;
typedef ngx_int_t (*ngx_http_cache_purge_fill_step_pt)(ngx_http_request_t *r,
    ngx_http_cache_t *c, ngx_temp_file_t *tf, ngx_uint_t phase,
    ngx_int_t result, void *data);

/* Worker event loop only. Called from the complete-body store delegate.
 * The caller retains slot for this cache operation. A step returning AGAIN
 * must complete exactly once, asynchronously, including failure/timeout.
 * FINISH reconciles every outcome, including a cancelled or failed PREPARE.
 * Callbacks must not replace r->cache, finalize the request, or update/free c.
 * No database or filesystem transaction is implied by this lifecycle alone. */
ngx_int_t ngx_http_cache_purge_fill_start(ngx_http_request_t *r,
    ngx_temp_file_t *tf, ngx_http_cache_purge_fill_t **slot,
    ngx_http_cache_purge_fill_step_pt step, void *data);
void ngx_http_cache_purge_fill_complete(ngx_http_cache_purge_fill_t *fill,
    ngx_int_t result);

#endif
#endif
