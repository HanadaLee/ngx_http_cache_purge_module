/* Copyright (C) Hanada */
#ifndef _NGX_HTTP_CACHE_PURGE_FILL_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_FILL_H_INCLUDED_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#if (NGX_HTTP_CACHE)

#define NGX_HTTP_CACHE_PURGE_FILL_PREPARE  1
#define NGX_HTTP_CACHE_PURGE_FILL_FINISH   2

typedef struct ngx_http_cache_purge_fill_s  ngx_http_cache_purge_fill_t;
typedef ngx_int_t (*ngx_http_cache_purge_fill_step_pt)(
    ngx_http_request_t *r, ngx_http_cache_t *c, ngx_temp_file_t *tf,
    ngx_uint_t phase, ngx_int_t result, void *data);

/* Worker event loop only. Arm from an auxiliary header filter before native
 * cache header construction. DECLINED means this is not a cacheable fill.
 * Complete bodies start PREPARE asynchronously after upstream finalization.
 * A step returning AGAIN must complete exactly once, including failure/timeout.
 * FINISH reconciles every attempted PREPARE. Its NGX_OK result means rename
 * was attempted, not that it succeeded: inspect the actual final instance.
 * Callbacks must not replace r->cache, finalize the request, or update/free c.
 * This lifecycle uses existing interfaces on native nginx, with no core hook.
 * Consumers still own durable indexing, bounded deadlines and file leases. */
ngx_int_t ngx_http_cache_purge_fill_arm(ngx_http_request_t               *r,
                                        ngx_http_cache_purge_fill_t     **slot,
                                        ngx_http_cache_purge_fill_step_pt step,
                                        void                             *data);
void      ngx_http_cache_purge_fill_complete(ngx_http_cache_purge_fill_t *fill,
    ngx_int_t result);

#endif
#endif /* _NGX_HTTP_CACHE_PURGE_FILL_H_INCLUDED_ */
