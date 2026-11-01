
/*
 * Copyright (C) Hanada
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include "ngx_http_cache_rocksdb.h"


static ngx_int_t ngx_http_cache_rocksdb_preconfiguration(ngx_conf_t *cf);


static ngx_http_module_t  ngx_http_cache_rocksdb_module_ctx = {
    ngx_http_cache_rocksdb_preconfiguration, /* preconfiguration */
    NULL,                                    /* postconfiguration */

    NULL, /* create main configuration */
    NULL, /* init main configuration */

    NULL, /* create server configuration */
    NULL, /* merge server configuration */

    NULL, /* create location configuration */
    NULL  /* merge location configuration */
};


ngx_module_t  ngx_http_cache_rocksdb_module = {
    NGX_MODULE_V1,
    &ngx_http_cache_rocksdb_module_ctx, /* module context */
    NULL,                               /* module directives */
    NGX_HTTP_MODULE,                    /* module type */
    NULL,                               /* init master */
    NULL,                               /* init module */
    NULL,                               /* init process */
    NULL,                               /* init thread */
    NULL,                               /* exit thread */
    NULL,                               /* exit process */
    NULL,                               /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_http_cache_rocksdb_preconfiguration(ngx_conf_t *cf)
{
    if (ngx_http_cache_rocksdb_abi_1() != NGX_HTTP_CACHE_ROCKSDB_ABI) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "incompatible cache RocksDB library");

        return NGX_ERROR;
    }

    return NGX_OK;
}
