/*
 * Copyright (c) 2009-2014, FRiCKLE <info@frickle.com>
 * Copyright (c) 2009-2014, Piotr Sikora <piotr.sikora@frickle.com>
 * Copyright (C) 2016-2026, Denys Denysov
 * Copyright (C) Hanada
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <nginx.h>
#include <ngx_http.h>
#include "ngx_http_cache_purge.h"

#if (NGX_EXPR)
#include <ngx_http_expr_module.h>
#endif


static void *ngx_http_cache_purge_create_main_conf(ngx_conf_t *cf);
static void *ngx_http_cache_purge_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_cache_purge_merge_loc_conf(ngx_conf_t *cf, void *parent,
    void *child);


static ngx_command_t  ngx_http_cache_purge_module_commands[] = {

    { ngx_string("cache_purge_store"), NGX_HTTP_MAIN_CONF | NGX_CONF_1MORE,
      ngx_http_cache_purge_store_conf, NGX_HTTP_MAIN_CONF_OFFSET, 0, NULL },

    { ngx_string("cache_purge_state_zone"), NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE2,
      ngx_http_cache_purge_state_conf, NGX_HTTP_MAIN_CONF_OFFSET, 0, NULL },

    { ngx_string("cache_purge_propagation_timeout"),
      NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_propagation_conf, NGX_HTTP_MAIN_CONF_OFFSET, 0,
      NULL },

    { ngx_string("cache_purge_api"), NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS,
      ngx_http_cache_purge_api_conf, NGX_HTTP_LOC_CONF_OFFSET, 0, NULL },

    { ngx_string("cache_purge_type_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, method_header[0]), NULL },

    { ngx_string("cache_purge_action_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, method_header[1]), NULL },

    { ngx_string("cache_purge_tags_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, method_header[2]), NULL },

    { ngx_string("cache_purge_operation_id_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, method_header[3]), NULL },

    { ngx_string("cache_purge_issued_at_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, method_header[4]), NULL },

    { ngx_string("cache_purge_tag_namespace_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, tag_namespace_header), NULL },

    { ngx_string("cache_purge_propagation_time_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, propagation_header[0]), NULL },

    { ngx_string("cache_purge_propagation_mode_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, propagation_header[1]), NULL },

    { ngx_string("cache_purge_propagation_fence_header"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF
      | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_api_header_conf, NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, propagation_header[2]), NULL },

    { ngx_string("cache_purge_propagation"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
      | NGX_HTTP_MAIN_WHEN_CONF | NGX_HTTP_SRV_WHEN_CONF
      | NGX_HTTP_LOC_WHEN_CONF
#endif
      | NGX_CONF_FLAG,
#if (NGX_EXPR)
      ngx_conf_set_conditional_flag_slot,
#else
      ngx_conf_set_flag_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, propagation), NULL },

    { ngx_string("cache_purge_ignore_propagation"),
      NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
      | NGX_HTTP_MAIN_WHEN_CONF | NGX_HTTP_SRV_WHEN_CONF
      | NGX_HTTP_LOC_WHEN_CONF
#endif
      | NGX_CONF_FLAG,
#if (NGX_EXPR)
      ngx_conf_set_conditional_flag_slot,
#else
      ngx_conf_set_flag_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t, ignore_propagation), NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_cache_purge_module_ctx = {
    NULL,                                         /* preconfiguration */
    ngx_http_cache_purge_runtime_post,            /* postconfiguration */
    ngx_http_cache_purge_create_main_conf,        /* create main conf */
    NULL,                                         /* init main conf */
    NULL,                                         /* create srv conf */
    NULL,                                         /* merge srv conf */
    ngx_http_cache_purge_create_loc_conf,         /* create loc conf */
    ngx_http_cache_purge_merge_loc_conf           /* merge loc conf */
};


ngx_module_t  ngx_http_cache_purge_module = {
    NGX_MODULE_V1,
    &ngx_http_cache_purge_module_ctx,
    ngx_http_cache_purge_module_commands,
    NGX_HTTP_MODULE,
    NULL,                                         /* init master */
    ngx_http_cache_purge_runtime_module_init,     /* init module */
    ngx_http_cache_purge_runtime_process_init,    /* init process */
    NULL,                                         /* init thread */
    NULL,                                         /* exit thread */
    ngx_http_cache_purge_runtime_worker_exit,     /* exit process */
    NULL,                                         /* exit master */
    NGX_MODULE_V1_PADDING
};



static void *
ngx_http_cache_purge_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_cache_purge_main_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf == NULL) {
        return NULL;
    }

    conf->runtime = ngx_http_cache_purge_runtime_create(cf);
    if (conf->runtime == NULL) {
        return NULL;
    }

    return conf;
}


static void *
ngx_http_cache_purge_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_cache_purge_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf == NULL) {
        return NULL;
    }

#if !(NGX_EXPR)
    conf->ignore_propagation = NGX_CONF_UNSET;
    conf->propagation = NGX_CONF_UNSET;
#endif

    return conf;
}


static char *
ngx_http_cache_purge_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_cache_purge_loc_conf_t  *prev = parent;
    ngx_http_cache_purge_loc_conf_t  *conf = child;

#if (NGX_EXPR)
    if (ngx_conf_merge_expr_flag_value(cf, &conf->ignore_propagation,
                                       prev->ignore_propagation, 0) != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }
    if (ngx_conf_merge_expr_flag_value(cf, &conf->propagation,
                                       prev->propagation, 0) != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }
#else
    ngx_conf_merge_value(conf->ignore_propagation, prev->ignore_propagation, 0);
    ngx_conf_merge_value(conf->propagation, prev->propagation, 0);
#endif

    return ngx_http_cache_purge_api_merge(cf, prev, conf);
}
