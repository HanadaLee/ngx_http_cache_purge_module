
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


#include "ngx_http_cache_purge.h"


static void *ngx_http_cache_purge_create_main_conf(ngx_conf_t *cf);
static char *ngx_http_cache_purge_init_main_conf(ngx_conf_t *cf, void *conf);
static void *ngx_http_cache_purge_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_cache_purge_merge_loc_conf(ngx_conf_t *cf, void *parent,
    void *child);
static char *ngx_http_cache_purge_set_group(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);
static char *ngx_http_cache_purge_set_group_slot(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);
static char *ngx_http_cache_purge_header_name(ngx_str_t value);
static char *ngx_http_cache_purge_index_store(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);
static char *ngx_http_cache_purge_state_zone(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);
static char *ngx_http_cache_purge_api(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static ngx_int_t ngx_http_cache_purge_path_contains(ngx_str_t a, ngx_str_t b);
static ngx_int_t ngx_http_cache_purge_init(ngx_conf_t *cf);
static ngx_int_t ngx_http_cache_purge_init_module(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_cache_purge_init_process(ngx_cycle_t *cycle);
static ngx_msec_t ngx_http_cache_purge_manager(void *data);
static void ngx_http_cache_purge_exit_process(ngx_cycle_t *cycle);


static ngx_str_t  ngx_http_cache_purge_keys[] = {
    ngx_string("cache_key_misc"),
    ngx_string("cache_key_namespace"),
    ngx_string("cache_key_host")
};


static ngx_str_t  ngx_http_cache_purge_default_values[] = {
    ngx_string("$http_x_cache_purge_type"),
    ngx_string("$http_x_cache_purge_action")
};


static ngx_str_t  ngx_http_cache_purge_operation_parameters[] = {
    ngx_string("id="),
    ngx_string("issued_at=")
};


static ngx_str_t  ngx_http_cache_purge_header_parameters[] = {
    ngx_string("time="),
    ngx_string("action=")
};


static ngx_str_t  ngx_http_cache_purge_group_defaults[][2] = {
    { ngx_null_string, ngx_null_string },
    { ngx_string("X-Cache-Purge-Operation-Id"),
      ngx_string("X-Cache-Purge-Issued-At") },
    { ngx_string("X-Cache-Purge-Time"),
      ngx_string("X-Cache-Purge-Action") }
};


static ngx_command_t  ngx_http_cache_purge_commands[] = {

    { ngx_string("cache_purge_index_store"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
      ngx_http_cache_purge_index_store,
      NGX_HTTP_MAIN_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("cache_purge_state_zone"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
      ngx_http_cache_purge_state_zone,
      NGX_HTTP_MAIN_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("cache_purge_propagation_timeout"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_main_conf_t, propagation),
      NULL },

    { ngx_string("cache_purge_api"),
      NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_cache_purge_api,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("cache_purge_tags"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_1MORE,
      ngx_http_cache_purge_set_group,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               group[NGX_HTTP_CACHE_PURGE_TAGS]),
      NULL },

    { ngx_string("cache_purge_operation_headers"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE2,
      ngx_http_cache_purge_set_group,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               group[NGX_HTTP_CACHE_PURGE_OPERATION]),
      ngx_http_cache_purge_operation_parameters },

    { ngx_string("cache_purge_propagation_headers"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE2,
      ngx_http_cache_purge_set_group,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               group[NGX_HTTP_CACHE_PURGE_HEADERS]),
      ngx_http_cache_purge_header_parameters },

    { ngx_string("cache_purge_type"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE1,
#if (NGX_EXPR)
      ngx_http_set_expr_complex_value_slot,
#else
      ngx_http_set_complex_value_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               value[NGX_HTTP_CACHE_PURGE_TYPE]),
      NULL },

    { ngx_string("cache_purge_action"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE1,
#if (NGX_EXPR)
      ngx_http_set_expr_complex_value_slot,
#else
      ngx_http_set_complex_value_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               value[NGX_HTTP_CACHE_PURGE_ACTION]),
      NULL },

    { ngx_string("proxy_cache_purge"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_FLAG,
#if (NGX_EXPR)
      ngx_conf_set_conditional_flag_slot,
#else
      ngx_conf_set_flag_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               flag[NGX_HTTP_CACHE_PURGE_PROXY]),
      NULL },

    { ngx_string("fastcgi_cache_purge"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_FLAG,
#if (NGX_EXPR)
      ngx_conf_set_conditional_flag_slot,
#else
      ngx_conf_set_flag_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               flag[NGX_HTTP_CACHE_PURGE_FASTCGI]),
      NULL },

    { ngx_string("scgi_cache_purge"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_FLAG,
#if (NGX_EXPR)
      ngx_conf_set_conditional_flag_slot,
#else
      ngx_conf_set_flag_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               flag[NGX_HTTP_CACHE_PURGE_SCGI]),
      NULL },

    { ngx_string("uwsgi_cache_purge"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_FLAG,
#if (NGX_EXPR)
      ngx_conf_set_conditional_flag_slot,
#else
      ngx_conf_set_flag_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               flag[NGX_HTTP_CACHE_PURGE_UWSGI]),
      NULL },

    { ngx_string("proxy_cache_purge_propagation"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_FLAG,
#if (NGX_EXPR)
      ngx_conf_set_conditional_flag_slot,
#else
      ngx_conf_set_flag_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               flag[NGX_HTTP_CACHE_PURGE_PROPAGATION]),
      NULL },

    { ngx_string("cache_purge_ignore_propagation"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_FLAG,
#if (NGX_EXPR)
      ngx_conf_set_conditional_flag_slot,
#else
      ngx_conf_set_flag_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_loc_conf_t,
               flag[NGX_HTTP_CACHE_PURGE_IGNORE_PROPAGATION]),
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_cache_purge_module_ctx = {
    ngx_http_cache_purge_add_variables,     /* preconfiguration */
    ngx_http_cache_purge_init,              /* postconfiguration */

    ngx_http_cache_purge_create_main_conf,  /* create main configuration */
    ngx_http_cache_purge_init_main_conf,    /* init main configuration */

    NULL,                                   /* create server configuration */
    NULL,                                   /* merge server configuration */

    ngx_http_cache_purge_create_loc_conf,   /* create location configuration */
    ngx_http_cache_purge_merge_loc_conf     /* merge location configuration */
};


ngx_module_t  ngx_http_cache_purge_module = {
    NGX_MODULE_V1,
    &ngx_http_cache_purge_module_ctx,       /* module context */
    ngx_http_cache_purge_commands,          /* module directives */
    NGX_HTTP_MODULE,                        /* module type */
    NULL,                                   /* init master */
    ngx_http_cache_purge_init_module,       /* init module */
    ngx_http_cache_purge_init_process,      /* init process */
    NULL,                                   /* init thread */
    NULL,                                   /* exit thread */
    ngx_http_cache_purge_exit_process,      /* exit process */
    NULL,                                   /* exit master */
    NGX_MODULE_V1_PADDING
};


static void *
ngx_http_cache_purge_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_pcalloc(cf->pool, sizeof(*main));

    if (main == NULL) {
        return NULL;
    }

    if (ngx_array_init(&main->caches, cf->pool, 4,
                       sizeof(ngx_http_file_cache_t *)) != NGX_OK)
    {
        return NULL;
    }

    if (ngx_array_init(&main->locations, cf->pool, 4, sizeof(void **))
        != NGX_OK)
    {
        return NULL;
    }

    main->propagation = NGX_CONF_UNSET_MSEC;
    ngx_http_cache_rocksdb_init_options(&main->options);

    return main;
}


static char *
ngx_http_cache_purge_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_http_cache_purge_main_conf_t  *main = conf;

    ngx_uint_t                        i, j;
    ngx_http_compile_complex_value_t  ccv;
    ngx_http_complex_value_t         *value;

    ngx_conf_init_msec_value(main->propagation, 120000);

    for (i = 0; i < NGX_HTTP_CACHE_PURGE_KEYS; i++) {
        main->keys[i] =
            ngx_http_get_variable_index(cf, &ngx_http_cache_purge_keys[i]);

        if (main->keys[i] == NGX_ERROR) {
            return NGX_CONF_ERROR;
        }
    }

    for (i = 0; i < NGX_HTTP_CACHE_PURGE_VALUES; i++) {
        ngx_memzero(&ccv, sizeof(ccv));
        ccv.cf = cf;
        ccv.value = &ngx_http_cache_purge_default_values[i];
        ccv.complex_value = &main->defaults[i];

        if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
            return NGX_CONF_ERROR;
        }
    }

    for (i = 0; i < NGX_HTTP_CACHE_PURGE_GROUPS; i++) {
        if (ngx_array_init(&main->groups[i], cf->pool, 2,
                           sizeof(ngx_http_complex_value_t)) != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }

        if (i == NGX_HTTP_CACHE_PURGE_TAGS) {
            continue;
        }

        for (j = 0; j < 2; j++) {
            value = ngx_array_push(&main->groups[i]);

            if (value == NULL) {
                return NGX_CONF_ERROR;
            }

            ngx_memzero(&ccv, sizeof(ccv));
            ccv.cf = cf;
            ccv.value = &ngx_http_cache_purge_group_defaults[i][j];
            ccv.complex_value = value;

            if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
                return NGX_CONF_ERROR;
            }
        }
    }

    return NGX_CONF_OK;
}


static void *
ngx_http_cache_purge_create_loc_conf(ngx_conf_t *cf)
{
    ngx_uint_t                        i;
    ngx_http_cache_purge_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(*conf));

    if (conf == NULL) {
        return NULL;
    }

    for (i = 0; i < NGX_HTTP_CACHE_PURGE_VALUES; i++) {
        conf->value[i] = NGX_CONF_UNSET_PTR;
    }

    for (i = 0; i < NGX_HTTP_CACHE_PURGE_GROUPS; i++) {
        conf->group[i] = NGX_CONF_UNSET_PTR;
    }

#if !(NGX_EXPR)
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_FLAGS; i++) {
        conf->flag[i] = NGX_CONF_UNSET;
    }
#endif

    return conf;
}


static char *
ngx_http_cache_purge_header_name(ngx_str_t value)
{
    size_t  i;
    u_char  ch;

    if (value.len == 0 || value.len > 255) {
        return "requires a header name of 1-255 bytes";
    }

    for (i = 0; i < value.len; i++) {
        ch = value.data[i];

        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
              || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_'))
        {
            return "requires a literal HTTP header name";
        }
    }

    return NGX_CONF_OK;
}


static char *
ngx_http_cache_purge_set_group(ngx_conf_t *cf, ngx_command_t *cmd, void *data)
{
#if (NGX_EXPR)
    return ngx_expr_call_ptr_slot(cf, cmd, data,
                                  sizeof(ngx_conf_expr_ptr_ctx_t),
                                  offsetof(ngx_conf_expr_ptr_ctx_t, value),
                                  offsetof(ngx_conf_expr_ptr_ctx_t, expr_id),
                                  ngx_http_cache_purge_set_group_slot);
#else
    return ngx_http_cache_purge_set_group_slot(cf, cmd, data);
#endif
}


static char *
ngx_http_cache_purge_set_group_slot(ngx_conf_t *cf, ngx_command_t *cmd,
    void *data)
{
    char                             *rc;
    ngx_str_t                        *value, *names, selected[2], text;
    ngx_uint_t                        i, j, count, seen;
    ngx_array_t                     **slot, *group;
    ngx_http_complex_value_t         *compiled;
    ngx_http_compile_complex_value_t  ccv;

    slot = (ngx_array_t **) ((u_char *) data + cmd->offset);

    if (*slot != NGX_CONF_UNSET_PTR) {
        return "is duplicate";
    }

    value = cf->args->elts;
    names = cmd->post;
    count = cf->args->nelts - 1;

    if (names != NULL) {
        seen = 0;

        for (i = 1; i < cf->args->nelts; i++) {
            for (j = 0; j < 2; j++) {
                if (value[i].len >= names[j].len
                    && ngx_strncmp(value[i].data, names[j].data, names[j].len)
                       == 0)
                {
                    break;
                }
            }

            if (j == 2 || (seen & (1 << j))) {
                return "has an unknown or duplicate parameter";
            }

            selected[j].data = value[i].data + names[j].len;
            selected[j].len = value[i].len - names[j].len;
            seen |= 1 << j;
        }

        if (seen != 3) {
            return "requires both parameters";
        }

        for (i = 0; i < 2; i++) {
            rc = ngx_http_cache_purge_header_name(selected[i]);

            if (rc != NGX_CONF_OK) {
                return rc;
            }
        }

        if (selected[0].len == selected[1].len
            && ngx_strncasecmp(selected[0].data, selected[1].data,
                               selected[0].len) == 0)
        {
            return "requires distinct header names";
        }
    }

    group = ngx_array_create(cf->pool, count,
                             sizeof(ngx_http_complex_value_t));

    if (group == NULL) {
        return NGX_CONF_ERROR;
    }

    for (i = 0; i < count; i++) {
        text = names != NULL ? selected[i] : value[i + 1];
        compiled = ngx_array_push(group);

        if (compiled == NULL) {
            return NGX_CONF_ERROR;
        }

        ngx_memzero(&ccv, sizeof(ccv));
        ccv.cf = cf;
        ccv.value = &text;
        ccv.complex_value = compiled;

        if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
            return NGX_CONF_ERROR;
        }
    }

    *slot = group;

    return NGX_CONF_OK;
}


static char *
ngx_http_cache_purge_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_cache_purge_loc_conf_t   *prev = parent;
    ngx_http_cache_purge_loc_conf_t   *conf = child;

    void                            ***location;
    ngx_uint_t                         i;
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_module);

    for (i = 0; i < NGX_HTTP_CACHE_PURGE_VALUES; i++) {
#if (NGX_EXPR)
        if (ngx_conf_merge_expr_ptr_value(cf, &conf->value[i], prev->value[i],
                                          &main->defaults[i])
            != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }
#else
        ngx_conf_merge_ptr_value(conf->value[i], prev->value[i],
                                 &main->defaults[i]);
#endif
    }

    for (i = 0; i < NGX_HTTP_CACHE_PURGE_GROUPS; i++) {
#if (NGX_EXPR)
        if (ngx_conf_merge_expr_ptr_value(cf, &conf->group[i], prev->group[i],
                                          &main->groups[i]) != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }
#else
        ngx_conf_merge_ptr_value(conf->group[i], prev->group[i],
                                 &main->groups[i]);
#endif
    }

    for (i = 0; i < NGX_HTTP_CACHE_PURGE_FLAGS; i++) {
#if (NGX_EXPR)
        if (ngx_conf_merge_expr_flag_value(cf, &conf->flag[i], prev->flag[i],
                                           0)
            != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }
#else
        ngx_conf_merge_value(conf->flag[i], prev->flag[i], 0);
#endif
    }

    ngx_conf_merge_str_value(conf->api, prev->api, "");
    location = ngx_array_push(&main->locations);

    if (location == NULL) {
        return NGX_CONF_ERROR;
    }

    *location = ((ngx_http_conf_ctx_t *) cf->ctx)->loc_conf;

    return NGX_CONF_OK;
}


static char *
ngx_http_cache_purge_index_store(ngx_conf_t *cf, ngx_command_t *cmd,
    void *data)
{
    ngx_http_cache_purge_main_conf_t  *main = data;

    ssize_t     parsed;
    ngx_str_t  *value, size;
    ngx_uint_t  i, seen;

    if (main->path != NULL) {
        return "is duplicate";
    }

    value = cf->args->elts;
    main->path = ngx_pcalloc(cf->pool, sizeof(*main->path));

    if (main->path == NULL) {
        return NGX_CONF_ERROR;
    }

    main->path->name = value[1];
    main->path->manager = ngx_http_cache_purge_manager;
    main->path->data = main;

    while (main->path->name.len > 1
           && main->path->name.data[main->path->name.len - 1] == '/')
    {
        main->path->name.data[--main->path->name.len] = 0;
    }

    if (ngx_conf_full_name(cf->cycle, &main->path->name, 0) != NGX_OK
        || ngx_add_path(cf, &main->path) != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }

    seen = 0;

    for (i = 2; i < cf->args->nelts; i++) {
        if (value[i].len > 6 && ngx_strncmp(value[i].data, "cache=", 6) == 0
            && !(seen & 1))
        {
            size.data = value[i].data + 6;
            size.len = value[i].len - 6;
            parsed = ngx_parse_size(&size);

            if (parsed < 4 * 1024 * 1024) {
                return "requires cache >= 4m";
            }

            main->options.cache_bytes = (size_t) parsed;
            seen |= 1;
        } else if (value[i].len > 7
                   && ngx_strncmp(value[i].data, "buffer=", 7) == 0
                   && !(seen & 2))
        {
            size.data = value[i].data + 7;
            size.len = value[i].len - 7;
            parsed = ngx_parse_size(&size);

            if (parsed < 1024 * 1024) {
                return "requires buffer >= 1m";
            }

            main->options.write_buffer_bytes = (size_t) parsed;
            seen |= 2;
        } else if (value[i].len > 8
                   && ngx_strncmp(value[i].data, "io_rate=", 8) == 0
                   && !(seen & 4))
        {
            size.data = value[i].data + 8;
            size.len = value[i].len - 8;
            parsed = ngx_parse_size(&size);

            if (parsed != 0 && parsed < 1024) {
                return "requires io_rate=0 or io_rate >= 1k";
            }

            main->options.io_rate = (uint64_t) parsed;
            seen |= 4;
        } else {
            return "accepts a path, cache=SIZE, buffer=SIZE and io_rate=SIZE";
        }
    }

    return NGX_CONF_OK;
}


static char *
ngx_http_cache_purge_state_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *data)
{
    ngx_http_cache_purge_main_conf_t  *main = data;

    u_char     *p;
    ssize_t     size;
    ngx_str_t  *value, name, file, s;
    ngx_uint_t  i;
#if (NGX_RESTY_EXT && NGX_RESTY_SHM_PERSISTENCE)
    ngx_shm_zone_params_t  zp;
#endif

    if (main->zone != NULL) {
        return "is duplicate";
    }

    value = cf->args->elts;
    ngx_str_null(&name);
    ngx_str_null(&file);
    size = 0;

    for (i = 1; i < cf->args->nelts; i++) {
        if (ngx_strncmp(value[i].data, "zone=", 5) == 0) {
            if (name.len != 0) {
                return "has a duplicate zone parameter";
            }

            name.data = value[i].data + 5;
            p = ngx_strlchr(name.data, value[i].data + value[i].len, ':');

            if (p == NULL || p == name.data) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid zone \"%V\", "
                                   "expected zone=name:size",
                                   &value[i]);
                return NGX_CONF_ERROR;
            }

            name.len = p - name.data;
            s.data = p + 1;
            s.len = value[i].data + value[i].len - s.data;
            size = ngx_parse_size(&s);

            if (size == NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid zone size \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "state=", 6) == 0) {
            if (file.data != NULL) {
                return "has a duplicate state parameter";
            }

            file.data = value[i].data + 6;
            file.len = value[i].len - 6;

            if (file.len == 0) {
                return "requires a nonempty state file name";
            }

            if (ngx_conf_full_name(cf->cycle, &file, 0) != NGX_OK) {
                return NGX_CONF_ERROR;
            }

            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    if (name.len == 0) {
        return "requires zone=name:size";
    }

    if (size < 1024 * 1024) {
        return "requires a shared zone of at least 1m";
    }

    if (file.data != NULL) {
#if (NGX_RESTY_EXT && NGX_RESTY_SHM_PERSISTENCE)
        ngx_memzero(&zp, sizeof(ngx_shm_zone_params_t));
        zp.name = name;
        zp.size = size;
        zp.file = file;
        zp.tag = &ngx_http_cache_purge_module;
        zp.restorable = 1;

        if (ngx_http_cache_purge_state_signature(cf, &zp.signature) != NGX_OK) {
            return NGX_CONF_ERROR;
        }

        main->zone = ngx_shared_memory_add_ext(cf, &zp);
#else
        return "state requires nginx-ext shared memory persistence support";
#endif

    } else {
        main->zone = ngx_shared_memory_add(cf, &name, (size_t) size,
                                           &ngx_http_cache_purge_module);
    }

    if (main->zone == NULL) {
        return NGX_CONF_ERROR;
    }

    main->zone->init = ngx_http_cache_purge_state_init;
    main->zone->data = &main->state;

    return NGX_CONF_OK;
}


static char *
ngx_http_cache_purge_api(ngx_conf_t *cf, ngx_command_t *cmd, void *data)
{
    ngx_http_cache_purge_loc_conf_t  *conf = data;

    ngx_http_core_loc_conf_t  *core;

    core = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    conf->api = core->name;
    core->handler = ngx_http_cache_purge_api_handler;

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_cache_purge_path_contains(ngx_str_t a, ngx_str_t b)
{
    while (a.len > 1 && a.data[a.len - 1] == '/') {
        a.len--;
    }

    return b.len >= a.len && ngx_memcmp(a.data, b.data, a.len) == 0
           && (b.len == a.len || a.len == 1 || b.data[a.len] == '/');
}


static ngx_int_t
ngx_http_cache_purge_init(ngx_conf_t *cf)
{
    void         *conf;
    ngx_uint_t    i, j;
    ngx_array_t  *array;

    ngx_module_t                      *module;
    ngx_command_t                     *command;
    ngx_http_file_cache_t            **caches, **slot;
    ngx_http_cache_purge_main_conf_t  *main, *old;

    main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_module);
    old = NULL;

    if (cf->cycle->old_cycle != NULL && cf->cycle->old_cycle->conf_ctx != NULL
        && cf->cycle->old_cycle->modules != NULL)
    {
        for (i = 0; cf->cycle->old_cycle->modules[i] != NULL; i++) {
            if (cf->cycle->old_cycle->modules[i]
                == &ngx_http_cache_purge_module)
            {
                old = ngx_http_cycle_get_module_main_conf(
                    cf->cycle->old_cycle, ngx_http_cache_purge_module);
                break;
            }
        }
    }

    if (old != NULL && old->path != NULL
        && (main->path == NULL || main->zone == NULL
            || main->path->name.len != old->path->name.len
            || ngx_memcmp(main->path->name.data, old->path->name.data,
                          old->path->name.len)
               != 0
            || main->zone->shm.size != old->zone->shm.size
            || main->zone->shm.name.len != old->zone->shm.name.len
            || ngx_memcmp(main->zone->shm.name.data, old->zone->shm.name.data,
                          old->zone->shm.name.len)
               != 0))
    {
        ngx_conf_log_error(
            NGX_LOG_EMERG, cf, 0,
            "changing cache purge store or state zone requires a full restart");

        return NGX_ERROR;
    }

    if ((main->path == NULL) != (main->zone == NULL)
        || main->options.write_buffer_bytes > main->options.cache_bytes / 2)
    {
        ngx_conf_log_error(
            NGX_LOG_EMERG, cf, 0,
            "cache purge requires store and state_zone together; "
            "buffer must not exceed half of cache");

        return NGX_ERROR;
    }

    for (i = 0; cf->cycle->modules[i] != NULL; i++) {
        module = cf->cycle->modules[i];

        if (module->type != NGX_HTTP_MODULE || module->commands == NULL) {
            continue;
        }

        for (command = module->commands; command->name.len; command++) {
            if (command->set != ngx_http_file_cache_set_slot
                || command->conf != NGX_HTTP_MAIN_CONF_OFFSET)
            {
                continue;
            }

            conf =
                ((ngx_http_conf_ctx_t *) cf->ctx)->main_conf[module->ctx_index];
            array = (ngx_array_t *) ((u_char *) conf + command->offset);
            caches = array->elts;

            for (j = 0; j < array->nelts; j++) {
                slot = ngx_array_push(&main->caches);

                if (slot == NULL) {
                    return NGX_ERROR;
                }

                *slot = caches[j];

                if (main->path != NULL
                    && (ngx_http_cache_purge_path_contains(
                            main->path->name, caches[j]->path->name)
                        || ngx_http_cache_purge_path_contains(
                            caches[j]->path->name, main->path->name)))
                {
                    ngx_conf_log_error(
                        NGX_LOG_EMERG, cf, 0,
                        "cache purge store must be outside native cache paths");

                    return NGX_ERROR;
                }
            }
        }
    }

    return ngx_http_cache_purge_init_handlers(cf);
}


static ngx_int_t
ngx_http_cache_purge_init_module(ngx_cycle_t *cycle)
{
    ngx_str_t                          store, root;
    ngx_uint_t                         i;
    ngx_http_file_cache_t            **caches;
    ngx_http_cache_purge_main_conf_t  *main;
    u_char                             names[2][NGX_MAX_PATH];

    main = ngx_http_cycle_get_module_main_conf(cycle,
                                               ngx_http_cache_purge_module);

    if (main == NULL || main->path == NULL) {
        return NGX_OK;
    }

    if (ngx_realpath(main->path->name.data, names[0]) == NULL) {
        return NGX_ERROR;
    }

    store.data = names[0];
    store.len = ngx_strlen(store.data);
    caches = main->caches.elts;

    for (i = 0; i < main->caches.nelts; i++) {
        if (ngx_realpath(caches[i]->path->name.data, names[1]) == NULL) {
            return NGX_ERROR;
        }

        root.data = names[1];
        root.len = ngx_strlen(root.data);

        if (ngx_http_cache_purge_path_contains(store, root)
            || ngx_http_cache_purge_path_contains(root, store))
        {
            ngx_log_error(NGX_LOG_EMERG, cycle->log, 0,
                          "cache purge store overlaps a resolved cache path");

            return NGX_ERROR;
        }
    }

    return ngx_http_cache_purge_channel_init(cycle, &main->state);
}


static ngx_int_t
ngx_http_cache_purge_init_process(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_cycle_get_module_main_conf(cycle,
                                               ngx_http_cache_purge_module);

    if (main == NULL || main->path == NULL) {
        return NGX_OK;
    }

    if (ngx_process == NGX_PROCESS_SINGLE) {
        return ngx_http_cache_purge_owner_start(
            cycle, &main->state, &main->path->name, &main->options,
            main->caches.elts, main->caches.nelts, main->propagation, 1,
            &main->owner);
    }

    if (ngx_process != NGX_PROCESS_HELPER && main->state.channel[0] != -1) {
        (void) close(main->state.channel[0]);
        main->state.channel[0] = -1;
    }

    return NGX_OK;
}


static ngx_msec_t
ngx_http_cache_purge_manager(void *data)
{
    ngx_http_cache_purge_main_conf_t  *main = data;

    if (main->owner == NULL) {
        if (ngx_http_cache_purge_owner_start(
                (ngx_cycle_t *) ngx_cycle, &main->state, &main->path->name,
                &main->options, main->caches.elts, main->caches.nelts,
                main->propagation, 0, &main->owner)
            != NGX_OK)
        {
            ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, 0,
                          "cache purge manager initialization failed");
            exit(2);
        }
    }

    return 1000;
}


static void
ngx_http_cache_purge_exit_process(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_cycle_get_module_main_conf(cycle,
                                               ngx_http_cache_purge_module);

    if (main != NULL && main->path != NULL) {
        ngx_http_cache_purge_owner_stop(main->owner);
        ngx_http_cache_purge_channel_close(&main->state);
    }
}


ngx_flag_t
ngx_http_cache_purge_get_flag(ngx_http_request_t *r, ngx_uint_t n)
{
    ngx_http_cache_purge_loc_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
#if (NGX_EXPR)
    return ngx_http_get_expr_flag_value(r, conf->flag[n]);
#else
    return conf->flag[n];
#endif
}


ngx_int_t
ngx_http_cache_purge_get_value(ngx_http_request_t *r,
    ngx_http_cache_purge_value_e n,
    ngx_str_t *value)
{
    ngx_http_cache_purge_loc_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
#if (NGX_EXPR)
    return ngx_http_get_expr_complex_value(r, conf->value[n], value);
#else
    return ngx_http_complex_value(r, conf->value[n], value);
#endif
}


ngx_int_t
ngx_http_cache_purge_get_key(ngx_http_request_t *r,
    ngx_http_cache_purge_key_e n,
    ngx_str_t *value)
{
    ngx_http_variable_value_t         *v;
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    v = ngx_http_get_flushed_variable(r, main->keys[n]);

    if (v == NULL || v->not_found) {
        return NGX_ERROR;
    }

    value->data = v->data;
    value->len = v->len;

    return NGX_OK;
}


ngx_array_t *
ngx_http_cache_purge_get_group(ngx_http_request_t *r,
    ngx_http_cache_purge_group_e n)
{
    ngx_http_cache_purge_loc_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
#if (NGX_EXPR)
    return ngx_http_get_expr_ptr_value(r, conf->group[n]);
#else
    return conf->group[n];
#endif
}


ngx_int_t
ngx_http_cache_purge_get_pair(ngx_http_request_t *r,
    ngx_http_cache_purge_group_e n,
    ngx_str_t value[2])
{
    ngx_uint_t                 i;
    ngx_array_t               *group;
    ngx_http_complex_value_t  *compiled;

    group = ngx_http_cache_purge_get_group(r, n);

    if (group == NULL || group->nelts != 2) {
        return NGX_ERROR;
    }

    compiled = group->elts;

    for (i = 0; i < 2; i++) {
        if (ngx_http_complex_value(r, &compiled[i], &value[i]) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}
