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


#if (NGX_EXPR)
#define NGX_HTTP_CACHE_PURGE_SCOPE                                           \
    (NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF               \
     | NGX_HTTP_MAIN_WHEN_CONF | NGX_HTTP_SRV_WHEN_CONF                       \
         | NGX_HTTP_LOC_WHEN_CONF)
#else
#define NGX_HTTP_CACHE_PURGE_SCOPE                                           \
    (NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF)
#endif


static void *ngx_http_cache_purge_main_conf(ngx_conf_t *cf);
static void *ngx_http_cache_purge_loc_conf(ngx_conf_t *cf);
static char *ngx_http_cache_purge_merge(ngx_conf_t *cf, void *parent,
    void *child);
static char *ngx_http_cache_purge_expression(ngx_conf_t *cf,
    ngx_command_t  *cmd, void *conf);
static char *ngx_http_cache_purge_store(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_cache_purge_zone(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_cache_purge_api_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static ngx_int_t ngx_http_cache_purge_post(ngx_conf_t *cf);
static ngx_int_t ngx_http_cache_purge_init(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_cache_purge_worker(ngx_cycle_t *cycle);
static ngx_msec_t ngx_http_cache_purge_manager(void *data);
static void ngx_http_cache_purge_exit(ngx_cycle_t *cycle);


#define ngx_purge_value(name, n)                                             \
    { ngx_string(name), NGX_HTTP_CACHE_PURGE_SCOPE | NGX_CONF_TAKE1,           \
      ngx_http_cache_purge_expression, NGX_HTTP_LOC_CONF_OFFSET,              \
      offsetof(ngx_http_cache_purge_loc_conf_t, value)                         \
          + (n) * sizeof(ngx_http_cache_purge_value_t *), NULL }

#if (NGX_EXPR)
#define NGX_HTTP_CACHE_PURGE_SET_FLAG  ngx_conf_set_conditional_flag_slot
#else
#define NGX_HTTP_CACHE_PURGE_SET_FLAG  ngx_conf_set_flag_slot
#endif

#define ngx_purge_flag(name, n)                                              \
    { ngx_string(name), NGX_HTTP_CACHE_PURGE_SCOPE | NGX_CONF_FLAG,            \
      NGX_HTTP_CACHE_PURGE_SET_FLAG, NGX_HTTP_LOC_CONF_OFFSET,                 \
      offsetof(ngx_http_cache_purge_loc_conf_t, flag)                          \
          + (n) * sizeof(ngx_http_cache_purge_flag_t), NULL }


static ngx_command_t  ngx_http_cache_purge_commands[] = {

    { ngx_string("cache_purge_store"),
      NGX_HTTP_MAIN_CONF | NGX_CONF_1MORE,
      ngx_http_cache_purge_store,
      NGX_HTTP_MAIN_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("cache_purge_state_zone"),
      NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE2,
      ngx_http_cache_purge_zone,
      NGX_HTTP_MAIN_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("cache_purge_propagation_timeout"),
      NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_main_conf_t, propagation),
      NULL },

    { ngx_string("cache_purge_api"),
      NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS,
      ngx_http_cache_purge_api_conf,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    ngx_purge_value("cache_purge_key_url", NGX_HTTP_CACHE_PURGE_URL),
    ngx_purge_value("cache_purge_key_misc", NGX_HTTP_CACHE_PURGE_MISC),
    ngx_purge_value("cache_purge_key_namespace",
                    NGX_HTTP_CACHE_PURGE_NAMESPACE),
    ngx_purge_value("cache_purge_tag", NGX_HTTP_CACHE_PURGE_TAG),
    ngx_purge_value("cache_purge_type_header",
                    NGX_HTTP_CACHE_PURGE_TYPE_HEADER),
    ngx_purge_value("cache_purge_action_header",
                    NGX_HTTP_CACHE_PURGE_ACTION_HEADER),
    ngx_purge_value("cache_purge_tags_header",
                    NGX_HTTP_CACHE_PURGE_TAGS_HEADER),
    ngx_purge_value("cache_purge_operation_id_header",
                    NGX_HTTP_CACHE_PURGE_ID_HEADER),
    ngx_purge_value("cache_purge_issued_at_header",
                    NGX_HTTP_CACHE_PURGE_ISSUED_HEADER),
    ngx_purge_value("cache_purge_propagation_time_header",
                    NGX_HTTP_CACHE_PURGE_TIME_HEADER),
    ngx_purge_value("cache_purge_propagation_mode_header",
                    NGX_HTTP_CACHE_PURGE_MODE_HEADER),

    ngx_purge_flag("proxy_cache_purge", 0),
    ngx_purge_flag("fastcgi_cache_purge", 1),
    ngx_purge_flag("scgi_cache_purge", 2),
    ngx_purge_flag("uwsgi_cache_purge", 3),
    ngx_purge_flag("cache_purge_propagation", 4),
    ngx_purge_flag("cache_purge_ignore_propagation", 5),

      ngx_null_command
};


static ngx_http_module_t  ngx_http_cache_purge_context = {
    ngx_http_cache_purge_variables,
    ngx_http_cache_purge_post,
    ngx_http_cache_purge_main_conf, NULL, NULL, NULL,
    ngx_http_cache_purge_loc_conf, ngx_http_cache_purge_merge
};

ngx_module_t  ngx_http_cache_purge_module = {
    NGX_MODULE_V1,
    &ngx_http_cache_purge_context,
    ngx_http_cache_purge_commands,
    NGX_HTTP_MODULE,
    NULL, ngx_http_cache_purge_init, ngx_http_cache_purge_worker,
    NULL, NULL, ngx_http_cache_purge_exit, NULL,
    NGX_MODULE_V1_PADDING
};


static void *
ngx_http_cache_purge_main_conf(ngx_conf_t *cf)
{
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_uint_t                         i;
    static ngx_str_t                   defaults[] = {
        ngx_string(""), ngx_string(""), ngx_string(""), ngx_string(""),
        ngx_string("X-Cache-Purge-Type"), ngx_string("X-Cache-Purge-Action"),
        ngx_string("X-Cache-Purge-Tags"),
        ngx_string("X-Cache-Purge-Operation-Id"),
        ngx_string("X-Cache-Purge-Issued-At"),
        ngx_string("X-Cache-Purge-Time"), ngx_string("X-Cache-Purge-Mode")
    };

    main = ngx_pcalloc(cf->pool, sizeof(*main));
    if (main == NULL
        || ngx_array_init(&main->caches, cf->pool, 4,
                          sizeof(ngx_http_file_cache_t *)) != NGX_OK
        || ngx_array_init(&main->locations, cf->pool, 4, sizeof(void **))
           != NGX_OK)
    {
        return NULL;
    }
    main->propagation = NGX_CONF_UNSET_MSEC;
    ngx_http_cache_rocksdb_options(&main->options);
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_VALUES; i++) {
        main->defaults[i].value = defaults[i];
    }
    return main;
}


static void *
ngx_http_cache_purge_loc_conf(ngx_conf_t *cf)
{
    ngx_http_cache_purge_loc_conf_t  *conf;
    ngx_uint_t                        i;

    conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf == NULL) {
        return NULL;
    }
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_VALUES; i++) {
        conf->value[i] = NGX_CONF_UNSET_PTR;
    }
#if !(NGX_EXPR)
    for (i = 0; i < 6; i++) {
        conf->flag[i] = NGX_CONF_UNSET;
    }
#endif
    return conf;
}


static char *
ngx_http_cache_purge_expression(ngx_conf_t *cf, ngx_command_t *cmd, void *data)
{
    ngx_http_cache_purge_loc_conf_t  *conf;
    ngx_str_t                        *value;
    ngx_uint_t                        n, i;
    u_char                            ch;

    conf = data;
    value = cf->args->elts;
    n = (cmd->offset - offsetof(ngx_http_cache_purge_loc_conf_t, value))
        / sizeof(ngx_http_cache_purge_value_t *);
    if (n == NGX_HTTP_CACHE_PURGE_URL) {
        conf->url_set = 1;
        if (ngx_http_script_variables_count(&value[1]) == 0
            && !((value[1].len > 7
                  && ngx_strncasecmp(value[1].data,
                                      (u_char *) "http://", 7) == 0)
                 || (value[1].len > 8
                     && ngx_strncasecmp(value[1].data,
                                         (u_char *) "https://", 8) == 0)))
        {
            return "requires an absolute HTTP/HTTPS URL or an expression";
        }
    }
    if (n >= NGX_HTTP_CACHE_PURGE_TYPE_HEADER) {
        if (value[1].len == 0 || value[1].len > 255) {
            return "requires a header name of 1-255 bytes";
        }
        for (i = 0; i < value[1].len; i++) {
            ch = value[1].data[i];
            if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
                  || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_'))
            {
                return "requires a literal HTTP header name";
            }
        }
    }
#if (NGX_EXPR)
    return ngx_http_set_expr_complex_value_slot(cf, cmd, data);
#else
    return ngx_http_set_complex_value_slot(cf, cmd, data);
#endif
}


static char *
ngx_http_cache_purge_merge(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_cache_purge_loc_conf_t   *prev = parent;
    ngx_http_cache_purge_loc_conf_t   *conf = child;
    ngx_http_cache_purge_main_conf_t  *main;
    void                            ***location;
    ngx_uint_t                         i;

    main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_module);
    conf->url_set |= prev->url_set;
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_VALUES; i++) {
#if (NGX_EXPR)
        if (ngx_conf_merge_expr_ptr_value(cf, &conf->value[i], prev->value[i],
                                         &main->defaults[i]) != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }
#else
        ngx_conf_merge_ptr_value(conf->value[i], prev->value[i],
                                 &main->defaults[i]);
#endif
    }
    for (i = 0; i < 6; i++) {
#if (NGX_EXPR)
        if (ngx_conf_merge_expr_flag_value(cf, &conf->flag[i], prev->flag[i], 0)
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
ngx_http_cache_purge_store(ngx_conf_t *cf, ngx_command_t *cmd, void *data)
{
    ngx_http_cache_purge_main_conf_t  *main = data;
    ngx_str_t  *value, size;
    ngx_uint_t  i, seen;
    ssize_t     parsed;

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
ngx_http_cache_purge_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *data)
{
    ngx_http_cache_purge_main_conf_t  *main = data;
    ngx_str_t  *value;
    ssize_t     size;

    if (main->zone != NULL) {
        return "is duplicate";
    }
    value = cf->args->elts;
    size = ngx_parse_size(&value[2]);
    if (size < 1024 * 1024) {
        return "requires a shared zone of at least 1m";
    }
    main->zone = ngx_shared_memory_add(cf, &value[1], (size_t) size,
                                       &ngx_http_cache_purge_module);
    if (main->zone == NULL) {
        return NGX_CONF_ERROR;
    }
    main->zone->init = ngx_http_cache_purge_state_init;
    main->zone->data = &main->state;
    return NGX_CONF_OK;
}


static char *
ngx_http_cache_purge_api_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *data)
{
    ngx_http_cache_purge_loc_conf_t  *conf = data;
    ngx_http_core_loc_conf_t  *core;

    core = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    conf->api = core->name;
    core->handler = ngx_http_cache_purge_api;
    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_cache_purge_contains(ngx_str_t a, ngx_str_t b)
{
    while (a.len > 1 && a.data[a.len - 1] == '/') {
        a.len--;
    }
    return b.len >= a.len && ngx_memcmp(a.data, b.data, a.len) == 0
           && (b.len == a.len || a.len == 1 || b.data[a.len] == '/');
}


static ngx_int_t
ngx_http_cache_purge_post(ngx_conf_t *cf)
{
    ngx_http_cache_purge_main_conf_t  *main, *old;
    ngx_module_t                      *module;
    ngx_command_t                     *command;
    ngx_array_t                       *array;
    ngx_http_file_cache_t            **caches, **slot;
    void                              *conf;
    ngx_uint_t                         i, j;

    main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_module);
    ngx_conf_init_msec_value(main->propagation, 120000);
    old = NULL;
    if (cf->cycle->old_cycle != NULL
        && cf->cycle->old_cycle->conf_ctx != NULL
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
                          old->path->name.len) != 0
            || main->zone->shm.size != old->zone->shm.size
            || main->zone->shm.name.len != old->zone->shm.name.len
            || ngx_memcmp(main->zone->shm.name.data, old->zone->shm.name.data,
                          old->zone->shm.name.len) != 0))
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "changing cache purge store or state zone requires a full restart");
        return NGX_ERROR;
    }
    if ((main->path == NULL) != (main->zone == NULL)
        || main->options.write_buffer_bytes > main->options.cache_bytes / 2)
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
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
            conf = ((ngx_http_conf_ctx_t *) cf->ctx)
                       ->main_conf[module->ctx_index];
            array = (ngx_array_t *) ((u_char *) conf + command->offset);
            caches = array->elts;
            for (j = 0; j < array->nelts; j++) {
                slot = ngx_array_push(&main->caches);
                if (slot == NULL) {
                    return NGX_ERROR;
                }
                *slot = caches[j];
                if (main->path != NULL
                    && (ngx_http_cache_purge_contains(main->path->name,
                                                        caches[j]->path->name)
                        || ngx_http_cache_purge_contains(caches[j]->path->name,
                                                         main->path->name)))
                {
                    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                        "cache purge store must be outside native cache paths");
                    return NGX_ERROR;
                }
            }
        }
    }
    return ngx_http_cache_purge_request_init(cf);
}


static ngx_int_t
ngx_http_cache_purge_init(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_http_file_cache_t            **caches;
    ngx_str_t                          store, root;
    u_char                             names[2][NGX_MAX_PATH];
    ngx_uint_t                         i;

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
        if (ngx_http_cache_purge_contains(store, root)
            || ngx_http_cache_purge_contains(root, store))
        {
            ngx_log_error(NGX_LOG_EMERG, cycle->log, 0,
                          "cache purge store overlaps a resolved cache path");
            return NGX_ERROR;
        }
    }
    return ngx_http_cache_purge_channel_init(cycle, &main->state);
}


static ngx_int_t
ngx_http_cache_purge_worker(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_cycle_get_module_main_conf(cycle,
                                               ngx_http_cache_purge_module);
    if (main == NULL || main->path == NULL) {
        return NGX_OK;
    }
    if (ngx_process == NGX_PROCESS_SINGLE)
    {
        return ngx_http_cache_purge_owner_start(cycle, &main->state,
                &main->path->name, &main->options, main->caches.elts,
                main->caches.nelts, main->propagation, 1, &main->owner);
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
        if (ngx_http_cache_purge_owner_start((ngx_cycle_t *) ngx_cycle,
                &main->state, &main->path->name, &main->options,
                main->caches.elts, main->caches.nelts, main->propagation, 0,
                &main->owner)
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
ngx_http_cache_purge_exit(ngx_cycle_t *cycle)
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
ngx_http_cache_purge_flag(ngx_http_request_t *r, ngx_uint_t n)
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
ngx_http_cache_purge_value(ngx_http_request_t *r, ngx_uint_t n,
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
