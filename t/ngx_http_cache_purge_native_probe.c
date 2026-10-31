/* Copyright (C) Hanada */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_md5.h>
#include "ngx_http_cache_purge.h"


#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
typedef struct {
    ngx_http_cache_purge_state_t   state;
    ngx_http_cache_purge_owner_t  *owner;
    ngx_str_t                      store;
    ngx_array_t                    caches;
} ngx_http_cache_purge_probe_main_t;


typedef struct {
    ngx_http_request_t              *request;
    ngx_http_cache_purge_state_t    *state;
    ngx_http_cache_purge_message_t  *message;
    ngx_event_t                      event;
    ngx_msec_t                       deadline;
    ngx_uint_t                       status;
} ngx_http_cache_purge_probe_wait_t;


static void *ngx_http_cache_purge_probe_main(ngx_conf_t *cf);
static char *ngx_http_cache_purge_probe_store(ngx_conf_t *cf,
    ngx_command_t  *cmd, void *conf);
static ngx_int_t ngx_http_cache_purge_probe_init(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_cache_purge_probe_process(ngx_cycle_t *cycle);
static void ngx_http_cache_purge_probe_exit(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_cache_purge_probe_async(ngx_http_request_t *r,
    ngx_http_cache_purge_probe_main_t *main);


#endif

static ngx_int_t ngx_http_cache_purge_probe_variables(ngx_conf_t *cf);
static ngx_int_t ngx_http_cache_purge_probe_post(ngx_conf_t *cf);
static char *ngx_http_cache_purge_probe_conf(ngx_conf_t *cf,
    ngx_command_t  *cmd, void *conf);

static ngx_http_file_cache_t  *ngx_http_cache_purge_probe_cache;

static ngx_command_t  ngx_http_cache_purge_probe_commands[] = {

#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
    { ngx_string("native_cache_test_store"),
      NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
      ngx_http_cache_purge_probe_store,
      NGX_HTTP_MAIN_CONF_OFFSET,
      0,
      NULL },

#endif

    { ngx_string("native_cache_test"),
      NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS,
      ngx_http_cache_purge_probe_conf,
      0,
      0,
      NULL },

      ngx_null_command
};

static ngx_http_module_t  ngx_http_cache_purge_probe_context = {
    ngx_http_cache_purge_probe_variables,
    ngx_http_cache_purge_probe_post,
#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
    ngx_http_cache_purge_probe_main,
#else
    NULL,
#endif
    NULL, NULL, NULL, NULL, NULL
};

ngx_module_t  ngx_http_cache_purge_native_probe_module = {
    NGX_MODULE_V1,
    &ngx_http_cache_purge_probe_context,
    ngx_http_cache_purge_probe_commands,
    NGX_HTTP_MODULE,
    NULL,
#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
    ngx_http_cache_purge_probe_init,
    ngx_http_cache_purge_probe_process,
#else
    NULL, NULL,
#endif
    NULL, NULL,
#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
    ngx_http_cache_purge_probe_exit,
#else
    NULL,
#endif
    NULL,
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
ngx_http_cache_purge_probe_guard(ngx_http_request_t *r,
    ngx_http_variable_value_t  *value, uintptr_t data)
{
    ngx_http_cache_t  *cache;
    ngx_int_t          purged;

    cache = r->cache;
    if (cache != NULL && cache->file_cache != NULL) {
        ngx_http_cache_purge_probe_cache = cache->file_cache;
        purged = ngx_http_cache_purge_native_node(cache->file_cache,
                  cache->key, 0, NGX_HTTP_CACHE_PURGE_NODE_READ);
        cache->purged |= purged == 1;
    }

    *value = ngx_http_variable_null_value;
    value->data = (u_char *) "0";
    value->len = 1;

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_probe_log(ngx_http_request_t *r)
{
    ngx_http_cache_t     *cache;
    ngx_http_upstream_t  *upstream;
    ngx_file_uniq_t       expected;
#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
    ngx_http_cache_purge_probe_main_t  *main;
    ngx_http_cache_purge_entry_t        entry;
    ngx_http_cache_purge_message_t     *message;
    ngx_str_t                          *root;
    ngx_file_uniq_t                     uniq;

#endif

    cache = r->cache;
    upstream = r->upstream;
    if (cache == NULL || upstream == NULL || cache->file_cache == NULL) {
        return NGX_OK;
    }

    expected = 0;
    if (upstream->cache_status == NGX_HTTP_CACHE_REVALIDATED) {
        expected = cache->uniq;

    } else if (r->cached || !upstream->cacheable || upstream->pipe == NULL
               || !upstream->pipe->upstream_done || !cache->updated)
    {
        return NGX_OK;
    }

    (void) ngx_http_cache_purge_native_node(cache->file_cache, cache->key,
                            expected, NGX_HTTP_CACHE_PURGE_NODE_CLEAR);

#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
    main = ngx_http_get_module_main_conf(r,
                                     ngx_http_cache_purge_native_probe_module);
    root = &cache->file_cache->path->name;
    if (main->store.len != 0 && cache->file.name.len > root->len
        && ngx_http_cache_purge_native_identity(cache->file_cache, cache->key,
                                                &uniq) == NGX_OK)
    {
        ngx_memzero(&entry, sizeof(entry));
        ngx_memcpy(entry.hash, cache->key, 16);
        entry.uniq = uniq;
        entry.created = (uint64_t) cache->date;
        entry.url.data = r->uri.data;
        entry.url.len = r->uri.len;
        entry.host.data = r->headers_in.server.data;
        entry.host.len = r->headers_in.server.len;
        entry.root.data = root->data;
        entry.root.len = root->len;
        entry.zone.data = cache->file_cache->shm_zone->shm.name.data;
        entry.zone.len = cache->file_cache->shm_zone->shm.name.len;
        entry.path.data = cache->file.name.data + root->len + 1;
        entry.path.len = cache->file.name.len - root->len - 1;
        entry.tags[0].data = (u_char *) "native";
        entry.tags[0].len = 6;
        entry.tag_count = 1;
        if (ngx_http_cache_purge_state_enqueue(&main->state,
                      NGX_HTTP_CACHE_PURGE_RECORD, &entry, &message) != NGX_OK)
        {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "native test index queue rejected fill");
        }
    }
#endif

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_probe_handler(ngx_http_request_t *r)
{
    ngx_http_cache_purge_native_file_t  result;
    ngx_http_file_cache_t              *cache;
    ngx_str_t                           target, action;
    ngx_md5_t                           md5;
    ngx_int_t                           rc;
    ngx_uint_t                          invalidate;
    size_t                              length;
    u_char                             *name, *p, key[NGX_HTTP_CACHE_KEY_LEN];
#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
    ngx_http_cache_purge_probe_main_t  *main;

    main = ngx_http_get_module_main_conf(r,
                                     ngx_http_cache_purge_native_probe_module);
    if (main->store.len != 0) {
        return ngx_http_cache_purge_probe_async(r, main);
    }
#endif

    cache = ngx_http_cache_purge_probe_cache;
    if (cache == NULL || ngx_http_arg(r, (u_char *) "target", 6, &target)
                        != NGX_OK
        || ngx_http_arg(r, (u_char *) "action", 6, &action) != NGX_OK)
    {
        return NGX_HTTP_BAD_REQUEST;
    }

    invalidate = action.len == 10
                 && ngx_strncmp(action.data, "invalidate", 10) == 0;
    if (!invalidate && (action.len != 6
                       || ngx_strncmp(action.data, "delete", 6) != 0))
    {
        return NGX_HTTP_BAD_REQUEST;
    }

    ngx_md5_init(&md5);
    ngx_md5_update(&md5, target.data, target.len);
    ngx_md5_final(key, &md5);

    length = cache->path->name.len + 1 + cache->path->len
             + 2 * NGX_HTTP_CACHE_KEY_LEN;
    name = ngx_pnalloc(r->pool, length + 1);
    if (name == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    p = ngx_cpymem(name, cache->path->name.data, cache->path->name.len);
    p += 1 + cache->path->len;
    *ngx_hex_dump(p, key, NGX_HTTP_CACHE_KEY_LEN) = '\0';
    ngx_create_hashed_filename(cache->path, name, length);

    rc = ngx_http_cache_purge_native_file((char *) name, invalidate, 0,
                                         &result);
    if (rc == NGX_OK) {
        rc = ngx_http_cache_purge_native_node(cache, key, result.uniq,
                invalidate ? NGX_HTTP_CACHE_PURGE_NODE_INVALIDATE
                           : NGX_HTTP_CACHE_PURGE_NODE_DELETE);
    }

    if (rc != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    return NGX_HTTP_NO_CONTENT;
}


static ngx_int_t
ngx_http_cache_purge_probe_variables(ngx_conf_t *cf)
{
    ngx_http_variable_t  *value;
    ngx_str_t            name = ngx_string("native_cache_test_guard");

    value = ngx_http_add_variable(cf, &name, NGX_HTTP_VAR_NOCACHEABLE);
    if (value == NULL) {
        return NGX_ERROR;
    }

    value->get_handler = ngx_http_cache_purge_probe_guard;

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_probe_post(ngx_conf_t *cf)
{
    ngx_http_core_main_conf_t  *core;
    ngx_http_handler_pt        *handler;
#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
    ngx_http_cache_purge_probe_main_t  *main;
    ngx_module_t                       *module;
    ngx_command_t                      *command;
    ngx_array_t                        *configured;
    ngx_http_file_cache_t             **caches, **slot;
    void                               *conf;
    ngx_uint_t                          i, j;

    main = ngx_http_conf_get_module_main_conf(cf,
                                     ngx_http_cache_purge_native_probe_module);
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
            configured = (ngx_array_t *) ((u_char *) conf + command->offset);
            caches = configured->elts;
            for (j = 0; j < configured->nelts; j++) {
                slot = ngx_array_push(&main->caches);
                if (slot == NULL) {
                    return NGX_ERROR;
                }
                *slot = caches[j];
            }
        }
    }
#endif

    core = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    handler = ngx_array_push(&core->phases[NGX_HTTP_LOG_PHASE].handlers);
    if (handler == NULL) {
        return NGX_ERROR;
    }

    *handler = ngx_http_cache_purge_probe_log;

    return NGX_OK;
}


static char *
ngx_http_cache_purge_probe_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *core;

    core = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    core->handler = ngx_http_cache_purge_probe_handler;

    return NGX_CONF_OK;
}


#if (NGX_HTTP_CACHE_PURGE_OWNER_TEST)
static void *
ngx_http_cache_purge_probe_main(ngx_conf_t *cf)
{
    ngx_http_cache_purge_probe_main_t  *main;

    main = ngx_pcalloc(cf->pool, sizeof(*main));
    if (main == NULL || ngx_array_init(&main->caches, cf->pool, 2,
                                    sizeof(ngx_http_file_cache_t *)) != NGX_OK)
    {
        return NULL;
    }
    return main;
}


static char *
ngx_http_cache_purge_probe_store(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_cache_purge_probe_main_t  *main;
    ngx_shm_zone_t                     *zone;
    ngx_str_t                          *value;
    ngx_str_t                         name = ngx_string("native_purge_state");

    main = conf;
    value = cf->args->elts;
    main->store = value[1];
    if (ngx_conf_full_name(cf->cycle, &main->store, 0) != NGX_OK) {
        return NGX_CONF_ERROR;
    }
    zone = ngx_shared_memory_add(cf, &name, 1024 * 1024,
                                   &ngx_http_cache_purge_native_probe_module);
    if (zone == NULL) {
        return NGX_CONF_ERROR;
    }
    zone->init = ngx_http_cache_purge_state_init;
    zone->data = &main->state;
    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_cache_purge_probe_init(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_probe_main_t  *main;

    main = ngx_http_cycle_get_module_main_conf(cycle,
                                     ngx_http_cache_purge_native_probe_module);
    return main->store.len ? ngx_http_cache_purge_channel_init(cycle,
                                                   &main->state) : NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_probe_process(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_probe_main_t  *main;
    ngx_http_cache_rocksdb_options_t    options;

    main = ngx_http_cycle_get_module_main_conf(cycle,
                                     ngx_http_cache_purge_native_probe_module);
    if (main->store.len == 0) {
        return NGX_OK;
    }
    if (ngx_process == NGX_PROCESS_SINGLE
        || (ngx_process == NGX_PROCESS_WORKER && ngx_worker == 0))
    {
        ngx_http_cache_rocksdb_options(&options);
        return ngx_http_cache_purge_owner_start(cycle, &main->state,
                &main->store, &options, main->caches.elts, main->caches.nelts,
                &main->owner);
    }
    if (main->state.channel[0] != -1) {
        (void) close(main->state.channel[0]);
        main->state.channel[0] = -1;
    }
    return NGX_OK;
}


static void
ngx_http_cache_purge_probe_exit(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_probe_main_t  *main;

    main = ngx_http_cycle_get_module_main_conf(cycle,
                                     ngx_http_cache_purge_native_probe_module);
    if (main->store.len != 0) {
        ngx_http_cache_purge_owner_stop(main->owner);
        ngx_http_cache_purge_channel_close(&main->state);
    }
}


static void
ngx_http_cache_purge_probe_cleanup(void *data)
{
    ngx_http_cache_purge_probe_wait_t  *ctx;

    ctx = data;
    if (ctx->event.timer_set) {
        ngx_del_timer(&ctx->event);
    }
    ngx_http_cache_purge_state_detach(ctx->state, &ctx->message);
}


static void
ngx_http_cache_purge_probe_poll(ngx_event_t *event)
{
    ngx_http_cache_purge_probe_wait_t  *ctx;
    ngx_http_cache_purge_job_t          job;
    ngx_http_request_t                 *r;
    ngx_buf_t                          *buffer;
    ngx_chain_t                         out;
    ngx_int_t                           rc;
    int                                 result;

    ctx = event->data;
    r = ctx->request;
    rc = ngx_http_cache_purge_state_poll(ctx->state, &ctx->message,
                                          &result, &job);
    if (rc == NGX_AGAIN
        && (ngx_msec_int_t) (ngx_current_msec - ctx->deadline) < 0)
    {
        ngx_add_timer(event, 10);
        return;
    }
    if (rc != NGX_OK || result != 0) {
        ngx_http_finalize_request(r, NGX_HTTP_SERVICE_UNAVAILABLE);
        return;
    }
    buffer = ngx_create_temp_buf(r->pool, 128);
    if (buffer == NULL) {
        ngx_http_finalize_request(r, NGX_ERROR);
        return;
    }
    buffer->last = ngx_sprintf(buffer->last,
        "{\"done\":%ui,\"affected\":%uL,\"visited\":%uL,\"error\":%ui}",
        (ngx_uint_t) job.done, job.affected, job.visited,
        (ngx_uint_t) job.error);
    buffer->last_buf = 1;
    r->headers_out.status = ctx->status;
    r->headers_out.content_length_n = buffer->last - buffer->pos;
    ngx_str_set(&r->headers_out.content_type, "application/json");
    rc = ngx_http_send_header(r);
    if (rc == NGX_OK && !r->header_only) {
        out.buf = buffer;
        out.next = NULL;
        rc = ngx_http_output_filter(r, &out);
    }
    ngx_http_finalize_request(r, rc);
}


static ngx_int_t
ngx_http_cache_purge_probe_async(ngx_http_request_t *r,
    ngx_http_cache_purge_probe_main_t *main)
{
    ngx_str_t                           action, target, id;
    ngx_http_cache_purge_job_t          job;
    ngx_http_cache_purge_probe_wait_t  *ctx;
    ngx_pool_cleanup_t                 *cleanup;
    ngx_int_t                           number, rc;
    ngx_uint_t                          type;

    if (!ngx_http_cache_purge_state_ready(&main->state)) {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    if (ngx_http_arg(r, (u_char *) "action", 6, &action) != NGX_OK) {
        return NGX_HTTP_BAD_REQUEST;
    }
    if (action.len == 5 && ngx_strncmp(action.data, "ready", 5) == 0) {
        return NGX_HTTP_NO_CONTENT;
    }
    if (ngx_http_arg(r, (u_char *) "id", 2, &id) != NGX_OK) {
        return NGX_HTTP_BAD_REQUEST;
    }
    number = ngx_atoi(id.data, id.len);
    if (number < 1 || number > 65535) {
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_memzero(&job, sizeof(job));
    job.id[14] = (u_char) (number >> 8);
    job.id[15] = (u_char) number;
    type = NGX_HTTP_CACHE_PURGE_SUBMIT;
    if (action.len == 6 && ngx_strncmp(action.data, "status", 6) == 0) {
        type = NGX_HTTP_CACHE_PURGE_STATUS;
    } else {
        if (ngx_http_arg(r, (u_char *) "target", 6, &target) != NGX_OK) {
            return NGX_HTTP_BAD_REQUEST;
        }
        job.select.type = NGX_HTTP_CACHE_PURGE_SELECT_URL;
        job.select.value.data = target.data;
        job.select.value.len = target.len;
        job.issued = (uint64_t) ngx_time();
        job.retain_until = job.issued + 120;
        if (action.len == 10
            && ngx_strncmp(action.data, "invalidate", 10) == 0)
        {
            job.action = NGX_HTTP_CACHE_PURGE_INVALIDATE;
        } else if (action.len != 6
                   || ngx_strncmp(action.data, "delete", 6) != 0)
        {
            return NGX_HTTP_BAD_REQUEST;
        }
    }
    cleanup = ngx_pool_cleanup_add(r->pool, sizeof(*ctx));
    if (cleanup == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    ctx = cleanup->data;
    ngx_memzero(ctx, sizeof(*ctx));
    ctx->request = r;
    ctx->state = &main->state;
    ctx->status = type == NGX_HTTP_CACHE_PURGE_SUBMIT ? NGX_HTTP_ACCEPTED
                                                   : NGX_HTTP_OK;
    ctx->deadline = ngx_current_msec + 5000;
    ctx->event.data = ctx;
    ctx->event.log = r->connection->log;
    ctx->event.handler = ngx_http_cache_purge_probe_poll;
    cleanup->handler = ngx_http_cache_purge_probe_cleanup;
    rc = ngx_http_cache_purge_state_enqueue(ctx->state, type, &job,
                                           &ctx->message);
    if (rc != NGX_OK) {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    r->main->count++;
    r->read_event_handler = ngx_http_test_reading;
    r->write_event_handler = ngx_http_request_empty_handler;
    ngx_add_timer(&ctx->event, 1);
    return NGX_DONE;
}

#endif
