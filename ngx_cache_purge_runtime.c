/* Copyright (C) Hanada */
#include "ngx_cache_purge.h"
#include "ngx_cache_purge_runtime.h"
#include "ngx_cache_purge_component.h"
#include <sys/un.h>

#if (!HAVE_PRIVILEGED_PROCESS_PATCH)
#error "persistent cache purge requires the existing OpenResty privileged agent capability"
#endif

#define NGX_PURGE_RUNTIME_READERS  1024
#define NGX_PURGE_RUNTIME_VERSION 1

typedef struct {
    ngx_uint_t  version;
    void       *memory;
    size_t      bytes;
    ngx_str_t   path;
} ngx_http_cache_purge_shared_t;

struct ngx_http_cache_purge_runtime_s {
    ngx_path_t                       *path;
    ngx_shm_zone_t                   *zone;
    ngx_http_cache_purge_shared_t    *shared;
    ngx_cache_purge_component_options_t options;
    ngx_cache_purge_component_t      *component;
    ngx_cache_purge_control_reader_t *reader;
    ngx_msec_t                       propagation;
    ngx_uint_t                       started;
    ngx_uint_t                       failed;
    ngx_array_t                      roots;
#if (HAVE_PRIVILEGED_PROCESS_PATCH)
    ngx_event_t                      event;
#endif
};

static ngx_msec_t ngx_http_cache_purge_runtime_tick(void *data);
static void ngx_http_cache_purge_runtime_exit(void *data);
static ngx_int_t ngx_http_cache_purge_runtime_shared(ngx_shm_zone_t *zone, void *old);
static ngx_int_t ngx_http_cache_purge_runtime_paths(ngx_http_cache_purge_runtime_t *rt, ngx_log_t *log);
#if (HAVE_PRIVILEGED_PROCESS_PATCH)
static void ngx_http_cache_purge_runtime_agent(ngx_event_t *ev);
#endif

ngx_http_cache_purge_runtime_t *
ngx_http_cache_purge_runtime_create(ngx_conf_t *cf)
{
    ngx_http_cache_purge_runtime_t *rt;

    rt = ngx_pcalloc(cf->pool, sizeof(*rt));
    if (rt == NULL) {
        return NULL;
    }
    ngx_cache_purge_component_options(&rt->options);
    if (rt->options.abi != NGX_CACHE_PURGE_COMPONENT_ABI
        || ngx_cache_purge_component_abi_3() != NGX_CACHE_PURGE_COMPONENT_ABI
        || ngx_array_init(&rt->roots, cf->pool, 2, sizeof(ngx_str_t)) != NGX_OK)
    {
        return NULL;
    }
    rt->propagation = NGX_CONF_UNSET_MSEC;
    return rt;
}

char *
ngx_http_cache_purge_store_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_cache_purge_main_conf_t *main = conf;
    ngx_http_cache_purge_runtime_t   *rt = main->runtime;
    ngx_str_t *value, size;
    ngx_uint_t i, cache_seen = 0, buffer_seen = 0;
    ssize_t parsed;

    if (rt->path != NULL) {
        return "is duplicate";
    }
    value = cf->args->elts;
    rt->path = ngx_pcalloc(cf->pool, sizeof(ngx_path_t));
    if (rt->path == NULL) {
        return NGX_CONF_ERROR;
    }
    rt->path->name = value[1];
    if (ngx_conf_full_name(cf->cycle, &rt->path->name, 0) != NGX_OK) {
        return NGX_CONF_ERROR;
    }
    while (rt->path->name.len > 1
           && rt->path->name.data[rt->path->name.len - 1] == '/')
    {
        rt->path->name.data[--rt->path->name.len] = '\0';
    }
    if (rt->path->name.len + sizeof("/component.sock") > sizeof(((struct sockaddr_un *) 0)->sun_path)
        || rt->path->name.len == 1)
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                          "cache_purge_store path \"%V\" cannot hold its Unix socket",
                          &rt->path->name);
        return NGX_CONF_ERROR;
    }
    for (i = 0; i < rt->path->name.len; i++) {
        if (rt->path->name.data[i] == '\0'
            || (rt->path->name.data[i] == '/' && rt->path->name.data[i + 1] == '/')
            || (rt->path->name.data[i] == '/'
                && rt->path->name.data[i + 1] == '.'
                && (rt->path->name.data[i + 2] == '\0'
                    || rt->path->name.data[i + 2] == '/'
                    || (rt->path->name.data[i + 2] == '.'
                        && (rt->path->name.data[i + 3] == '\0'
                            || rt->path->name.data[i + 3] == '/')))))
        {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                              "cache_purge_store requires a normalized absolute path");
            return NGX_CONF_ERROR;
        }
    }
    for (i = 2; i < cf->args->nelts; i++) {
        if (value[i].len > 6 && ngx_strncmp(value[i].data, "cache=", 6) == 0 && !cache_seen) {
            size.data = value[i].data + 6; size.len = value[i].len - 6;
            parsed = ngx_parse_size(&size);
            if (parsed <= 0) {
                goto invalid;
            }
            rt->options.store.cache_bytes = (size_t) parsed; cache_seen = 1;
        } else if (value[i].len > 13 && ngx_strncmp(value[i].data, "write_buffer=", 13) == 0 && !buffer_seen) {
            size.data = value[i].data + 13; size.len = value[i].len - 13;
            parsed = ngx_parse_size(&size);
            if (parsed <= 0) {
                goto invalid;
            }
            rt->options.store.write_buffer_bytes = (size_t) parsed; buffer_seen = 1;
        } else {
            goto invalid;
        }
    }
    if (rt->options.store.cache_bytes < 4 * 1024 * 1024
        || rt->options.store.write_buffer_bytes < 1024 * 1024
        || rt->options.store.write_buffer_bytes > rt->options.store.cache_bytes / 2)
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                          "cache_purge_store requires cache >= 4m and "
                          "1m <= write_buffer <= cache/2");
        return NGX_CONF_ERROR;
    }
    return NGX_CONF_OK;

invalid:
    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                      "invalid cache_purge_store parameter \"%V\"; "
                      "expected cache=SIZE or write_buffer=SIZE once each", &value[i]);
    return NGX_CONF_ERROR;
}

char *
ngx_http_cache_purge_state_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_cache_purge_main_conf_t *main = conf;
    ngx_http_cache_purge_runtime_t   *rt = main->runtime;
    ngx_str_t *value, name;
    ssize_t size;

    if (rt->zone != NULL) {
        return "is duplicate";
    }
    value = cf->args->elts;
    size = ngx_parse_size(&value[2]);
    if (value[1].len == 0 || value[1].len > 128 || size < 1024 * 1024 || size > 128 * 1024 * 1024) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                          "cache_purge_state_zone requires a name of 1-128 bytes "
                          "and a size from 1m to 128m");
        return NGX_CONF_ERROR;
    }
    name.len = sizeof("cache_purge_state:") - 1 + value[1].len;
    name.data = ngx_pnalloc(cf->pool, name.len);
    if (name.data == NULL) {
        return NGX_CONF_ERROR;
    }
    ngx_sprintf(name.data, "cache_purge_state:%V", &value[1]);
    rt->zone = ngx_shared_memory_add(cf, &name, size, &ngx_http_cache_purge_module);
    if (rt->zone == NULL || rt->zone->data != NULL) {
        return NGX_CONF_ERROR;
    }
    rt->zone->data = rt;
    rt->zone->init = ngx_http_cache_purge_runtime_shared;
    return NGX_CONF_OK;
}

char *
ngx_http_cache_purge_propagation_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_cache_purge_main_conf_t *main = conf;
    ngx_command_t local = *cmd;
    local.offset = offsetof(ngx_http_cache_purge_runtime_t, propagation);
    return ngx_conf_set_msec_slot(cf, &local, main->runtime);
}

static ngx_int_t
ngx_http_cache_purge_runtime_shared(ngx_shm_zone_t *zone, void *old)
{
    ngx_http_cache_purge_runtime_t *rt = zone->data, *previous = old;
    ngx_slab_pool_t *pool = (ngx_slab_pool_t *) zone->shm.addr;
    ngx_http_cache_purge_shared_t *shared;
    ngx_cache_purge_store_error_t error;
    size_t available, overhead, bank_bytes, bytes;
    void *allocation;

    if (previous != NULL || zone->shm.exists) {
        shared = previous ? previous->shared : pool->data;
        if (shared == NULL || shared->version != NGX_PURGE_RUNTIME_VERSION
            || shared->path.len != rt->path->name.len
            || ngx_memcmp(shared->path.data, rt->path->name.data, shared->path.len))
        {
            ngx_log_error(NGX_LOG_EMERG, zone->shm.log, 0,
                          "cache purge shared state/path mismatch; full restart required");
            return NGX_ERROR;
        }
        rt->shared = shared;
        return NGX_OK;
    }
    shared = ngx_slab_calloc(pool, sizeof(*shared) + rt->path->name.len);
    if (shared == NULL) {
        return NGX_ERROR;
    }
    shared->path.data = (u_char *) (shared + 1);
    shared->path.len = rt->path->name.len;
    ngx_memcpy(shared->path.data, rt->path->name.data, shared->path.len);
    overhead = ngx_cache_purge_control_size(4096, NGX_PURGE_RUNTIME_READERS) - 8192;
    available = pool->pfree * ngx_pagesize;
    if (available < overhead + 8192 + 63) {
        return NGX_ERROR;
    }
    bank_bytes = ((available - overhead - 63) / 2) & ~(size_t) 63;
    bank_bytes = ngx_min(bank_bytes, 64 * 1024 * 1024);
    bytes = ngx_cache_purge_control_size(bank_bytes, NGX_PURGE_RUNTIME_READERS);
    allocation = ngx_slab_alloc(pool, bytes + 63);
    if (allocation == NULL) {
        return NGX_ERROR;
    }
    shared->memory = ngx_align_ptr(allocation, 64); shared->bytes = bytes;
    if (ngx_cache_purge_control_init(shared->memory, bytes, bank_bytes,
                                     NGX_PURGE_RUNTIME_READERS, &error) != 0)
    {
        ngx_log_error(NGX_LOG_EMERG, zone->shm.log, 0, "cache purge shared init: %s", error.message);
        return NGX_ERROR;
    }
    shared->version = NGX_PURGE_RUNTIME_VERSION;
    rt->shared = shared; pool->data = shared;
    return NGX_OK;
}

static ngx_uint_t
ngx_http_cache_purge_contains(ngx_str_t *a, ngx_str_t *b)
{
    size_t len = a->len;
    while (len > 1 && a->data[len - 1] == '/') { len--; }
    return b->len >= len && ngx_memcmp(a->data, b->data, len) == 0
           && (b->len == len || b->data[len] == '/' || len == 1);
}

ngx_int_t
ngx_http_cache_purge_runtime_post(ngx_conf_t *cf)
{
    ngx_http_cache_purge_main_conf_t *main, *old_main = NULL;
    ngx_http_cache_purge_runtime_t *rt, *old;
    ngx_path_t **paths;
    ngx_str_t *root;
    ngx_uint_t i;

    main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_module);
    rt = main->runtime;
    ngx_conf_init_msec_value(rt->propagation, 300000);
    if (rt->propagation > UINT32_MAX) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "cache_purge_propagation_timeout exceeds supported milliseconds");
        return NGX_ERROR;
    }
    rt->options.propagation_ms = (uint32_t) rt->propagation;
    if ((rt->path == NULL) != (rt->zone == NULL)) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "cache_purge_store and cache_purge_state_zone must be configured together");
        return NGX_ERROR;
    }
    if (cf->cycle->old_cycle && cf->cycle->old_cycle->conf_ctx
        && cf->cycle->old_cycle->conf_ctx[ngx_http_module.index])
    {
        for (i = 0; cf->cycle->old_cycle->modules[i]; i++) {
            if (cf->cycle->old_cycle->modules[i] == &ngx_http_cache_purge_module) {
                old_main = ngx_http_cycle_get_module_main_conf(cf->cycle->old_cycle, ngx_http_cache_purge_module);
                break;
            }
        }
    }
    old = old_main ? old_main->runtime : NULL;
    if (old && old->path
        && (!rt->path || old->path->name.len != rt->path->name.len
            || ngx_memcmp(old->path->name.data, rt->path->name.data, old->path->name.len)
            || old->zone->shm.size != rt->zone->shm.size
            || old->zone->shm.name.len != rt->zone->shm.name.len
            || ngx_memcmp(old->zone->shm.name.data, rt->zone->shm.name.data, old->zone->shm.name.len)))
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "changing/removing cache purge store or state zone requires a full restart");
        return NGX_ERROR;
    }
    if (rt->path == NULL) {
        return NGX_OK;
    }
    paths = cf->cycle->paths.elts;
    for (i = 0; i < cf->cycle->paths.nelts; i++) {
        if (paths[i] == rt->path || paths[i]->loader == NULL) { continue; }
        root = ngx_array_push(&rt->roots);
        if (root == NULL) { return NGX_ERROR; }
        *root = paths[i]->name;
        if (ngx_http_cache_purge_contains(&paths[i]->name, &rt->path->name)
            || ngx_http_cache_purge_contains(&rt->path->name, &paths[i]->name))
        {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "cache purge database path must be outside cache path \"%V\"", &paths[i]->name);
            return NGX_ERROR;
        }
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_runtime_paths(ngx_http_cache_purge_runtime_t *rt, ngx_log_t *log)
{
    u_char database[NGX_MAX_PATH], cache[NGX_MAX_PATH];
    ngx_str_t db, current, *roots;
    ngx_uint_t i;

    if (ngx_realpath(rt->path->name.data, database) == NULL) {
        ngx_log_error(NGX_LOG_EMERG, log, ngx_errno, "cannot resolve cache purge store path");
        return NGX_ERROR;
    }
    db.data = database; db.len = ngx_strlen(database);
    roots = rt->roots.elts;
    for (i = 0; i < rt->roots.nelts; i++) {
        if (ngx_realpath(roots[i].data, cache) == NULL) {
            ngx_log_error(NGX_LOG_EMERG, log, ngx_errno, "cannot resolve cache path \"%V\"", &roots[i]);
            return NGX_ERROR;
        }
        current.data = cache; current.len = ngx_strlen(cache);
        if (ngx_http_cache_purge_contains(&db, &current)
            || ngx_http_cache_purge_contains(&current, &db))
        {
            ngx_log_error(NGX_LOG_EMERG, log, 0, "resolved cache purge database must be outside cache path \"%V\"", &current);
            return NGX_ERROR;
        }
    }
    return NGX_OK;
}

ngx_int_t
ngx_http_cache_purge_runtime_module_init(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t *main;
    ngx_core_conf_t *core;
    ngx_http_cache_purge_runtime_t *rt;
    ngx_file_info_t info;
    ngx_err_t err;
    ngx_uint_t access;

    main = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cache_purge_module);
    if (main && main->runtime->path) {
        core = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
        if (!core->master) {
            ngx_log_error(NGX_LOG_EMERG, cycle->log, 0, "persistent cache purge requires master_process on");
            return NGX_ERROR;
        }
#if (HAVE_PRIVILEGED_PROCESS_PATCH)
        /* Enable the existing core process; no DB or threads before fork. */
        core->privileged_agent = 1;
        main->runtime->options.client_uid = (uint32_t)
            (geteuid() == 0 ? core->user : geteuid());
#endif
        rt = main->runtime;
        access = rt->options.client_uid == (uint32_t) geteuid() ? 0700 : 0711;
        /* DB storage is not a native cache path: ngx_create_paths() would
         * chown it to the HTTP worker UID, including on every reload. */
        err = ngx_create_full_path(rt->path->name.data, access);
        if (err != 0) {
            ngx_log_error(NGX_LOG_EMERG, cycle->log, err, "cannot create cache purge store parents");
            return NGX_ERROR;
        }
        if (ngx_create_dir(rt->path->name.data, access) == NGX_FILE_ERROR
            && ngx_errno != NGX_EEXIST)
        {
            ngx_log_error(NGX_LOG_EMERG, cycle->log, ngx_errno, "cannot create cache purge store directory");
            return NGX_ERROR;
        }
        if (ngx_http_cache_purge_runtime_paths(rt, cycle->log) != NGX_OK) {
            return NGX_ERROR;
        }
        if (ngx_link_info(rt->path->name.data, &info) == NGX_FILE_ERROR
            || !ngx_is_dir(&info) || info.st_uid != geteuid()
            || (info.st_mode & 0022) != 0
            || (access == 0711 && (info.st_mode & 0001) == 0))
        {
            ngx_log_error(NGX_LOG_EMERG, cycle->log, 0,
                          "cache purge store requires a real directory owned by the agent UID, "
                          "without group/other write access and with worker traversal access");
            return NGX_ERROR;
        }
        return NGX_OK;
    }
    return NGX_OK;
}

static ngx_msec_t
ngx_http_cache_purge_runtime_tick(void *data)
{
    ngx_http_cache_purge_runtime_t *rt = data;
    ngx_cache_purge_store_error_t error;
    ngx_cache_purge_component_status_t status;

    if (!rt->started) {
        rt->started = 1;
        if (ngx_http_cache_purge_runtime_paths(rt, ngx_cycle->log) != NGX_OK) {
            rt->failed = 1;
            return 1000;
        }
        rt->options.control_memory = rt->shared->memory; rt->options.control_bytes = rt->shared->bytes;
        if (ngx_cache_purge_component_start((char *) rt->path->name.data, &rt->options, &rt->component, &error) != 0) {
            rt->failed = 1;
            ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, 0, "cache purge component start: %s", error.message);
        }
    }
    if (rt->component && !rt->failed
        && ngx_cache_purge_component_status(rt->component, &status, &error) == 0
        && status.state == NGX_PURGE_COMPONENT_FAILED)
    {
        rt->failed = 1;
        ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, 0, "cache purge component failed (code %ui); control gate remains closed", (ngx_uint_t) status.error_code);
    }
    return 1000;
}

ngx_int_t
ngx_http_cache_purge_runtime_process_init(ngx_cycle_t *cycle)
{
#if (HAVE_PRIVILEGED_PROCESS_PATCH)
    ngx_http_cache_purge_main_conf_t *main;
    ngx_http_cache_purge_runtime_t *rt;

    if (!ngx_is_privileged_agent) { return NGX_OK; }
    main = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cache_purge_module);
    rt = main ? main->runtime : NULL;
    if (rt == NULL || rt->path == NULL) { return NGX_OK; }

    /* Later module init_process failures may exit without exit_process.
     * Delay thread creation until all initialization callbacks succeeded. */
    rt->event.handler = ngx_http_cache_purge_runtime_agent;
    rt->event.data = rt;
    rt->event.log = cycle->log;
    rt->event.cancelable = 1;
    ngx_add_timer(&rt->event, 0);
#endif
    return NGX_OK;
}

#if (HAVE_PRIVILEGED_PROCESS_PATCH)
static void
ngx_http_cache_purge_runtime_agent(ngx_event_t *ev)
{
    ngx_http_cache_purge_runtime_t *rt = ev->data;

    if (ngx_terminate || ngx_quit || ngx_exiting) { return; }
    ngx_add_timer(ev, ngx_http_cache_purge_runtime_tick(rt));
}
#endif

static void
ngx_http_cache_purge_runtime_exit(void *data)
{
    ngx_http_cache_purge_runtime_t *rt = data;
    ngx_cache_purge_store_error_t error;

    if (rt->component && ngx_cache_purge_component_stop(&rt->component, &error) != 0) {
        ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, 0, "cache purge component close: %s", error.message);
    }
}

int
ngx_http_cache_purge_runtime_reader(ngx_http_request_t *r,
    ngx_cache_purge_control_reader_t **reader, ngx_cache_purge_store_error_t *error)
{
    ngx_http_cache_purge_main_conf_t *main;
    ngx_http_cache_purge_runtime_t *rt;
    int rc;

    if (error) { ngx_memzero(error, sizeof(*error)); }
    if (r == NULL || reader == NULL) {
        if (error) { error->code = NGX_PURGE_STORE_INVALID; }
        return NGX_PURGE_STORE_INVALID;
    }
    *reader = NULL;
    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    rt = main ? main->runtime : NULL;
    if (rt == NULL || rt->shared == NULL) {
        if (error) { error->code = NGX_PURGE_STORE_NOT_FOUND; }
        return NGX_PURGE_STORE_NOT_FOUND;
    }
    if (rt->reader == NULL) {
        rc = ngx_cache_purge_control_reader_open(rt->shared->memory, rt->shared->bytes, &rt->reader, error);
        if (rc) { return rc; }
    }
    *reader = rt->reader;
    return NGX_PURGE_STORE_OK;
}

void
ngx_http_cache_purge_runtime_worker_exit(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t *main;
    ngx_cache_purge_store_error_t error;
    int rc;

#if (HAVE_PRIVILEGED_PROCESS_PATCH)
    if (ngx_is_privileged_agent) {
        main = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cache_purge_module);
        if (main && main->runtime) {
            if (main->runtime->event.timer_set) {
                ngx_del_timer(&main->runtime->event);
            }
            ngx_http_cache_purge_runtime_exit(main->runtime);
        }
        return;
    }
#endif

    if (ngx_process != NGX_PROCESS_WORKER) { return; }
    main = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cache_purge_module);
    if (main && main->runtime->reader) {
        rc = ngx_cache_purge_control_reader_close(&main->runtime->reader, &error);
        if (rc && rc != NGX_PURGE_STORE_BUSY) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, 0, "cache purge reader close: %s", error.message);
        }
    }
}
