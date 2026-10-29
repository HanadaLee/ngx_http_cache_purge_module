/* Copyright (C) Hanada */
#include "ngx_http_cache_purge.h"

#if (NGX_EXPR)
#include <ngx_http_expr_module.h>
#endif
#include "ngx_http_cache_purge_internal.h"
#include <sys/un.h>
#include <openssl/sha.h>


#define NGX_HTTP_CACHE_PURGE_RUNTIME_READERS  1024
#define NGX_HTTP_CACHE_PURGE_RUNTIME_VERSION 2

typedef struct {
    ngx_uint_t  version;
    void       *memory;
    size_t      bytes;
    ngx_str_t   path;
} ngx_http_cache_purge_shared_t;

typedef struct {
    ngx_http_file_cache_t  *cache;
    ngx_str_t               path;
} ngx_http_cache_purge_native_t;

struct ngx_http_cache_purge_runtime_s {
    ngx_path_t                               *path;
    ngx_shm_zone_t                           *zone;
    ngx_http_cache_purge_shared_t            *shared;
    ngx_http_cache_purge_component_options_t  options;
    ngx_http_cache_purge_component_t         *component;
    ngx_http_cache_purge_control_reader_t    *reader;
    ngx_http_cache_purge_client_t            *client;
    uid_t                                     owner_uid;
    ngx_msec_t                                propagation;
    ngx_uint_t                                started;
    ngx_uint_t                                failed;
    ngx_array_t                               roots;
    ngx_array_t                               file_roots;
    ngx_array_t                               native;
    ngx_connection_t                         *notification;
    ngx_event_t                               owner_event;
};

static ngx_msec_t ngx_http_cache_purge_runtime_tick(void *data);
static void       ngx_http_cache_purge_runtime_exit(void *data);
static ngx_int_t  ngx_http_cache_purge_runtime_shared(ngx_shm_zone_t *zone,
    void *old);
static ngx_int_t
ngx_http_cache_purge_runtime_paths(ngx_http_cache_purge_runtime_t *rt,
    ngx_log_t *log);
static void ngx_http_cache_purge_runtime_notify(ngx_event_t *event);
static void ngx_http_cache_purge_runtime_unlisten(
    ngx_http_cache_purge_runtime_t *rt);

ngx_flag_t
ngx_http_cache_purge_runtime_configured(ngx_conf_t *cf)
{
    ngx_http_cache_purge_main_conf_t *conf;
    conf = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_module);
    return conf->runtime && conf->runtime->path && conf->runtime->zone;
}

ngx_http_cache_purge_runtime_t *
ngx_http_cache_purge_runtime_create(ngx_conf_t *cf)
{
    ngx_http_cache_purge_runtime_t       *rt;
    ngx_http_cache_purge_native_layout_t *layout;

    rt = ngx_pcalloc(cf->pool, sizeof(*rt));
    if (rt == NULL) {
        return NULL;
    }
    ngx_http_cache_purge_component_options(&rt->options);
    layout = &rt->options.native_layout;
    layout->size = sizeof(ngx_http_file_cache_header_t);
    layout->version = NGX_HTTP_CACHE_VERSION;
    layout->version_offset = offsetof(ngx_http_file_cache_header_t, version);
    layout->version_size =
        sizeof(((ngx_http_file_cache_header_t *) 0)->version);
    layout->header_start_offset =
        offsetof(ngx_http_file_cache_header_t, header_start);
    layout->crc32_offset = offsetof(ngx_http_file_cache_header_t, crc32);
    layout->vary_len_offset = offsetof(ngx_http_file_cache_header_t, vary_len);
    layout->vary_offset = offsetof(ngx_http_file_cache_header_t, vary);
    layout->vary_capacity = sizeof(((ngx_http_file_cache_header_t *) 0)->vary);
    layout->variant_offset = offsetof(ngx_http_file_cache_header_t, variant);
    if (rt->options.abi != NGX_HTTP_CACHE_PURGE_COMPONENT_ABI
        || ngx_http_cache_purge_component_abi_13()
               != NGX_HTTP_CACHE_PURGE_COMPONENT_ABI
        || ngx_array_init(&rt->roots, cf->pool, 2, sizeof(ngx_str_t)) != NGX_OK
        || ngx_array_init(&rt->file_roots, cf->pool, 8,
                          sizeof(ngx_http_cache_purge_root_t))
               != NGX_OK)
    {
        return NULL;
    }
    if (ngx_array_init(&rt->native, cf->pool, 8,
                      sizeof(ngx_http_cache_purge_native_t)) != NGX_OK)
    {
        return NULL;
    }
    rt->propagation = NGX_CONF_UNSET_MSEC;
    return rt;
}

char *
ngx_http_cache_purge_store_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_cache_purge_main_conf_t  *main = conf;
    ngx_http_cache_purge_runtime_t    *rt = main->runtime;
    ngx_str_t                         *value, size;
    ngx_uint_t                         i, cache_seen = 0, buffer_seen = 0;
    ssize_t                            parsed;

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
    if (rt->path->name.len + sizeof("/component.sock")
            > sizeof(((struct sockaddr_un *) 0)->sun_path)
        || rt->path->name.len == 1)
    {
        ngx_conf_log_error(
            NGX_LOG_EMERG, cf, 0,
            "cache_purge_store path \"%V\" cannot hold its Unix socket",
            &rt->path->name);
        return NGX_CONF_ERROR;
    }
    for (i = 0; i < rt->path->name.len; i++) {
        if (rt->path->name.data[i] == '\0'
            || (rt->path->name.data[i] == '/'
                && rt->path->name.data[i + 1] == '/')
            || (rt->path->name.data[i] == '/'
                && rt->path->name.data[i + 1] == '.'
                && (rt->path->name.data[i + 2] == '\0'
                    || rt->path->name.data[i + 2] == '/'
                    || (rt->path->name.data[i + 2] == '.'
                        && (rt->path->name.data[i + 3] == '\0'
                            || rt->path->name.data[i + 3] == '/')))))
        {
            ngx_conf_log_error(
                NGX_LOG_EMERG, cf, 0,
                "cache_purge_store requires a normalized absolute path");
            return NGX_CONF_ERROR;
        }
    }
    for (i = 2; i < cf->args->nelts; i++) {
        if (value[i].len > 6 && ngx_strncmp(value[i].data, "cache=", 6) == 0
            && !cache_seen)
        {
            size.data = value[i].data + 6;
            size.len = value[i].len - 6;
            parsed = ngx_parse_size(&size);
            if (parsed <= 0) {
                goto invalid;
            }
            rt->options.store.cache_bytes = (size_t) parsed;
            cache_seen = 1;
        } else if (value[i].len > 13
                   && ngx_strncmp(value[i].data, "write_buffer=", 13) == 0
                   && !buffer_seen)
        {
            size.data = value[i].data + 13;
            size.len = value[i].len - 13;
            parsed = ngx_parse_size(&size);
            if (parsed <= 0) {
                goto invalid;
            }
            rt->options.store.write_buffer_bytes = (size_t) parsed;
            buffer_seen = 1;
        } else {
            goto invalid;
        }
    }
    if (rt->options.store.cache_bytes < 4 * 1024 * 1024
        || rt->options.store.write_buffer_bytes < 1024 * 1024
        || rt->options.store.write_buffer_bytes
               > rt->options.store.cache_bytes / 2)
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
                       "expected cache=SIZE or write_buffer=SIZE once each",
                       &value[i]);
    return NGX_CONF_ERROR;
}

char *
ngx_http_cache_purge_state_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_cache_purge_main_conf_t  *main = conf;
    ngx_http_cache_purge_runtime_t    *rt = main->runtime;
    ngx_str_t                         *value, name;
    ssize_t                            size;

    if (rt->zone != NULL) {
        return "is duplicate";
    }
    value = cf->args->elts;
    size = ngx_parse_size(&value[2]);
    if (value[1].len == 0 || value[1].len > 128 || size < 1024 * 1024
        || size > 128 * 1024 * 1024)
    {
        ngx_conf_log_error(
            NGX_LOG_EMERG, cf, 0,
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
    rt->zone =
        ngx_shared_memory_add(cf, &name, size, &ngx_http_cache_purge_module);
    if (rt->zone == NULL || rt->zone->data != NULL) {
        return NGX_CONF_ERROR;
    }
    rt->zone->data = rt;
    rt->zone->init = ngx_http_cache_purge_runtime_shared;
    return NGX_CONF_OK;
}

char *
ngx_http_cache_purge_propagation_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_http_cache_purge_main_conf_t  *main = conf;
    ngx_command_t                      local = *cmd;
    local.offset = offsetof(ngx_http_cache_purge_runtime_t, propagation);
    return ngx_conf_set_msec_slot(cf, &local, main->runtime);
}

static ngx_int_t
ngx_http_cache_purge_runtime_shared(ngx_shm_zone_t *zone, void *old)
{
    ngx_http_cache_purge_runtime_t *rt = zone->data, *previous = old;
    ngx_slab_pool_t                *pool = (ngx_slab_pool_t *) zone->shm.addr;
    ngx_http_cache_purge_shared_t  *shared;
    ngx_http_cache_rocksdb_error_t  error;
    size_t                          available, overhead, bank_bytes, bytes;
    void                           *allocation;

    if (previous != NULL || zone->shm.exists) {
        shared = previous ? previous->shared : pool->data;
        if (shared == NULL
            || shared->version != NGX_HTTP_CACHE_PURGE_RUNTIME_VERSION
            || shared->path.len != rt->path->name.len
            || ngx_memcmp(shared->path.data, rt->path->name.data,
                          shared->path.len))
        {
            ngx_log_error(NGX_LOG_EMERG, zone->shm.log, 0,
                          "cache purge shared state/path mismatch; full "
                          "restart required");
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
    overhead = ngx_http_cache_purge_control_size(
                   4096, NGX_HTTP_CACHE_PURGE_RUNTIME_READERS)
               - 8192;
    available = pool->pfree * ngx_pagesize;
    if (available < overhead + 8192 + 63) {
        return NGX_ERROR;
    }
    bank_bytes = ((available - overhead - 63) / 2) & ~(size_t) 63;
    bank_bytes = ngx_min(bank_bytes, 64 * 1024 * 1024);
    bytes = ngx_http_cache_purge_control_size(
        bank_bytes, NGX_HTTP_CACHE_PURGE_RUNTIME_READERS);
    allocation = ngx_slab_alloc(pool, bytes + 63);
    if (allocation == NULL) {
        return NGX_ERROR;
    }
    shared->memory = ngx_align_ptr(allocation, 64);
    shared->bytes = bytes;
    if (ngx_http_cache_purge_control_init(shared->memory, bytes, bank_bytes,
                                          NGX_HTTP_CACHE_PURGE_RUNTIME_READERS,
                                          &error)
        != 0)
    {
        ngx_log_error(NGX_LOG_EMERG, zone->shm.log, 0,
                      "cache purge shared init: %s", error.message);
        return NGX_ERROR;
    }
    shared->version = NGX_HTTP_CACHE_PURGE_RUNTIME_VERSION;
    rt->shared = shared;
    pool->data = shared;
    return NGX_OK;
}

static ngx_uint_t
ngx_http_cache_purge_contains(ngx_str_t *a, ngx_str_t *b)
{
    size_t len = a->len;
    while (len > 1 && a->data[len - 1] == '/') {
        len--;
    }
    return b->len >= len && ngx_memcmp(a->data, b->data, len) == 0
           && (b->len == len || b->data[len] == '/' || len == 1);
}

ngx_int_t
ngx_http_cache_purge_runtime_post(ngx_conf_t *cf)
{
    ngx_http_cache_purge_main_conf_t  *main, *old_main = NULL;
    ngx_http_cache_purge_runtime_t    *rt, *old;
    ngx_path_t                       **paths;
    ngx_str_t                         *root;
    ngx_uint_t                         i, j;
    ngx_http_cache_purge_root_t       *registered;
    ngx_http_file_cache_t            **caches;
    ngx_http_cache_purge_native_t     *native;
    ngx_module_t                      *module;
    ngx_command_t                     *command;
    ngx_array_t                       *configured;
    void                              *main_conf;

    main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_module);
    rt = main->runtime;
    ngx_conf_init_msec_value(rt->propagation, 120000);
    if (rt->propagation > UINT32_MAX) {
        ngx_conf_log_error(
            NGX_LOG_EMERG, cf, 0,
            "cache_purge_propagation_timeout exceeds supported milliseconds");
        return NGX_ERROR;
    }
    rt->options.propagation_ms = (uint32_t) rt->propagation;
    if ((rt->path == NULL) != (rt->zone == NULL)) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "cache_purge_store and cache_purge_state_zone must "
                           "be configured together");
        return NGX_ERROR;
    }
    if (cf->cycle->old_cycle && cf->cycle->old_cycle->conf_ctx
        && cf->cycle->old_cycle->conf_ctx[ngx_http_module.index])
    {
        for (i = 0; cf->cycle->old_cycle->modules[i]; i++) {
            if (cf->cycle->old_cycle->modules[i]
                == &ngx_http_cache_purge_module)
            {
                old_main = ngx_http_cycle_get_module_main_conf(
                    cf->cycle->old_cycle, ngx_http_cache_purge_module);
                break;
            }
        }
    }
    old = old_main ? old_main->runtime : NULL;
    if (old && old->path
        && (!rt->path || old->path->name.len != rt->path->name.len
            || ngx_memcmp(old->path->name.data, rt->path->name.data,
                          old->path->name.len)
            || old->zone->shm.size != rt->zone->shm.size
            || old->zone->shm.name.len != rt->zone->shm.name.len
            || ngx_memcmp(old->zone->shm.name.data, rt->zone->shm.name.data,
                          old->zone->shm.name.len)))
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "changing/removing cache purge store or state zone "
                           "requires a full restart");
        return NGX_ERROR;
    }
    if (rt->path == NULL) {
        return NGX_OK;
    }
    /* The native cache_path setter publishes its main-conf array offset.
     * Reuse that metadata rather than private protocol configuration layouts
     * or undeclared cache-loader symbols. */
    for (i = 0; cf->cycle->modules[i]; i++) {
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
            main_conf =
                ((ngx_http_conf_ctx_t *) cf->ctx)->main_conf[module->ctx_index];
            if (main_conf == NULL) {
                continue;
            }
            configured =
                (ngx_array_t *) ((u_char *) main_conf + command->offset);
            if (configured->nelts == 0) {
                continue;
            }
            if (configured->size != sizeof(ngx_http_file_cache_t *)) {
                return NGX_ERROR;
            }
            caches = configured->elts;
            for (j = 0; j < configured->nelts; j++) {
                registered = ngx_array_push(&rt->file_roots);
                if (registered == NULL) {
                    return NGX_ERROR;
                }
                ngx_memzero(registered, sizeof(*registered));
                registered->path.data = caches[j]->path->name.data;
                registered->path.len = caches[j]->path->name.len;
                registered->kind = NGX_HTTP_CACHE_PURGE_ROOT_NATIVE;
                if (ngx_strcmp(command->name.data, "proxy_cache_path") == 0) {
                    registered->kind = NGX_HTTP_CACHE_PURGE_ROOT_PROXY;
                }
                registered->zone.data = caches[j]->shm_zone->shm.name.data;
                registered->zone.len = caches[j]->shm_zone->shm.name.len;
                native = ngx_array_push(&rt->native);
                if (native == NULL) {
                    return NGX_ERROR;
                }
                native->cache = caches[j];
                native->path.data = ngx_pnalloc(cf->pool, NGX_MAX_PATH);
                if (native->path.data == NULL) {
                    return NGX_ERROR;
                }
                native->path.len = 0;
            }
        }
    }
    paths = cf->cycle->paths.elts;
    for (i = 0; i < cf->cycle->paths.nelts; i++) {
        if (paths[i] != rt->path && paths[i]->loader == NULL
            && paths[i]->manager == NULL)
        {
            registered = ngx_array_push(&rt->file_roots);
            if (registered == NULL) {
                return NGX_ERROR;
            }
            ngx_memzero(registered, sizeof(*registered));
            registered->path.data = paths[i]->name.data;
            registered->path.len = paths[i]->name.len;
            registered->kind = NGX_HTTP_CACHE_PURGE_ROOT_TEMP;
        }
        if (paths[i] == rt->path || paths[i]->loader == NULL) {
            continue;
        }
        root = ngx_array_push(&rt->roots);
        if (root == NULL) {
            return NGX_ERROR;
        }
        *root = paths[i]->name;
        if (ngx_http_cache_purge_contains(&paths[i]->name, &rt->path->name)
            || ngx_http_cache_purge_contains(&rt->path->name, &paths[i]->name))
        {
            ngx_conf_log_error(
                NGX_LOG_EMERG, cf, 0,
                "cache purge database path must be outside cache path \"%V\"",
                &paths[i]->name);
            return NGX_ERROR;
        }
    }
    rt->options.roots = rt->file_roots.elts;
    rt->options.root_count = rt->file_roots.nelts;
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_runtime_paths(ngx_http_cache_purge_runtime_t *rt,
    ngx_log_t *log)
{
    ngx_http_cache_purge_native_t  *native;
    u_char                          database[NGX_MAX_PATH];
    u_char                          cache[NGX_MAX_PATH];
    ngx_str_t                       db, current, *roots;
    ngx_uint_t                      i;

    if (ngx_realpath(rt->path->name.data, database) == NULL) {
        ngx_log_error(NGX_LOG_EMERG, log, ngx_errno,
                      "cannot resolve cache purge store path");
        return NGX_ERROR;
    }
    db.data = database;
    db.len = ngx_strlen(database);
    roots = rt->roots.elts;
    for (i = 0; i < rt->roots.nelts; i++) {
        if (ngx_realpath(roots[i].data, cache) == NULL) {
            ngx_log_error(NGX_LOG_EMERG, log, ngx_errno,
                          "cannot resolve cache path \"%V\"", &roots[i]);
            return NGX_ERROR;
        }
        current.data = cache;
        current.len = ngx_strlen(cache);
        if (ngx_http_cache_purge_contains(&db, &current)
            || ngx_http_cache_purge_contains(&current, &db))
        {
            ngx_log_error(NGX_LOG_EMERG, log, 0,
                          "resolved cache purge database must be outside cache "
                          "path \"%V\"",
                          &current);
            return NGX_ERROR;
        }
    }
    native = rt->native.elts;
    for (i = 0; i < rt->native.nelts; i++) {
        if (ngx_realpath(native[i].cache->path->name.data,
                         native[i].path.data) == NULL)
        {
            return NGX_ERROR;
        }
        native[i].path.len = ngx_strlen(native[i].path.data);
    }
    return NGX_OK;
}

ngx_int_t
ngx_http_cache_purge_runtime_module_init(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_core_conf_t                   *core;
    ngx_http_cache_purge_runtime_t    *rt;
    ngx_file_info_t                    info;
    ngx_err_t                          err;
    ngx_fd_t                           fd;
    gid_t                              group;

    main =
        ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cache_purge_module);
    if (main == NULL || main->runtime->path == NULL) {
        return NGX_OK;
    }
    if (ngx_http_cache_purge_capture_ready(cycle) != NGX_OK) {
        return NGX_ERROR;
    }
    core = (ngx_core_conf_t *) ngx_get_conf(cycle->conf_ctx, ngx_core_module);
    if (!core->master) {
        ngx_log_error(NGX_LOG_EMERG, cycle->log, 0,
                      "persistent cache purge requires master_process on");
        return NGX_ERROR;
    }
    rt = main->runtime;
    rt->owner_uid = geteuid() == 0 ? core->user : geteuid();
    group = geteuid() == 0 ? core->group : getegid();
    rt->options.client_uid = (uint32_t) rt->owner_uid;

    /* Create only new storage for the worker UID. Existing storage
     * is never recursively chowned, including on reload or an agent migration.
     * */
    err = ngx_create_full_path(rt->path->name.data,
                               rt->owner_uid == geteuid() ? 0700 : 0711);
    if (err != 0) {
        ngx_log_error(NGX_LOG_EMERG, cycle->log, err,
                      "cannot create cache purge store parents");
        return NGX_ERROR;
    }
    if (ngx_create_dir(rt->path->name.data, 0700) != NGX_FILE_ERROR) {
        fd = open((char *) rt->path->name.data,
                  O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (fd == -1) {
            return NGX_ERROR;
        }
        if (fchown(fd, rt->owner_uid, group) == -1) {
            err = ngx_errno;
            (void) close(fd);
            ngx_log_error(NGX_LOG_EMERG, cycle->log, err,
                          "cannot assign new cache purge store to worker UID");
            return NGX_ERROR;
        }
        (void) close(fd);
    } else if (ngx_errno != NGX_EEXIST) {
        ngx_log_error(NGX_LOG_EMERG, cycle->log, ngx_errno,
                      "cannot create cache purge store directory");
        return NGX_ERROR;
    }
    if (ngx_http_cache_purge_runtime_paths(rt, cycle->log) != NGX_OK) {
        return NGX_ERROR;
    }
    if (ngx_link_info(rt->path->name.data, &info) == NGX_FILE_ERROR
        || !ngx_is_dir(&info) || info.st_uid != rt->owner_uid
        || (info.st_mode & 0077) != 0)
    {
        ngx_log_error(NGX_LOG_EMERG, cycle->log, 0,
                      "cache purge store requires a private directory owned by "
                      "the worker UID; "
                      "migrate existing agent-owned storage while stopped");
        return NGX_ERROR;
    }
    return NGX_OK;
}

static int
ngx_http_cache_purge_runtime_node(ngx_http_file_cache_t *cache,
    const ngx_http_cache_purge_cleanup_t *cleanup, ngx_log_t *log)
{
    ngx_rbtree_node_t           *node, *sentinel;
    ngx_http_file_cache_node_t  *file;
    ngx_rbtree_key_t             key;
    u_char                       hash[NGX_HTTP_CACHE_KEY_LEN];
    const u_char                *p;
    ngx_uint_t                   i;
    ngx_int_t                    value;
    off_t                        released;
    int                          compare, rc;
#if (NGX_DEBUG)
    off_t                        remaining = 0;
#endif

    if (cleanup->outcome == NGX_HTTP_CACHE_PURGE_REPLACED) {
        return 0;
    }
    if (cleanup->path.len < 32
        || (cleanup->path.len > 32
            && cleanup->path.data[cleanup->path.len - 33] != '/'))
    {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }
    p = cleanup->path.data + cleanup->path.len - 32;
    for (i = 0; i < NGX_HTTP_CACHE_KEY_LEN; i++) {
        value = ngx_hextoi((u_char *) p + 2 * i, 2);
        if (value == NGX_ERROR) {
            return NGX_HTTP_CACHE_ROCKSDB_INVALID;
        }
        hash[i] = (u_char) value;
    }
    ngx_memcpy(&key, hash, sizeof(key));
    if (!ngx_shmtx_trylock(&cache->shpool->mutex)) {
        return NGX_HTTP_CACHE_ROCKSDB_BUSY;
    }
    released = 0;
    rc = 0;
    node = cache->sh->rbtree.root;
    sentinel = cache->sh->rbtree.sentinel;
    while (node != sentinel) {
        if (key != node->key) {
            node = key < node->key ? node->left : node->right;
            continue;
        }
        file = (ngx_http_file_cache_node_t *) node;
        compare = ngx_memcmp(hash + sizeof(key), file->key,
                             NGX_HTTP_CACHE_KEY_LEN - sizeof(key));
        if (compare != 0) {
            node = compare < 0 ? node->left : node->right;
            continue;
        }
        if (!file->exists
            || (file->uniq != 0 && (uint64_t) file->uniq != cleanup->inode))
        {
            break;
        }
        /* The executor holds the path lease. A loader or an existing reader
         * can still finish native accounting, so retry after they drain. */
        if (file->count != 0 || file->deleting || cache->sh->loading) {
            rc = NGX_HTTP_CACHE_ROCKSDB_BUSY;
            break;
        }
        if (file->fs_size < 0 || file->fs_size > cache->sh->size) {
            rc = NGX_HTTP_CACHE_ROCKSDB_INVALID;
            break;
        }
        released = file->fs_size;
        cache->sh->size -= released;
#if (NGX_DEBUG)
        remaining = cache->sh->size;
#endif
        file->fs_size = 0;
        file->exists = 0;
        break;
    }
    ngx_shmtx_unlock(&cache->shpool->mutex);
    if (released != 0) {
        ngx_log_debug3(NGX_LOG_DEBUG_HTTP, log, 0,
            "cache purge native cleanup inode:%uL blocks:%O size:%O",
            cleanup->inode, released, remaining);
    }
    return rc;
}

static void
ngx_http_cache_purge_runtime_notify(ngx_event_t *event)
{
    ngx_connection_t                *connection;
    ngx_http_cache_purge_runtime_t  *rt;
    ngx_http_cache_purge_native_t   *native;
    ngx_http_cache_purge_cleanup_t   cleanup;
    ngx_http_cache_rocksdb_error_t   error;
    ngx_str_t                       *zone;
    ngx_uint_t                       i;
    int                              rc, result;

    connection = event->data;
    rt = connection->data;
    event->ready = 0;
    rc = ngx_http_cache_purge_component_cleanup_next(rt->component,
                                                       &cleanup, &error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
        return;
    }
    if (rc == 0) {
        result = NGX_HTTP_CACHE_ROCKSDB_INVALID;
        native = rt->native.elts;
        for (i = 0; i < rt->native.nelts; i++) {
            zone = &native[i].cache->shm_zone->shm.name;
            if (zone->len == cleanup.zone.len
                && native[i].path.len == cleanup.root.len
                && ngx_memcmp(zone->data, cleanup.zone.data, zone->len) == 0
                && ngx_memcmp(native[i].path.data, cleanup.root.data,
                              cleanup.root.len) == 0)
            {
                result = ngx_http_cache_purge_runtime_node(native[i].cache,
                                                           &cleanup,
                                                           event->log);
                break;
            }
        }
        rc = ngx_http_cache_purge_component_cleanup_complete(rt->component,
                                        cleanup.token, result, &error);
    }
    if (rc != 0) {
        rt->failed = 1;
        ngx_log_error(NGX_LOG_ALERT, event->log, 0,
                      "cache purge native cleanup: %s", error.message);
        ngx_http_cache_purge_runtime_unlisten(rt);
        (void) ngx_http_cache_purge_component_request_stop(rt->component,
                                                           &error);
    }
}

static void
ngx_http_cache_purge_runtime_unlisten(ngx_http_cache_purge_runtime_t *rt)
{
    ngx_connection_t  *connection;

    connection = rt->notification;
    if (connection == NULL) {
        return;
    }
    if (connection->read->active) {
        (void) ngx_del_event(connection->read, NGX_READ_EVENT, NGX_CLOSE_EVENT);
    }
    if (connection->read->posted) {
        ngx_delete_posted_event(connection->read);
    }
    ngx_free_connection(connection);
    connection->fd = (ngx_socket_t) -1;
    rt->notification = NULL;
    /* The component closes its descriptor only after joining the executor. */
}

static ngx_msec_t
ngx_http_cache_purge_runtime_tick(void *data)
{
    ngx_http_cache_purge_runtime_t          *rt = data;
    ngx_http_cache_rocksdb_error_t           error;
    ngx_http_cache_purge_component_status_t  status;
    ngx_connection_t                        *connection;
    int                                      fd;

    if (!rt->started) {
        rt->started = 1;
        if (ngx_http_cache_purge_runtime_paths(rt, ngx_cycle->log) != NGX_OK) {
            rt->failed = 1;
            return 1000;
        }
        rt->options.control_memory = rt->shared->memory;
        rt->options.control_bytes = rt->shared->bytes;
        if (ngx_http_cache_purge_component_start((char *) rt->path->name.data,
                                                 &rt->options, &rt->component,
                                                 &error)
            != 0)
        {
            rt->failed = 1;
            ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, 0,
                          "cache purge component start: %s", error.message);
        }
        if (!rt->failed) {
            fd = ngx_http_cache_purge_component_cleanup_fd(rt->component);
            connection = ngx_get_connection(fd, ngx_cycle->log);
            if (connection == NULL) {
                rt->failed = 1;
                (void) ngx_http_cache_purge_component_request_stop(
                    rt->component, &error);
                return 1000;
            }
            rt->notification = connection;
            connection->data = rt;
            connection->read->log = ngx_cycle->log;
            connection->read->handler = ngx_http_cache_purge_runtime_notify;
            if (ngx_handle_read_event(connection->read, 0) != NGX_OK) {
                ngx_http_cache_purge_runtime_unlisten(rt);
                rt->failed = 1;
                (void) ngx_http_cache_purge_component_request_stop(
                    rt->component, &error);
            }
        }
    }
    if (rt->component && !rt->failed
        && ngx_http_cache_purge_component_status(rt->component, &status, &error)
               == 0
        && status.state == NGX_HTTP_CACHE_PURGE_COMPONENT_FAILED)
    {
        rt->failed = 1;
        ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, 0,
                      "cache purge component failed (code %ui); control gate "
                      "remains closed",
                      (ngx_uint_t) status.error_code);
    }
    return 1000;
}

static void
ngx_http_cache_purge_worker_owner_tick(ngx_event_t *ev)
{
    ngx_http_cache_purge_runtime_t  *rt = ev->data;
    ngx_http_cache_rocksdb_error_t   error;

    if (ngx_terminate || ngx_quit || ngx_exiting) {
        ngx_http_cache_purge_runtime_unlisten(rt);
        /* Let the executor release the DB without blocking an old worker's
         * still-active HTTP connections. Join only in exit_process. */
        if (rt->component
            && ngx_http_cache_purge_component_request_stop(rt->component,
                                                           &error)
                   != 0)
        {
            ngx_log_error(NGX_LOG_ALERT, ev->log, 0,
                          "cache purge owner shutdown: %s", error.message);
        }
        return;
    }
    ngx_add_timer(ev, ngx_http_cache_purge_runtime_tick(rt));
}

ngx_int_t
ngx_http_cache_purge_runtime_process_init(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_http_cache_purge_runtime_t    *rt;
    if (ngx_process != NGX_PROCESS_WORKER || ngx_worker != 0) {
        return NGX_OK;
    }
    main =
        ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cache_purge_module);
    if (main == NULL || main->runtime->path == NULL) {
        return NGX_OK;
    }
    rt = main->runtime;
    rt->owner_event.handler = ngx_http_cache_purge_worker_owner_tick;
    rt->owner_event.data = rt;
    rt->owner_event.log = cycle->log;
    /* Later init_process failures must not leave a running DB thread. */
    rt->owner_event.cancelable = 1;
    ngx_add_timer(&rt->owner_event, 0);
    return NGX_OK;
}

static void
ngx_http_cache_purge_runtime_exit(void *data)
{
    ngx_http_cache_purge_runtime_t  *rt = data;
    ngx_http_cache_rocksdb_error_t   error;

    ngx_http_cache_purge_runtime_unlisten(rt);
    if (rt->component
        && ngx_http_cache_purge_component_stop(&rt->component, &error) != 0)
    {
        ngx_log_error(NGX_LOG_ALERT, ngx_cycle->log, 0,
                      "cache purge component close: %s", error.message);
    }
}

int
ngx_http_cache_purge_runtime_reader(ngx_http_request_t *r,
    ngx_http_cache_purge_control_reader_t **reader,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_http_cache_purge_runtime_t    *rt;
    int                                rc;

    if (error) {
        ngx_memzero(error, sizeof(*error));
    }
    if (r == NULL || reader == NULL) {
        if (error) {
            error->code = NGX_HTTP_CACHE_ROCKSDB_INVALID;
        }
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }
    *reader = NULL;
    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    rt = main ? main->runtime : NULL;
    if (rt == NULL || rt->shared == NULL) {
        if (error) {
            error->code = NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
        }
        return NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
    }
    if (rt->reader == NULL) {
        rc = ngx_http_cache_purge_control_reader_open(
            rt->shared->memory, rt->shared->bytes, &rt->reader, error);
        if (rc) {
            return rc;
        }
    }
    *reader = rt->reader;
    return NGX_HTTP_CACHE_ROCKSDB_OK;
}

void
ngx_http_cache_purge_runtime_worker_exit(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_http_cache_rocksdb_error_t     error;
    int                                rc;


    if (ngx_process != NGX_PROCESS_WORKER) {
        return;
    }
    main =
        ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cache_purge_module);
    if (main && main->runtime) {
        if (main->runtime->owner_event.timer_set) {
            ngx_del_timer(&main->runtime->owner_event);
        }
        ngx_http_cache_purge_runtime_exit(main->runtime);
        ngx_http_cache_purge_client_stop(main->runtime->client);
    }
    if (main && main->runtime->reader) {
        rc = ngx_http_cache_purge_control_reader_close(&main->runtime->reader,
                                                       &error);
        if (rc && rc != NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            ngx_log_error(NGX_LOG_ALERT, cycle->log, 0,
                          "cache purge reader close: %s", error.message);
        }
    }
}

int
ngx_http_cache_purge_runtime_client(ngx_http_request_t *r,
    ngx_http_cache_purge_client_t **client)
{
    ngx_http_cache_purge_main_conf_t      *main;
    ngx_http_cache_purge_runtime_t        *rt;
    ngx_http_cache_purge_client_options_t  options;
    ngx_str_t                              path;
    u_char  socket_path[sizeof(((struct sockaddr_un *) 0)->sun_path)];
    u_char *p;

    if (r == NULL || client == NULL || ngx_process != NGX_PROCESS_WORKER) {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }
    *client = NULL;
    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    rt = main ? main->runtime : NULL;
    if (rt == NULL || rt->path == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
    }
    if (rt->client == NULL) {
        path.len = rt->path->name.len + sizeof("/component.sock") - 1;
        if (path.len >= sizeof(socket_path)) {
            return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
        }
        path.data = socket_path;
        p = ngx_cpymem(path.data, rt->path->name.data, rt->path->name.len);
        ngx_memcpy(p, "/component.sock", sizeof("/component.sock") - 1);
        ngx_http_cache_purge_client_options(&options);
        options.server_uid = rt->owner_uid;
        rt->client = ngx_http_cache_purge_client_create(
            ngx_cycle->pool, ngx_cycle->log, &path, &options);
        if (rt->client == NULL) {
            return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
        }
    }
    *client = rt->client;
    return NGX_HTTP_CACHE_ROCKSDB_OK;
}


/* Copyright (C) Hanada */

#define NGX_HTTP_CACHE_PURGE_CLIENT_IO_BUDGET  65536

struct ngx_http_cache_purge_call_s {
    ngx_queue_t                     queue;
    ngx_http_cache_purge_client_t  *client;
    ngx_event_t                     deadline;
    ngx_http_cache_purge_client_done_pt done;
    void     *data;
    u_char   *packet;
    size_t    length, sent;
    uint64_t  id;
    uint16_t  opcode;
    int       fds[2];
    size_t    fd_count;
    unsigned  completing : 1;
};

struct ngx_http_cache_purge_client_s {
    ngx_http_cache_purge_client_options_t  options;
    ngx_http_cache_purge_client_stats_t    stats;
    ngx_log_t                             *log;
    struct sockaddr_un                     address;
    socklen_t                              address_len;
    ngx_str_t                              name;
    ngx_connection_t                      *connection;
    ngx_queue_t                            queue;
    ngx_http_cache_purge_call_t           *current;
    ngx_event_t                            pump, idle;
    u_char                header[NGX_HTTP_CACHE_PURGE_FRAME_SIZE];

    size_t                                 header_used, received;
    ngx_str_t                              response;
    uint32_t                               result;
    int                                    lease_fd;
    uint64_t                               next_id;
    unsigned                               connecting : 1;
    unsigned                               stopped : 1;
};

static void ngx_http_cache_purge_client_pump(ngx_event_t *ev);
static void ngx_http_cache_purge_client_read(ngx_event_t *ev);
static void ngx_http_cache_purge_client_write(ngx_event_t *ev);
static void ngx_http_cache_purge_client_deadline(ngx_event_t *ev);
static void ngx_http_cache_purge_client_idle(ngx_event_t *ev);
static void
ngx_http_cache_purge_client_close(ngx_http_cache_purge_client_t *client);
static void
ngx_http_cache_purge_client_finish(ngx_http_cache_purge_call_t *call,
    ngx_http_cache_purge_client_result_e result, ngx_uint_t notify);

static void
ngx_http_cache_purge_client_put(u_char *p, uint64_t value, ngx_uint_t width)
{
    while (width) {
        p[--width] = (u_char) value;
        value >>= 8;
    }
}

static uint64_t
ngx_http_cache_purge_client_get(const u_char *p, ngx_uint_t width)
{
    uint64_t value = 0;
    while (width--) {
        value = (value << 8) | *p++;
    }
    return value;
}

static void
ngx_http_cache_purge_client_peak(ngx_http_cache_purge_client_t *client)
{
    size_t bytes = client->stats.request_bytes + client->stats.response_bytes;
    if (bytes > client->stats.peak_bytes) {
        client->stats.peak_bytes = bytes;
    }
}

void
ngx_http_cache_purge_client_options(
    ngx_http_cache_purge_client_options_t *options)
{
    ngx_memzero(options, sizeof(*options));
    options->max_pending = 64;
    options->request_bytes = 512 * 1024 + NGX_HTTP_CACHE_PURGE_FRAME_SIZE;
    options->max_payload = 512 * 1024;
    options->idle_timeout = 1000;
    options->server_uid = geteuid();
}

ngx_http_cache_purge_client_t *
ngx_http_cache_purge_client_create(ngx_pool_t *pool, ngx_log_t *log,
    const ngx_str_t *path, const ngx_http_cache_purge_client_options_t *options)
{
    ngx_http_cache_purge_client_t *client;

    if (pool == NULL || log == NULL || path == NULL || path->data == NULL
        || path->len == 0 || path->data[0] != '/'
        || path->len >= sizeof(client->address.sun_path)
        || ngx_strlchr(path->data, path->data + path->len, '\0') != NULL
        || options == NULL || options->max_pending == 0
        || options->max_pending > 1024 || options->max_payload == 0
        || options->max_payload > 1024 * 1024
        || options->request_bytes
               < options->max_payload + NGX_HTTP_CACHE_PURGE_FRAME_SIZE
        || options->request_bytes > 16 * 1024 * 1024
        || options->idle_timeout == 0 || options->idle_timeout > 30000)
    {
        return NULL;
    }
    client = ngx_pcalloc(pool, sizeof(*client));
    if (client == NULL) {
        return NULL;
    }
    client->options = *options;
    client->lease_fd = -1;
    client->log = log;
    client->address.sun_family = AF_UNIX;
    ngx_memcpy(client->address.sun_path, path->data, path->len);
    client->address_len =
        (socklen_t) (offsetof(struct sockaddr_un, sun_path) + path->len + 1);
    client->name.data = (u_char *) client->address.sun_path;
    client->name.len = path->len;
    ngx_queue_init(&client->queue);
    client->pump.data = client;
    client->pump.handler = ngx_http_cache_purge_client_pump;
    client->pump.log = log;
    client->idle.data = client;
    client->idle.handler = ngx_http_cache_purge_client_idle;
    client->idle.log = log;
    client->idle.cancelable = 1;
    return client;
}

static ngx_int_t
ngx_http_cache_purge_client_submit_internal(
    ngx_http_cache_purge_client_t *client, uint16_t opcode,
    const ngx_str_t *payload, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **out, const int *fds, size_t fd_count)
{
    ngx_http_cache_purge_call_t  *call;
    size_t                        length, i;

    if (out != NULL) {
        *out = NULL;
    }
    if (client == NULL || client->stopped || out == NULL || done == NULL
        || opcode == 0 || payload == NULL
        || (payload->len && payload->data == NULL) || timeout == 0
        || timeout > 300000
        || fd_count
               != (opcode == NGX_HTTP_CACHE_PURGE_OP_VALIDATE_FILE ? 2U :
                   opcode == NGX_HTTP_CACHE_PURGE_OP_PREPARE_FILE ? 2U :
                   opcode == NGX_HTTP_CACHE_PURGE_OP_FINISH_FILE  ? 1U :
                   opcode == NGX_HTTP_CACHE_PURGE_OP_INSPECT_FILE ? 1U :
                   opcode == NGX_HTTP_CACHE_PURGE_OP_ADMIT_FILE   ? 1U :
                   opcode == NGX_HTTP_CACHE_PURGE_OP_RETIRE_SELECTED ? 1U :
                                                                    0U)
        || (fd_count && fds == NULL))
    {
        return NGX_ERROR;
    }
    if (payload->len > client->options.max_payload) {
        client->stats.rejected++;
        return NGX_BUSY;
    }
    length = NGX_HTTP_CACHE_PURGE_FRAME_SIZE + payload->len;
    if (client->stats.pending >= client->options.max_pending
        || length > client->options.request_bytes - client->stats.request_bytes)
    {
        client->stats.rejected++;
        return NGX_BUSY;
    }
    call = ngx_calloc(sizeof(*call), client->log);
    if (call == NULL) {
        return NGX_ERROR;
    }
    call->packet = ngx_alloc(length, client->log);
    if (call->packet == NULL) {
        ngx_free(call);
        return NGX_ERROR;
    }
    for (i = 0; i < fd_count; i++) {
        call->fds[i] = fcntl(fds[i], F_DUPFD_CLOEXEC, 0);
        if (call->fds[i] < 0) {
            while (i) {
                close(call->fds[--i]);
            }
            ngx_free(call->packet);
            ngx_free(call);
            return NGX_ERROR;
        }
    }
    call->fd_count = fd_count;
    call->client = client;
    call->opcode = opcode;
    call->id = ++client->next_id;
    call->done = done;
    call->data = data;
    call->length = length;
    ngx_memcpy(call->packet, "NCP1", 4);
    ngx_http_cache_purge_client_put(call->packet + 4,
                                    NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION, 2);
    ngx_http_cache_purge_client_put(call->packet + 6, opcode, 2);
    ngx_http_cache_purge_client_put(call->packet + 8, call->id, 8);
    ngx_http_cache_purge_client_put(call->packet + 16, payload->len, 4);
    ngx_memzero(call->packet + 20, 4);
    if (payload->len) {
        ngx_memcpy(call->packet + NGX_HTTP_CACHE_PURGE_FRAME_SIZE,
                   payload->data, payload->len);
    }
    call->deadline.data = call;
    call->deadline.handler = ngx_http_cache_purge_client_deadline;
    call->deadline.log = client->log;
    ngx_queue_insert_tail(&client->queue, &call->queue);
    client->stats.accepted++;
    client->stats.pending++;
    client->stats.request_bytes += length;
    ngx_http_cache_purge_client_peak(client);
    ngx_add_timer(&call->deadline, timeout);
    *out = call;
    ngx_post_event(&client->pump, &ngx_posted_events);
    return NGX_OK;
}

ngx_int_t
ngx_http_cache_purge_client_submit(ngx_http_cache_purge_client_t *client,
    uint16_t opcode, const ngx_str_t *payload, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **out)
{
    return ngx_http_cache_purge_client_submit_internal(
        client, opcode, payload, timeout, done, data, out, NULL, 0);
}

ngx_int_t
ngx_http_cache_purge_client_submit_files(ngx_http_cache_purge_client_t *client,
    const ngx_str_t *payload, const int fds[2], ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **out)
{
    return ngx_http_cache_purge_client_submit_internal(
        client, NGX_HTTP_CACHE_PURGE_OP_PREPARE_FILE, payload, timeout, done,
        data, out, fds, 2);
}

ngx_int_t
ngx_http_cache_purge_client_finish_file(ngx_http_cache_purge_client_t *client,
    const ngx_str_t *payload, int lease_fd, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **out)
{
    return ngx_http_cache_purge_client_submit_internal(
        client, NGX_HTTP_CACHE_PURGE_OP_FINISH_FILE, payload, timeout, done,
        data, out, &lease_fd, 1);
}

ngx_int_t
ngx_http_cache_purge_client_inspect_file(ngx_http_cache_purge_client_t *client,
    const ngx_str_t *payload, int cache_fd, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **out)
{
    return ngx_http_cache_purge_client_submit_internal(
        client, NGX_HTTP_CACHE_PURGE_OP_INSPECT_FILE, payload, timeout, done,
        data, out, &cache_fd, 1);
}

ngx_int_t
ngx_http_cache_purge_client_retire_selected(
    ngx_http_cache_purge_client_t *client, const ngx_str_t *payload,
    int cache_fd, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **out)
{
    return ngx_http_cache_purge_client_submit_internal(
        client, NGX_HTTP_CACHE_PURGE_OP_RETIRE_SELECTED, payload, timeout,
        done, data, out, &cache_fd, 1);
}

ngx_int_t
ngx_http_cache_purge_client_admit_file(ngx_http_cache_purge_client_t *client,
    const ngx_str_t *payload, int cache_fd, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **out)
{
    return ngx_http_cache_purge_client_submit_internal(
        client, NGX_HTTP_CACHE_PURGE_OP_ADMIT_FILE, payload, timeout, done,
        data, out, &cache_fd, 1);
}

ngx_int_t
ngx_http_cache_purge_client_validate_file(ngx_http_cache_purge_client_t *client,
    const ngx_str_t *payload, const int fds[2], ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **out)
{
    return ngx_http_cache_purge_client_submit_internal(
        client, NGX_HTTP_CACHE_PURGE_OP_VALIDATE_FILE, payload, timeout, done,
        data, out, fds, 2);
}

static ngx_int_t
ngx_http_cache_purge_client_connected(ngx_http_cache_purge_client_t *client)
{
    int           error;
    socklen_t     length;
    struct ucred  peer;

    length = sizeof(error);
    if (getsockopt(client->connection->fd, SOL_SOCKET, SO_ERROR, &error,
                   &length)
            == -1
        || error != 0)
    {
        return NGX_HTTP_CACHE_PURGE_CLIENT_IO;
    }
    length = sizeof(peer);
    if (getsockopt(client->connection->fd, SOL_SOCKET, SO_PEERCRED, &peer,
                   &length)
            == -1
        || length != sizeof(peer) || peer.uid != client->options.server_uid)
    {
        return NGX_HTTP_CACHE_PURGE_CLIENT_OWNER;
    }
    client->connecting = 0;
    return NGX_HTTP_CACHE_PURGE_CLIENT_OK;
}

static ngx_int_t
ngx_http_cache_purge_client_connect(ngx_http_cache_purge_client_t *client)
{
    ngx_connection_t  *c;
    ngx_socket_t       s;
    ngx_err_t          err;
    int                rc;

    s = ngx_socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (s == (ngx_socket_t) -1) {
        ngx_log_error(NGX_LOG_ALERT, client->log, ngx_socket_errno,
                      "cache purge " ngx_socket_n " failed");
        return NGX_ERROR;
    }

    rc = connect(s, (struct sockaddr *) &client->address, client->address_len);
    if (rc == -1) {
        err = ngx_socket_errno;
        if (err != NGX_EINPROGRESS) {
            ngx_close_socket(s);
            if (err == NGX_ENOENT || err == NGX_ECONNREFUSED
                || err == NGX_EAGAIN || err == NGX_EINTR)
            {
                /*
                 * No frame was sent; the original call deadline still applies.
                 */
                ngx_log_debug1(NGX_LOG_DEBUG_EVENT, client->log, err,
                               "cache purge component unavailable: %V",
                               &client->name);
                return NGX_BUSY;
            }
            ngx_log_error(NGX_LOG_ERR, client->log, err,
                          "cache purge connect() to %V failed", &client->name);
            return NGX_ERROR;
        }
    }

    c = ngx_get_connection(s, client->log);
    if (c == NULL) {
        ngx_close_socket(s);
        return NGX_ERROR;
    }
    client->connection = c;
    c->type = SOCK_STREAM;
    c->recv = ngx_recv;
    c->send = ngx_send;
    c->log_error = NGX_ERROR_ERR;
    c->number = ngx_atomic_fetch_add(ngx_connection_counter, 1);
    c->start_time = ngx_current_msec;
    c->data = client;
    c->read->log = client->log;
    c->write->log = client->log;
    c->read->handler = ngx_http_cache_purge_client_read;
    c->write->handler = ngx_http_cache_purge_client_write;
    client->connecting = 1;

    if (ngx_add_conn) {
        if (ngx_add_conn(c) != NGX_OK) {
            goto failed;
        }
    } else if (rc == -1 && ngx_handle_write_event(c->write, 0) != NGX_OK) {
        goto failed;
    }
    client->stats.connections++;
    if (rc == -1) {
        return NGX_AGAIN;
    }
    c->write->ready = 1;
    return NGX_OK;

failed:

    ngx_http_cache_purge_client_close(client);
    return NGX_ERROR;
}

static void
ngx_http_cache_purge_client_pump(ngx_event_t *ev)
{
    ngx_http_cache_purge_client_t  *client = ev->data;
    ngx_connection_t               *c;
    ngx_int_t                       rc;

    if (client->stopped || client->current || ev->timer_set
        || ngx_queue_empty(&client->queue))
    {
        return;
    }
    if (client->idle.timer_set) {
        ngx_del_timer(&client->idle);
    }
    c = client->connection;
    if (c && (c->close || c->error || c->read->eof)) {
        ngx_http_cache_purge_client_close(client);
        c = NULL;
    }
    client->current = ngx_queue_data(ngx_queue_head(&client->queue),
                                     ngx_http_cache_purge_call_t, queue);
    if (c == NULL) {
        rc = ngx_http_cache_purge_client_connect(client);
        if (rc == NGX_BUSY) {
            client->current = NULL;
            ngx_add_timer(&client->pump, 10);
            return;
        }
        if (rc == NGX_ERROR) {
            ngx_http_cache_purge_client_finish(
                client->current, NGX_HTTP_CACHE_PURGE_CLIENT_IO, 1);
            return;
        }
        if (rc == NGX_AGAIN) {
            return;
        }
        c = client->connection;
    }
    ngx_reusable_connection(c, 0);
    c->idle = 0;
    ngx_http_cache_purge_client_write(c->write);
}

static void
ngx_http_cache_purge_client_write(ngx_event_t *ev)
{
    ngx_connection_t               *c = ev->data;
    ngx_http_cache_purge_client_t  *client = c->data;
    ngx_http_cache_purge_call_t    *call = client->current;
    size_t                          budget =
        NGX_HTTP_CACHE_PURGE_CLIENT_IO_BUDGET, amount;
    ssize_t                         n;
    ngx_int_t                       rc;

    if (call == NULL) {
        return;
    }
    if (client->connecting) {
        rc = ngx_http_cache_purge_client_connected(client);
        if (rc != NGX_HTTP_CACHE_PURGE_CLIENT_OK) {
            ngx_http_cache_purge_client_finish(call, rc, 1);
            return;
        }
    }
    while (budget && call->sent < call->length) {
        amount = ngx_min(budget, call->length - call->sent);
        if (call->fd_count) {
            n = ngx_http_cache_purge_transport_send(
                c->fd, call->packet + call->sent, 1, call->fds, call->fd_count);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                c->write->ready = 0;
                n = NGX_AGAIN;
            }
            if (n > 0) {
                while (call->fd_count) {
                    close(call->fds[--call->fd_count]);
                }
            }
        } else {
            n = c->send(c, call->packet + call->sent, amount);
        }
        if (n == NGX_AGAIN) {
            if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_IO, 1);
            }
            return;
        }
        if (n <= 0) {
            ngx_http_cache_purge_client_finish(
                call, NGX_HTTP_CACHE_PURGE_CLIENT_IO, 1);
            return;
        }
        call->sent += n;
        budget -= n;
    }
    if (call->sent != call->length) {
        ngx_post_event(c->write, &ngx_posted_next_events);
        return;
    }
    if (c->write->active
        && ngx_del_event(c->write, NGX_WRITE_EVENT, 0) != NGX_OK)
    {
        ngx_http_cache_purge_client_finish(call, NGX_HTTP_CACHE_PURGE_CLIENT_IO,
                                           1);
        return;
    }
    if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
        ngx_http_cache_purge_client_finish(call, NGX_HTTP_CACHE_PURGE_CLIENT_IO,
                                           1);
        return;
    }
    ngx_http_cache_purge_client_read(c->read);
}

static void
ngx_http_cache_purge_client_read(ngx_event_t *ev)
{
    ngx_connection_t               *c = ev->data;
    ngx_http_cache_purge_client_t  *client = c->data;
    ngx_http_cache_purge_call_t    *call = client->current;
    u_char                          byte, *destination;
    size_t                          budget =
        NGX_HTTP_CACHE_PURGE_CLIENT_IO_BUDGET, remaining;
    ssize_t                         n;
    int                             rights[NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS],
                                    saved;
    size_t                          count, i;
    ngx_uint_t                      lease_reply;

    if (call == NULL) {
        if (c->close || c->recv(c, &byte, 1) != NGX_AGAIN) {
            ngx_http_cache_purge_client_close(client);
        } else if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
            ngx_http_cache_purge_client_close(client);
        }
        return;
    }
    if (client->connecting || call->sent != call->length) {
        ngx_http_cache_purge_client_write(c->write);
        return;
    }
    lease_reply =
        call->opcode == NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_FILE_LEASE
        || call->opcode == NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION;
    while (budget) {
        if (client->header_used < sizeof(client->header)) {
            destination = client->header + client->header_used;
            remaining = sizeof(client->header) - client->header_used;
        } else {
            destination = client->response.data + client->received;
            remaining = client->response.len - client->received;
        }
        /* Only lease replies need a one-byte boundary for ancillary rights. */
        if (client->header_used == 0 && lease_reply) {
            remaining = 1;
        }
        n = ngx_http_cache_purge_transport_recv(
            c->fd, destination, ngx_min(budget, remaining),
            client->header_used == 0 && lease_reply, rights, &count);
        saved = n < 0 ? errno : 0;
        if (n < 0 && saved == EINTR) {
            continue;
        }
        if (n < 0 && (saved == EAGAIN || saved == EWOULDBLOCK)) {
            c->read->ready = 0;
            if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_IO, 1);
            }
            return;
        }
        if (n <= 0) {
            ngx_http_cache_purge_client_finish(
                call,
                saved == EPROTO ? NGX_HTTP_CACHE_PURGE_CLIENT_PROTOCOL :
                                  NGX_HTTP_CACHE_PURGE_CLIENT_IO,
                1);
            return;
        }
        if (count) {
            if (count != 1 || client->lease_fd >= 0 || !lease_reply) {
                for (i = 0; i < count; i++) {
                    close(rights[i]);
                }
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_PROTOCOL, 1);
                return;
            }
            client->lease_fd = rights[0];
        }
        budget -= n;
        if (client->header_used < sizeof(client->header)) {
            client->header_used += n;
            if (client->header_used < sizeof(client->header)) {
                continue;
            }
            if (ngx_memcmp(client->header, "NCP1", 4)
                || ngx_http_cache_purge_client_get(client->header + 4, 2)
                       != NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION
                || ngx_http_cache_purge_client_get(client->header + 6, 2)
                       != call->opcode
                || ngx_http_cache_purge_client_get(client->header + 8, 8)
                       != call->id)
            {
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_PROTOCOL, 1);
                return;
            }
            client->response.len = (size_t) ngx_http_cache_purge_client_get(
                client->header + 16, 4);
            client->result = (uint32_t) ngx_http_cache_purge_client_get(
                client->header + 20, 4);
            if ((client->lease_fd >= 0) != (lease_reply && client->result == 0))
            {
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_PROTOCOL, 1);
                return;
            }
            if (client->response.len > client->options.max_payload) {
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_LIMIT, 1);
                return;
            }
            if (client->response.len == 0) {
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_OK, 1);
                return;
            }
            client->response.data =
                ngx_alloc(client->response.len, client->log);
            if (client->response.data == NULL) {
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_LIMIT, 1);
                return;
            }
            client->stats.response_bytes = client->response.len;
            ngx_http_cache_purge_client_peak(client);
        } else {
            client->received += n;
            if (client->received == client->response.len) {
                ngx_http_cache_purge_client_finish(
                    call, NGX_HTTP_CACHE_PURGE_CLIENT_OK, 1);
                return;
            }
        }
    }
    ngx_post_event(c->read, &ngx_posted_next_events);
}

static void
ngx_http_cache_purge_client_close(ngx_http_cache_purge_client_t *client)
{
    if (client->idle.timer_set) {
        ngx_del_timer(&client->idle);
    }
    if (client->connection) {
        ngx_close_connection(client->connection);
        client->connection = NULL;
    }
    client->connecting = 0;
}

static void
ngx_http_cache_purge_client_finish(ngx_http_cache_purge_call_t *call,
    ngx_http_cache_purge_client_result_e result, ngx_uint_t notify)
{
    ngx_http_cache_purge_client_t  *client = call->client;
    ngx_http_cache_purge_reply_t    reply;
    u_char                         *response = NULL;
    int                             lease_fd = -1;

    call->completing = 1;
    ngx_memzero(&reply, sizeof(reply));
    reply.lease_fd = -1;
    reply.transport = result;
    reply.may_have_executed = call->sent != 0;
    if (client->current == call) {
        if (result != NGX_HTTP_CACHE_PURGE_CLIENT_OK) {
            ngx_http_cache_purge_client_close(client);
        } else {
            reply.result = client->result;
            reply.payload = client->response;
            reply.lease_fd = client->lease_fd;
        }
        lease_fd = client->lease_fd;
        client->lease_fd = -1;
        response = client->response.data;
        ngx_str_null(&client->response);
        client->current = NULL;
        client->received = client->header_used = 0;
        client->result = 0;
    }
    if (call->deadline.timer_set) {
        ngx_del_timer(&call->deadline);
    }
    ngx_queue_remove(&call->queue);
    while (call->fd_count) {
        close(call->fds[--call->fd_count]);
    }
    client->stats.pending--;
    client->stats.request_bytes -= call->length;
    ngx_free(call->packet);
    if (notify) {
        client->stats.completed++;
        call->done(call, call->data, &reply);
    } else {
        client->stats.cancelled++;
    }
    if (response) {
        ngx_free(response);
        client->stats.response_bytes = 0;
    }
    if (lease_fd >= 0) {
        close(lease_fd);
    }
    ngx_free(call);
    if (!client->stopped) {
        if (!ngx_queue_empty(&client->queue)) {
            ngx_post_event(&client->pump, &ngx_posted_events);
        } else if (client->connection) {
            client->connection->idle = 1;
            ngx_reusable_connection(client->connection, 1);
            ngx_add_timer(&client->idle, client->options.idle_timeout);
        }
    }
}

static void
ngx_http_cache_purge_client_deadline(ngx_event_t *ev)
{
    ngx_http_cache_purge_client_finish(ev->data,
                                       NGX_HTTP_CACHE_PURGE_CLIENT_TIMEOUT, 1);
}

static void
ngx_http_cache_purge_client_idle(ngx_event_t *ev)
{
    ngx_http_cache_purge_client_close(ev->data);
}

void
ngx_http_cache_purge_client_cancel(ngx_http_cache_purge_call_t *call)
{
    if (call && !call->completing) {
        ngx_http_cache_purge_client_finish(call, NGX_HTTP_CACHE_PURGE_CLIENT_IO,
                                           0);
    }
}

void
ngx_http_cache_purge_client_stop(ngx_http_cache_purge_client_t *client)
{
    ngx_http_cache_purge_call_t *call;
    if (client == NULL || client->stopped) {
        return;
    }
    client->stopped = 1;
    if (client->pump.timer_set) {
        ngx_del_timer(&client->pump);
    }
    if (client->pump.posted) {
        ngx_delete_posted_event(&client->pump);
    }
    ngx_http_cache_purge_client_close(client);
    while (!ngx_queue_empty(&client->queue)) {
        call = ngx_queue_data(ngx_queue_head(&client->queue),
                              ngx_http_cache_purge_call_t, queue);
        ngx_http_cache_purge_client_finish(
            call, NGX_HTTP_CACHE_PURGE_CLIENT_STOPPED, 1);
    }
}

void
ngx_http_cache_purge_client_stats(ngx_http_cache_purge_client_t *client,
    ngx_http_cache_purge_client_stats_t *stats)
{
    *stats = client->stats;
}


#define NGX_HTTP_CACHE_PURGE_API_BODY  262144
#define NGX_HTTP_CACHE_PURGE_API_REPLY 131072
#define NGX_HTTP_CACHE_PURGE_API_TIME  5000
#define NGX_HTTP_CACHE_PURGE_API_JSON  192

typedef struct {
    ngx_str_t  value;
    ngx_int_t  child, next;
    u_char     kind;
    unsigned   used:1;
} ngx_http_cache_purge_json_value_t;

typedef struct {
    u_char                            *at, *end;
    ngx_uint_t                         count;
    ngx_uint_t                         failed;
    ngx_http_cache_purge_json_value_t  values[NGX_HTTP_CACHE_PURGE_API_JSON];
} ngx_http_cache_purge_json_t;

typedef struct {
    ngx_http_request_t             *request;
    ngx_http_cache_purge_client_t  *client;
    ngx_http_cache_purge_call_t    *call;
    ngx_event_t                     timer;
    ngx_msec_t                      deadline;
    ngx_str_t                       id, payload, record;
    size_t                          capacity;
    uint16_t                        opcode;
    unsigned                        post:1;
    unsigned                        capabilities:1;
    unsigned                        submitted:1;
    unsigned                        finished:1;
} ngx_http_cache_purge_api_t;

typedef struct {
    u_char     *at, *end;
    ngx_uint_t  failed;
} ngx_http_cache_purge_api_decode_t;

static void ngx_http_cache_purge_api_cleanup(void *data);
static void ngx_http_cache_purge_api_body(ngx_http_request_t *r);
static void ngx_http_cache_purge_api_timer(ngx_event_t *event);
static void ngx_http_cache_purge_api_reply(ngx_http_cache_purge_call_t *call,
    void *data, const ngx_http_cache_purge_reply_t *reply);
static ngx_int_t ngx_http_cache_purge_api_call(ngx_http_cache_purge_api_t *ctx,
    uint16_t  opcode);
static void ngx_http_cache_purge_api_finish(ngx_http_cache_purge_api_t *ctx,
    ngx_uint_t  status, const char *error);

char *
ngx_http_cache_purge_api_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *data)
{
    ngx_http_cache_purge_loc_conf_t  *conf = data;
    ngx_http_core_loc_conf_t         *core;

    core = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    if (conf->api_prefix.data != NULL || core->handler != NULL) {
        return "conflicts with an existing content handler";
    }
    if (core->name.len == 0 || core->name.len > 1024
        || core->name.data[0] != '/'
        || core->name.data[core->name.len - 1] != '/' || core->exact_match
        || core->noname || core->named
#if (NGX_PCRE)
        || core->regex != NULL
#endif
        )
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                          "%V requires a plain prefix location ending in /",
                          &cmd->name);
        return NGX_CONF_ERROR;
    }
    conf->api_prefix = core->name;
    core->handler = ngx_http_cache_purge_api_handler;
    return NGX_CONF_OK;
}

char *
ngx_http_cache_purge_api_header_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *data)
{
    ngx_str_t                        *value, *slot;
    ngx_uint_t                        i, n;
    u_char                            ch;
    static const char *reserved[] = {
        "host", "content-length", "transfer-encoding", "connection",
        "authorization", "proxy-authorization", "content-type", "upgrade",
        "te", "trailer", "expect", "cookie", "set-cookie", "cache-control",
        "etag", "last-modified", "expires", "range", "if-range",
        "if-modified-since", "if-unmodified-since", "if-match", "if-none-match",
        "vary", "accept-encoding"
    };

    slot = (ngx_str_t *) ((u_char *) data + cmd->offset);
    if (slot->data != NULL) {
        return "is duplicate";
    }
    value = cf->args->elts;
    if (value[1].len == 0 || value[1].len > 128) {
        return "requires a header name of 1 to 128 bytes";
    }
    for (i = 0; i < value[1].len; i++) {
        ch = value[1].data[i];
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
              || (ch >= '0' && ch <= '9')
              || (ch != 0 && ngx_strchr("!#$%&'*+-.^_`|~", ch) != NULL)))
        {
            return "requires an HTTP token header name";
        }
    }
    for (n = 0; n < sizeof(reserved) / sizeof(reserved[0]); n++) {
        if (value[1].len == ngx_strlen(reserved[n])
            && ngx_strncasecmp(value[1].data, (u_char *) reserved[n],
                               value[1].len) == 0)
        {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                              "%V cannot use reserved header %V",
                              &cmd->name, &value[1]);
            return NGX_CONF_ERROR;
        }
    }
    *slot = value[1];
    return NGX_CONF_OK;
}

ngx_flag_t
ngx_http_cache_purge_ignore_propagation(ngx_http_request_t *r)
{
    ngx_http_cache_purge_loc_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
#if (NGX_EXPR)
    return ngx_http_get_expr_flag_value(r, conf->ignore_propagation);
#else
    return conf->ignore_propagation;
#endif
}

ngx_flag_t
ngx_http_cache_purge_propagation_possible(ngx_http_request_t *r)
{
    ngx_http_cache_purge_loc_conf_t  *conf;
#if (NGX_EXPR)
    ngx_conf_expr_flag_ctx_t         *values;
    ngx_uint_t                        i;
#endif

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
#if (NGX_EXPR)
    if (conf->propagation == NULL
        || conf->propagation == NGX_CONF_UNSET_PTR)
    {
        return 0;
    }
    values = conf->propagation->elts;
    for (i = 0; i < conf->propagation->nelts; i++) {
        if (values[i].value == 1) {
            return 1;
        }
    }
    return 0;
#else
    return conf->propagation == 1;
#endif
}

ngx_flag_t
ngx_http_cache_purge_propagation_enabled(ngx_http_request_t *r)
{
    ngx_http_cache_purge_loc_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
#if (NGX_EXPR)
    return ngx_http_get_expr_flag_value(r, conf->propagation);
#else
    return conf->propagation;
#endif
}

char *
ngx_http_cache_purge_api_merge(ngx_conf_t *cf,
    ngx_http_cache_purge_loc_conf_t *prev,
    ngx_http_cache_purge_loc_conf_t *conf)
{
    ngx_http_core_loc_conf_t  *core;
    ngx_http_core_srv_conf_t  *server;
    ngx_str_t                *headers[4], *previous[4];
    ngx_uint_t                i, j;
    static const char *defaults[] = {
        "X-Cache-Purge-Namespace", "X-Cache-Purge-Time", "X-Cache-Purge-Mode",
        "X-Cache-Purge-Fence"
    };

    headers[0] = &conf->tag_namespace_header;
    previous[0] = &prev->tag_namespace_header;
    for (i = 1; i < 4; i++) {
        headers[i] = &conf->propagation_header[i - 1];
        previous[i] = &prev->propagation_header[i - 1];
    }
    server = ngx_http_conf_get_module_srv_conf(cf, ngx_http_core_module);
    for (i = 0; i < 4; i++) {
        if (headers[i]->data == NULL) {
            *headers[i] = *previous[i];
        }
        if (headers[i]->data == NULL) {
            headers[i]->data = (u_char *) defaults[i];
            headers[i]->len = ngx_strlen(defaults[i]);
        }
        if (!server->underscores_in_headers && server->ignore_invalid_headers
            && ngx_strlchr(headers[i]->data,
                           headers[i]->data + headers[i]->len, '_') != NULL)
        {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "cache purge header %V would be ignored; enable "
                "underscores_in_headers or choose another name", headers[i]);
            return NGX_CONF_ERROR;
        }
        for (j = 0; j < i; j++) {
            if (headers[i]->len == headers[j]->len
                && ngx_strncasecmp(headers[i]->data, headers[j]->data,
                                   headers[i]->len) == 0)
            {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                    "cache purge control headers must have distinct names: "
                    "%V", headers[i]);
                return NGX_CONF_ERROR;
            }
        }
    }
    core = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    if (conf->api_prefix.data == NULL
        && core->handler == ngx_http_cache_purge_api_handler)
    {
        conf->api_prefix = prev->api_prefix;
    }
    if (conf->api_prefix.data == NULL) {
        return NGX_CONF_OK;
    }
    if (!ngx_http_cache_purge_runtime_configured(cf)
        || core->handler != ngx_http_cache_purge_api_handler)
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
            "cache_purge_api requires store/state_zone and its own handler");
        return NGX_CONF_ERROR;
    }
    if (core->client_body_in_file_only) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                          "cache_purge_api requires memory request bodies");
        return NGX_CONF_ERROR;
    }
    if (core->client_max_body_size <= 0
        || core->client_max_body_size > NGX_HTTP_CACHE_PURGE_API_BODY)
    {
        core->client_max_body_size = NGX_HTTP_CACHE_PURGE_API_BODY;
    }
    if (core->client_body_buffer_size == NGX_CONF_UNSET_SIZE
        || core->client_body_buffer_size < NGX_HTTP_CACHE_PURGE_API_BODY)
    {
        core->client_body_buffer_size = NGX_HTTP_CACHE_PURGE_API_BODY;
    }
    return NGX_CONF_OK;
}

static ngx_uint_t
ngx_http_cache_purge_api_equal(ngx_str_t value, const char *text)
{
    return value.len == ngx_strlen(text)
           && ngx_memcmp(value.data, text, value.len) == 0;
}

static ngx_uint_t
ngx_http_cache_purge_api_utf8(ngx_str_t value)
{
    u_char   *p, *end;
    uint32_t  ch;

    if (value.len == 0) {
        return 1;
    }
    p = value.data;
    end = p + value.len;
    while (p < end) {
        if (*p < 0x80) {
            p++;
            continue;
        }
        ch = ngx_utf8_decode(&p, end - p);
        if (ch > 0x10ffff || (ch >= 0xd800 && ch <= 0xdfff)) {
            return 0;
        }
    }
    return 1;
}

static void
ngx_http_cache_purge_json_space(ngx_http_cache_purge_json_t *json)
{
    while (json->at < json->end
           && (*json->at == ' ' || *json->at == '\r'
               || *json->at == '\n' || *json->at == '\t'))
    {
        json->at++;
    }
}

static uint32_t
ngx_http_cache_purge_json_hex(ngx_http_cache_purge_json_t *json)
{
    uint32_t    value;
    ngx_int_t   ch;
    ngx_uint_t  i;

    if (json->end - json->at < 4) {
        json->failed = 1;
        return 0;
    }
    value = 0;
    for (i = 0; i < 4; i++) {
        ch = ngx_hextoi(json->at++, 1);
        if (ch == NGX_ERROR) {
            json->failed = 1;
            return 0;
        }
        value = value * 16 + ch;
    }
    return value;
}

static ngx_int_t
ngx_http_cache_purge_json_parse(ngx_http_cache_purge_json_t *json,
    ngx_uint_t depth)
{
    ngx_http_cache_purge_json_value_t  *value;
    ngx_int_t                           index, child, last, key, n;
    uint32_t                            ch, low;
    u_char                             *start, *out, end;
    const char                         *literal;

    ngx_http_cache_purge_json_space(json);
    if (json->failed || json->at == json->end || depth > 5
        || json->count == NGX_HTTP_CACHE_PURGE_API_JSON)
    {
        json->failed = 1;
        return -1;
    }
    index = json->count++;
    value = &json->values[index];
    value->child = value->next = -1;
    ch = *json->at++;
    value->kind = (u_char) ch;
    if (ch == '"') {
        start = out = json->at;
        while (json->at < json->end && !json->failed) {
            ch = *json->at++;
            if (ch == '"') {
                value->value.data = start;
                value->value.len = out - start;
                json->failed = !ngx_http_cache_purge_api_utf8(value->value);
                return index;
            }
            if (ch < 0x20) {
                break;
            }
            if (ch != '\\') {
                *out++ = (u_char) ch;
                continue;
            }
            if (json->at == json->end) {
                break;
            }
            ch = *json->at++;
            switch (ch) {
            case '"': case '\\': case '/': *out++ = (u_char) ch; break;
            case 'b': *out++ = '\b'; break;
            case 'f': *out++ = '\f'; break;
            case 'n': *out++ = '\n'; break;
            case 'r': *out++ = '\r'; break;
            case 't': *out++ = '\t'; break;
            case 'u':
                ch = ngx_http_cache_purge_json_hex(json);
                if (ch >= 0xd800 && ch <= 0xdbff) {
                    if (json->end - json->at < 6 || json->at[0] != '\\'
                        || json->at[1] != 'u')
                    {
                        json->failed = 1;
                        break;
                    }
                    json->at += 2;
                    low = ngx_http_cache_purge_json_hex(json);
                    if (low < 0xdc00 || low > 0xdfff) {
                        json->failed = 1;
                        break;
                    }
                    ch = 0x10000 + ((ch - 0xd800) << 10) + low - 0xdc00;
                } else if (ch >= 0xdc00 && ch <= 0xdfff) {
                    json->failed = 1;
                    break;
                }
                if (ch < 0x80) {
                    *out++ = (u_char) ch;
                } else if (ch < 0x800) {
                    *out++ = (u_char) (0xc0 | (ch >> 6));
                    *out++ = (u_char) (0x80 | (ch & 63));
                } else if (ch < 0x10000) {
                    *out++ = (u_char) (0xe0 | (ch >> 12));
                    *out++ = (u_char) (0x80 | ((ch >> 6) & 63));
                    *out++ = (u_char) (0x80 | (ch & 63));
                } else {
                    *out++ = (u_char) (0xf0 | (ch >> 18));
                    *out++ = (u_char) (0x80 | ((ch >> 12) & 63));
                    *out++ = (u_char) (0x80 | ((ch >> 6) & 63));
                    *out++ = (u_char) (0x80 | (ch & 63));
                }
                break;
            default: json->failed = 1;
            }
        }
        json->failed = 1;
        return -1;
    }
    if (ch == '{' || ch == '[') {
        end = ch == '{' ? '}' : ']';
        last = -1;
        ngx_http_cache_purge_json_space(json);
        if (json->at < json->end && *json->at == end) {
            json->at++;
            return index;
        }
        for ( ;; ) {
            child = ngx_http_cache_purge_json_parse(json, depth + 1);
            if (child < 0) {
                return -1;
            }
            if (last < 0) {
                value->child = child;
            } else {
                json->values[last].next = child;
            }
            last = child;
            if (ch == '{') {
                if (json->values[child].kind != '"') {
                    break;
                }
                for (key = value->child; key != child;
                     key = json->values[key].next)
                {
                    if (json->values[key].value.len
                        == json->values[child].value.len
                        && ngx_memcmp(json->values[key].value.data,
                                      json->values[child].value.data,
                                      json->values[key].value.len) == 0)
                    {
                        json->failed = 1;
                        return -1;
                    }
                }
                ngx_http_cache_purge_json_space(json);
                if (json->at == json->end || *json->at++ != ':') {
                    break;
                }
                n = ngx_http_cache_purge_json_parse(json, depth + 1);
                if (n < 0) {
                    return -1;
                }
                json->values[child].child = n;
            }
            ngx_http_cache_purge_json_space(json);
            if (json->at == json->end) {
                break;
            }
            if (*json->at == end) {
                json->at++;
                return index;
            }
            if (*json->at++ != ',') {
                break;
            }
        }
        json->failed = 1;
        return -1;
    }
    literal = ch == 't' ? "rue" : ch == 'f' ? "alse" :
              ch == 'n' ? "ull" : NULL;
    if (literal != NULL
        && (size_t) (json->end - json->at) >= ngx_strlen(literal)
        && ngx_memcmp(json->at, literal, ngx_strlen(literal)) == 0)
    {
        json->at += ngx_strlen(literal);
        return index;
    }
    json->failed = 1;
    return -1;
}

static ngx_http_cache_purge_json_value_t *
ngx_http_cache_purge_json_member(ngx_http_cache_purge_json_t *json,
    ngx_http_cache_purge_json_value_t *object, const char *name, u_char kind,
    ngx_uint_t required)
{
    ngx_int_t                           i;
    ngx_http_cache_purge_json_value_t  *key, *value;

    if (object == NULL || object->kind != '{') {
        json->failed = 1;
        return NULL;
    }
    for (i = object->child; i >= 0; i = key->next) {
        key = &json->values[i];
        if (ngx_http_cache_purge_api_equal(key->value, name)) {
            key->used = 1;
            value = &json->values[key->child];
            if (kind != 0 && value->kind != kind) {
                json->failed = 1;
                return NULL;
            }
            return value;
        }
    }
    json->failed |= required;
    return NULL;
}

static ngx_uint_t
ngx_http_cache_purge_json_known(ngx_http_cache_purge_json_t *json,
    ngx_http_cache_purge_json_value_t *object)
{
    ngx_int_t  i;
    for (i = object->child; i >= 0; i = json->values[i].next) {
        if (!json->values[i].used) {
            return 0;
        }
    }
    return !json->failed;
}

static ngx_str_t
ngx_http_cache_purge_api_text(ngx_http_cache_purge_json_t *json,
    ngx_http_cache_purge_json_value_t *object, const char *name,
    ngx_uint_t required)
{
    ngx_http_cache_purge_json_value_t  *value;
    ngx_str_t                           empty = ngx_null_string;

    value = ngx_http_cache_purge_json_member(json, object, name, '"', required);
    return value ? value->value : empty;
}

static ngx_uint_t
ngx_http_cache_purge_api_id(ngx_str_t id)
{
    size_t  i;
    u_char  ch;
    if (id.len == 0 || id.len > 128) {
        return 0;
    }
    if (ngx_http_cache_purge_api_equal(id, ".")
        || ngx_http_cache_purge_api_equal(id, ".."))
    {
        return 0;
    }
    for (i = 0; i < id.len; i++) {
        ch = id.data[i];
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
              || (ch >= '0' && ch <= '9') || ch == '.' || ch == '_'
              || ch == '-' || ch == ':'))
        {
            return 0;
        }
    }
    return 1;
}

static ngx_uint_t
ngx_http_cache_purge_api_decimal(ngx_str_t value, uint64_t *out)
{
    size_t    i;
    uint64_t  n;
    unsigned  ch;

    n = 0;
    if (value.len == 0 || value.len > 20) {
        return 0;
    }
    for (i = 0; i < value.len; i++) {
        ch = value.data[i] - '0';
        if (ch > 9 || n > (UINT64_MAX - ch) / 10) {
            return 0;
        }
        n = n * 10 + ch;
    }
    *out = n;
    return 1;
}

static uint64_t
ngx_http_cache_purge_api_time(ngx_str_t value)
{
    ngx_str_t                part;
    uint64_t                 fields[7];
    time_t                   stamp;
    u_char                   text[64], *p;
    ngx_uint_t               i;
    static const ngx_uint_t  at[] = {0, 5, 8, 11, 14, 17, 20};
    static const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    if ((value.len != 20 && value.len != 24) || value.data[4] != '-'
        || value.data[7] != '-' || value.data[10] != 'T'
        || value.data[13] != ':' || value.data[16] != ':'
        || value.data[value.len - 1] != 'Z'
        || (value.len == 24 && value.data[19] != '.'))
    {
        return 0;
    }
    ngx_memzero(fields, sizeof(fields));
    for (i = 0; i < (value.len == 24 ? 7U : 6U); i++) {
        part.data = value.data + at[i];
        part.len = i == 0 ? 4 : i == 6 ? 3 : 2;
        if (!ngx_http_cache_purge_api_decimal(part, &fields[i])) {
            return 0;
        }
    }
    if (fields[0] < 1970 || fields[1] == 0 || fields[1] > 12) {
        return 0;
    }
    p = ngx_sprintf(text, "Sun, %02uL %s %04uL %02uL:%02uL:%02uL GMT",
                    fields[2], months[fields[1] - 1], fields[0], fields[3],
                    fields[4], fields[5]);
    stamp = ngx_parse_http_time(text, p - text);
    return stamp == (time_t) NGX_ERROR ? 0 :
           (uint64_t) stamp * 1000 + fields[6];
}

static u_char *
ngx_http_cache_purge_api_number(u_char *p, uint64_t value, unsigned width)
{
    while (width) {
        *p++ = (u_char) (value >> (--width * 8));
    }
    return p;
}

static u_char *
ngx_http_cache_purge_api_string(u_char *p, ngx_str_t value)
{
    p = ngx_http_cache_purge_api_number(p, value.len, 4);
    return value.len ? ngx_cpymem(p, value.data, value.len) : p;
}

static ngx_int_t
ngx_http_cache_purge_api_header(ngx_http_request_t *r, ngx_str_t name,
    ngx_str_t *value)
{
    ngx_list_part_t  *part;
    ngx_table_elt_t  *headers;
    ngx_uint_t        i, found;

    part = &r->headers_in.headers.part;
    headers = part->elts;
    found = 0;
    ngx_str_null(value);
    for (i = 0; /* void */; i++) {
        if (i == part->nelts) {
            if (part->next == NULL) {
                break;
            }
            part = part->next;
            headers = part->elts;
            i = 0;
        }
        if (headers[i].hash && headers[i].key.len == name.len
            && ngx_strncasecmp(headers[i].key.data, name.data, name.len) == 0)
        {
            if (found++) {
                return NGX_ERROR;
            }
            *value = headers[i].value;
        }
    }
    return found ? NGX_OK : NGX_DECLINED;
}

static int
ngx_http_cache_purge_api_tag_compare(const void *left, const void *right)
{
    const ngx_str_t  *a = left;
    const ngx_str_t  *b = right;

    return ngx_memn2cmp(a->data, b->data, a->len, b->len);
}

static ngx_int_t
ngx_http_cache_purge_api_value(ngx_http_request_t *r, ngx_str_t encoding,
    ngx_str_t input, size_t limit, ngx_str_t *value)
{
    ngx_str_t  encoded;

    *value = input;
    if (encoding.len && !ngx_http_cache_purge_api_equal(encoding, "utf8")) {
        if (!ngx_http_cache_purge_api_equal(encoding, "base64")
            || input.len > ngx_base64_encoded_length(limit))
        {
            return NGX_HTTP_BAD_REQUEST;
        }
        value->len = ngx_base64_decoded_length(input.len);
        value->data = ngx_pnalloc(r->pool, value->len ? value->len : 1);
        if (value->data == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        if (ngx_decode_base64(value, &input) != NGX_OK) {
            return NGX_HTTP_BAD_REQUEST;
        }
        encoded.len = ngx_base64_encoded_length(value->len);
        encoded.data = ngx_pnalloc(r->pool, encoded.len ? encoded.len : 1);
        if (encoded.data == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        ngx_encode_base64(&encoded, value);
        if (encoded.len != input.len
            || ngx_memcmp(encoded.data, input.data, input.len) != 0)
        {
            return NGX_HTTP_BAD_REQUEST;
        }
    }
    return value->len == 0 || value->len > limit ?
           NGX_HTTP_BAD_REQUEST : NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_api_prepare(ngx_http_cache_purge_api_t *ctx,
    ngx_str_t body, const char **error)
{
    ngx_http_cache_purge_json_t        *json;
    ngx_http_cache_purge_json_value_t  *root, *selector, *event, *item, *values;
    ngx_http_cache_purge_loc_conf_t    *conf;
    ngx_str_t version, mode, type, field, value, scope, encoding, tags[64];
    ngx_str_t            issuer, stream, serial, issued, match, tag_scope,
                         binding;
    ngx_http_request_t  *r;
    uint64_t             timestamp, sequence;
    ngx_uint_t           kind, action;
    ngx_uint_t           count, unique, i;
    ngx_int_t            member, rc;
    size_t               size, prefix;
    u_char              *canonical, *p, digest[SHA256_DIGEST_LENGTH];

    *error = "invalid_request";
    r = ctx->request;
    json = ngx_pcalloc(r->pool, sizeof(*json));
    if (json == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    json->at = body.data;
    json->end = body.data + body.len;
    if (ngx_http_cache_purge_json_parse(json, 0) != 0 || json->failed) {
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_http_cache_purge_json_space(json);
    if (json->at != json->end || json->values[0].kind != '{') {
        return NGX_HTTP_BAD_REQUEST;
    }
    root = &json->values[0];
    version = ngx_http_cache_purge_api_text(json, root, "version", 1);
    if (json->failed) {
        return NGX_HTTP_BAD_REQUEST;
    }
    if (!ngx_http_cache_purge_api_equal(version, "ngx-http-cache-purge/v2")) {
        *error = "unsupported_version";
        return NGX_HTTP_PRECONDITION_FAILED;
    }
    ctx->id = ngx_http_cache_purge_api_text(json, root, "operation_id", 1);
    mode = ngx_http_cache_purge_api_text(json, root, "mode", 1);
    selector = ngx_http_cache_purge_json_member(json, root, "selector", '{', 1);
    event = ngx_http_cache_purge_json_member(json, root, "event", '{', 1);
    if (json->failed || !ngx_http_cache_purge_api_id(ctx->id)) {
        return NGX_HTTP_BAD_REQUEST;
    }
    action = ngx_http_cache_purge_api_equal(mode, "invalidate") ? 1 :
             ngx_http_cache_purge_api_equal(mode, "delete") ? 2 : 0;
    type = ngx_http_cache_purge_api_text(json, selector, "type", 1);
    encoding = ngx_http_cache_purge_api_text(json, selector, "encoding", 0);
    ngx_str_null(&scope);
    ngx_str_null(&value);
    kind = 0;
    if (ngx_http_cache_purge_api_equal(type, "tag")) {
        values = ngx_http_cache_purge_json_member(json, selector,
                                                 "values", '[', 1);
        match = ngx_http_cache_purge_api_text(json, selector, "match", 0);
        tag_scope = ngx_http_cache_purge_api_text(json, selector, "scope", 0);
        if (json->failed || values->child < 0) {
            return NGX_HTTP_BAD_REQUEST;
        }
        if (match.len && !ngx_http_cache_purge_api_equal(match, "any")) {
            return NGX_HTTP_BAD_REQUEST;
        }
        count = 0;
        for (member = values->child; member >= 0; member = item->next) {
            item = &json->values[member];
            if (count == 64) {
                *error = "too_many_tags";
                return NGX_HTTP_PRECONDITION_FAILED;
            }
            if (item->kind != '"') {
                return NGX_HTTP_BAD_REQUEST;
            }
            rc = ngx_http_cache_purge_api_value(r, encoding, item->value,
                                               1024, &tags[count]);
            if (rc != NGX_OK) {
                return rc;
            }
            count++;
        }
        ngx_qsort(tags, count, sizeof(ngx_str_t),
                  ngx_http_cache_purge_api_tag_compare);
        unique = 1;
        for (i = 1; i < count; i++) {
            if (ngx_http_cache_purge_api_tag_compare(&tags[unique - 1],
                                                      &tags[i]) != 0)
            {
                tags[unique++] = tags[i];
            }
        }
        value = tags[0];
        kind = tag_scope.len == 0
               || ngx_http_cache_purge_api_equal(tag_scope, "namespace") ? 5 :
               ngx_http_cache_purge_api_equal(tag_scope, "host") ? 6 : 99;
        if (unique > 1) {
            size = 1;
            for (i = 0; i < unique; i++) {
                size += 2 + tags[i].len;
            }
            if (size > 32768) {
                *error = "tag_set_too_large";
                return NGX_HTTP_BAD_REQUEST;
            }
            value.data = ngx_pnalloc(r->pool, size);
            value.len = size;
            if (value.data == NULL) {
                return NGX_HTTP_INTERNAL_SERVER_ERROR;
            }
            p = value.data;
            *p++ = (u_char) unique;
            for (i = 0; i < unique; i++) {
                p = ngx_http_cache_purge_api_number(p, tags[i].len, 2);
                p = ngx_cpymem(p, tags[i].data, tags[i].len);
            }
            kind += 2;
        }
        conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
        if (ngx_http_cache_purge_api_header(r, conf->tag_namespace_header,
                                           &scope) != NGX_OK
            || ((kind == 6 || kind == 8) && scope.len == 0)
            || scope.len > 32768
            || !ngx_http_cache_purge_api_utf8(scope))
        {
            *error = "invalid_tag_scope_header";
            return NGX_HTTP_BAD_REQUEST;
        }
    } else {
        value = ngx_http_cache_purge_api_text(json, selector, "value", 1);
        if (ngx_http_cache_purge_api_equal(type, "url")) {
            kind = 1;
        } else if (ngx_http_cache_purge_api_equal(type, "key")) {
            kind = 0;
        } else if (ngx_http_cache_purge_api_equal(type, "prefix")) {
            field = ngx_http_cache_purge_api_text(json, selector, "field", 1);
            kind = ngx_http_cache_purge_api_equal(field, "key") ? 2 :
                   ngx_http_cache_purge_api_equal(field, "url") ? 3 : 99;
        } else if (ngx_http_cache_purge_api_equal(type, "host")) {
            kind = 4;
            item = ngx_http_cache_purge_json_member(json, selector,
                                                   "include_subdomains", 0, 0);
            if (item != NULL && item->kind != 'f') {
                *error = "subdomains_not_supported";
                return NGX_HTTP_PRECONDITION_FAILED;
            }
        } else {
            kind = 99;
        }
    }
    if (kind < 5) {
        rc = ngx_http_cache_purge_api_value(r, encoding, value, 32768, &value);
        if (rc != NGX_OK) {
            return rc;
        }
    }
    if (action == 0 || kind > 8 || value.len == 0 || value.len > 32768
        || !ngx_http_cache_purge_json_known(json, selector))
    {
        return NGX_HTTP_BAD_REQUEST;
    }
    issuer = ngx_http_cache_purge_api_text(json, event, "issuer", 1);
    stream = ngx_http_cache_purge_api_text(json, event, "stream_id", 1);
    serial = ngx_http_cache_purge_api_text(json, event, "sequence", 1);
    issued = ngx_http_cache_purge_api_text(json, event, "issued_at", 1);
    timestamp = ngx_http_cache_purge_api_time(issued);
    if (!ngx_http_cache_purge_json_known(json, root)
        || !ngx_http_cache_purge_json_known(json, event)
        || issuer.len == 0 || issuer.len > 96
        || stream.len == 0 || stream.len > 96
        || !ngx_http_cache_purge_api_decimal(serial, &sequence) || sequence == 0
        || timestamp == 0)
    {
        return NGX_HTTP_BAD_REQUEST;
    }
    prefix = 4 + ctx->id.len + 1 + 8 + 1 + 4 + value.len + 4 + scope.len;
    size = prefix + 4 + issuer.len + 4 + stream.len + 8;
    canonical = ngx_pnalloc(r->pool, size);
    binding.len = 44 + issuer.len + stream.len;
    binding.data = ngx_pnalloc(r->pool, binding.len);
    ctx->payload.len = prefix + 4 + binding.len;
    ctx->payload.data = ngx_pnalloc(r->pool, ctx->payload.len);
    if (canonical == NULL || binding.data == NULL || ctx->payload.data == NULL)
    {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    p = ngx_http_cache_purge_api_string(canonical, ctx->id);
    *p++ = (u_char) action;
    p = ngx_http_cache_purge_api_number(p, timestamp, 8);
    *p++ = (u_char) kind;
    p = ngx_http_cache_purge_api_string(p, value);
    p = ngx_http_cache_purge_api_string(p, scope);
    p = ngx_http_cache_purge_api_string(p, issuer);
    p = ngx_http_cache_purge_api_string(p, stream);
    ngx_http_cache_purge_api_number(p, sequence, 8);
    if (SHA256(canonical, size, digest) == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    binding.data[0] = 'H';
    binding.data[1] = 1;
    binding.data[2] = (u_char) issuer.len;
    binding.data[3] = (u_char) stream.len;
    p = ngx_http_cache_purge_api_number(binding.data + 4, sequence, 8);
    p = ngx_cpymem(p, digest, sizeof(digest));
    p = ngx_cpymem(p, issuer.data, issuer.len);
    ngx_memcpy(p, stream.data, stream.len);
    p = ngx_cpymem(ctx->payload.data, canonical, prefix);
    ngx_http_cache_purge_api_string(p, binding);
    return NGX_OK;
}

static uint64_t
ngx_http_cache_purge_api_read(ngx_http_cache_purge_api_decode_t *d,
    unsigned width)
{
    uint64_t  value;
    if ((size_t) (d->end - d->at) < width) {
        d->failed = 1;
        return 0;
    }
    value = 0;
    while (width--) {
        value = (value << 8) | *d->at++;
    }
    return value;
}

static ngx_str_t
ngx_http_cache_purge_api_read_string(ngx_http_cache_purge_api_decode_t *d)
{
    ngx_str_t  value;
    value.len = (size_t) ngx_http_cache_purge_api_read(d, 4);
    value.data = d->at;
    if (value.len > (size_t) (d->end - d->at)) {
        value.len = 0;
        d->failed = 1;
    }
    d->at += value.len;
    return value;
}

static u_char *
ngx_http_cache_purge_api_quote(u_char *p, ngx_str_t value)
{
    size_t         i;
    u_char         ch;
    static u_char  hex[] = "0123456789abcdef";

    *p++ = '"';
    for (i = 0; i < value.len; i++) {
        ch = value.data[i];
        if (ch == '"' || ch == '\\') {
            *p++ = '\\'; *p++ = ch;
        } else if (ch < 0x20) {
            *p++ = '\\'; *p++ = 'u'; *p++ = '0'; *p++ = '0';
            *p++ = hex[ch >> 4]; *p++ = hex[ch & 15];
        } else {
            *p++ = ch;
        }
    }
    *p++ = '"';
    return p;
}

static u_char *
ngx_http_cache_purge_api_date(u_char *p, uint64_t msec)
{
    ngx_tm_t  tm;
    if (msec == 0 || msec > UINT64_C(253402300799999)) {
        return ngx_cpymem(p, "null", 4);
    }
    ngx_gmtime((time_t) (msec / 1000), &tm);
    return ngx_sprintf(p, "\"%04d-%02d-%02dT%02d:%02d:%02d.%03uLZ\"",
                      tm.ngx_tm_year, tm.ngx_tm_mon, tm.ngx_tm_mday,
                      tm.ngx_tm_hour, tm.ngx_tm_min, tm.ngx_tm_sec,
                      msec % 1000);
}

static ngx_int_t
ngx_http_cache_purge_api_send(ngx_http_request_t *r, ngx_uint_t status,
    ngx_buf_t *buffer)
{
    ngx_chain_t       out;
    ngx_table_elt_t  *header;
    ngx_int_t         rc;

    r->headers_out.status = status;
    r->headers_out.content_length_n = buffer->last - buffer->pos;
    ngx_str_set(&r->headers_out.content_type, "application/json");
    header = ngx_list_push(&r->headers_out.headers);
    if (header == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    header->hash = 1;
    ngx_str_set(&header->key, "Cache-Control");
    ngx_str_set(&header->value, "no-store");
    if (r->method == NGX_HTTP_POST && r->request_body == NULL) {
        r->keepalive = 0;
        r->lingering_close = 1;
    }
    buffer->last_buf = 1;
    out.buf = buffer;
    out.next = NULL;
    rc = ngx_http_send_header(r);
    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }
    return ngx_http_output_filter(r, &out);
}

static ngx_int_t
ngx_http_cache_purge_api_render(ngx_http_cache_purge_api_t *ctx,
    ngx_uint_t status, const char *error)
{
    ngx_http_request_t                *r;
    ngx_http_cache_purge_loc_conf_t   *conf;
    ngx_http_cache_purge_api_decode_t  d;
    ngx_http_cache_rocksdb_bytes_t     raw, tags[64];
    ngx_buf_t                         *buffer;
    ngx_str_t                          id, value, scope, binding, message,
                                       encoded, route;
    uint64_t                           publication, last, retired, live, bytes,
                                       policy;
    uint64_t                           issued, sequence, revision, accepted,
                                       effective, until;
    uint64_t                           visits, pending, failed, checkpoint;
    ngx_uint_t                         state, mode, kind, stage, done,
                                       propagation, ready;
    ngx_uint_t                         binary;
    u_char                             ledger[32], *p, *identity;
    size_t                             size, count, i;
    static const char *types[] = {"key", "url", "prefix", "prefix",
                                  "host", "tag", "tag", "tag", "tag"};
    static const char *roles[] = {"propagation_time", "propagation_mode",
                                  "propagation_fence"};

    r = ctx->request;
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    if (error != NULL) {
        buffer = ngx_create_temp_buf(r->pool, 2048);
        if (buffer == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        p = ngx_sprintf(buffer->last,
            "{\"version\":\"ngx-http-cache-purge/v2\",\"error\":\"%s\","
            "\"outcome\":\"%s\",\"operation_id\":", error,
            !ctx->post || ctx->submitted ? "unknown" : "not_submitted");
        if (ctx->id.len && ctx->id.len <= 128
            && ngx_http_cache_purge_api_utf8(ctx->id))
        {
            p = ngx_http_cache_purge_api_quote(p, ctx->id);
        } else {
            p = ngx_cpymem(p, "null", 4);
        }
        buffer->last = ngx_cpymem(p, "}\n", 2);
        return ngx_http_cache_purge_api_send(r, status, buffer);
    }
    if (ctx->record.data == NULL || ctx->record.len < 64) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    d.at = ctx->record.data;
    d.end = d.at + ctx->record.len;
    d.failed = 0;
    state = (ngx_uint_t) ngx_http_cache_purge_api_read(&d, 4);
    publication = ngx_http_cache_purge_api_read(&d, 8);
    if (d.end - d.at < 16) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    identity = d.at;
    ngx_hex_dump(ledger, identity, 16);
    d.at += 16;
    last = ngx_http_cache_purge_api_read(&d, 8);
    retired = ngx_http_cache_purge_api_read(&d, 8);
    live = ngx_http_cache_purge_api_read(&d, 8);
    bytes = ngx_http_cache_purge_api_read(&d, 8);
    policy = ngx_http_cache_purge_api_read(&d, 4);
    ready = state == NGX_HTTP_CACHE_PURGE_COORD_READY;
    if (d.failed) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    if (ctx->capabilities) {
        if (d.at != d.end) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        buffer = ngx_create_temp_buf(r->pool, 8192);
        if (buffer == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        p = ngx_sprintf(buffer->last,
            "{\"version\":\"ngx-http-cache-purge/v2\","
            "\"authentication\":\"nginx-configuration\","
            "\"index_id\":\"%*s\",\"ready\":%s,\"rules_revision\":\"%uL\","
            "\"last_sequence\":\"%uL\",\"retired_sequence\":\"%uL\","
            "\"live_operations\":\"%uL\",\"selector_bytes\":\"%uL\","
            "\"durable_tasks\":true,\"idempotency\":true,\"status_query\":true,"
            "\"selectors\":[\"host\",\"url\",\"key\",\"prefix\",\"tag\"],"
            "\"modes\":[\"invalidate\",\"delete\"],\"binary_selectors\":true,"
            "\"tag_scopes\":[\"namespace\",\"host\"],\"max_tags\":64,"
            "\"index_schema\":12,\"ledger_schema\":4,"
            "\"propagation_timeout_ms\":\"%uL\","
            "\"propagation_protocols\":[],\"proxy_propagation_ready\":false,"
            "\"proxy_header_transport\":\"%s\","
            "\"invalidate_stale_policy\":\"%s\","
            "\"limits\":{\"body_bytes\":262144,\"selector_bytes\":32768,"
            "\"tag_bytes\":1024,\"tag_set_bytes\":32768,"
            "\"namespace_bytes\":32768,"
            "\"operation_id_bytes\":128,\"acceptance_timeout_ms\":5000},"
            "\"control_headers\":{\"tag_namespace\":",
            (size_t) 32, ledger, ready ? "true" : "false", publication,
            last, retired, live, bytes, policy,
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
            "filter",
#else
            "variables",
#endif
#if (NGX_RESTY_EXT)
            "forbid"
#else
            "native"
#endif
            );
        p = ngx_http_cache_purge_api_quote(p, conf->tag_namespace_header);
        for (i = 0; i < 3; i++) {
            p = ngx_sprintf(p, ",\"%s\":", roles[i]);
            p = ngx_http_cache_purge_api_quote(p, conf->propagation_header[i]);
        }
        buffer->last = ngx_cpymem(p, "}}\n", 3);
        return ngx_http_cache_purge_api_send(r, status, buffer);
    }
    id = ngx_http_cache_purge_api_read_string(&d);
    mode = (ngx_uint_t) ngx_http_cache_purge_api_read(&d, 1);
    issued = ngx_http_cache_purge_api_read(&d, 8);
    kind = (ngx_uint_t) ngx_http_cache_purge_api_read(&d, 1);
    value = ngx_http_cache_purge_api_read_string(&d);
    scope = ngx_http_cache_purge_api_read_string(&d);
    binding = ngx_http_cache_purge_api_read_string(&d);
    sequence = ngx_http_cache_purge_api_read(&d, 8);
    revision = ngx_http_cache_purge_api_read(&d, 8);
    accepted = ngx_http_cache_purge_api_read(&d, 8);
    effective = ngx_http_cache_purge_api_read(&d, 8);
    until = ngx_http_cache_purge_api_read(&d, 8);
    stage = (ngx_uint_t) ngx_http_cache_purge_api_read(&d, 1);
    (void) ngx_http_cache_purge_api_read(&d, 4);
    propagation = (ngx_uint_t) ngx_http_cache_purge_api_read(&d, 1);
    (void) ngx_http_cache_purge_api_read_string(&d);
    done = (ngx_uint_t) ngx_http_cache_purge_api_read(&d, 1);
    (void) ngx_http_cache_purge_api_read(&d, 8);
    visits = ngx_http_cache_purge_api_read(&d, 8);
    pending = ngx_http_cache_purge_api_read(&d, 8);
    failed = ngx_http_cache_purge_api_read(&d, 8);
    checkpoint = ngx_http_cache_purge_api_read(&d, 8);
    message = ngx_http_cache_purge_api_read_string(&d);
    if (d.failed || d.at != d.end || kind > 8 || mode < 1 || mode > 2
        || stage < 1 || stage > 4 || id.len != ctx->id.len
        || ngx_memcmp(id.data, ctx->id.data, id.len) || sequence > last
        || !ngx_http_cache_purge_api_utf8(scope)
        || !ngx_http_cache_purge_api_utf8(message))
    {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    if (ctx->post && (stage == 1 || !ready)) {
        return NGX_AGAIN;
    }
    size = 8192 + 6 * (id.len + value.len + scope.len + message.len
                      + conf->api_prefix.len);
    buffer = ngx_create_temp_buf(r->pool, size);
    if (buffer == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    p = ngx_sprintf(buffer->last,
        "{\"version\":\"ngx-http-cache-purge/v2\",\"operation_id\":");
    p = ngx_http_cache_purge_api_quote(p, id);
    p = ngx_sprintf(p, ",\"index_id\":\"%*s\",\"mode\":\"%s\","
        "\"state\":\"%s\",\"durable\":true,\"effective\":%s,\"ready\":%s,"
        "\"rules_revision\":\"%uL\",\"sequence\":\"%uL\","
        "\"revision\":\"%uL\",\"accepted_at\":",
        (size_t) 32, ledger, mode == 1 ? "invalidate" : "delete",
        stage == 1 ? "preparing" : stage == 4 ? "materialized" : "effective",
        effective ? "true" : "false", ready ? "true" : "false",
        publication, sequence, revision);
    p = ngx_http_cache_purge_api_date(p, accepted);
    p = ngx_cpymem(p, ",\"effective_at\":", sizeof(",\"effective_at\":") - 1);
    p = ngx_http_cache_purge_api_date(p, effective);
    p = ngx_cpymem(p, ",\"propagation_until\":",
                  sizeof(",\"propagation_until\":") - 1);
    p = ngx_http_cache_purge_api_date(p, until);
    p = ngx_sprintf(p, ",\"propagation_retained\":%s,\"issued_at\":",
                    propagation ? "true" : "false");
    p = ngx_http_cache_purge_api_date(p, issued);
    p = ngx_sprintf(p, ",\"selector\":{\"type\":\"%s\"", types[kind]);
    if (kind == 2 || kind == 3) {
        p = ngx_sprintf(p, ",\"field\":\"%s\"", kind == 2 ? "key" : "url");
    }
    count = 1;
    raw.data = value.data;
    raw.len = value.len;
    tags[0] = raw;
    if (kind > 6 && ngx_http_cache_purge_tags_decode(raw, tags, &count) != 0) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    binary = 0;
    for (i = 0; i < count; i++) {
        value.data = (u_char *) tags[i].data;
        value.len = tags[i].len;
        binary |= !ngx_http_cache_purge_api_utf8(value);
    }
    if (binary) {
        p = ngx_cpymem(p, ",\"encoding\":\"base64\"",
                      sizeof(",\"encoding\":\"base64\"") - 1);
    }
    if (kind >= 5) {
        p = ngx_sprintf(p, ",\"scope\":\"%s\",\"match\":\"any\",\"values\":[",
                        kind == 5 || kind == 7 ? "namespace" : "host");
    } else {
        p = ngx_cpymem(p, ",\"value\":", sizeof(",\"value\":") - 1);
    }
    for (i = 0; i < count; i++) {
        if (i != 0) {
            *p++ = ',';
        }
        value.data = (u_char *) tags[i].data;
        value.len = tags[i].len;
        if (binary) {
            encoded.len = ngx_base64_encoded_length(value.len);
            encoded.data = ngx_pnalloc(r->pool, encoded.len);
            if (encoded.data == NULL) {
                return NGX_HTTP_INTERNAL_SERVER_ERROR;
            }
            ngx_encode_base64(&encoded, &value);
            value = encoded;
        }
        p = ngx_http_cache_purge_api_quote(p, value);
    }
    if (kind >= 5) {
        p = ngx_cpymem(p, "]}", 2);
        p = ngx_sprintf(p, ",\"%s\":",
                        kind == 5 || kind == 7 ? "tag_namespace" : "tag_host");
        p = ngx_http_cache_purge_api_quote(p, scope);
    } else {
        *p++ = '}';
    }
    p = ngx_cpymem(p, ",\"request_digest\":",
                  sizeof(",\"request_digest\":") - 1);
    if (binding.len >= 44 && binding.data[0] == 'H' && binding.data[1] == 1
        && binding.len == (size_t) (44 + binding.data[2] + binding.data[3]))
    {
        *p++ = '"';
        p = ngx_hex_dump(p, binding.data + 12, 32);
        *p++ = '"';
    } else {
        p = ngx_cpymem(p, "null", 4);
    }
    p = ngx_sprintf(p, ",\"cleanup\":{\"state\":\"%s\",\"scan_done\":%s,"
        "\"visits\":\"%uL\",\"pending\":\"%uL\",\"failed\":\"%uL\","
        "\"checkpoint\":\"%uL\",\"last_error\":",
        stage == 4 ? "done" : failed ? "retrying" : stage == 3 ? "draining" :
        checkpoint ? "running" : "queued", done ? "true" : "false",
        visits, pending, failed, checkpoint);
    p = ngx_http_cache_purge_api_quote(p, message);
    p = ngx_cpymem(p, "},\"status_url\":", sizeof("},\"status_url\":") - 1);
    route.len = conf->api_prefix.len + sizeof("operations/") - 1 + id.len;
    route.data = ngx_pnalloc(r->pool, route.len);
    if (route.data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    identity = ngx_cpymem(route.data, conf->api_prefix.data,
                         conf->api_prefix.len);
    identity = ngx_cpymem(identity, "operations/", sizeof("operations/") - 1);
    ngx_memcpy(identity, id.data, id.len);
    p = ngx_http_cache_purge_api_quote(p, route);
    buffer->last = ngx_cpymem(p, "}\n", 2);
    return ngx_http_cache_purge_api_send(r, status, buffer);
}

static void
ngx_http_cache_purge_api_cleanup(void *data)
{
    ngx_http_cache_purge_api_t  *ctx = data;
    ctx->finished = 1;
    if (ctx->timer.timer_set) {
        ngx_del_timer(&ctx->timer);
    }
    if (ctx->call != NULL) {
        ngx_http_cache_purge_client_cancel(ctx->call);
        ctx->call = NULL;
    }
}

static void
ngx_http_cache_purge_api_finish(ngx_http_cache_purge_api_t *ctx,
    ngx_uint_t status, const char *error)
{
    ngx_int_t            rc;
    ngx_http_request_t  *r;

    if (ctx->finished) {
        return;
    }
    r = ctx->request;
    rc = ngx_http_cache_purge_api_render(ctx, status, error);
    if (rc == NGX_AGAIN && ctx->post && !r->header_sent) {
        if ((ngx_msec_int_t) (ctx->deadline - ngx_current_msec) > 10) {
            ctx->opcode = NGX_HTTP_CACHE_PURGE_OP_GET_RECEIPT;
            ngx_add_timer(&ctx->timer, 10);
            return;
        }
        rc = ngx_http_cache_purge_api_render(ctx,
            NGX_HTTP_SERVICE_UNAVAILABLE, "acceptance_pending");
    }
    if (rc == NGX_HTTP_INTERNAL_SERVER_ERROR && !r->header_sent) {
        rc = ngx_http_cache_purge_api_render(ctx,
            NGX_HTTP_SERVICE_UNAVAILABLE, "invalid_backend_response");
    }
    ctx->finished = 1;
    ngx_http_finalize_request(r, rc);
}

static ngx_int_t
ngx_http_cache_purge_api_call(ngx_http_cache_purge_api_t *ctx, uint16_t opcode)
{
    ngx_str_t       payload;
    u_char          id[132];
    ngx_msec_int_t  left;
    ngx_int_t       rc;

    left = (ngx_msec_int_t) (ctx->deadline - ngx_current_msec);
    if (left <= 0 || ctx->finished) {
        return NGX_ERROR;
    }
    ngx_str_null(&payload);
    if (opcode == NGX_HTTP_CACHE_PURGE_OP_SUBMIT_OPERATION) {
        payload = ctx->payload;
    } else if (opcode == NGX_HTTP_CACHE_PURGE_OP_GET_RECEIPT) {
        if (ctx->id.len > 128) {
            return NGX_ERROR;
        }
        payload.data = id;
        payload.len = 4 + ctx->id.len;
        ngx_http_cache_purge_api_string(id, ctx->id);
    }
    ctx->opcode = opcode;
    rc = ngx_http_cache_purge_client_submit(ctx->client, opcode, &payload,
                (ngx_msec_t) left, ngx_http_cache_purge_api_reply,
                ctx, &ctx->call);
    if (rc == NGX_OK && opcode == NGX_HTTP_CACHE_PURGE_OP_SUBMIT_OPERATION) {
        ctx->submitted = 1;
    }
    return rc;
}

static void
ngx_http_cache_purge_api_timer(ngx_event_t *event)
{
    ngx_http_cache_purge_api_t  *ctx = event->data;
    if (ngx_http_cache_purge_api_call(ctx, ctx->opcode) != NGX_OK) {
        ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_SERVICE_UNAVAILABLE,
                                       "acceptance_pending");
    }
}

static void
ngx_http_cache_purge_api_reply(ngx_http_cache_purge_call_t *call, void *data,
    const ngx_http_cache_purge_reply_t *reply)
{
    ngx_http_cache_purge_api_t  *ctx = data;
    ngx_uint_t                   status;
    size_t                       size;
    u_char                      *buffer;
    const char                  *error;

    ctx->call = NULL;
    if (ctx->finished) {
        return;
    }
    if (reply->transport != NGX_HTTP_CACHE_PURGE_CLIENT_OK) {
        ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_SERVICE_UNAVAILABLE,
                                       "backend_unavailable");
        return;
    }
    if (reply->result == NGX_HTTP_CACHE_ROCKSDB_BUSY
        && ctx->post
        && (ngx_msec_int_t) (ctx->deadline - ngx_current_msec) > 10)
    {
        ngx_add_timer(&ctx->timer, 10);
        return;
    }
    if (reply->result != 0) {
        status = NGX_HTTP_SERVICE_UNAVAILABLE;
        error = "backend_unavailable";
        if (reply->result == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            status = NGX_HTTP_NOT_FOUND;
            error = "operation_not_found";
        } else if (reply->result == NGX_HTTP_CACHE_ROCKSDB_CONFLICT) {
            status = NGX_HTTP_CONFLICT;
            error = "identity_conflict";
        } else if (reply->result == NGX_HTTP_CACHE_ROCKSDB_INVALID) {
            status = NGX_HTTP_BAD_REQUEST;
            error = "invalid_request";
        } else if (reply->result == NGX_HTTP_CACHE_ROCKSDB_LIMIT) {
            error = "capacity_exhausted";
        }
        ngx_http_cache_purge_api_finish(ctx, status, error);
        return;
    }
    if (ctx->opcode == NGX_HTTP_CACHE_PURGE_OP_SUBMIT_OPERATION) {
        if (ngx_http_cache_purge_api_call(ctx,
                NGX_HTTP_CACHE_PURGE_OP_GET_RECEIPT) != NGX_OK)
        {
            ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_SERVICE_UNAVAILABLE,
                                           "acceptance_pending");
        }
        return;
    }
    if (reply->payload.len > NGX_HTTP_CACHE_PURGE_API_REPLY) {
        ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_SERVICE_UNAVAILABLE,
                                       "invalid_backend_response");
        return;
    }
    if (ctx->capacity < reply->payload.len) {
        size = 2048;
        while (size < reply->payload.len) {
            size *= 2;
        }
        buffer = ngx_pnalloc(ctx->request->pool, size);
        if (buffer == NULL) {
            ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_INTERNAL_SERVER_ERROR,
                                           "allocation_failed");
            return;
        }
        ctx->capacity = size;
        ctx->record.data = buffer;
    }
    ctx->record.len = reply->payload.len;
    if (ctx->record.len) {
        ngx_memcpy(ctx->record.data, reply->payload.data, reply->payload.len);
    }
    ngx_http_cache_purge_api_finish(ctx,
        ctx->post ? NGX_HTTP_ACCEPTED : NGX_HTTP_OK, NULL);
}

static void
ngx_http_cache_purge_api_body(ngx_http_request_t *r)
{
    ngx_http_cleanup_t          *cleanup;
    ngx_http_cache_purge_api_t  *ctx;
    ngx_chain_t                 *cl;
    ngx_str_t                    body;
    size_t                       size;
    u_char                      *p;
    ngx_int_t                    rc;
    const char                  *error;

    ctx = NULL;
    for (cleanup = r->cleanup; cleanup != NULL; cleanup = cleanup->next) {
        if (cleanup->handler == ngx_http_cache_purge_api_cleanup) {
            ctx = cleanup->data;
            break;
        }
    }
    if (ctx == NULL || ctx->finished) {
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }
    size = 0;
    if (r->request_body == NULL || r->request_body->temp_file != NULL) {
        ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_REQUEST_ENTITY_TOO_LARGE,
                                       "body_requires_memory_buffer");
        return;
    }
    for (cl = r->request_body->bufs; cl != NULL; cl = cl->next) {
        if (ngx_buf_special(cl->buf)) {
            continue;
        }
        if (!ngx_buf_in_memory(cl->buf) || cl->buf->in_file
            || (size_t) (cl->buf->last - cl->buf->pos)
               > NGX_HTTP_CACHE_PURGE_API_BODY - size)
        {
            ngx_http_cache_purge_api_finish(ctx,
                                           NGX_HTTP_REQUEST_ENTITY_TOO_LARGE,
                                           "body_too_large");
            return;
        }
        size += cl->buf->last - cl->buf->pos;
    }
    if (size == 0) {
        ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_BAD_REQUEST,
                                       "empty_request");
        return;
    }
    body.data = ngx_pnalloc(r->pool, size);
    body.len = size;
    if (body.data == NULL) {
        ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_INTERNAL_SERVER_ERROR,
                                       "allocation_failed");
        return;
    }
    p = body.data;
    for (cl = r->request_body->bufs; cl != NULL; cl = cl->next) {
        if (!ngx_buf_special(cl->buf)) {
            p = ngx_cpymem(p, cl->buf->pos, cl->buf->last - cl->buf->pos);
        }
    }
    rc = ngx_http_cache_purge_api_prepare(ctx, body, &error);
    if (rc != NGX_OK) {
        ngx_http_cache_purge_api_finish(ctx, (ngx_uint_t) rc, error);
        return;
    }
    ctx->deadline = ngx_current_msec + NGX_HTTP_CACHE_PURGE_API_TIME;
    r->read_event_handler = ngx_http_test_reading;
    if (ngx_http_cache_purge_api_call(ctx,
            NGX_HTTP_CACHE_PURGE_OP_SUBMIT_OPERATION) != NGX_OK)
    {
        ngx_http_cache_purge_api_finish(ctx, NGX_HTTP_SERVICE_UNAVAILABLE,
                                       "backend_unavailable");
    }
}

ngx_int_t
ngx_http_cache_purge_api_handler(ngx_http_request_t *r)
{
    ngx_http_cache_purge_loc_conf_t  *conf;
    ngx_http_cache_purge_api_t       *ctx;
    ngx_http_cleanup_t               *cleanup;
    ngx_str_t                         route, content_type, content_name =
        ngx_string("Content-Type");
    ngx_table_elt_t                  *allow;
    ngx_int_t                         rc;
    ngx_uint_t                        status;
    const char                       *error, *methods;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    ctx = ngx_pcalloc(r->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    ctx->request = r;
    ctx->deadline = ngx_current_msec + NGX_HTTP_CACHE_PURGE_API_TIME;
    ctx->timer.data = ctx;
    ctx->timer.handler = ngx_http_cache_purge_api_timer;
    ctx->timer.log = r->connection->log;
    cleanup = ngx_http_cleanup_add(r, 0);
    if (cleanup == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    cleanup->handler = ngx_http_cache_purge_api_cleanup;
    cleanup->data = ctx;
    if (r != r->main) {
        return ngx_http_cache_purge_api_render(ctx, NGX_HTTP_BAD_REQUEST,
                                             "subrequest_not_supported");
    }
    if (r->args.len != 0 || r->uri.len < conf->api_prefix.len
        || ngx_memcmp(r->uri.data, conf->api_prefix.data, conf->api_prefix.len))
    {
        return ngx_http_cache_purge_api_render(ctx, NGX_HTTP_BAD_REQUEST,
                                             "invalid_route");
    }
    route.data = r->uri.data + conf->api_prefix.len;
    route.len = r->uri.len - conf->api_prefix.len;
    error = NULL;
    status = NGX_HTTP_NOT_FOUND;
    methods = "GET, HEAD";
    if (ngx_http_cache_purge_api_equal(route, "capabilities")) {
        ctx->capabilities = 1;
        ctx->opcode = NGX_HTTP_CACHE_PURGE_OP_GET_RUNTIME_STATUS;
        if (!(r->method & (NGX_HTTP_GET|NGX_HTTP_HEAD))) {
            error = "method_not_allowed"; status = NGX_HTTP_NOT_ALLOWED;
        }
    } else if (ngx_http_cache_purge_api_equal(route, "operations")) {
        methods = "POST";
        ctx->post = 1;
        if (r->method != NGX_HTTP_POST) {
            error = "method_not_allowed"; status = NGX_HTTP_NOT_ALLOWED;
        }
    } else if (route.len > sizeof("operations/") - 1
               && ngx_strncmp(route.data, "operations/",
                              sizeof("operations/") - 1) == 0)
    {
        ctx->id.data = route.data + sizeof("operations/") - 1;
        ctx->id.len = route.len - (sizeof("operations/") - 1);
        ctx->opcode = NGX_HTTP_CACHE_PURGE_OP_GET_RECEIPT;
        if (!ngx_http_cache_purge_api_id(ctx->id)) {
            error = "invalid_operation_id"; status = NGX_HTTP_BAD_REQUEST;
        } else if (!(r->method & (NGX_HTTP_GET|NGX_HTTP_HEAD))) {
            error = "method_not_allowed"; status = NGX_HTTP_NOT_ALLOWED;
        }
    } else {
        error = "route_not_found";
    }
    if (error != NULL) {
        if (status == NGX_HTTP_NOT_ALLOWED) {
            allow = ngx_list_push(&r->headers_out.headers);
            if (allow == NULL) {
                return NGX_HTTP_INTERNAL_SERVER_ERROR;
            }
            allow->hash = 1;
            ngx_str_set(&allow->key, "Allow");
            allow->value.data = (u_char *) methods;
            allow->value.len = ngx_strlen(methods);
        }
        return ngx_http_cache_purge_api_render(ctx, status, error);
    }
    if (ngx_http_cache_purge_runtime_client(r, &ctx->client) != 0) {
        return ngx_http_cache_purge_api_render(ctx,
                                             NGX_HTTP_SERVICE_UNAVAILABLE,
                                             "backend_unavailable");
    }
    if (ctx->post) {
        if (r->headers_in.content_length_n > NGX_HTTP_CACHE_PURGE_API_BODY) {
            return ngx_http_cache_purge_api_render(ctx,
                          NGX_HTTP_REQUEST_ENTITY_TOO_LARGE, "body_too_large");
        }
        if (ngx_http_cache_purge_api_header(r, content_name, &content_type)
            != NGX_OK || content_type.len < sizeof("application/json") - 1
            || ngx_strncasecmp(content_type.data,
                               (u_char *) "application/json",
                               sizeof("application/json") - 1) != 0
            || (content_type.len != sizeof("application/json") - 1
                && content_type.data[sizeof("application/json") - 1] != ';'))
        {
            return ngx_http_cache_purge_api_render(ctx,
                                NGX_HTTP_UNSUPPORTED_MEDIA_TYPE,
                                "application_json_required");
        }
        r->request_body_in_single_buf = 1;
        rc = ngx_http_read_client_request_body(r,
                                             ngx_http_cache_purge_api_body);
        return rc >= NGX_HTTP_SPECIAL_RESPONSE ? rc : NGX_DONE;
    }
    if (r->headers_in.content_length_n > 0 || r->headers_in.chunked) {
        r->keepalive = 0;
        return ngx_http_cache_purge_api_render(ctx, NGX_HTTP_BAD_REQUEST,
                                             "unexpected_body");
    }
    if (ngx_http_cache_purge_api_call(ctx, ctx->opcode) != NGX_OK) {
        return ngx_http_cache_purge_api_render(ctx,
                                             NGX_HTTP_SERVICE_UNAVAILABLE,
                                             "backend_unavailable");
    }
    r->read_event_handler = ngx_http_test_reading;
    r->main->count++;
    return NGX_DONE;
}
