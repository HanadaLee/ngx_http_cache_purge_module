/* Copyright (C) Hanada */

#ifndef _NGX_HTTP_CACHE_PURGE_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_H_INCLUDED_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_http_cache_purge_internal.h"


typedef struct {
    ngx_file_uniq_t  uniq;
    off_t            size;
    ngx_err_t        error;
} ngx_http_cache_purge_native_file_t;

/* Background-only native file operations. No module metadata is stored. */
ngx_int_t ngx_http_cache_purge_native_file(const char *name,
    ngx_uint_t  invalidate, ngx_file_uniq_t expected,
    ngx_http_cache_purge_native_file_t *result);

enum {
    NGX_HTTP_CACHE_PURGE_NODE_READ = 0,
    NGX_HTTP_CACHE_PURGE_NODE_INVALIDATE,
    NGX_HTTP_CACHE_PURGE_NODE_DELETE,
    NGX_HTTP_CACHE_PURGE_NODE_CLEAR,
    NGX_HTTP_CACHE_PURGE_NODE_CLEAR_ERROR
};

ngx_int_t ngx_http_cache_purge_native_node(ngx_http_file_cache_t *cache,
    const u_char  *key, ngx_file_uniq_t expected, ngx_uint_t action);
ngx_int_t ngx_http_cache_purge_native_identity(ngx_http_file_cache_t *cache,
    const u_char  *key, ngx_file_uniq_t *uniq);

typedef struct ngx_http_cache_purge_shared_s   ngx_http_cache_purge_shared_t;
typedef struct ngx_http_cache_purge_message_s  ngx_http_cache_purge_message_t;

typedef struct {
    ngx_slab_pool_t                *pool;
    ngx_http_cache_purge_shared_t  *shared;
    ngx_socket_t                    channel[2];
} ngx_http_cache_purge_state_t;

typedef struct {
    uint64_t  delete_before;
    uint64_t  invalidate_before;
    uint64_t  propagate_delete;
    uint64_t  propagate_invalidate;
} ngx_http_cache_purge_match_t;

enum {
    NGX_HTTP_CACHE_PURGE_RECORD = 0,
    NGX_HTTP_CACHE_PURGE_SUBMIT,
    NGX_HTTP_CACHE_PURGE_STATUS,
    NGX_HTTP_CACHE_PURGE_LOOKUP
};

#define NGX_HTTP_CACHE_PURGE_MESSAGE_MAX  (256 * 1024)

/* Queued work owns its strings in the trailing allocation. It contains no
 * request, cache-node, shared-memory or database pointers. */
typedef struct {
    ngx_uint_t                    type;
    size_t                        size;
    ngx_http_cache_purge_match_t  match;
    union {
        ngx_http_cache_purge_entry_t  entry;
        ngx_http_cache_purge_job_t    job;
    } data;
} ngx_http_cache_purge_work_t;

/* zone->data points to a cycle-owned state context. */
ngx_int_t ngx_http_cache_purge_state_init(ngx_shm_zone_t *zone, void *old);
ngx_int_t ngx_http_cache_purge_state_enqueue(
    ngx_http_cache_purge_state_t  *state, ngx_uint_t type, const void *input,
    ngx_http_cache_purge_message_t **message);

/* Only the database owner consumes work. The caller supplies an
 * aligned MESSAGE_MAX-byte private buffer; task completion runs on its event
 * loop. Record-only batches may complete on the database thread.
 * Detached submissions remain accepted and continue in the background.
 * Completion/poll output is a match for LOOKUP, otherwise a job. */
ngx_int_t ngx_http_cache_purge_state_take(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_work_t  *work,
    ngx_http_cache_purge_message_t **message);
void ngx_http_cache_purge_state_complete(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_message_t  *message, int result,
    const void *output);
ngx_int_t ngx_http_cache_purge_state_poll(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_message_t  **message, int *result,
    void *output);
void ngx_http_cache_purge_state_detach(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_message_t **message);

ngx_int_t ngx_http_cache_purge_state_install(
    ngx_http_cache_purge_state_t  *state,
    const ngx_http_cache_purge_job_t *job);
void ngx_http_cache_purge_state_finish(ngx_http_cache_purge_state_t *state,
    const u_char  id[16]);
ngx_int_t ngx_http_cache_purge_state_match(ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_entry_t  *identity, uint64_t now,
    ngx_http_cache_purge_match_t *match);
ngx_int_t ngx_http_cache_purge_state_match_local(
    ngx_http_cache_purge_state_t        *state,
    const ngx_http_cache_purge_entry_t  *identity, uint64_t now,
    ngx_http_cache_purge_match_t *match);
ngx_int_t ngx_http_cache_purge_state_match_url(
    ngx_http_cache_purge_state_t        *state,
    const ngx_http_cache_purge_entry_t  *identity, uint64_t now,
    ngx_http_cache_purge_match_t *match);
void ngx_http_cache_purge_state_collect(ngx_http_cache_purge_state_t *state,
    uint64_t now);

void ngx_http_cache_purge_state_stats(ngx_http_cache_purge_state_t *state,
    ngx_uint_t  *rules, ngx_uint_t *messages, size_t *bytes);
void ngx_http_cache_purge_state_cleanup_stats(
    ngx_http_cache_purge_state_t *state, uint64_t stats[3]);

typedef struct ngx_http_cache_purge_owner_s  ngx_http_cache_purge_owner_t;

/* Create the notification pipe in init_module, before workers fork. */
ngx_int_t ngx_http_cache_purge_channel_init(ngx_cycle_t *cycle,
    ngx_http_cache_purge_state_t *state);
void ngx_http_cache_purge_channel_close(void *data);
ngx_uint_t ngx_http_cache_purge_state_ready(
    ngx_http_cache_purge_state_t *state);
ngx_int_t ngx_http_cache_purge_owner_start(ngx_cycle_t *cycle,
    ngx_http_cache_purge_state_t            *state, const ngx_str_t *path,
    const ngx_http_cache_rocksdb_options_t  *options,
    ngx_http_file_cache_t                  **caches, ngx_uint_t count,
    ngx_msec_t propagation, ngx_uint_t threaded,
    ngx_http_cache_purge_owner_t **owner);
void ngx_http_cache_purge_owner_stop(ngx_http_cache_purge_owner_t *owner);

#if (NGX_EXPR)
#include <ngx_http_expr_module.h>
typedef ngx_array_t   ngx_http_cache_purge_value_t;
typedef ngx_array_t  *ngx_http_cache_purge_flag_t;
#else
typedef ngx_http_complex_value_t  ngx_http_cache_purge_value_t;
typedef ngx_flag_t                ngx_http_cache_purge_flag_t;
#endif

enum {
    NGX_HTTP_CACHE_PURGE_URL = 0,
    NGX_HTTP_CACHE_PURGE_MISC,
    NGX_HTTP_CACHE_PURGE_NAMESPACE,
    NGX_HTTP_CACHE_PURGE_TAG,
    NGX_HTTP_CACHE_PURGE_TYPE_HEADER,
    NGX_HTTP_CACHE_PURGE_ACTION_HEADER,
    NGX_HTTP_CACHE_PURGE_TAGS_HEADER,
    NGX_HTTP_CACHE_PURGE_ID_HEADER,
    NGX_HTTP_CACHE_PURGE_ISSUED_HEADER,
    NGX_HTTP_CACHE_PURGE_TIME_HEADER,
    NGX_HTTP_CACHE_PURGE_PROPAGATION_ACTION_HEADER,
    NGX_HTTP_CACHE_PURGE_VALUES
};

typedef void (*ngx_http_cache_purge_finalize_pt)(ngx_http_request_t *r,
    ngx_int_t rc);
typedef ngx_int_t (*ngx_http_cache_purge_create_pt)(ngx_http_request_t *r);

typedef struct {
    ngx_http_cache_purge_state_t       state;
    ngx_http_cache_purge_owner_t      *owner;
    ngx_path_t                        *path;
    ngx_shm_zone_t                    *zone;
    ngx_msec_t                         propagation;
    ngx_http_cache_rocksdb_options_t   options;
    ngx_array_t                        caches;
    ngx_array_t                        locations;
    ngx_http_complex_value_t           defaults[NGX_HTTP_CACHE_PURGE_VALUES];
    ngx_http_complex_value_t           guard[4];
    ngx_buf_tag_t                      protocol[4];
    ngx_http_cache_purge_finalize_pt   finalize[4];
    ngx_http_cache_purge_create_pt     create[4];
} ngx_http_cache_purge_main_conf_t;

typedef struct {
    ngx_http_cache_purge_value_t  *value[NGX_HTTP_CACHE_PURGE_VALUES];
    ngx_http_cache_purge_flag_t    flag[6];
    ngx_uint_t                     url_set;
    ngx_str_t                      api;
} ngx_http_cache_purge_loc_conf_t;

extern ngx_module_t  ngx_http_cache_purge_module;

ngx_int_t ngx_http_cache_purge_variables(ngx_conf_t *cf);
ngx_int_t ngx_http_cache_purge_request_init(ngx_conf_t *cf);
ngx_int_t ngx_http_cache_purge_api(ngx_http_request_t *r);
ngx_flag_t ngx_http_cache_purge_flag(ngx_http_request_t *r, ngx_uint_t n);
ngx_int_t ngx_http_cache_purge_value(ngx_http_request_t *r, ngx_uint_t n,
    ngx_str_t *value);
ngx_uint_t ngx_http_cache_purge_state_active(
    ngx_http_cache_purge_state_t *state);
ngx_uint_t ngx_http_cache_purge_state_filtering(
    ngx_http_cache_purge_state_t *state, uint64_t date);
ngx_uint_t ngx_http_cache_purge_state_complex(
    ngx_http_cache_purge_state_t *state);
ngx_uint_t ngx_http_cache_purge_state_tagged(
    ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_entry_t *entry, uint64_t now);


#endif /* _NGX_HTTP_CACHE_PURGE_H_INCLUDED_ */
