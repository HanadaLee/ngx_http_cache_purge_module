#ifndef _NGX_HTTP_CACHE_PURGE_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include <nginx.h>
#include <ngx_http.h>
#include <ngx_event_connect.h>
#include "ngx_http_cache_purge_internal.h"

/* Copyright (C) Hanada */


typedef struct ngx_http_cache_purge_client_s  ngx_http_cache_purge_client_t;
typedef struct ngx_http_cache_purge_call_s   ngx_http_cache_purge_call_t;

ngx_int_t ngx_http_cache_purge_request_identity(ngx_http_request_t *r,
    ngx_str_t *url, ngx_str_t *misc, ngx_str_t *namespace, ngx_str_t *host,
    ngx_str_t *key);
ngx_int_t ngx_http_cache_purge_method_handler(ngx_http_request_t *r);

typedef enum {
    NGX_HTTP_CACHE_PURGE_CLIENT_OK = 0,
    NGX_HTTP_CACHE_PURGE_CLIENT_IO,
    NGX_HTTP_CACHE_PURGE_CLIENT_TIMEOUT,
    NGX_HTTP_CACHE_PURGE_CLIENT_PROTOCOL,
    NGX_HTTP_CACHE_PURGE_CLIENT_LIMIT,
    NGX_HTTP_CACHE_PURGE_CLIENT_OWNER,
    NGX_HTTP_CACHE_PURGE_CLIENT_STOPPED
} ngx_http_cache_purge_client_result_e;

typedef struct {
    ngx_uint_t  max_pending;
    size_t      request_bytes;
    size_t      max_payload;
    ngx_msec_t  idle_timeout;
    uid_t       server_uid;
} ngx_http_cache_purge_client_options_t;

typedef struct {
    uint64_t    accepted, completed, cancelled, rejected, connections;
    ngx_uint_t  pending;
    size_t      request_bytes, response_bytes, peak_bytes;
} ngx_http_cache_purge_client_stats_t;

typedef struct {
    ngx_http_cache_purge_client_result_e transport;
    uint32_t   result; /* Component result, meaningful only for transport OK. */
    ngx_str_t  payload; /* Borrowed bytes, valid only inside the callback. */

    /*
     * Borrowed OFD lease, or -1. Callback may dup with CLOEXEC; the
     * consumer must bound/close its retained references.
     */
    int        lease_fd;

    unsigned may_have_executed : 1;
} ngx_http_cache_purge_reply_t;

typedef void (*ngx_http_cache_purge_client_done_pt)(
    ngx_http_cache_purge_call_t *call, void *data,
    const ngx_http_cache_purge_reply_t *reply);

void ngx_http_cache_purge_client_options(
    ngx_http_cache_purge_client_options_t *options);
ngx_http_cache_purge_client_t *ngx_http_cache_purge_client_create(
    ngx_pool_t *pool, ngx_log_t *log, const ngx_str_t *socket_path,
    const ngx_http_cache_purge_client_options_t *options);

/* Worker event loop only; no HTTP request retention is implicit.
 * OK accepts a copied payload and guarantees a later callback, unless
 * cancelled.
 * BUSY/ERROR accepts nothing and invokes no callback. The deadline includes
 * queueing. No request is automatically replayed after a transport failure.
 * Callers must retain callback data until completion or cancel from cleanup. */
ngx_int_t ngx_http_cache_purge_client_submit(
    ngx_http_cache_purge_client_t *client, uint16_t opcode,
    const ngx_str_t *payload, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **call);

/* PREPARE_FILE accepts two borrowed input FDs (lease, complete temp file).
 * Successful submit copies them with CLOEXEC; the caller keeps its originals.
 * Copies are bounded by 2 * max_pending and released on send/cancel/failure.
 * FINISH_FILE separately accepts one lease descriptor. */
ngx_int_t ngx_http_cache_purge_client_submit_files(
    ngx_http_cache_purge_client_t *client, const ngx_str_t *payload,
    const int fds[2], ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **call);
/* FINISH_FILE copies one lease FD and otherwise follows submit_files ownership.
 * Invoke only after native writing has ceased; the result may be RETIRED. */
ngx_int_t ngx_http_cache_purge_client_finish_file(
    ngx_http_cache_purge_client_t *client, const ngx_str_t *payload,
    int lease_fd, ngx_msec_t timeout, ngx_http_cache_purge_client_done_pt done,
    void *data, ngx_http_cache_purge_call_t **call);

/* INSPECT_FILE copies the selected cache FD, with the same bounded ownership
 * as submit_files. Success proves physical publication, not purge freshness. */
ngx_int_t ngx_http_cache_purge_client_inspect_file(
    ngx_http_cache_purge_client_t *client, const ngx_str_t *payload,
    int cache_fd, ngx_msec_t timeout, ngx_http_cache_purge_client_done_pt done,
    void *data, ngx_http_cache_purge_call_t **call);

/* VALIDATE_FILE copies the lease and selected cache FD. A lost reply can leave
 * an accepted transaction for recovery; cancellation does not roll it back. */
ngx_int_t ngx_http_cache_purge_client_validate_file(
    ngx_http_cache_purge_client_t *client, const ngx_str_t *payload,
    const int fds[2], ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **call);

/* Retire only the selected native header/instance, then let the existing
 * persistent retired-file collector finish physical deletion and accounting. */
ngx_int_t ngx_http_cache_purge_client_retire_selected(
    ngx_http_cache_purge_client_t *client, const ngx_str_t *payload,
    int cache_fd, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **call);
/* Observe this request's URL/host and verify every persisted alias. The reply
 * carries the publication stamp required by the final shared-memory guard. */
ngx_int_t ngx_http_cache_purge_client_admit_file(
    ngx_http_cache_purge_client_t *client, const ngx_str_t *payload,
    int cache_fd, ngx_msec_t timeout,
    ngx_http_cache_purge_client_done_pt done, void *data,
    ngx_http_cache_purge_call_t **call);

/* Cancel consumes the handle and suppresses its callback. Cancellation does
 * not roll back a request already sent. Clear the caller's handle afterwards.
 * */
void ngx_http_cache_purge_client_cancel(ngx_http_cache_purge_call_t *call);
/* stop rejects new work, closes the socket and completes outstanding calls
 * synchronously with STOPPED. Invoke from exit_process, not from a thread. */
void ngx_http_cache_purge_client_stop(ngx_http_cache_purge_client_t *client);
void
ngx_http_cache_purge_client_stats(ngx_http_cache_purge_client_t *client,
    ngx_http_cache_purge_client_stats_t *stats);

/* Copyright (C) Hanada */


typedef struct ngx_http_cache_purge_runtime_s  ngx_http_cache_purge_runtime_t;

ngx_http_cache_purge_runtime_t *
ngx_http_cache_purge_runtime_create(ngx_conf_t *cf);
ngx_flag_t ngx_http_cache_purge_runtime_configured(ngx_conf_t *cf);
char      *ngx_http_cache_purge_store_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
char      *ngx_http_cache_purge_state_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
char *ngx_http_cache_purge_propagation_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
ngx_int_t ngx_http_cache_purge_runtime_post(ngx_conf_t *cf);
ngx_int_t ngx_http_cache_purge_runtime_module_init(ngx_cycle_t *cycle);
ngx_int_t ngx_http_cache_purge_capture_ready(ngx_cycle_t *cycle);
ngx_int_t ngx_http_cache_purge_runtime_process_init(ngx_cycle_t *cycle);
void      ngx_http_cache_purge_runtime_worker_exit(ngx_cycle_t *cycle);

/* Internal native-hook entry: lazily register this worker's reader. No DB I/O.
 * NOT_FOUND means the persistent runtime was not configured; BUSY must never
 * be interpreted as a cache miss or as permission to serve an unchecked HIT. */
int ngx_http_cache_purge_runtime_reader(ngx_http_request_t *r,
    ngx_http_cache_purge_control_reader_t **reader,
    ngx_http_cache_rocksdb_error_t *error);

/* Lazily create this worker's bounded IPC client; no connect or DB I/O here.
 * Returns NOT_FOUND when no persistent runtime is configured. */
int ngx_http_cache_purge_runtime_client(ngx_http_request_t *r,
    ngx_http_cache_purge_client_t **client);
int ngx_http_cache_purge_runtime_read_client(ngx_http_request_t *r,
    ngx_http_cache_purge_client_t **client);


extern ngx_module_t  ngx_http_cache_purge_module;


typedef struct {
    ngx_http_cache_purge_runtime_t  *runtime;
} ngx_http_cache_purge_main_conf_t;


typedef struct {
    ngx_str_t     api_prefix;
    ngx_str_t     tag_namespace_header;
    ngx_str_t     propagation_header[3];
    ngx_str_t     method_header[5];
#if (NGX_EXPR)
    ngx_array_t  *ignore_propagation;
    ngx_array_t  *propagation;
#else
    ngx_flag_t    ignore_propagation;
    ngx_flag_t    propagation;
#endif
} ngx_http_cache_purge_loc_conf_t;


char *ngx_http_cache_purge_api_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
char *ngx_http_cache_purge_api_header_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
char *ngx_http_cache_purge_api_merge(ngx_conf_t *cf,
    ngx_http_cache_purge_loc_conf_t *prev,
    ngx_http_cache_purge_loc_conf_t *conf);
ngx_int_t ngx_http_cache_purge_api_handler(ngx_http_request_t *r);
ngx_flag_t ngx_http_cache_purge_ignore_propagation(ngx_http_request_t *r);
ngx_flag_t ngx_http_cache_purge_propagation_enabled(ngx_http_request_t *r);
ngx_flag_t ngx_http_cache_purge_propagation_possible(ngx_http_request_t *r);


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
ngx_int_t ngx_http_cache_purge_fill_arm(ngx_http_request_t *r,
    ngx_http_cache_purge_fill_t **slot, ngx_http_cache_purge_fill_step_pt step,
    void *data);
void      ngx_http_cache_purge_fill_complete(ngx_http_cache_purge_fill_t *fill,
    ngx_int_t result);


/* Use exported core layouts where available; keep old-ABI adapters together. */

#if (NGX_HTTP_FASTCGI)
extern ngx_module_t ngx_http_fastcgi_module;

#if (nginx_version >= 1007009)
typedef struct {
    ngx_array_t caches;
} ngx_http_fastcgi_main_conf_t;
#endif

#if (nginx_version >= 1007008)
typedef struct {
    ngx_array_t  *flushes;
    ngx_array_t  *lengths;
    ngx_array_t  *values;
    ngx_uint_t    number;
    ngx_hash_t    hash;
} ngx_http_fastcgi_params_t;
#endif

typedef struct {
    ngx_http_upstream_conf_t  upstream;
    ngx_str_t                 index;

#if (nginx_version >= 1007008)
    ngx_http_fastcgi_params_t  params;
    ngx_http_fastcgi_params_t  params_cache;
#else
    ngx_array_t  *flushes;
    ngx_array_t  *params_len;
    ngx_array_t  *params;
#endif

    ngx_array_t  *params_source;
    ngx_array_t  *catch_stderr;
    ngx_array_t  *fastcgi_lengths;
    ngx_array_t  *fastcgi_values;

#if (nginx_version >= 8040) && (nginx_version < 1007008)
    ngx_hash_t  headers_hash;
    ngx_uint_t  header_params;
#endif

#if (nginx_version >= 1001004)
    ngx_flag_t keep_conn;
#endif

#if (NGX_RESTY_EXT)
    ngx_flag_t params_inherit;
#endif

    ngx_http_complex_value_t cache_key;

#if (NGX_PCRE)
    ngx_regex_t  *split_regex;
    ngx_str_t     split_name;
#endif
} ngx_http_fastcgi_loc_conf_t;

#endif

#if (NGX_HTTP_PROXY)
#if (nginx_version >= 1029004)
/* Prefer the exported nginx header to a private definition. */
#include <ngx_http_proxy_module.h>
#else
extern ngx_module_t ngx_http_proxy_module;

typedef struct {
    ngx_str_t  key_start;
    ngx_str_t  schema;
    ngx_str_t  host_header;
    ngx_str_t  port;
    ngx_str_t  uri;
} ngx_http_proxy_vars_t;

#if (nginx_version >= 1007009)
typedef struct {
    ngx_array_t caches;
} ngx_http_proxy_main_conf_t;
#endif

#if (nginx_version >= 1007008)
typedef struct {
    ngx_array_t  *flushes;
    ngx_array_t  *lengths;
    ngx_array_t  *values;
    ngx_hash_t    hash;
} ngx_http_proxy_headers_t;
#endif

typedef struct {
    ngx_http_upstream_conf_t upstream;

#if (nginx_version >= 1007008)
    ngx_array_t              *body_flushes;
    ngx_array_t              *body_lengths;
    ngx_array_t              *body_values;
    ngx_str_t                 body_source;
    ngx_http_proxy_headers_t  headers;
    ngx_http_proxy_headers_t  headers_cache;
#else
    ngx_array_t  *flushes;
    ngx_array_t  *body_set_len;
    ngx_array_t  *body_set;
    ngx_array_t  *headers_set_len;
    ngx_array_t  *headers_set;
    ngx_hash_t    headers_set_hash;
#endif

    ngx_array_t *headers_source;
#if (nginx_version < 8040)
    ngx_array_t *headers_names;
#endif

    ngx_array_t  *proxy_lengths;
    ngx_array_t  *proxy_values;
    ngx_array_t  *redirects;

#if (nginx_version >= 1001015)
    ngx_array_t  *cookie_domains;
    ngx_array_t  *cookie_paths;
#endif
#if (nginx_version >= 1019003)
    ngx_array_t *cookie_flags;
#endif
#if (nginx_version < 1007008)
    ngx_str_t body_source;
#endif

#if (nginx_version >= 1011006)
    ngx_http_complex_value_t *method;
#else
    ngx_str_t method;
#endif
    ngx_str_t  location;
    ngx_str_t  url;

    ngx_http_complex_value_t  cache_key;
    ngx_http_proxy_vars_t     vars;
    ngx_flag_t                redirect;

#if (nginx_version >= 1001004)
    ngx_uint_t http_version;
#endif

    ngx_uint_t  headers_hash_max_size;
    ngx_uint_t  headers_hash_bucket_size;

#if (NGX_HTTP_SSL)
#if (nginx_version >= 1005006)
    ngx_uint_t  ssl;
    ngx_uint_t  ssl_protocols;
    ngx_str_t   ssl_ciphers;
#endif
#if (nginx_version >= 1007000)
    ngx_uint_t  ssl_verify_depth;
    ngx_str_t   ssl_trusted_certificate;
    ngx_str_t   ssl_crl;
#endif
#if (nginx_version >= 1007008) && (nginx_version < 1021000)
    ngx_str_t     ssl_certificate;
    ngx_str_t     ssl_certificate_key;
    ngx_array_t  *ssl_passwords;
#endif
#if (nginx_version >= 1019004)
    ngx_array_t *ssl_conf_commands;
#endif
#endif
} ngx_http_proxy_loc_conf_t;
#endif /* nginx_version >= 1029004 */

#endif

#if (NGX_HTTP_SCGI)
extern ngx_module_t ngx_http_scgi_module;

#if (nginx_version >= 1007009)
typedef struct {
    ngx_array_t caches;
} ngx_http_scgi_main_conf_t;
#endif

#if (nginx_version >= 1007008)
typedef struct {
    ngx_array_t  *flushes;
    ngx_array_t  *lengths;
    ngx_array_t  *values;
    ngx_uint_t    number;
    ngx_hash_t    hash;
} ngx_http_scgi_params_t;
#endif

typedef struct {
    ngx_http_upstream_conf_t upstream;

#if (nginx_version >= 1007008)
    ngx_http_scgi_params_t  params;
    ngx_http_scgi_params_t  params_cache;
    ngx_array_t            *params_source;
#else
    ngx_array_t  *flushes;
    ngx_array_t  *params_len;
    ngx_array_t  *params;
    ngx_array_t  *params_source;
    ngx_hash_t    headers_hash;
    ngx_uint_t    header_params;
#endif

    ngx_array_t              *scgi_lengths;
    ngx_array_t              *scgi_values;
    ngx_http_complex_value_t  cache_key;

#if (NGX_RESTY_EXT)
    ngx_flag_t params_inherit;
#endif
} ngx_http_scgi_loc_conf_t;

#endif

#if (NGX_HTTP_UWSGI)
extern ngx_module_t ngx_http_uwsgi_module;

#if (nginx_version >= 1007009)
typedef struct {
    ngx_array_t caches;
} ngx_http_uwsgi_main_conf_t;
#endif

#if (nginx_version >= 1007008)
typedef struct {
    ngx_array_t  *flushes;
    ngx_array_t  *lengths;
    ngx_array_t  *values;
    ngx_uint_t    number;
    ngx_hash_t    hash;
} ngx_http_uwsgi_params_t;
#endif

typedef struct {
    ngx_http_upstream_conf_t upstream;

#if (nginx_version >= 1007008)
    ngx_http_uwsgi_params_t  params;
    ngx_http_uwsgi_params_t  params_cache;
    ngx_array_t             *params_source;
#else
    ngx_array_t  *flushes;
    ngx_array_t  *params_len;
    ngx_array_t  *params;
    ngx_array_t  *params_source;
    ngx_hash_t    headers_hash;
    ngx_uint_t    header_params;
#endif

    ngx_array_t              *uwsgi_lengths;
    ngx_array_t              *uwsgi_values;
    ngx_http_complex_value_t  cache_key;

#if (NGX_RESTY_EXT)
    ngx_flag_t params_inherit;
#endif

    ngx_str_t                 uwsgi_string;
    ngx_uint_t                modifier1;
    ngx_uint_t                modifier2;

#if (NGX_HTTP_SSL)
#if (nginx_version >= 1005008)
    ngx_uint_t  ssl;
    ngx_uint_t  ssl_protocols;
    ngx_str_t   ssl_ciphers;
#endif
#if (nginx_version >= 1007000)
    ngx_uint_t  ssl_verify_depth;
    ngx_str_t   ssl_trusted_certificate;
    ngx_str_t   ssl_crl;
#endif
#if (nginx_version >= 1007008) && (nginx_version < 1021000)
    ngx_str_t     ssl_certificate;
    ngx_str_t     ssl_certificate_key;
    ngx_array_t  *ssl_passwords;
#endif
#if (nginx_version >= 1019004)
    ngx_array_t *ssl_conf_commands;
#endif
#endif
} ngx_http_uwsgi_loc_conf_t;

#endif


#endif /* _NGX_HTTP_CACHE_PURGE_H_INCLUDED_ */
