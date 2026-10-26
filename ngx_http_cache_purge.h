#ifndef _NGX_HTTP_CACHE_PURGE_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_H_INCLUDED_

/* Private contracts shared by the module's separately compiled components. */
#include <ngx_config.h>
#include <ngx_core.h>
#include <nginx.h>
#include <ngx_http.h>

#define NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_HTML  1
#define NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_XML   2
#define NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_JSON  3
#define NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_TEXT  4
#define NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_NONE  5

#define NGX_HTTP_CACHE_PURGE_QUEUE_SIZE_DEFAULT   1024
#define NGX_HTTP_CACHE_PURGE_BATCH_SIZE_DEFAULT   10
#define NGX_HTTP_CACHE_PURGE_BATCH_SIZE_MAX       64
/*
 * A bare integer denotes milliseconds, as does an explicit ms ending.
 */
#define NGX_HTTP_CACHE_PURGE_THROTTLE_MS_DEFAULT  10  /* milliseconds */
#define NGX_HTTP_CACHE_PURGE_KEY_MAX_LEN          512
#define NGX_HTTP_CACHE_PURGE_ENTRY_KEY_MAX        8192
#define NGX_HTTP_CACHE_PURGE_STATUS_BODY_MAX      8192
/* Widest "namespaces" entry (32-byte name) and the closing text after the
 * last entry; the list stops early and says so instead of failing the reply. */
#define NGX_HTTP_CACHE_PURGE_STATUS_NS_MAX        160
#define NGX_HTTP_CACHE_PURGE_STATUS_NS_TAIL       48
#define NGX_HTTP_CACHE_PURGE_SHM_VERSION          7
#define NGX_HTTP_CACHE_PURGE_REFRESH_BATCH        32
#define NGX_HTTP_CACHE_PURGE_REFRESH_PROBES       16
#define NGX_HTTP_CACHE_PURGE_QUEUE_TIMEOUT        60000   /* ms */
#define NGX_HTTP_CACHE_PURGE_WALK_ENTRIES         64
#define NGX_HTTP_CACHE_PURGE_WALK_MSEC            5
/*
 * Cache file format:
 *
 *     binary header | "\nKEY: " | key | "\n" | cached response
 *
 * The offset skips the six-byte marker after the binary header.
 */
#define NGX_HTTP_CACHE_PURGE_KEY_HDR_OFFSET       6

/*
 * Reserve at least eight native pages for slab metadata and task storage.
 * The exact slab overhead depends on the host and nginx version.
 */
#define NGX_HTTP_CACHE_PURGE_SHM_MIN_PAGES        8


#if (NGX_HTTP_CACHE)
#if (NGX_HTTP_CACHE_PURGE_ROCKSDB)
#include <ngx_event_connect.h>
#include "ngx_http_cache_purge_internal.h"

/* Copyright (C) Hanada */


typedef struct ngx_http_cache_purge_client_s  ngx_http_cache_purge_client_t;
typedef struct ngx_http_cache_purge_call_s   ngx_http_cache_purge_call_t;

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
#endif
/* Generation state is separate from the task deduplication rbtree. */
typedef struct ngx_http_cache_purge_generation_s
    ngx_http_cache_purge_generation_t;

typedef struct {
    ngx_str_t                           name;
    ngx_shm_zone_t                     *zone;
    ngx_path_t                         *state_path;
    ngx_pool_t                         *pool;
    ngx_http_cache_purge_generation_t  *shared;
} ngx_http_cache_purge_namespace_t;

#define NGX_HTTP_CACHE_PURGE_LOGICAL_KEY_MAX  8096

ngx_http_cache_purge_namespace_t *
ngx_http_cache_purge_namespace_create(ngx_conf_t *cf, ngx_str_t *name,
    size_t size, const ngx_str_t *state_path);
ngx_int_t
ngx_http_cache_purge_generation_key(ngx_http_request_t *r,
    ngx_http_cache_purge_namespace_t *ns, ngx_http_complex_value_t *expression,
    ngx_str_t *logical, ngx_str_t *encoded);
ngx_int_t
ngx_http_cache_purge_generation_invalidate(ngx_http_cache_purge_namespace_t *ns,
    ngx_str_t *key, ngx_flag_t all, ngx_str_t *expected);
ngx_int_t
ngx_http_cache_purge_generation_stale(ngx_http_cache_purge_namespace_t *ns,
    ngx_str_t *key);

ngx_int_t
ngx_http_cache_purge_generation_current(ngx_http_cache_purge_namespace_t *ns,
    ngx_str_t *key, ngx_str_t *logical);

/* Consistent copy of the namespace generation state, for status reporting. */
typedef struct {
    u_char      epoch[10];
    uint64_t    serial;
    uint64_t    baseline;
    ngx_flag_t  rules;
} ngx_http_cache_purge_generation_view_t;

void ngx_http_cache_purge_generation_view(ngx_http_cache_purge_namespace_t *ns,
    ngx_http_cache_purge_generation_view_t *view);
#if (NGX_WIN32)
/* Windows filesystem helpers. */
static ngx_inline ngx_uint_t
ngx_http_cache_purge_process_alive(ngx_pid_t pid)
{
    HANDLE process;
    DWORD  state;

    process = OpenProcess(SYNCHRONIZE, 0, pid);
    if (process == NULL) {
        return GetLastError() != ERROR_INVALID_PARAMETER;
    }
    state = WaitForSingleObject(process, 0);
    CloseHandle(process);
    return state != WAIT_OBJECT_0;
}


static ngx_inline ngx_uint_t
ngx_http_cache_purge_path_separator(u_char c)
{
    return c == '/' || c == '\\';
}


static ngx_inline u_char
ngx_http_cache_purge_path_char(u_char c)
{
    if (ngx_http_cache_purge_path_separator(c)) {
        return '/';
    }

    return ngx_tolower(c);
}


static ngx_inline size_t
ngx_http_cache_purge_path_min_length(const ngx_str_t *path)
{
    return (path->len >= 3 && path->data[1] == ':'
            && ngx_http_cache_purge_path_separator(path->data[2])) ?
               3 :
               1;
}


/* Directory entries carry metadata; reparse points are skipped. */
static ngx_inline ngx_int_t
ngx_http_cache_purge_de_info(ngx_str_t *path, ngx_dir_t *dir,
    const char **failed)
{
    (void) path;
    (void) failed;

    if (dir->finddata.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) {
        return NGX_DECLINED;
    }

    return NGX_OK;
}


static ngx_inline ngx_int_t
ngx_http_cache_purge_file_info(ngx_str_t *path, ngx_dir_t *dir,
    const char **failed)
{
    return ngx_http_cache_purge_de_info(path, dir, failed) == NGX_OK
                   && ngx_de_is_file(dir) ?
               NGX_OK :
               NGX_DECLINED;
}


static ngx_inline ngx_int_t
ngx_http_cache_purge_open_dir(ngx_str_t *path, ngx_dir_t *parent,
    ngx_dir_t *dir, const char **failed)
{
    ngx_file_info_t info;

    if (parent != NULL) {
        *failed = ngx_link_info_n;
        if (ngx_link_info(path->data, &info) == NGX_FILE_ERROR) {
            return NGX_ERROR;
        }
        if (!ngx_is_dir(&info)
            || (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT))
        {
            return NGX_DECLINED;
        }
    }

    *failed = ngx_open_dir_n;
    return ngx_open_dir(path, dir);
}


static ngx_inline ngx_fd_t
ngx_http_cache_purge_open_entry(ngx_str_t *path, ngx_dir_t *parent,
    const char **failed)
{
    (void) parent;

    *failed = ngx_open_file_n;
    return ngx_open_file(path->data, NGX_FILE_RDONLY, NGX_FILE_OPEN, 0);
}


static ngx_inline ngx_int_t
ngx_http_cache_purge_delete_entry(u_char *name, ngx_dir_t *parent,
    const char **failed)
{
    (void) parent;

    *failed = ngx_delete_file_n;
    return ngx_delete_file(name);
}
#else
/* POSIX filesystem helpers. */
static ngx_inline ngx_uint_t
ngx_http_cache_purge_process_alive(ngx_pid_t pid)
{
    return kill(pid, 0) == 0 || ngx_errno != NGX_ESRCH;
}


static ngx_inline ngx_uint_t
ngx_http_cache_purge_path_separator(u_char c)
{
    return c == '/';
}


static ngx_inline u_char
ngx_http_cache_purge_path_char(u_char c)
{
#if (NGX_HAVE_CASELESS_FILESYSTEM)
    return ngx_tolower(c);
#else
    return c;
#endif
}


static ngx_inline size_t
ngx_http_cache_purge_path_min_length(const ngx_str_t *path)
{
    (void) path;

    return 1;
}


/* Known entry types avoid stat calls; links are never followed. */
static ngx_inline ngx_int_t
ngx_http_cache_purge_de_info(ngx_str_t *path, ngx_dir_t *dir,
    const char **failed)
{
#if (NGX_HAVE_OPENAT && NGX_HTTP_CACHE_PURGE_HAVE_AT)
    (void) path;
#endif
    if (!dir->valid_info
#if (NGX_HAVE_D_TYPE)
        && dir->type == 0
#endif
    )
    {
#if (NGX_HAVE_OPENAT && NGX_HTTP_CACHE_PURGE_HAVE_AT)
        *failed = ngx_file_at_info_n;
        if (ngx_file_at_info(dirfd(dir->dir), ngx_de_name(dir), &dir->info,
                             AT_SYMLINK_NOFOLLOW)
            == NGX_FILE_ERROR)
#else
        *failed = ngx_de_link_info_n;
        if (ngx_de_link_info(path->data, dir) == NGX_FILE_ERROR)
#endif
        {
            return NGX_ERROR;
        }
        dir->valid_info = 1;
    }

    return NGX_OK;
}


static ngx_inline ngx_int_t
ngx_http_cache_purge_file_info(ngx_str_t *path, ngx_dir_t *dir,
    const char **failed)
{
    if (!dir->valid_info) {
        dir->type = 0;
        if (ngx_http_cache_purge_de_info(path, dir, failed) != NGX_OK) {
            return NGX_ERROR;
        }
    }
    return ngx_de_is_file(dir) ? NGX_OK : NGX_DECLINED;
}


/* Only the explicitly configured root may resolve through a symbolic link. */
static ngx_inline ngx_int_t
ngx_http_cache_purge_open_dir(ngx_str_t *path, ngx_dir_t *parent,
    ngx_dir_t *dir, const char **failed)
{
#if (NGX_HAVE_OPENAT && NGX_HTTP_CACHE_PURGE_HAVE_AT)
    ngx_fd_t   fd;
    ngx_err_t  err;

    if (parent != NULL) {
        *failed = ngx_openat_file_n;
        fd = ngx_openat_file(dirfd(parent->dir), ngx_de_name(parent),
                             NGX_FILE_RDONLY | NGX_FILE_NONBLOCK
                                 | NGX_FILE_NOFOLLOW | O_DIRECTORY,
                             NGX_FILE_OPEN, 0);
        if (fd == NGX_INVALID_FILE) {
            if (ngx_errno == NGX_ELOOP || ngx_errno == NGX_ENOTDIR) {
                return NGX_DECLINED;
            }
            return NGX_ERROR;
        }
        ngx_memzero(dir, sizeof(*dir));
        *failed = "fdopendir()";
        dir->dir = fdopendir(fd);
        if (dir->dir == NULL) {
            err = ngx_errno;
            (void) ngx_close_file(fd);
            ngx_set_errno(err);
            return NGX_ERROR;
        }
        return NGX_OK;
    }
#else
    ngx_file_info_t info;

    if (parent != NULL) {
        *failed = ngx_link_info_n;
        if (ngx_link_info(path->data, &info) == NGX_FILE_ERROR) {
            return NGX_ERROR;
        }
        if (!ngx_is_dir(&info) || ngx_is_link(&info)) {
            return NGX_DECLINED;
        }
    }
#endif

    *failed = ngx_open_dir_n;
    return ngx_open_dir(path, dir);
}


static ngx_inline ngx_fd_t
ngx_http_cache_purge_open_entry(ngx_str_t *path, ngx_dir_t *parent,
    const char **failed)
{
#if (NGX_HAVE_OPENAT && NGX_HTTP_CACHE_PURGE_HAVE_AT)
    if (parent != NULL) {
        *failed = ngx_openat_file_n;
        return ngx_openat_file(dirfd(parent->dir), ngx_de_name(parent),
                               NGX_FILE_RDONLY | NGX_FILE_NONBLOCK
                                   | NGX_FILE_NOFOLLOW,
                               NGX_FILE_OPEN, 0);
    }
#else
    (void) parent;
#endif

    *failed = ngx_open_file_n;
    return ngx_open_file(path->data, NGX_FILE_RDONLY, NGX_FILE_OPEN, 0);
}


static ngx_inline ngx_int_t
ngx_http_cache_purge_delete_entry(u_char *name, ngx_dir_t *parent,
    const char **failed)
{
#if (NGX_HAVE_OPENAT && NGX_HTTP_CACHE_PURGE_HAVE_AT)
    if (parent != NULL) {
        /* No core unlinkat wrapper; all use is behind the complete probe. */
        *failed = "unlinkat()";
        return unlinkat(dirfd(parent->dir), (char *) ngx_de_name(parent), 0);
    }
#else
    (void) parent;
#endif

    *failed = ngx_delete_file_n;
    return ngx_delete_file(name);
}
/* End filesystem helpers. */
#endif

extern ngx_module_t ngx_http_cache_purge_module;

/* -- forward declarations ----------------------------------------------- */

typedef struct ngx_http_cache_purge_queue_item_s
                                            ngx_http_cache_purge_queue_item_t;
typedef struct ngx_http_cache_purge_queue_s  ngx_http_cache_purge_queue_t;
typedef struct ngx_http_cache_purge_main_conf_s
                                           ngx_http_cache_purge_main_conf_t;
typedef struct ngx_http_cache_purge_path_s  ngx_http_cache_purge_path_t;
typedef struct ngx_http_cache_purge_protection_s
    ngx_http_cache_purge_protection_t;


/* One wire contract for every module-owned administrative response. */
typedef enum {
    NGX_HTTP_CACHE_PURGE_RESULT_PURGED,
    NGX_HTTP_CACHE_PURGE_RESULT_NO_MATCH,
    NGX_HTTP_CACHE_PURGE_RESULT_ACCEPTED,
    NGX_HTTP_CACHE_PURGE_RESULT_INVALIDATED,
    NGX_HTTP_CACHE_PURGE_RESULT_STATUS,
    NGX_HTTP_CACHE_PURGE_RESULT_REFRESH_COMPLETE,
    NGX_HTTP_CACHE_PURGE_RESULT_REFRESH_INCOMPLETE,
    NGX_HTTP_CACHE_PURGE_RESULT_INVALID_SELECTOR,
    NGX_HTTP_CACHE_PURGE_RESULT_INVALID_CURSOR,
    NGX_HTTP_CACHE_PURGE_RESULT_METHOD_NOT_ALLOWED,
    NGX_HTTP_CACHE_PURGE_RESULT_ACCESS_DENIED,
    NGX_HTTP_CACHE_PURGE_RESULT_CACHE_UNAVAILABLE,
    NGX_HTTP_CACHE_PURGE_RESULT_KEY_MISMATCH,
    NGX_HTTP_CACHE_PURGE_RESULT_DELETE_FAILED,
    NGX_HTTP_CACHE_PURGE_RESULT_SCAN_FAILED,
    NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR,
    NGX_HTTP_CACHE_PURGE_RESULT_COUNT
} ngx_http_cache_purge_result_code_e;

typedef struct {
    ngx_http_cache_purge_result_code_e code;
    ngx_uint_t  deleted;
    ngx_uint_t  delete_errors;
    ngx_uint_t  scan_errors;
    unsigned    counted : 1;
} ngx_http_cache_purge_result_t;

/* -- data structures ---------------------------------------------------- */

struct ngx_http_cache_purge_queue_item_s {
    ngx_str_node_t                      node;
    ngx_queue_t                         link;
    ngx_str_t                           pool_name;
    ngx_flag_t                          purge_all;
    ngx_flag_t                          running;
    ngx_flag_t                          rerun;
    ngx_pid_t                           owner;
    ngx_uint_t                          owner_slot;
    uint64_t                            owner_epoch;
    ngx_msec_t                          enqueued_at;
    ngx_msec_t                          retry_at;
    ngx_uint_t                          retries;
    ngx_http_cache_purge_path_t        *path;
    ngx_http_cache_purge_protection_t  *protection;
    ngx_http_cache_purge_protection_t  *next_protection;
};

enum {
    NGX_HTTP_CACHE_PURGE_ENQUEUED,
    NGX_HTTP_CACHE_PURGE_ENQUEUED_FULL,
    NGX_HTTP_CACHE_PURGE_STARTED,
    NGX_HTTP_CACHE_PURGE_STARTED_FULL,
    NGX_HTTP_CACHE_PURGE_FINISHED,
    NGX_HTTP_CACHE_PURGE_DEDUPLICATED,
    NGX_HTTP_CACHE_PURGE_WAITING,
    NGX_HTTP_CACHE_PURGE_OVERFLOW,
    NGX_HTTP_CACHE_PURGE_DELETED,
    NGX_HTTP_CACHE_PURGE_DELETE_ERRORS,
    NGX_HTTP_CACHE_PURGE_DELETED_BYTES,
    NGX_HTTP_CACHE_PURGE_HIT,
    NGX_HTTP_CACHE_PURGE_MISS,
    NGX_HTTP_CACHE_PURGE_INVALIDATED,
    NGX_HTTP_CACHE_PURGE_SCAN_ERRORS,
    NGX_HTTP_CACHE_PURGE_RETRIED,
    NGX_HTTP_CACHE_PURGE_EXHAUSTED,
    NGX_HTTP_CACHE_PURGE_METRICS
};

/*
 * One event-loop writer per slot; pool threads only update their work context.
 */
typedef struct {
    volatile ngx_uint_t  sequence;
    volatile uint64_t    value[NGX_HTTP_CACHE_PURGE_METRICS];
} ngx_http_cache_purge_stats_t;

typedef struct {
    uint64_t  pending, pending_full, active;
    uint64_t  overflow, deleted, deduplicated, waiting;
    uint64_t  errors, bytes, hits, misses;
    uint64_t  invalidated;
    uint64_t  scan_errors, retries, exhausted;
} ngx_http_cache_purge_cache_stat_t;

/* The zone owns names independently of pending tasks. */
struct ngx_http_cache_purge_path_s {
    ngx_str_t  name;
    u_char    *stats;
    size_t     stats_stride;
    /* Pending and running tasks remain indexed until final completion. */
    ngx_rbtree_t                  rbtree;
    ngx_rbtree_node_t             sentinel;
    ngx_http_cache_purge_path_t  *next;
};

/* A task retains the path settings captured when it was accepted. */
struct ngx_http_cache_purge_protection_s {
    ngx_str_t                          *names;
    ngx_uint_t                          nelts;
    ngx_http_cache_purge_protection_t  *next;
};

struct ngx_http_cache_purge_queue_s {
    ngx_uint_t   version;
    ngx_queue_t  pending;
    ngx_queue_t  active;
    /*
     * The zone lock protects all pending and active task state.
     * Namespace publication only uses a nested trylock, never waits. No
     * reverse namespace-to-queue acquisition is allowed.
     */
    ngx_uint_t                          size;
    ngx_uint_t                          running;
    ngx_msec_t                          oldest;
    ngx_pid_t                           owners[NGX_MAX_PROCESSES];
    uint64_t                            epochs[NGX_MAX_PROCESSES];
    ngx_uint_t                          probes[NGX_MAX_PROCESSES];
    ngx_slab_pool_t                    *shpool;
    ngx_uint_t                          max_size;
    ngx_http_cache_purge_path_t        *paths;
    ngx_http_cache_purge_protection_t  *protections;
};

struct ngx_http_cache_purge_main_conf_s {
#if (NGX_HTTP_CACHE_PURGE_ROCKSDB)
    ngx_http_cache_purge_runtime_t *runtime;
#endif
    ngx_http_cache_purge_queue_t  *queue;
    ngx_shm_zone_t                *shm_zone;
    ngx_uint_t                     queue_size;
    ngx_uint_t                     batch_size;
    ngx_msec_t                     throttle_ms;
    ngx_flag_t                     background_purge;
    ngx_flag_t                     legacy_status_seen;
    /*
     * vary_aware: when on, an exact-key purge walks the cache directory after
     * trying the primary entry and removes any remaining files that carry the
     * same KEY: string (i.e. Vary / gzip_vary variants at different paths).
     * Disabled by default because it adds a full cache walk per purge request.
     */
    ngx_flag_t                          vary_aware;
    ngx_array_t                         queue_status_locations;
    ngx_array_t                         namespaces;
    ngx_array_t                        *paths;
    ngx_http_cache_purge_protection_t  *protection;
};

typedef struct {
    ngx_flag_t    enable;
    ngx_str_t     method;
    ngx_flag_t    purge_all;
    ngx_array_t  *access; /* ngx_in_cidr_t  */
    ngx_array_t  *access6; /* ngx_in6_cidr_t */
} ngx_http_cache_purge_conf_t;

typedef struct {
#if (NGX_HTTP_FASTCGI)
    ngx_http_cache_purge_conf_t fastcgi;
#endif
#if (NGX_HTTP_PROXY)
    ngx_http_cache_purge_conf_t proxy;
#endif
#if (NGX_HTTP_SCGI)
    ngx_http_cache_purge_conf_t scgi;
#endif
#if (NGX_HTTP_UWSGI)
    ngx_http_cache_purge_conf_t uwsgi;
#endif

    ngx_http_cache_purge_conf_t *conf;
    ngx_http_handler_pt          handler;
    ngx_http_handler_pt          original_handler;
    ngx_uint_t                         response_type;
    ngx_str_t                          queue_status_cache_path;
    ngx_http_cache_purge_path_t       *queue_status_path;
    ngx_str_t                          thread_pool_name;
    ngx_http_cache_purge_namespace_t  *namespace;
    ngx_http_complex_value_t           logical_key;
#if (NGX_HTTP_PROXY)
    ngx_flag_t  refresh;
    ngx_str_t   refresh_location;
    ngx_str_t   refresh_prefix;
    ngx_msec_t  refresh_timeout;
#endif

#if (NGX_HTTP_PROXY)
    /*
     * Keep separate-location cache state independent of proxy_pass state.
     * This avoids altering core proxy location inheritance.
     */
    ngx_shm_zone_t            *proxy_separate_zone; /* static zone name  */
    ngx_http_complex_value_t  *proxy_separate_value; /* dynamic zone expr */
    ngx_http_complex_value_t   proxy_separate_key; /* purge key template*/
#endif
} ngx_http_cache_purge_loc_conf_t;

typedef struct {
    u_char     *key_partial;
    ngx_uint_t  key_len;

    u_char  key_buffer[NGX_HTTP_CACHE_PURGE_ENTRY_KEY_MAX + 1];

    size_t                              entry_key_len;
    ngx_http_file_cache_header_t        header;
    ngx_uint_t                          files_deleted;
    ngx_uint_t                          delete_errors;
    ngx_uint_t                          scan_errors;
    uint64_t                            bytes_deleted;
    off_t                               file_size;
    ngx_str_t                           cache_root;
    ngx_http_cache_purge_protection_t  *protection;
    ngx_array_t                        *paths;
    ngx_log_t                          *log;
    ngx_dir_t                          *parent_dir;
    ngx_http_cache_purge_namespace_t   *namespace;
    void (*visit)(void *ctx, ngx_str_t *path);
    /* Exact Vary matching also invalidates native cache nodes. */
    ngx_http_file_cache_t *cache;
} ngx_http_cache_purge_walk_ctx_t;

typedef struct ngx_http_cache_purge_dir_s  ngx_http_cache_purge_dir_t;

struct ngx_http_cache_purge_dir_s {
    ngx_dir_t                    dir;
    size_t                       len;
    ngx_http_cache_purge_dir_t  *parent;
};

typedef struct {
    ngx_http_cache_purge_walk_ctx_t  *ctx;
    ngx_http_cache_purge_dir_t       *top;
    ngx_str_t                         path;
    size_t                            allocated;
    unsigned                          started : 1;
    unsigned                          complete : 1;
} ngx_http_cache_purge_cursor_t;

typedef struct {
    ngx_http_request_t           *request;
    ngx_event_t                   event;
    ngx_http_cleanup_t           *cleanup;
    ngx_http_cache_purge_path_t  *path;
} ngx_http_cache_purge_request_t;

typedef struct {
    ngx_event_t                         event;
    ngx_http_cache_purge_main_conf_t   *conf;
    ngx_http_cache_purge_queue_item_t  *item;
    ngx_http_cache_purge_walk_ctx_t     ctx;
    ngx_http_cache_purge_cursor_t       cursor;
    ngx_uint_t                          batch;
    ngx_msec_t                          recover_at;
#if (NGX_THREADS && !NGX_WIN32)
    ngx_thread_pool_t  *pool;
    ngx_thread_task_t  *task;
    ngx_atomic_t        finished;
#endif
} ngx_http_cache_purge_work_t;

/* Snapshot of pending and active task state. */
typedef struct {
    struct {
        ngx_uint_t  size;
        ngx_uint_t  max_size;
        ngx_uint_t  running;
        ngx_msec_t  wait_time;
        ngx_flag_t  full;
    } queue;

    ngx_http_cache_purge_cache_stat_t cache;
} ngx_http_cache_purge_queue_stat_t;


static ngx_inline ngx_uint_t
ngx_http_cache_purge_path_equal(u_char *a, u_char *b, size_t len)
{
    size_t i;

    for (i = 0; i < len; i++) {
        if (ngx_http_cache_purge_path_char(a[i])
            != ngx_http_cache_purge_path_char(b[i]))
        {
            return 0;
        }
    }

    return 1;
}


static ngx_inline size_t
ngx_http_cache_purge_path_length(const ngx_str_t *path)
{
    size_t len, min;

    len = path->len;
    min = ngx_http_cache_purge_path_min_length(path);

    while (len > min
           && ngx_http_cache_purge_path_separator(path->data[len - 1]))
    {
        len--;
    }

    return len;
}


static ngx_inline ngx_uint_t
ngx_http_cache_purge_path_contains(const ngx_str_t *path, const ngx_str_t *root)
{
    size_t len;

    len = ngx_http_cache_purge_path_length(root);
    if (len == 0 || path->len < len
        || !ngx_http_cache_purge_path_equal(path->data, root->data, len))
    {
        return 0;
    }

    return path->len == len
           || ngx_http_cache_purge_path_separator(root->data[len - 1])
           || ngx_http_cache_purge_path_separator(path->data[len]);
}

ngx_int_t ngx_http_cache_purge_access_handler(ngx_http_request_t *r);
ngx_int_t
ngx_http_cache_purge_delete_variants(ngx_http_request_t *r,
    ngx_http_file_cache_t *cache, ngx_http_cache_purge_walk_ctx_t *ctx);
#if (NGX_HTTP_FASTCGI || NGX_HTTP_PROXY || NGX_HTTP_SCGI || NGX_HTTP_UWSGI)
ngx_int_t ngx_http_cache_purge_enqueue(ngx_http_request_t *r,
    ngx_http_file_cache_t *cache, ngx_str_t *key, ngx_flag_t purge_all,
    ngx_str_t *expected);
#endif
void       ngx_http_cache_purge_exit_worker(ngx_cycle_t *cycle);
ngx_int_t  ngx_http_cache_purge_init_shm_zone(ngx_shm_zone_t *shm_zone,
    void *data);
ngx_int_t  ngx_http_cache_purge_init_worker(ngx_cycle_t *cycle);
ngx_uint_t ngx_http_cache_purge_is_cache_file(ngx_str_t *path);
ngx_uint_t
ngx_http_cache_purge_is_protected_path(ngx_http_cache_purge_walk_ctx_t *ctx,
    ngx_str_t *path);
ngx_int_t ngx_http_cache_purge_log(ngx_http_request_t *r);
ngx_int_t ngx_http_cache_purge_probe_credit(ngx_http_cache_purge_queue_t *queue,
    ngx_flag_t take);
ngx_int_t ngx_http_cache_purge_queue_status_handler(ngx_http_request_t *r);
ssize_t   ngx_http_cache_purge_read_key(ngx_http_cache_purge_walk_ctx_t *ctx,
    ngx_str_t *path, size_t len);
ngx_int_t
ngx_http_cache_purge_send_result(ngx_http_request_t *r,
    ngx_http_cache_purge_result_t *result);
ngx_int_t ngx_http_cache_purge_reply(ngx_http_request_t *r,
    ngx_http_cache_purge_result_code_e code);
ngx_int_t
ngx_http_cache_purge_response_headers(ngx_http_request_t *r,
    ngx_http_cache_purge_result_code_e code);
ngx_int_t ngx_http_cache_purge_method_response(ngx_http_request_t *r,
    ngx_str_t *allow);
void      ngx_http_cache_purge_stat_add(ngx_http_cache_purge_path_t *path,
    ngx_uint_t field, uint64_t value);
#if (NGX_HTTP_FASTCGI || NGX_HTTP_PROXY || NGX_HTTP_SCGI || NGX_HTTP_UWSGI)
void ngx_http_cache_purge_stat_wait(ngx_http_cache_purge_path_t *path,
    ngx_int_t delta);
#endif
ngx_flag_t ngx_http_cache_purge_threaded(void);
ngx_int_t  ngx_http_cache_purge_unlink(ngx_log_t *log, u_char *name,
    ngx_dir_t *parent);
ngx_int_t  ngx_http_cache_purge_walk(ngx_http_cache_purge_walk_ctx_t *ctx);
void ngx_http_cache_purge_walk_close(ngx_http_cache_purge_cursor_t *cursor);
void
ngx_http_cache_purge_walk_init(ngx_http_cache_purge_walk_ctx_t *ctx,
    ngx_str_t *root, ngx_http_cache_purge_protection_t *protection,
    ngx_array_t *paths, ngx_log_t *log);
ngx_int_t ngx_http_cache_purge_walk_step(ngx_http_cache_purge_cursor_t *cursor,
    ngx_uint_t limit);
#if (NGX_HTTP_PROXY)
char *ngx_http_cache_refresh_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
#endif
#if (NGX_HTTP_PROXY)
ngx_int_t ngx_http_cache_refresh_guard(ngx_http_request_t *r);
#endif
#if (NGX_HTTP_PROXY)
ngx_int_t ngx_http_cache_refresh_handler(ngx_http_request_t *r);
#endif
#if (NGX_HTTP_PROXY)
char *ngx_http_cache_refresh_profile(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
#endif
#if (NGX_HTTP_PROXY)
ngx_int_t ngx_http_cache_refresh_start(ngx_http_request_t *r);
#endif
#if (NGX_HTTP_PROXY)
ngx_int_t ngx_http_cache_refresh_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data);
#endif
#if (NGX_HTTP_FASTCGI)
ngx_int_t ngx_http_fastcgi_cache_purge_handler(ngx_http_request_t *r);
#endif
#if (NGX_HTTP_PROXY)
char *ngx_http_proxy_cache_purge_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
#endif
#if (NGX_HTTP_PROXY)
ngx_int_t ngx_http_proxy_cache_purge_handler(ngx_http_request_t *r);
#endif
#if (NGX_HTTP_SCGI)
ngx_int_t ngx_http_scgi_cache_purge_handler(ngx_http_request_t *r);
#endif
#if (NGX_HTTP_UWSGI)
ngx_int_t ngx_http_uwsgi_cache_purge_handler(ngx_http_request_t *r);
#endif

/* Copyright (C) Hanada */


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
ngx_int_t ngx_http_cache_purge_fill_arm(ngx_http_request_t *r,
    ngx_http_cache_purge_fill_t **slot, ngx_http_cache_purge_fill_step_pt step,
    void *data);
void      ngx_http_cache_purge_fill_complete(ngx_http_cache_purge_fill_t *fill,
    ngx_int_t result);

#endif

/* Use exported core layouts where available; keep old-ABI adapters together. */

#if (NGX_HTTP_CACHE)
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

#endif

#endif /* NGX_HTTP_CACHE */
#endif /* _NGX_HTTP_CACHE_PURGE_H_INCLUDED_ */
