/*
 * Copyright (C) Hanada
 */

#ifndef _NGX_HTTP_CACHE_PURGE_INTERNAL_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_INTERNAL_H_INCLUDED_

/* Executor, file and shared-control contracts; independent of nginx pools. */

/* Copyright (C) Hanada */

#include "rocksdb/ngx_http_cache_rocksdb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_HTTP_CACHE_PURGE_COMPONENT_ABI 5
#define NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION 1
#define NGX_HTTP_CACHE_PURGE_FRAME_SIZE 24

typedef struct ngx_http_cache_purge_component_s
    ngx_http_cache_purge_component_t;

typedef enum {
    NGX_HTTP_CACHE_PURGE_COMPONENT_STARTING = 0,
    NGX_HTTP_CACHE_PURGE_COMPONENT_STANDBY,
    NGX_HTTP_CACHE_PURGE_COMPONENT_OPEN,
    NGX_HTTP_CACHE_PURGE_COMPONENT_STOPPING,
    NGX_HTTP_CACHE_PURGE_COMPONENT_STOPPED,
    NGX_HTTP_CACHE_PURGE_COMPONENT_FAILED
} ngx_http_cache_purge_component_state_e;

typedef enum {
    NGX_HTTP_CACHE_PURGE_OP_PING = 1,
    NGX_HTTP_CACHE_PURGE_OP_GET,
    NGX_HTTP_CACHE_PURGE_OP_PREPARE,
    NGX_HTTP_CACHE_PURGE_OP_COMMIT,
    NGX_HTTP_CACHE_PURGE_OP_ABORT,
    NGX_HTTP_CACHE_PURGE_OP_RETIRE,
    NGX_HTTP_CACHE_PURGE_OP_COLLECT,
    NGX_HTTP_CACHE_PURGE_OP_QUERY,
    NGX_HTTP_CACHE_PURGE_OP_PENDING,
    NGX_HTTP_CACHE_PURGE_OP_RETIRED,
    NGX_HTTP_CACHE_PURGE_OP_COLLECT_KEYS,
    NGX_HTTP_CACHE_PURGE_OP_ADD_URL,
    NGX_HTTP_CACHE_PURGE_OP_UPDATE,
    NGX_HTTP_CACHE_PURGE_OP_ACCEPT_OPERATION,
    NGX_HTTP_CACHE_PURGE_OP_GET_OPERATION,
    NGX_HTTP_CACHE_PURGE_OP_GET_CONTROL_STATUS,
    /* string canonical address -> digest[16], offset u64 + one FD */
    NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_FILE_LEASE,
    /* file, previous ID, temp path; two FDs: lease, temp */
    NGX_HTTP_CACHE_PURGE_OP_PREPARE_FILE,
    /* file ID, prepared revision; one FD: lease */
    NGX_HTTP_CACHE_PURGE_OP_FINISH_FILE,
    /* root, zone, path -> canonical root, prior ID, digest/offset +
     * lease FD */
    NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION,
    /* root, zone, selected path; one FD -> GET-format verified file record */
    NGX_HTTP_CACHE_PURGE_OP_INSPECT_FILE,
    /* ID, revision, transaction, before/after, TAG mode/count/values,
     * request origin ledger[16]/sequence u64;
     * two FDs -> GET record, publication u64, Invalidate sequence u64 */
    NGX_HTTP_CACHE_PURGE_OP_VALIDATE_FILE,
    /* operation sequence u64 -> number of older active worker registrations */
    NGX_HTTP_CACHE_PURGE_OP_GET_WRITERS,
  /* ID/revision, one path lease FD -> removal outcome u32 and retired match */
    NGX_HTTP_CACHE_PURGE_OP_REMOVE_FILE,
    /* root/zone/path/key/URL, one FD -> GET record, publication u64,
     * required Invalidate sequence u64 (zero permits ordinary reuse).
     * Registers the observed URL and checks all persisted aliases. */
    NGX_HTTP_CACHE_PURGE_OP_ADMIT_FILE
} ngx_http_cache_purge_opcode_e;

#define NGX_HTTP_CACHE_PURGE_ROOT_CACHE 1
#define NGX_HTTP_CACHE_PURGE_ROOT_TEMP  2
typedef struct {
    ngx_http_cache_rocksdb_bytes_t path, zone;
    uint32_t                       kind;
} ngx_http_cache_purge_root_t;

typedef struct {
    uint32_t                          abi;
    ngx_http_cache_rocksdb_options_t  store;
    uint32_t                          max_connections;
    uint32_t                          max_payload_bytes;

    /* total retained connection payloads */
    uint32_t buffer_bytes;


    /* frame or response deadline, not idle extension */
    uint32_t io_timeout_ms;

    uint32_t owner_retry_ms;

    /* UINT32_MAX: creator UID; socket owner/client */
    uint32_t client_uid;

    uint32_t  propagation_ms;
    uint32_t  max_live_operations;
    uint64_t  live_selector_bytes;

    /* shared mapping owned by nginx, valid until stop */
    void *control_memory;

    size_t control_bytes;
    const ngx_http_cache_purge_root_t
            *roots; /* Copied by start; opened only on executor. */
    uint32_t root_count;
} ngx_http_cache_purge_component_options_t;

typedef struct {
    uint32_t  state;
    uint32_t  error_code;
    uint64_t  connections;
    uint64_t  requests;
    uint64_t  rejected;
    uint64_t  buffered_bytes;
    uint64_t  peak_buffered_bytes;
    uint32_t  control_state; /* 0: no table; otherwise Coordination state */
    uint64_t  control_publication;
} ngx_http_cache_purge_component_status_t;

void ngx_http_cache_purge_component_options(
    ngx_http_cache_purge_component_options_t *options);
/* Link-time header/archive compatibility marker. */
uint32_t ngx_http_cache_purge_component_abi_5(void);

/* Only start after fork in the selected nginx worker, never while parsing
 * configuration or in master. Copies its inputs and starts one executor thread.
 * OPEN means index transport is available, not that unfinished cache writes or
 * purge fences have been recovered. Socket: <path>/component.sock, mode 0600.
 * Only the creator UID and client_uid may connect. A different client_uid
 * requires a root creator and receives ownership of the socket, not the DB.
 * No nginx request, pool, log or configuration pointers cross the thread. */
int ngx_http_cache_purge_component_start(const char *path,
    const ngx_http_cache_purge_component_options_t *options,
    ngx_http_cache_purge_component_t **component,
    ngx_http_cache_rocksdb_error_t *error);

/* These calls belong to the starting thread/process. status never queries DB.
 * request_stop only signals/wakes the executor; it neither joins nor does I/O
 * on the DB. Use it while an HTTP worker is draining long-lived requests.
 * stop wakes STANDBY/poll, joins the executor and closes DB before returning.
 * It can wait for a running sync write; it does not finish an entire purge job.
 * A lost response is ambiguous: query/retry by durable identity, never assume
 * that disconnect or shutdown rolled back an acknowledged database write. */
int ngx_http_cache_purge_component_status(
    ngx_http_cache_purge_component_t *component,
    ngx_http_cache_purge_component_status_t *status,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_component_request_stop(
    ngx_http_cache_purge_component_t *component,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_component_stop(
    ngx_http_cache_purge_component_t **component,
    ngx_http_cache_rocksdb_error_t *error);

#ifdef __cplusplus
}
#endif


/* Copyright (C) Hanada */

#include "rocksdb/ngx_http_cache_rocksdb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_HTTP_CACHE_PURGE_CONTROL_ABI 3
#define NGX_HTTP_CACHE_PURGE_CONTROL_FENCE 1
#define NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE 2
#define NGX_HTTP_CACHE_PURGE_CONTROL_PREPARING 4

typedef struct ngx_http_cache_purge_control_reader_s
    ngx_http_cache_purge_control_reader_t;
typedef struct ngx_http_cache_purge_control_writer_s
    ngx_http_cache_purge_control_writer_t;

typedef struct {
    unsigned char  ledger[16];
    uint64_t       sequence;
} ngx_http_cache_purge_watermark_t;

#define NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE 28

typedef struct {
    ngx_http_cache_rocksdb_bytes_t        key, url, host, tag_namespace;
    const ngx_http_cache_rocksdb_bytes_t  *tags;
    size_t                                 tag_count;
    uint64_t                               now_ms;
    int                                    proxy;
} ngx_http_cache_purge_control_context_t;

typedef struct {
    uint64_t                       sequence, issued_at_ms, propagation_until_ms;
    uint32_t                       flags, mode, kind;
    ngx_http_cache_rocksdb_bytes_t operation_id, value, scope;
} ngx_http_cache_purge_control_match_t;

/* Called synchronously while the immutable image is pinned. Copy any required
 * bytes before returning; never retain image pointers or perform asynchronous
 * work from this callback. Ignore all partial output unless match returns OK.
 * */
typedef int (*ngx_http_cache_purge_control_visit_pt)(
    void *data, const ngx_http_cache_purge_control_match_t *match);

size_t ngx_http_cache_purge_control_size(size_t bank_bytes, uint32_t readers);
/* The caller owns shared mmap/SHM storage and must initialize it exactly once,
 * before concurrent attachment. The address must be 64-byte aligned. */
int ngx_http_cache_purge_control_init(void *memory, size_t bytes,
    size_t bank_bytes, uint32_t readers, ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_reader_open(void *memory, size_t bytes,
    ngx_http_cache_purge_control_reader_t **reader,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_reader_close(
    ngx_http_cache_purge_control_reader_t **reader,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_match(
    ngx_http_cache_purge_control_reader_t *reader,
    const ngx_http_cache_purge_control_context_t *context, size_t max_matches,
    size_t max_bytes, ngx_http_cache_purge_control_visit_pt visit, void *data,
    uint64_t *publication, ngx_http_cache_rocksdb_error_t *error);
/* Copies the complete accepted local sequence from one serving image. It is
 * a local origin-start boundary, not proof of an upstream CDN's freshness. */
int ngx_http_cache_purge_control_watermark(
    ngx_http_cache_purge_control_reader_t *reader,
    ngx_http_cache_purge_watermark_t *watermark, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error);
/* Announce active/unknown before taking an origin snapshot, then publish the
 * oldest active snapshot. active=0 clears it only after those requests cannot
 * issue more writes. One reader coalesces all requests on its owning thread. */
int ngx_http_cache_purge_control_origins(
    ngx_http_cache_purge_control_reader_t *reader,
    const ngx_http_cache_purge_watermark_t *oldest, int active,
    ngx_http_cache_rocksdb_error_t *error);
/* Pin the exact matched publication across a short synchronous admission step.
 * A new effective publication cannot reopen its gate until this guard leaves.
 * Do not hold a guard across event waits, upstream I/O or response body
 * streaming. */
int ngx_http_cache_purge_control_guard_begin(
    ngx_http_cache_purge_control_reader_t *reader, uint64_t publication,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_guard_end(
    ngx_http_cache_purge_control_reader_t *reader,
    ngx_http_cache_rocksdb_error_t *error);

/* Claim only while holding the durable database's exclusive owner lease.
 * Claim closes the serving gate until controls and native file state recover.
 * Reader registration and publishing use a robust process-shared mutex;
 * normal matching takes no mutex and performs no allocation or DB I/O. */
int ngx_http_cache_purge_control_writer_open(void *memory, size_t bytes,
    ngx_http_cache_purge_control_writer_t **writer,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_writer_close(
    ngx_http_cache_purge_control_writer_t **writer,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_gate_close(
    ngx_http_cache_purge_control_writer_t *writer,
    ngx_http_cache_rocksdb_error_t *error);
/* Open only after the new image and durable activation/recovery are complete.
 * BUSY while old image readers are still inside synchronous admission work. */
int ngx_http_cache_purge_control_gate_open(
    ngx_http_cache_purge_control_writer_t *writer,
    ngx_http_cache_rocksdb_error_t *error);

#ifdef __cplusplus
}
#endif


/* Copyright (C) Hanada */

#include "rocksdb/ngx_http_cache_rocksdb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_HTTP_CACHE_PURGE_FILES_ABI 2
#define NGX_HTTP_CACHE_PURGE_FILE_PREPARED 1
#define NGX_HTTP_CACHE_PURGE_FILE_READY 2
#define NGX_HTTP_CACHE_PURGE_FILE_MARKER_SIZE 40

typedef struct {
    unsigned char  id[16];
    unsigned char  address[16];
    uint32_t       state;
} ngx_http_cache_purge_file_marker_t;

/* The address is a canonical absolute cache filename, not a zone name or key.
 * Lock collisions only serialize unrelated files; marker matching compares
 * the independent 128-bit address digest as well as the full instance ID. */
int ngx_http_cache_purge_file_address(ngx_http_cache_rocksdb_bytes_t address,
    unsigned char digest[16], uint64_t *lock_offset,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_file_marker_read(int fd,
    ngx_http_cache_purge_file_marker_t *marker,
    ngx_http_cache_rocksdb_error_t *error);
/* Caller must hold the path lease. These descriptor operations may block and
 * belong on a bounded filesystem executor, never under an nginx SHM mutex.
 * durable=1 fsyncs the file; the caller separately syncs renamed directories.
 * Readiness must be written only after durable index commit_pending(). */
int ngx_http_cache_purge_file_marker_prepare(int fd, const unsigned char id[16],
    const unsigned char address[16], int durable,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_file_marker_publish(int fd, const unsigned char id[16],
    const unsigned char address[16], int durable,
    ngx_http_cache_rocksdb_error_t *error);

/* The lock file is persistent and must never be replaced/unlinked while any
 * worker can hold it. Only a newly created file may be chowned. The caller
 * validates the configured parent directory before enabling this resource. */
int ngx_http_cache_purge_file_locks_open(const char *path, uint32_t owner_uid,
    int create, int *fd, ngx_http_cache_rocksdb_error_t *error);
/* Linux open-file-description leases. Each acquisition opens an independent
 * description, so separate writers conflict even in the same process.
 * anchor_fd is the validated persistent lock file opened at initialization;
 * path must still name that inode. No process-lock fallback is safe here.
 * Opening/validating a lease belongs on the filesystem executor. Callers must
 * bound live transactions/descriptors, not retain one lease per cached file. */
int
ngx_http_cache_purge_file_lease_acquire(const char *path, int anchor_fd,
    uint64_t offset, int *lease_fd, ngx_http_cache_rocksdb_error_t *error);
/* Adopt an owned descriptor received with SCM_RIGHTS or duplicated in-process.
 * This checks the anchor inode/access mode and acquires the requested OFD lock
 * if necessary; it does NOT attest which PID previously held it. Peer UID,
 * operation identity and file marker still require independent validation.
 * Success retains the caller's fd and sets CLOEXEC; failure does not close it.
 * */
int
ngx_http_cache_purge_file_lease_adopt(int anchor_fd, int lease_fd,
    uint64_t offset, ngx_http_cache_rocksdb_error_t *error);
/* Consume one descriptor reference. Never issue F_UNLCK: other references may
 * still be doing filesystem work. The kernel releases the lease only when
 * the last reference closes, including references queued in a Unix socket.
 * After fork inherited references must be closed before unrelated work. */
int
ngx_http_cache_purge_file_lease_release(int *lease_fd,
    ngx_http_cache_rocksdb_error_t *error);

#ifdef __cplusplus
}
#endif


/* Copyright (C) Hanada */
#include <stddef.h>
#include <sys/types.h>
#ifdef __cplusplus
extern "C" {
#endif
#define NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS 2
/* Nonblocking Unix transport. Rights are attached only to one first frame byte.
 * A successful receive transfers ownership of count descriptors to the caller.
 * Invalid/truncated ancillary data closes all received rights and sets EPROTO.
 * Sending borrows descriptors; callers must not resend rights after n > 0. */
ssize_t ngx_http_cache_purge_transport_recv(int socket, void *data, size_t size,
    int allow_rights, int fds[NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS],
    size_t *count);
ssize_t ngx_http_cache_purge_transport_send(int socket, const void *data,
    size_t size, const int *fds, size_t count);
#ifdef __cplusplus
}
#endif

/*
 * Copyright (C) Hanada
 */


#ifdef __cplusplus
extern "C" {
#endif

#define NGX_HTTP_CACHE_PURGE_INVALIDATE    1
#define NGX_HTTP_CACHE_PURGE_DELETE        2

#define NGX_HTTP_CACHE_PURGE_PREPARING     1
#define NGX_HTTP_CACHE_PURGE_ACTIVE        2
#define NGX_HTTP_CACHE_PURGE_DRAINING      3
#define NGX_HTTP_CACHE_PURGE_MATERIALIZED  4

#define NGX_HTTP_CACHE_PURGE_KEY           0
#define NGX_HTTP_CACHE_PURGE_URL           1
#define NGX_HTTP_CACHE_PURGE_KEY_PREFIX    2
#define NGX_HTTP_CACHE_PURGE_URL_PREFIX    3
#define NGX_HTTP_CACHE_PURGE_HOST          4
#define NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG 5
#define NGX_HTTP_CACHE_PURGE_HOST_TAG      6


typedef struct {
    uint32_t                        kind;
    ngx_http_cache_rocksdb_bytes_t  value;
    ngx_http_cache_rocksdb_bytes_t  scope;
} ngx_http_cache_purge_selector_t;


typedef struct {
    ngx_http_cache_rocksdb_bytes_t   id;
    ngx_http_cache_purge_selector_t  selector;
    uint32_t                         mode;
    uint64_t                         issued_at_ms;
    ngx_http_cache_rocksdb_bytes_t   binding;
} ngx_http_cache_purge_operation_input_t;


typedef struct {
    ngx_http_cache_purge_operation_input_t  input;
    uint64_t                                sequence;
    uint64_t                                revision;
    uint32_t                                stage;
    uint64_t                                accepted_at_ms;
    uint64_t                                effective_at_ms;
    uint64_t                                propagation_until_ms;
    uint32_t                                propagation_ms;
    int                                     propagate;
    ngx_http_cache_rocksdb_bytes_t          bookmark;
    int                                     scan_done;
    uint64_t                                next_run_ms;
    uint64_t                                visits;
    uint64_t                                pending;
    uint64_t                                failed;
    uint64_t                                checkpoint_id;
    ngx_http_cache_rocksdb_bytes_t          last_error;

    /* Owned result bytes; free once, and do not copy this ownership field. */
    void *storage;
} ngx_http_cache_purge_operation_t;


typedef struct {
    uint64_t                        id;
    ngx_http_cache_rocksdb_bytes_t  bookmark;
    int                             scan_done;
    uint64_t                        next_run_ms;
    uint64_t                        visits_delta;
    uint64_t                        pending;
    uint64_t                        failed;
    ngx_http_cache_rocksdb_bytes_t  last_error;
} ngx_http_cache_purge_checkpoint_t;


typedef struct {
    uint32_t  live_records;
    uint64_t  live_selector_bytes;
} ngx_http_cache_purge_operation_limits_t;


typedef struct {
    uint64_t  live_records;
    uint64_t  live_selector_bytes;
    uint64_t  last_sequence;
    uint64_t  retired_sequence;
} ngx_http_cache_purge_operation_usage_t;


typedef struct {
    ngx_http_cache_purge_operation_t  *items;
    size_t                             count;
    size_t                             after_len;
    int                                done;
    unsigned char                      after[17];
} ngx_http_cache_purge_operation_page_t;


typedef struct {
    ngx_http_cache_rocksdb_t                *store;
    ngx_http_cache_purge_operation_limits_t  limits;
    unsigned char                            ledger[16];
} ngx_http_cache_purge_operations_t;


/* Executor-only C API. Results own their bytes and must be freed before reuse.
 * Failure returns an empty result. Inputs are borrowed for the duration of the
 * call. No function publishes a FENCE or proves file/reader/writer quiescence.
 */
int ngx_http_cache_purge_operations_init(ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_operation_limits_t *limits,
    ngx_http_cache_rocksdb_error_t *error);
/* Persistent selector requirements survive resident-rule retirement. Exact
 * key/URL lookups also inspect bytewise prefixes using predecessor seeks.
 * budget counts database lookups across a caller's complete file check.
 * Failure clears both outputs; no cursor survives the call. */
int ngx_http_cache_purge_operations_requirements(
    ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_selector_t *selector, size_t *budget,
    uint64_t *invalidate, uint64_t *remove,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_operations_preview(
    ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_operations_accept(
    ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_operations_get(ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_rocksdb_bytes_t id, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_operations_usage(
    ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_purge_operation_usage_t *out,
    ngx_http_cache_rocksdb_error_t *error);
/* Checks a new write's origin, not an existing committed file's freshness.
 * NULL is legacy/unknown and is allowed only before any retirement boundary.
 * The executor serializes this check with the subsequent catalogue commit.
 */
int ngx_http_cache_purge_operations_origin(
    ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_watermark_t *origin,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_operations_activate(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, uint64_t now, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_operations_checkpoint(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, const ngx_http_cache_purge_checkpoint_t *point,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error);
/* The caller must prove global old-origin quiescence and physical completion.
 * Advances the durable write floor atomically with FENCE removal.
 */
int ngx_http_cache_purge_operations_materialize(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, uint64_t quiescent_sequence,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_operations_page(ngx_http_cache_purge_operations_t *ops,
    int due, uint64_t now, ngx_http_cache_rocksdb_bytes_t after, size_t limit,
    ngx_http_cache_purge_operation_page_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_operations_expire(
    ngx_http_cache_purge_operations_t *ops, uint64_t now, size_t limit,
    size_t *expired, ngx_http_cache_rocksdb_error_t *error);
uint64_t ngx_http_cache_purge_operation_charge(
    const ngx_http_cache_purge_operation_input_t *input);
void ngx_http_cache_purge_operation_free(ngx_http_cache_purge_operation_t *op);
void ngx_http_cache_purge_operation_page_free(
    ngx_http_cache_purge_operation_page_t *page);

typedef struct ngx_http_cache_purge_control_image_s
    ngx_http_cache_purge_control_image_t;
typedef struct ngx_http_cache_purge_coordinator_s
    ngx_http_cache_purge_coordinator_t;

#define NGX_HTTP_CACHE_PURGE_COORD_CLAIMING   1
#define NGX_HTTP_CACHE_PURGE_COORD_LOADING    2
#define NGX_HTTP_CACHE_PURGE_COORD_BUILDING   3
#define NGX_HTTP_CACHE_PURGE_COORD_PUBLISHING 4
#define NGX_HTTP_CACHE_PURGE_COORD_DRAINING   5
#define NGX_HTTP_CACHE_PURGE_COORD_READY      6

/* Prepare copies borrowed operation fields into a bounded private image.
 * Publish requires a closed gate and the same owner/publication as prepare.
 * BUSY leaves the image reusable; free releases it even after a failed publish.
 * These calls neither activate a durable operation nor attest native FENCEs.
 */
int ngx_http_cache_purge_control_prepare(
    ngx_http_cache_purge_control_writer_t *writer,
    const ngx_http_cache_purge_operation_t *operations, size_t count,
    ngx_http_cache_purge_control_image_t **image,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_publish(
    ngx_http_cache_purge_control_writer_t *writer,
    const ngx_http_cache_purge_control_image_t *image, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error);
void ngx_http_cache_purge_control_image_free(
    ngx_http_cache_purge_control_image_t **image);

/* Single owning thread/process, while holding the store's owner lease.
 * The coordinator copies the operations context, borrowing its store and SHM.
 * Step advances one recovery page or publication transition; BUSY is retryable.
 * Accept returns a durable PREPARING record, not an effective-purge receipt.
 */
int ngx_http_cache_purge_coordinator_open(
    ngx_http_cache_purge_operations_t *operations, void *shared,
    size_t shared_bytes, ngx_http_cache_purge_coordinator_t **coordinator,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_coordinator_close(
    ngx_http_cache_purge_coordinator_t **coordinator,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_coordinator_status(
    ngx_http_cache_purge_coordinator_t *coordinator, uint32_t *state,
    uint64_t *publication, ngx_http_cache_rocksdb_error_t *error);
/* Counts worker registrations with pre-operation or unknown origins. Zero is
 * not permission to retire a rule: durable intents, queued IPC and physical
 * materialization require their own checks and a durable retirement floor. */
int ngx_http_cache_purge_coordinator_writers(
    ngx_http_cache_purge_coordinator_t *coordinator, uint64_t sequence,
    uint32_t *writers, ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_coordinator_step(
    ngx_http_cache_purge_coordinator_t *coordinator,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_coordinator_refresh(
    ngx_http_cache_purge_coordinator_t *coordinator,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_coordinator_accept(
    ngx_http_cache_purge_coordinator_t *coordinator,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error);

#define NGX_HTTP_CACHE_PURGE_QUERY_SELECT   0
#define NGX_HTTP_CACHE_PURGE_QUERY_PENDING  1
#define NGX_HTTP_CACHE_PURGE_QUERY_RETIRED  2

typedef struct ngx_http_cache_purge_query_s  ngx_http_cache_purge_query_t;

typedef struct {
    unsigned char  id[16];
    uint64_t       revision;
    uint32_t       state;
} ngx_http_cache_purge_index_match_t;

/* Executor-only, against an initialized index. Selectors and bookmarks are
 * borrowed only during open. A query owns bounded buffers and snapshot cursors;
 * close it even after DONE or EXPIRED. next returns OK, DONE, or an error.
 * bookmark is borrowed until the next call or close. It advances after a match
 * or a fully skipped position. Persist a match only after handling it. */
int ngx_http_cache_purge_query_open(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_selector_t *selector, unsigned mode,
    ngx_http_cache_rocksdb_bytes_t bookmark, ngx_http_cache_purge_query_t **out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_query_next(ngx_http_cache_purge_query_t *query,
    ngx_http_cache_purge_index_match_t *match,
    ngx_http_cache_rocksdb_error_t *error);
/* At most 4-4096 cursor advances; BUSY yields a resumable skipped position.
 * Close and reopen using bookmark after BUSY/EXPIRED; no match was returned. */
int ngx_http_cache_purge_query_step(ngx_http_cache_purge_query_t *query,
    size_t advances, ngx_http_cache_purge_index_match_t *match,
    ngx_http_cache_rocksdb_error_t *error);
ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_query_bookmark(const ngx_http_cache_purge_query_t *query);
int ngx_http_cache_purge_query_close(ngx_http_cache_purge_query_t **query,
    ngx_http_cache_rocksdb_error_t *error);

typedef struct {
    unsigned char  id[16];
    uint64_t       revision;
} ngx_http_cache_purge_index_expected_t;

typedef enum {
    NGX_HTTP_CACHE_PURGE_INDEX_COMMIT = 0,
    NGX_HTTP_CACHE_PURGE_INDEX_COMMIT_PENDING,
    NGX_HTTP_CACHE_PURGE_INDEX_PUBLISH,
    NGX_HTTP_CACHE_PURGE_INDEX_DISCARD,
    NGX_HTTP_CACHE_PURGE_INDEX_ABORT,
    NGX_HTTP_CACHE_PURGE_INDEX_RETIRE
} ngx_http_cache_purge_index_action_e;

int ngx_http_cache_purge_index_init(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_error_t *error);
/* Atomic state transitions on the executor. COMMIT and RETIRE accept 1-32
 * records; other actions accept one. matches has count caller-owned elements.
 * It is cleared on failure; the database never receives a partial batch. */
int ngx_http_cache_purge_index_apply(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_purge_index_action_e action,
    const ngx_http_cache_purge_index_expected_t *files, size_t count,
    ngx_http_cache_purge_index_match_t *matches,
    ngx_http_cache_rocksdb_error_t *error);

typedef struct {
    unsigned char  id[16];
    uint32_t       state;
    uint64_t       revision;
    unsigned char  previous[16];
    ngx_http_cache_rocksdb_bytes_t key, url, host, tag_namespace;
    ngx_http_cache_rocksdb_bytes_t root, zone, path, range, vary, metadata;
    ngx_http_cache_rocksdb_bytes_t  components[6];
    ngx_http_cache_rocksdb_bytes_t  tags[64];
    size_t                          tag_count;
    int                             structured_key;
    int                             structured_url;
    void                           *storage;
} ngx_http_cache_purge_index_file_t;

typedef struct {
    ngx_http_cache_purge_index_file_t  file;
    unsigned char                      previous[16];
} ngx_http_cache_purge_index_fill_t;

/* Inputs borrow bytes during the call. prepare requires state=1, revision=0;
 * NULL previous means no current instance is expected at the target path.
 * get returns one owned allocation; free it before reuse. Batch outputs have
 * count caller-owned entries and are populated only after atomic commit. */
int ngx_http_cache_purge_index_prepare(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_file_t *file,
    const unsigned char previous[16], ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_index_prepare_batch(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_fill_t *files, size_t count,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_index_get_file(ngx_http_cache_rocksdb_t *store,
    const unsigned char id[16], ngx_http_cache_purge_index_file_t *out,
    ngx_http_cache_rocksdb_error_t *error);
void
ngx_http_cache_purge_index_file_free(ngx_http_cache_purge_index_file_t *file);
int ngx_http_cache_purge_index_current(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_bytes_t root, ngx_http_cache_rocksdb_bytes_t path,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int
ngx_http_cache_purge_index_has_intent(ngx_http_cache_rocksdb_t *store,
    const unsigned char id[16], int *owned,
    ngx_http_cache_rocksdb_error_t *error);
/* A sparse, persistent requirement for an immutable file instance. Setting it
 * requires a committed file without a pending path intent. Replays retain the
 * maximum sequence in the same ledger; no file revision or proof is changed.
 * The executor supplies an accepted Invalidate boundary. The getter returns a
 * zero watermark when absent, including for prepared/retired instances. */
int ngx_http_cache_purge_index_require_validation(
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const ngx_http_cache_purge_watermark_t *required,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_index_validation_requirement(
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_purge_watermark_t *required,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_index_update(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    const ngx_http_cache_rocksdb_bytes_t *tags, size_t count,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);
/* Revalidation reuses the existing committed path intent. The first atomic
 * update changes TAGs/metadata and reserves the path without replacing the
 * body. Publication replaces its metadata proof and releases that intent. */
int ngx_http_cache_purge_index_update_pending(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    const ngx_http_cache_rocksdb_bytes_t *tags, size_t count,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_index_publish_update(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_index_add_url(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_bytes_t key, ngx_http_cache_rocksdb_bytes_t url,
    ngx_http_cache_rocksdb_error_t *error);
/* Visits every persisted URL for the file's key, without retaining the set.
 * The callback borrows each URL until it returns. Exceeding either bound fails
 * with LIMIT; callers must not treat partial enumeration as full coverage. */
typedef int (*ngx_http_cache_purge_index_url_pt)(void *data,
    ngx_http_cache_rocksdb_bytes_t url, ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_index_visit_urls(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file, size_t max_urls,
    size_t max_bytes, ngx_http_cache_purge_index_url_pt visit, void *data,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_index_collect(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *files, size_t count,
    ngx_http_cache_rocksdb_error_t *error);
int
ngx_http_cache_purge_index_collect_keys(ngx_http_cache_rocksdb_t *store,
    size_t budget, size_t *removed, ngx_http_cache_rocksdb_error_t *error);

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_HTTP_CACHE_PURGE_INDEX_PREPARED   1
#define NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED  2
#define NGX_HTTP_CACHE_PURGE_INDEX_RETIRED    3

typedef struct ngx_http_cache_purge_publications_s
    ngx_http_cache_purge_publications_t;

void ngx_http_cache_purge_watermark_encode(
    const ngx_http_cache_purge_watermark_t *watermark,
    unsigned char output[NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE]);
int ngx_http_cache_purge_watermark_decode(ngx_http_cache_rocksdb_bytes_t data,
    ngx_http_cache_purge_watermark_t *watermark,
    ngx_http_cache_rocksdb_error_t *error);
/* Extracts only committed NFP1/NFR1 user metadata. Missing legacy watermarks
 * return NOT_FOUND and never grant coverage of a later operation. */
int ngx_http_cache_purge_publication_watermark(
    ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_watermark_t *watermark,
    ngx_http_cache_rocksdb_error_t *error);
/* Conditional validation covers Invalidate only; full-entity origin covers
 * both modes. Legacy metadata without either boundary returns NOT_FOUND. */
int ngx_http_cache_purge_publication_watermarks(
    ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_watermark_t *origin,
    ngx_http_cache_purge_watermark_t *validated,
    ngx_http_cache_rocksdb_error_t *error);

typedef struct {
    ngx_http_cache_rocksdb_bytes_t  root;
    ngx_http_cache_rocksdb_bytes_t  address;
    unsigned char                   previous[16];
    void                           *storage;
} ngx_http_cache_purge_publication_target_t;

/* Executor-only filesystem/index publication. Open copies configured roots
 * and pins their canonical directories. Other inputs and descriptors are
 * borrowed for the call; target owns one allocation, released by target_free.
 * PREPARE persists intent before the temporary marker. FINISH checks the final
 * identity before publishing READY; it does not attest purge freshness.
 */
int ngx_http_cache_purge_publications_open(
    const ngx_http_cache_purge_root_t *roots, size_t count,
    const char *database, ngx_http_cache_purge_publications_t **publications,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_publications_close(
    ngx_http_cache_purge_publications_t **publications,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_publications_target(
    ngx_http_cache_purge_publications_t *publications,
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_bytes_t root,
    ngx_http_cache_rocksdb_bytes_t zone, ngx_http_cache_rocksdb_bytes_t path,
    ngx_http_cache_purge_publication_target_t *target,
    ngx_http_cache_rocksdb_error_t *error);
void ngx_http_cache_purge_publication_target_free(
    ngx_http_cache_purge_publication_target_t *target);
int ngx_http_cache_purge_publications_prepare(
    ngx_http_cache_purge_publications_t *publications,
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_fill_t *fill,
    ngx_http_cache_rocksdb_bytes_t temporary, int anchor_fd, int lease_fd,
    int temporary_fd, uint32_t peer_uid,
    ngx_http_cache_purge_index_match_t *match,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_publications_finish(
    ngx_http_cache_purge_publications_t *publications,
    ngx_http_cache_rocksdb_t *store, const unsigned char id[16],
    uint64_t prepared_revision, int anchor_fd, int lease_fd, uint32_t peer_uid,
    ngx_http_cache_purge_index_match_t *match,
    ngx_http_cache_rocksdb_error_t *error);
/* Production publishers check accepted rules before the first index commit.
 * CONFLICT retires an uncommitted candidate; BUSY keeps it recoverable. The
 * guard is never repeated after its catalogue commit has already happened. */
typedef int (*ngx_http_cache_purge_publication_guard_pt)(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_publications_finish_checked(
    ngx_http_cache_purge_publications_t *publications,
    ngx_http_cache_rocksdb_t *store, const unsigned char id[16],
    uint64_t prepared_revision, int anchor_fd, int lease_fd, uint32_t peer_uid,
    ngx_http_cache_purge_publication_guard_pt guard, void *data,
    ngx_http_cache_purge_index_match_t *match,
    ngx_http_cache_rocksdb_error_t *error);

/* Read-only executor check of the actual native cache descriptor. Success
 * returns an owned, committed record whose READY marker, current address and
 * same-filesystem publication proof match. Free it with index_file_free().
 * This neither repairs unknown files nor proves purge freshness. A consumer
 * still needs the final shared-control admission guard and 304 validation.
 * The descriptor is borrowed; no path lease or database snapshot is retained.
 */
int ngx_http_cache_purge_publications_inspect(
    ngx_http_cache_purge_publications_t *publications,
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_bytes_t root,
    ngx_http_cache_rocksdb_bytes_t zone, ngx_http_cache_rocksdb_bytes_t path,
    int fd, uint32_t peer_uid, ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error);

#define NGX_HTTP_CACHE_PURGE_REMOVED   1
#define NGX_HTTP_CACHE_PURGE_MISSING   2
#define NGX_HTTP_CACHE_PURGE_REPLACED  3
/* Per-file I/O failure keeps the executor available for other requests. */
#define NGX_HTTP_CACHE_PURGE_FILE_ERROR  64

/* Executor-only physical removal of one exact indexed instance. Requires the
 * exclusive OFD path lease and all cache writers to use that same lease.
 * Retires the index before unlink, then syncs the parent directories. A changed
 * instance is never unlinked. Retired metadata remains for task/node cleanup;
 * the caller may collect it only after its remaining obligations are complete.
 * A failure after retirement is recoverable by reading the current revision.
 */
int ngx_http_cache_purge_publications_remove(
    ngx_http_cache_purge_publications_t *publications,
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    int anchor_fd, int lease_fd, uint32_t peer_uid, uint32_t *outcome,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);

typedef struct {
    ngx_http_cache_purge_index_expected_t    file;
    unsigned char                            transaction[16];
    ngx_http_cache_rocksdb_bytes_t           before, after;
    const ngx_http_cache_rocksdb_bytes_t    *tags;
    size_t                                   tag_count;
    int                                      replace_tags;
    const ngx_http_cache_purge_watermark_t  *origin;
} ngx_http_cache_purge_validation_t;

/* Called on the owning executor immediately before the metadata transaction.
 * The caller must reject a conflicting accepted operation, including controls
 * not yet published to workers. No SHM guard may span the subsequent I/O. */
typedef int (*ngx_http_cache_purge_validation_guard_pt)(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error);

/* Prepare persists the bounded header plan, TAGs and existing path intent in
 * one transaction. It does not write the file. Finish writes only that prefix,
 * fsyncs it, and atomically publishes the new proof and releases the intent.
 * The native consumer supplies its fixed cache header, never entity bytes.
 * Both calls borrow the descriptors and require the exclusive OFD path lease.
 */
int ngx_http_cache_purge_publications_validate_prepare(
    ngx_http_cache_purge_publications_t *publications,
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_validation_t *validation,
    int anchor_fd, int lease_fd, int selected_fd, uint32_t peer_uid,
    ngx_http_cache_purge_validation_guard_pt guard, void *guard_data,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);
/* NULL transaction and revision zero select the persisted plan for recovery.
 * A missing/replaced final file may return a RETIRED result. */
int ngx_http_cache_purge_publications_validate_finish(
    ngx_http_cache_purge_publications_t *publications,
    ngx_http_cache_rocksdb_t *store, const unsigned char id[16],
    const unsigned char transaction[16], uint64_t revision,
    int anchor_fd, int lease_fd, uint32_t peer_uid,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* _NGX_HTTP_CACHE_PURGE_INTERNAL_H_INCLUDED_ */
