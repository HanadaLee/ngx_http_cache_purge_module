/* Copyright (C) Hanada */
#ifndef _NGX_HTTP_CACHE_PURGE_COMPONENT_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_COMPONENT_H_INCLUDED_

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
    NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION
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
    uint32_t                          buffer_bytes;


    /* frame or response deadline, not idle extension */
    uint32_t                          io_timeout_ms;

    uint32_t                          owner_retry_ms;

    /* UINT32_MAX: creator UID; socket owner/client */
    uint32_t                          client_uid;

    uint32_t                          propagation_ms;
    uint32_t                          max_live_operations;
    uint64_t                          live_selector_bytes;

    /* shared mapping owned by nginx, valid until stop */
    void                             *control_memory;

    size_t                            control_bytes;
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
uint32_t ngx_http_cache_purge_component_abi_5(
    void); /* Link-time header/archive compatibility marker. */

/* Only start after fork in the selected nginx worker, never while parsing
 * configuration or in master. Copies its inputs and starts one executor thread.
 * OPEN means index transport is available, not that unfinished cache writes or
 * purge fences have been recovered. Socket: <path>/component.sock, mode 0600.
 * Only the creator UID and client_uid may connect. A different client_uid
 * requires a root creator and receives ownership of the socket, not the DB.
 * No nginx request, pool, log or configuration pointers cross the thread. */
int ngx_http_cache_purge_component_start(
    const char *path, const ngx_http_cache_purge_component_options_t *options,
    ngx_http_cache_purge_component_t **component,
    ngx_http_cache_rocksdb_error_t    *error);

/* These calls belong to the starting thread/process. status never queries DB.
 * request_stop only signals/wakes the executor; it neither joins nor does I/O
 * on the DB. Use it while an HTTP worker is draining long-lived requests.
 * stop wakes STANDBY/poll, joins the executor and closes DB before returning.
 * It can wait for a running sync write; it does not finish an entire purge job.
 * A lost response is ambiguous: query/retry by durable identity, never assume
 * that disconnect or shutdown rolled back an acknowledged database write. */
int ngx_http_cache_purge_component_status(
    ngx_http_cache_purge_component_t        *component,
    ngx_http_cache_purge_component_status_t *status,
    ngx_http_cache_rocksdb_error_t          *error);
int ngx_http_cache_purge_component_request_stop(
    ngx_http_cache_purge_component_t *component,
    ngx_http_cache_rocksdb_error_t   *error);
int ngx_http_cache_purge_component_stop(
    ngx_http_cache_purge_component_t **component,
    ngx_http_cache_rocksdb_error_t    *error);

#ifdef __cplusplus
}
#endif
#endif /* _NGX_HTTP_CACHE_PURGE_COMPONENT_H_INCLUDED_ */
