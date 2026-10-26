/* Copyright (C) Hanada */
#ifndef NGX_CACHE_PURGE_COMPONENT_H
#define NGX_CACHE_PURGE_COMPONENT_H

#include "ngx_cache_purge_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_CACHE_PURGE_COMPONENT_ABI 5
#define NGX_CACHE_PURGE_PROTOCOL_VERSION 1
#define NGX_CACHE_PURGE_FRAME_SIZE 24

typedef struct ngx_cache_purge_component_s ngx_cache_purge_component_t;

typedef enum {
    NGX_PURGE_COMPONENT_STARTING = 0,
    NGX_PURGE_COMPONENT_STANDBY,
    NGX_PURGE_COMPONENT_OPEN,
    NGX_PURGE_COMPONENT_STOPPING,
    NGX_PURGE_COMPONENT_STOPPED,
    NGX_PURGE_COMPONENT_FAILED
} ngx_cache_purge_component_state_t;

typedef enum {
    NGX_PURGE_OP_PING = 1,
    NGX_PURGE_OP_GET,
    NGX_PURGE_OP_PREPARE,
    NGX_PURGE_OP_COMMIT,
    NGX_PURGE_OP_ABORT,
    NGX_PURGE_OP_RETIRE,
    NGX_PURGE_OP_COLLECT,
    NGX_PURGE_OP_QUERY,
    NGX_PURGE_OP_PENDING,
    NGX_PURGE_OP_RETIRED,
    NGX_PURGE_OP_COLLECT_KEYS,
    NGX_PURGE_OP_ADD_URL,
    NGX_PURGE_OP_UPDATE,
    NGX_PURGE_OP_ACCEPT_OPERATION,
    NGX_PURGE_OP_GET_OPERATION,
    NGX_PURGE_OP_GET_CONTROL_STATUS,
    NGX_PURGE_OP_ACQUIRE_FILE_LEASE, /* string canonical address -> digest[16], offset u64 + one FD */
    NGX_PURGE_OP_PREPARE_FILE, /* file, previous ID, temp path; two FDs: lease, temp */
    NGX_PURGE_OP_FINISH_FILE /* file ID, prepared revision; one FD: lease */
} ngx_cache_purge_opcode_t;

#define NGX_PURGE_ROOT_CACHE 1
#define NGX_PURGE_ROOT_TEMP  2
typedef struct {
    ngx_cache_purge_bytes_t path, zone;
    uint32_t kind;
} ngx_cache_purge_root_t;

typedef struct {
    uint32_t abi;
    ngx_cache_purge_store_options_t store;
    uint32_t max_connections;
    uint32_t max_payload_bytes;
    uint32_t buffer_bytes;       /* total retained connection payloads */
    uint32_t io_timeout_ms;      /* frame or response deadline, not idle extension */
    uint32_t owner_retry_ms;
    uint32_t client_uid;        /* UINT32_MAX: creator UID; socket owner/client */
    uint32_t propagation_ms;
    uint32_t max_live_operations;
    uint64_t live_selector_bytes;
    void    *control_memory;     /* shared mapping owned by nginx, valid until stop */
    size_t   control_bytes;
    const ngx_cache_purge_root_t *roots; /* Copied by start; opened only on executor. */
    uint32_t root_count;
} ngx_cache_purge_component_options_t;

typedef struct {
    uint32_t state;
    uint32_t error_code;
    uint64_t connections;
    uint64_t requests;
    uint64_t rejected;
    uint64_t buffered_bytes;
    uint64_t peak_buffered_bytes;
    uint32_t control_state;      /* 0: no table; otherwise Coordination state */
    uint64_t control_publication;
} ngx_cache_purge_component_status_t;

void ngx_cache_purge_component_options(ngx_cache_purge_component_options_t *options);
uint32_t ngx_cache_purge_component_abi_5(void); /* Link-time header/archive compatibility marker. */

/* Only start after fork in the selected nginx worker, never while parsing
 * configuration or in master. Copies its inputs and starts one executor thread.
 * OPEN means index transport is available, not that unfinished cache writes or
 * purge fences have been recovered. Socket: <path>/component.sock, mode 0600.
 * Only the creator UID and client_uid may connect. A different client_uid
 * requires a root creator and receives ownership of the socket, not the DB.
 * No nginx request, pool, log or configuration pointers cross the thread. */
int ngx_cache_purge_component_start(const char *path,
    const ngx_cache_purge_component_options_t *options,
    ngx_cache_purge_component_t **component, ngx_cache_purge_store_error_t *error);

/* These calls belong to the starting thread/process. status never queries DB.
 * request_stop only signals/wakes the executor; it neither joins nor does I/O
 * on the DB. Use it while an HTTP worker is draining long-lived requests.
 * stop wakes STANDBY/poll, joins the executor and closes DB before returning.
 * It can wait for a running sync write; it does not finish an entire purge job.
 * A lost response is ambiguous: query/retry by durable identity, never assume
 * that disconnect or shutdown rolled back an acknowledged database write. */
int ngx_cache_purge_component_status(ngx_cache_purge_component_t *component,
    ngx_cache_purge_component_status_t *status, ngx_cache_purge_store_error_t *error);
int ngx_cache_purge_component_request_stop(ngx_cache_purge_component_t *component,
    ngx_cache_purge_store_error_t *error);
int ngx_cache_purge_component_stop(ngx_cache_purge_component_t **component,
    ngx_cache_purge_store_error_t *error);

#ifdef __cplusplus
}
#endif
#endif
