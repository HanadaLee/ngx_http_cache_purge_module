/* Copyright (C) Hanada */
#ifndef _NGX_CACHE_PURGE_CLIENT_H_INCLUDED_
#define _NGX_CACHE_PURGE_CLIENT_H_INCLUDED_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_event.h>
#include <ngx_event_connect.h>
#include "ngx_cache_purge_component.h"

typedef struct ngx_cache_purge_client_s ngx_cache_purge_client_t;
typedef struct ngx_cache_purge_call_s ngx_cache_purge_call_t;

typedef enum {
    NGX_PURGE_CLIENT_OK = 0,
    NGX_PURGE_CLIENT_IO,
    NGX_PURGE_CLIENT_TIMEOUT,
    NGX_PURGE_CLIENT_PROTOCOL,
    NGX_PURGE_CLIENT_LIMIT,
    NGX_PURGE_CLIENT_OWNER,
    NGX_PURGE_CLIENT_STOPPED
} ngx_cache_purge_client_result_t;

typedef struct {
    ngx_uint_t max_pending;
    size_t request_bytes;
    size_t max_payload;
    ngx_msec_t idle_timeout;
    uid_t server_uid;
} ngx_cache_purge_client_options_t;

typedef struct {
    uint64_t accepted, completed, cancelled, rejected, connections;
    ngx_uint_t pending;
    size_t request_bytes, response_bytes, peak_bytes;
} ngx_cache_purge_client_stats_t;

typedef struct {
    ngx_cache_purge_client_result_t transport;
    uint32_t result; /* Component result, meaningful only for transport OK. */
    ngx_str_t payload; /* Borrowed bytes, valid only inside the callback. */
    int lease_fd; /* Borrowed OFD lease, or -1. Callback may dup with CLOEXEC;
                   * the consumer must bound/close its retained references. */
    unsigned may_have_executed:1;
} ngx_cache_purge_reply_t;

typedef void (*ngx_cache_purge_client_done_pt)(ngx_cache_purge_call_t *call,
    void *data, const ngx_cache_purge_reply_t *reply);

void ngx_cache_purge_client_options(ngx_cache_purge_client_options_t *options);
ngx_cache_purge_client_t *ngx_cache_purge_client_create(ngx_pool_t *pool,
    ngx_log_t *log, const ngx_str_t *socket_path,
    const ngx_cache_purge_client_options_t *options);

/* Worker event loop only; no HTTP request retention is implicit.
 * OK accepts a copied payload and guarantees a later callback, unless cancelled.
 * BUSY/ERROR accepts nothing and invokes no callback. The deadline includes
 * queueing. No request is automatically replayed after a transport failure.
 * Callers must retain callback data until completion or cancel from cleanup. */
ngx_int_t ngx_cache_purge_client_submit(ngx_cache_purge_client_t *client,
    uint16_t opcode, const ngx_str_t *payload, ngx_msec_t timeout,
    ngx_cache_purge_client_done_pt done, void *data,
    ngx_cache_purge_call_t **call);

/* PREPARE_FILE accepts two borrowed input FDs (lease, complete temp file).
 * Successful submit copies them with CLOEXEC; the caller keeps its originals.
 * Copies are bounded by 2 * max_pending and released on send/cancel/failure.
 * No other current opcode accepts request descriptors. */
ngx_int_t ngx_cache_purge_client_submit_files(ngx_cache_purge_client_t *client,
    const ngx_str_t *payload, const int fds[2], ngx_msec_t timeout,
    ngx_cache_purge_client_done_pt done, void *data, ngx_cache_purge_call_t **call);

/* Cancel consumes the handle and suppresses its callback. Cancellation does
 * not roll back a request already sent. Clear the caller's handle afterwards. */
void ngx_cache_purge_client_cancel(ngx_cache_purge_call_t *call);
/* stop rejects new work, closes the socket and completes outstanding calls
 * synchronously with STOPPED. Invoke from exit_process, not from a thread. */
void ngx_cache_purge_client_stop(ngx_cache_purge_client_t *client);
void ngx_cache_purge_client_stats(ngx_cache_purge_client_t *client,
    ngx_cache_purge_client_stats_t *stats);

#endif
