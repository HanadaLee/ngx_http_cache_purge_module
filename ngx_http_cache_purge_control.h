/* Copyright (C) Hanada */
#ifndef _NGX_HTTP_CACHE_PURGE_CONTROL_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_CONTROL_H_INCLUDED_

#include "rocksdb/ngx_http_cache_rocksdb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_HTTP_CACHE_PURGE_CONTROL_ABI 1
#define NGX_HTTP_CACHE_PURGE_CONTROL_FENCE 1
#define NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE 2
#define NGX_HTTP_CACHE_PURGE_CONTROL_PREPARING 4

typedef struct ngx_http_cache_purge_control_reader_s
    ngx_http_cache_purge_control_reader_t;
typedef struct ngx_http_cache_purge_control_writer_s
    ngx_http_cache_purge_control_writer_t;

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
                                      size_t bank_bytes, uint32_t readers,
                                      ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_reader_open(
    void *memory, size_t bytes, ngx_http_cache_purge_control_reader_t **reader,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_reader_close(
    ngx_http_cache_purge_control_reader_t **reader,
    ngx_http_cache_rocksdb_error_t         *error);
int ngx_http_cache_purge_control_match(
    ngx_http_cache_purge_control_reader_t        *reader,
    const ngx_http_cache_purge_control_context_t *context, size_t max_matches,
    size_t max_bytes, ngx_http_cache_purge_control_visit_pt visit, void *data,
    uint64_t *publication, ngx_http_cache_rocksdb_error_t *error);
/* Pin the exact matched publication across a short synchronous admission step.
 * A new effective publication cannot reopen its gate until this guard leaves.
 * Do not hold a guard across event waits, upstream I/O or response body
 * streaming. */
int ngx_http_cache_purge_control_guard_begin(
    ngx_http_cache_purge_control_reader_t *reader, uint64_t publication,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_guard_end(
    ngx_http_cache_purge_control_reader_t *reader,
    ngx_http_cache_rocksdb_error_t        *error);

/* Claim only while holding the durable database's exclusive owner lease.
 * Claim closes the serving gate until controls and native file state recover.
 * Reader registration and publishing use a robust process-shared mutex;
 * normal matching takes no mutex and performs no allocation or DB I/O. */
int ngx_http_cache_purge_control_writer_open(
    void *memory, size_t bytes, ngx_http_cache_purge_control_writer_t **writer,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_control_writer_close(
    ngx_http_cache_purge_control_writer_t **writer,
    ngx_http_cache_rocksdb_error_t         *error);
int ngx_http_cache_purge_control_gate_close(
    ngx_http_cache_purge_control_writer_t *writer,
    ngx_http_cache_rocksdb_error_t        *error);
/* Open only after the new image and durable activation/recovery are complete.
 * BUSY while old image readers are still inside synchronous admission work. */
int ngx_http_cache_purge_control_gate_open(
    ngx_http_cache_purge_control_writer_t *writer,
    ngx_http_cache_rocksdb_error_t        *error);

#ifdef __cplusplus
}
#endif
#endif /* _NGX_HTTP_CACHE_PURGE_CONTROL_H_INCLUDED_ */
