
/*
 * Copyright (C) Hanada
 */


#ifndef _NGX_HTTP_CACHE_ROCKSDB_H_INCLUDED_
#define _NGX_HTTP_CACHE_ROCKSDB_H_INCLUDED_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_HTTP_CACHE_ROCKSDB_ABI  1

typedef struct ngx_http_cache_rocksdb_s         ngx_http_cache_rocksdb_t;
typedef struct ngx_http_cache_rocksdb_cursor_s  ngx_http_cache_rocksdb_cursor_t;

typedef enum {
    NGX_HTTP_CACHE_ROCKSDB_DEFAULT = 0,
    NGX_HTTP_CACHE_ROCKSDB_URL,
    NGX_HTTP_CACHE_ROCKSDB_TAG,
    NGX_HTTP_CACHE_ROCKSDB_CF_COUNT
} ngx_http_cache_rocksdb_cf_e;


typedef enum {
    NGX_HTTP_CACHE_ROCKSDB_OK = 0,
    NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
    NGX_HTTP_CACHE_ROCKSDB_DONE,
    NGX_HTTP_CACHE_ROCKSDB_BUFFER,
    NGX_HTTP_CACHE_ROCKSDB_INVALID,
    NGX_HTTP_CACHE_ROCKSDB_LIMIT,
    NGX_HTTP_CACHE_ROCKSDB_BUSY,
    NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
    NGX_HTTP_CACHE_ROCKSDB_EXPIRED,
    NGX_HTTP_CACHE_ROCKSDB_OWNER,
    NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
    NGX_HTTP_CACHE_ROCKSDB_IO
} ngx_http_cache_rocksdb_result_e;


typedef struct {
    const unsigned char  *data;
    size_t                len;
} ngx_http_cache_rocksdb_bytes_t;


typedef struct {
    unsigned char  *data;
    size_t          capacity;
    size_t          length; /* required size, including on BUFFER */
} ngx_http_cache_rocksdb_buffer_t;


typedef struct {
    int   code;
    char  message[256];
} ngx_http_cache_rocksdb_error_t;


typedef struct {
    uint32_t  abi;
    size_t    cache_bytes; /* shared by all three CFs, including WBM charge */
    size_t    write_buffer_bytes;
    size_t    max_key_bytes;
    size_t    max_value_bytes;
    size_t    max_batch_bytes; /* inputs plus conservative per-entry overhead */
    uint32_t  max_batch_entries;
    uint32_t  max_cursors;
    uint32_t  cursor_lifetime_ms;
    uint32_t  max_open_files;
    uint32_t  background_jobs;
    uint64_t  io_rate; /* background bytes per second; zero disables limiting */
} ngx_http_cache_rocksdb_options_t;


typedef enum {
    NGX_HTTP_CACHE_ROCKSDB_PUT = 0,
    NGX_HTTP_CACHE_ROCKSDB_DELETE
} ngx_http_cache_rocksdb_mutation_type_e;


typedef struct {
    ngx_http_cache_rocksdb_cf_e             cf;
    ngx_http_cache_rocksdb_mutation_type_e  type;
    ngx_http_cache_rocksdb_bytes_t          key;
    ngx_http_cache_rocksdb_bytes_t          value;
} ngx_http_cache_rocksdb_mutation_t;


typedef struct {
    ngx_http_cache_rocksdb_cf_e     cf;
    ngx_http_cache_rocksdb_bytes_t  key;
    ngx_http_cache_rocksdb_bytes_t  value;

    /* 0: must be absent; 1: exact expected value */
    int                             exists;

} ngx_http_cache_rocksdb_condition_t;


typedef struct {
    uint64_t  cache_capacity;
    uint64_t  cache_usage;
    uint64_t  cache_pinned;
    uint64_t  write_buffer_usage;
    uint64_t  table_reader_memory;
    uint64_t  memtable_memory;
    uint64_t  active_cursors;
    uint64_t  cursor_handles;
    uint64_t  writes;
    uint64_t  conflicts;
} ngx_http_cache_rocksdb_stats_t;


void ngx_http_cache_rocksdb_init_options(
    ngx_http_cache_rocksdb_options_t *options);
uint32_t ngx_http_cache_rocksdb_abi_1(void);

/*
 * Blocking APIs: call only on the owning process and executor thread.
 * A handle belongs to its opening process/thread and must never cross fork,
 * IPC or SHM. The directory must already exist and be writable (nginx creates
 * it through ngx_add_path). Open synchronizes the recovered WAL prefix before
 * returning the handle. No C++ exception crosses this boundary.
 */
int ngx_http_cache_rocksdb_open(const char *path,
    const ngx_http_cache_rocksdb_options_t *options,
    ngx_http_cache_rocksdb_t **store, ngx_http_cache_rocksdb_error_t *error);

/*
 * The writer may create one bounded read-only handle sharing its DB, CFs and
 * cache. No database reopen or second cache. Before attach, only the creator
 * may close an abandoned handle. Hand it exclusively to the reader thread,
 * which must attach once before get/cursor/stats/close. The writer must not
 * touch the handed-off handle. Reader cursors have a separate budget no larger
 * than the writer's max_cursors; snapshots retain the same lifetime bound.
 * A reader cannot write, sync, inspect durability tickets or create readers.
 * Join the reader before closing the writer. No handle may cross fork.
 */
int ngx_http_cache_rocksdb_reader_create(ngx_http_cache_rocksdb_t *store,
    uint32_t max_cursors, ngx_http_cache_rocksdb_t **reader,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_rocksdb_reader_attach(ngx_http_cache_rocksdb_t *reader,
    ngx_http_cache_rocksdb_error_t *error);

/* BUSY with open cursors or reader; otherwise consumes *store even on error. */
int ngx_http_cache_rocksdb_close(ngx_http_cache_rocksdb_t **store,
    ngx_http_cache_rocksdb_error_t *error);

int ngx_http_cache_rocksdb_get(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_rocksdb_buffer_t *value,
    ngx_http_cache_rocksdb_error_t *error);

/*
 * Atomic cross-CF WriteBatch with WAL and sync=true. Conditions are evaluated
 * before any mutation by the single authorized writer. No partial success.
 */
int ngx_http_cache_rocksdb_write(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_rocksdb_mutation_t *mutations, size_t count,
    const ngx_http_cache_rocksdb_condition_t *conditions,
    size_t condition_count, ngx_http_cache_rocksdb_error_t *error);

/*
 * Same atomic WAL batch, with durability deferred for recoverable cache rows.
 * The owner must call sync periodically. A synchronous write also flushes all
 * earlier deferred batches. No extra thread or retained mutation buffer.
 */
int ngx_http_cache_rocksdb_write_deferred(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_rocksdb_mutation_t *mutations, size_t count,
    const ngx_http_cache_rocksdb_condition_t *conditions,
    size_t condition_count, ngx_http_cache_rocksdb_error_t *error);

int ngx_http_cache_rocksdb_sync(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_error_t *error);

/*
 * Nonblocking owner-thread snapshot of successful batch tickets since open.
 * A nonzero written ticket is durable when synced >= that ticket. Later
 * deferred writes do not revoke an earlier durable ticket. This only reads
 * counters; it does not sync the WAL or query RocksDB. Tickets belong to this
 * handle and must not be persisted, sent to workers or reused after reopen.
 * Zero means no batch on this handle; open synchronized the recovered prefix.
 * These counters alone do not validate any particular recovered file record.
 * Outputs are cleared on error and must be distinct non-null pointers.
 */
int ngx_http_cache_rocksdb_durability(ngx_http_cache_rocksdb_t *store,
    uint64_t *written, uint64_t *synced, ngx_http_cache_rocksdb_error_t *error);

/*
 * Bytewise prefix scan, with an optional exclusive resume key in that prefix.
 * An empty prefix scans the whole CF, including the reserved schema marker.
 * No prefix truncation or fixed-length prefix extractor is used. next/get use
 * a bounded-lifetime snapshot; forked cursors share its original deadline.
 */
int ngx_http_cache_rocksdb_cursor_open(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t prefix,
    ngx_http_cache_rocksdb_bytes_t after,
    ngx_http_cache_rocksdb_cursor_t **cursor,
    ngx_http_cache_rocksdb_error_t *error);
/*
 * Same bounds, snapshot, deadline and handle budget, but read blocks may enter
 * the existing shared cache. Use only for short probes; bulk scans use open.
 * Forks always keep the bulk no-fill policy, including from a cached parent.
 */
int ngx_http_cache_rocksdb_cursor_open_cached(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t prefix,
    ngx_http_cache_rocksdb_bytes_t after,
    ngx_http_cache_rocksdb_cursor_t **cursor,
    ngx_http_cache_rocksdb_error_t *error);
/*
 * Open a related range using the parent's exact snapshot and deadline. A child
 * remains valid after its parent closes, until that shared deadline expires.
 * Every cursor consumes one bounded slot and must be closed, even on EXPIRED.
 */
int ngx_http_cache_rocksdb_cursor_fork(ngx_http_cache_rocksdb_cursor_t *parent,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t prefix,
    ngx_http_cache_rocksdb_bytes_t after,
    ngx_http_cache_rocksdb_cursor_t **cursor,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_rocksdb_cursor_next(ngx_http_cache_rocksdb_cursor_t *cursor,
    ngx_http_cache_rocksdb_buffer_t *key,
    ngx_http_cache_rocksdb_buffer_t *value,
    ngx_http_cache_rocksdb_error_t *error);
/*
 * Seek to the greatest key <= target in this cursor's prefix and snapshot.
 * target must include the prefix. BUFFER does not consume the selected row;
 * success advances like next(). DONE means no predecessor in this prefix.
 */
int ngx_http_cache_rocksdb_cursor_seek_le(
    ngx_http_cache_rocksdb_cursor_t *cursor,
    ngx_http_cache_rocksdb_bytes_t target,
    ngx_http_cache_rocksdb_buffer_t *key,
    ngx_http_cache_rocksdb_buffer_t *value,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_rocksdb_cursor_get(ngx_http_cache_rocksdb_cursor_t *cursor,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_rocksdb_buffer_t *value,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_rocksdb_cursor_close(
    ngx_http_cache_rocksdb_cursor_t **cursor,
    ngx_http_cache_rocksdb_error_t *error);

int ngx_http_cache_rocksdb_stats(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_stats_t *stats,
    ngx_http_cache_rocksdb_error_t *error);

#ifdef __cplusplus
}
#endif
#endif /* _NGX_HTTP_CACHE_ROCKSDB_H_INCLUDED_ */
