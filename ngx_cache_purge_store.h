/* Copyright (C) Hanada */
#ifndef _NGX_CACHE_PURGE_STORE_H_INCLUDED_
#define _NGX_CACHE_PURGE_STORE_H_INCLUDED_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_CACHE_PURGE_STORE_ABI  1

typedef struct ngx_cache_purge_store_s ngx_cache_purge_store_t;
typedef struct ngx_cache_purge_cursor_s ngx_cache_purge_cursor_t;

typedef enum {
    NGX_PURGE_STORE_DEFAULT = 0,
    NGX_PURGE_STORE_URL,
    NGX_PURGE_STORE_TAG,
    NGX_PURGE_STORE_CF_COUNT
} ngx_cache_purge_cf_t;

typedef enum {
    NGX_PURGE_STORE_OK = 0,
    NGX_PURGE_STORE_NOT_FOUND,
    NGX_PURGE_STORE_DONE,
    NGX_PURGE_STORE_BUFFER,
    NGX_PURGE_STORE_INVALID,
    NGX_PURGE_STORE_LIMIT,
    NGX_PURGE_STORE_BUSY,
    NGX_PURGE_STORE_CONFLICT,
    NGX_PURGE_STORE_EXPIRED,
    NGX_PURGE_STORE_OWNER,
    NGX_PURGE_STORE_SCHEMA,
    NGX_PURGE_STORE_CORRUPT,
    NGX_PURGE_STORE_IO
} ngx_cache_purge_store_result_t;

typedef struct {
    const unsigned char *data;
    size_t               len;
} ngx_cache_purge_bytes_t;

typedef struct {
    unsigned char *data;
    size_t         capacity;
    size_t         length;     /* required size, including on BUFFER */
} ngx_cache_purge_buffer_t;

typedef struct {
    int   code;
    char  message[256];
} ngx_cache_purge_store_error_t;

typedef struct {
    uint32_t abi;
    size_t   cache_bytes;      /* shared by all three CFs, including WBM charge */
    size_t   write_buffer_bytes;
    size_t   max_key_bytes;
    size_t   max_value_bytes;
    size_t   max_batch_bytes;  /* inputs plus conservative per-entry overhead */
    uint32_t max_batch_entries;
    uint32_t max_cursors;
    uint32_t cursor_lifetime_ms;
    uint32_t max_open_files;
    uint32_t background_jobs;
} ngx_cache_purge_store_options_t;

typedef enum {
    NGX_PURGE_STORE_PUT = 0,
    NGX_PURGE_STORE_DELETE
} ngx_cache_purge_mutation_type_t;

typedef struct {
    ngx_cache_purge_cf_t             cf;
    ngx_cache_purge_mutation_type_t  type;
    ngx_cache_purge_bytes_t          key;
    ngx_cache_purge_bytes_t          value;
} ngx_cache_purge_mutation_t;

typedef struct {
    ngx_cache_purge_cf_t     cf;
    ngx_cache_purge_bytes_t  key;
    ngx_cache_purge_bytes_t  value;
    int                     exists; /* 0: must be absent; 1: exact expected value */
} ngx_cache_purge_condition_t;

typedef struct {
    uint64_t cache_capacity;
    uint64_t cache_usage;
    uint64_t cache_pinned;
    uint64_t write_buffer_usage;
    uint64_t table_reader_memory;
    uint64_t memtable_memory;
    uint64_t active_cursors;
    uint64_t cursor_handles;
    uint64_t writes;
    uint64_t conflicts;
} ngx_cache_purge_store_stats_t;

void ngx_cache_purge_store_options(ngx_cache_purge_store_options_t *options);

/* Blocking APIs: call only on the cache-manager component's executor thread.
 * A handle belongs to its opening process/thread and must never cross fork,
 * IPC or SHM. The directory must already exist and be writable (nginx creates
 * it through ngx_add_path). No C++ exception crosses this boundary. */
int ngx_cache_purge_store_open(const char *path,
    const ngx_cache_purge_store_options_t *options,
    ngx_cache_purge_store_t **store, ngx_cache_purge_store_error_t *error);

/* BUSY with open cursor handles; otherwise consumes *store even on close error. */
int ngx_cache_purge_store_close(ngx_cache_purge_store_t **store,
    ngx_cache_purge_store_error_t *error);

int ngx_cache_purge_store_get(ngx_cache_purge_store_t *store,
    ngx_cache_purge_cf_t cf, ngx_cache_purge_bytes_t key,
    ngx_cache_purge_buffer_t *value, ngx_cache_purge_store_error_t *error);

/* Atomic cross-CF WriteBatch with WAL and sync=true. Conditions are evaluated
 * before any mutation by the single authorized writer. No partial success. */
int ngx_cache_purge_store_write(ngx_cache_purge_store_t *store,
    const ngx_cache_purge_mutation_t *mutations, size_t count,
    const ngx_cache_purge_condition_t *conditions, size_t condition_count,
    ngx_cache_purge_store_error_t *error);

/* Bytewise prefix scan, with an optional exclusive resume key in that prefix.
 * No prefix truncation or fixed-length prefix extractor is used. Each cursor
 * owns a bounded-lifetime snapshot shared by cursor_next and cursor_get. */
int ngx_cache_purge_cursor_open(ngx_cache_purge_store_t *store,
    ngx_cache_purge_cf_t cf, ngx_cache_purge_bytes_t prefix,
    ngx_cache_purge_bytes_t after, ngx_cache_purge_cursor_t **cursor,
    ngx_cache_purge_store_error_t *error);
int ngx_cache_purge_cursor_next(ngx_cache_purge_cursor_t *cursor,
    ngx_cache_purge_buffer_t *key, ngx_cache_purge_buffer_t *value,
    ngx_cache_purge_store_error_t *error);
int ngx_cache_purge_cursor_get(ngx_cache_purge_cursor_t *cursor,
    ngx_cache_purge_cf_t cf, ngx_cache_purge_bytes_t key,
    ngx_cache_purge_buffer_t *value, ngx_cache_purge_store_error_t *error);
int ngx_cache_purge_cursor_close(ngx_cache_purge_cursor_t **cursor,
    ngx_cache_purge_store_error_t *error);

int ngx_cache_purge_store_stats(ngx_cache_purge_store_t *store,
    ngx_cache_purge_store_stats_t *stats, ngx_cache_purge_store_error_t *error);

#ifdef __cplusplus
}
#endif
#endif
