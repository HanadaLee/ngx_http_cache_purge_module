/* Copyright (C) Hanada */

#ifndef _NGX_HTTP_CACHE_PURGE_INTERNAL_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_INTERNAL_H_INCLUDED_

#include "rocksdb/ngx_http_cache_rocksdb.h"

#define NGX_HTTP_CACHE_PURGE_TAG_MAX       128
#define NGX_HTTP_CACHE_PURGE_TAG_SELECT    32
#define NGX_HTTP_CACHE_PURGE_TAG_LENGTH    1024
#define NGX_HTTP_CACHE_PURGE_TAG_LIST_MAX                                     \
    (NGX_HTTP_CACHE_PURGE_TAG_SELECT * (NGX_HTTP_CACHE_PURGE_TAG_LENGTH + 1)  \
        - 1)

#ifdef __cplusplus
extern "C" {
#endif

/* Compact external index; no cache-file metadata or publication states. */
typedef struct ngx_http_cache_purge_catalog_s  ngx_http_cache_purge_catalog_t;
typedef struct ngx_http_cache_purge_scan_s     ngx_http_cache_purge_scan_t;

enum {
    NGX_HTTP_CACHE_PURGE_SELECT_URL = 0,
    NGX_HTTP_CACHE_PURGE_SELECT_PREFIX,
    NGX_HTTP_CACHE_PURGE_SELECT_KEY,
    NGX_HTTP_CACHE_PURGE_SELECT_HOST,
    NGX_HTTP_CACHE_PURGE_SELECT_TAG
};

typedef struct {
    unsigned char                   id[24];
    unsigned char                   hash[16];
    uint64_t                        uniq;
    uint64_t                        created;
    uint64_t                        retain_until;
    ngx_http_cache_rocksdb_bytes_t  url;
    ngx_http_cache_rocksdb_bytes_t  host;
    ngx_http_cache_rocksdb_bytes_t  namespace_value;
    ngx_http_cache_rocksdb_bytes_t  misc;
    ngx_http_cache_rocksdb_bytes_t  root;
    ngx_http_cache_rocksdb_bytes_t  zone;
    ngx_http_cache_rocksdb_bytes_t  path;
    ngx_http_cache_rocksdb_bytes_t  tags[NGX_HTTP_CACHE_PURGE_TAG_MAX];
    size_t                          tag_count;
} ngx_http_cache_purge_entry_t;

typedef struct {
    unsigned                        type;
    unsigned                        host_scope;
    ngx_http_cache_rocksdb_bytes_t  value;
    ngx_http_cache_rocksdb_bytes_t  namespace_value;
    ngx_http_cache_rocksdb_bytes_t  misc;
} ngx_http_cache_purge_select_t;

int ngx_http_cache_purge_select_tags(ngx_http_cache_rocksdb_bytes_t value,
    ngx_http_cache_rocksdb_bytes_t *tags, size_t *count);

int ngx_http_cache_purge_catalog_open(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_purge_catalog_t  **catalog,
    ngx_http_cache_rocksdb_error_t *error);
void ngx_http_cache_purge_catalog_close(
    ngx_http_cache_purge_catalog_t *catalog);
int ngx_http_cache_purge_catalog_put(ngx_http_cache_purge_catalog_t *catalog,
    const ngx_http_cache_purge_entry_t  *entry,
    ngx_http_cache_rocksdb_error_t *error);
/* Entry strings are borrowed until the next catalog operation. Mutation
 * inputs must be caller-owned. The fixed id/hash members are copied. */
int ngx_http_cache_purge_catalog_get(ngx_http_cache_purge_catalog_t *catalog,
    const unsigned char  id[24], ngx_http_cache_purge_entry_t *entry,
    ngx_http_cache_rocksdb_error_t *error);
typedef int (*ngx_http_cache_purge_check_pt)(void *data,
    const ngx_http_cache_purge_entry_t *entry,
    ngx_http_cache_rocksdb_error_t *error);
/* Walk one configured root. Start with a zero bookmark; DONE ends the range.
 * A failed callback leaves that row for retry. Each call closes its snapshot.
 * */
int ngx_http_cache_purge_catalog_walk(ngx_http_cache_purge_catalog_t *catalog,
    ngx_http_cache_rocksdb_bytes_t root, unsigned char after[24], size_t limit,
    ngx_http_cache_purge_check_pt check, void *data,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_catalog_remove(ngx_http_cache_purge_catalog_t *catalog,
    const unsigned char  id[24], uint64_t expected,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_catalog_retain(ngx_http_cache_purge_catalog_t *catalog,
    const unsigned char  id[24], uint64_t expected, uint64_t until,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_catalog_collect(
    ngx_http_cache_purge_catalog_t *catalog, uint64_t now, size_t limit,
    size_t *removed, ngx_http_cache_rocksdb_error_t *error);
typedef int (*ngx_http_cache_purge_tag_pt)(void *data,
    ngx_http_cache_rocksdb_bytes_t tag);
int ngx_http_cache_purge_catalog_tags(ngx_http_cache_purge_catalog_t *catalog,
    const ngx_http_cache_purge_entry_t *identity,
    ngx_http_cache_purge_tag_pt handler, void *data,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_scan_open(ngx_http_cache_purge_catalog_t *catalog,
    const ngx_http_cache_purge_select_t *selector,
    ngx_http_cache_rocksdb_bytes_t after, int retained, uint64_t now,
    ngx_http_cache_purge_scan_t **scan, ngx_http_cache_rocksdb_error_t *error);
/* Returns file identity and location, without selector strings or TAGs. */
int ngx_http_cache_purge_scan_next(ngx_http_cache_purge_scan_t *scan,
    ngx_http_cache_purge_entry_t  *entry,
                                  ngx_http_cache_rocksdb_error_t *error);
ngx_http_cache_rocksdb_bytes_t ngx_http_cache_purge_scan_bookmark(
    ngx_http_cache_purge_scan_t *scan);
void ngx_http_cache_purge_scan_close(ngx_http_cache_purge_scan_t *scan);

/* Single-owner background jobs. All calls may block; never call on a HIT. */
typedef struct ngx_http_cache_purge_jobs_s  ngx_http_cache_purge_jobs_t;

enum {
    NGX_HTTP_CACHE_PURGE_DELETE = 0,
    NGX_HTTP_CACHE_PURGE_INVALIDATE
};

typedef struct {
    unsigned char                   id[16];
    unsigned                        action;
    unsigned                        done;
    uint64_t                        issued;
    uint64_t                        retain_until;
    uint64_t                        visited;
    uint64_t                        affected;
    uint64_t                        skipped;
    uint32_t                        error;
    ngx_http_cache_purge_select_t   select;
    ngx_http_cache_rocksdb_bytes_t  bookmark;
} ngx_http_cache_purge_job_t;

/* Apply one native file operation and arrange its native-node update.
 * NOT_FOUND means absent; CONFLICT means a replaced file was preserved.
 * Other errors leave this entry pending for a later background retry. */
typedef int (*ngx_http_cache_purge_apply_pt)(void *data,
    const ngx_http_cache_purge_entry_t  *entry, unsigned action,
    ngx_http_cache_rocksdb_error_t *error);

int ngx_http_cache_purge_jobs_open(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_purge_catalog_t  *catalog,
    ngx_http_cache_purge_jobs_t    **jobs,
    ngx_http_cache_rocksdb_error_t *error);
void ngx_http_cache_purge_jobs_close(ngx_http_cache_purge_jobs_t *jobs);

/* Submit inputs are caller-owned. Returned strings are borrowed until the
 * next jobs operation. A duplicate does not restart work or extend retention.
 * Successful submission is durable; the caller must publish its shared rule
 * before returning 202 or starting background file processing. */
int ngx_http_cache_purge_job_submit(ngx_http_cache_purge_jobs_t *jobs,
    const ngx_http_cache_purge_job_t  *job, int *duplicate,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_job_get(ngx_http_cache_purge_jobs_t *jobs,
    const unsigned char  id[16], ngx_http_cache_purge_job_t *job,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_job_next(ngx_http_cache_purge_jobs_t *jobs,
    const unsigned char  *after, ngx_http_cache_purge_job_t *job,
    ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_job_step(ngx_http_cache_purge_jobs_t *jobs,
    const unsigned char            id[16], uint64_t now, size_t limit,
    ngx_http_cache_purge_apply_pt  apply, void *data,
    ngx_http_cache_purge_job_t    *job, ngx_http_cache_rocksdb_error_t *error);

#ifdef __cplusplus
}
#endif

#endif /* _NGX_HTTP_CACHE_PURGE_INTERNAL_H_INCLUDED_ */
