/* Copyright (C) Hanada */

#include "ngx_http_cache_purge_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>


#define NGX_HTTP_CACHE_PURGE_JOB_TEXT    32768
#define NGX_HTTP_CACHE_PURGE_JOB_TOKEN   32822
#define NGX_HTTP_CACHE_PURGE_JOB_HEADER  68
#define NGX_HTTP_CACHE_PURGE_JOB_SIZE                                         \
    (NGX_HTTP_CACHE_PURGE_JOB_HEADER + 3 * NGX_HTTP_CACHE_PURGE_JOB_TEXT      \
     + NGX_HTTP_CACHE_PURGE_JOB_TOKEN)


struct ngx_http_cache_purge_jobs_s {
    ngx_http_cache_rocksdb_t        *store;
    ngx_http_cache_purge_catalog_t  *catalog;
    unsigned char                    input[NGX_HTTP_CACHE_PURGE_JOB_SIZE];
    unsigned char                    output[NGX_HTTP_CACHE_PURGE_JOB_SIZE];
    unsigned char                    token[NGX_HTTP_CACHE_PURGE_JOB_TOKEN];
};


static ngx_http_cache_rocksdb_bytes_t  ngx_http_cache_purge_job_empty = {
    NULL, 0
};


static int
ngx_http_cache_purge_job_error(ngx_http_cache_rocksdb_error_t *error,
    int  code, const char *message)
{
    if (error != NULL) {
        error->code = code;
        snprintf(error->message, sizeof(error->message), "%s", message);
    }

    return code;
}


static unsigned char *
ngx_http_cache_purge_job_number(unsigned char *p, uint64_t value, size_t size)
{
    while (size) {
        *p++ = (unsigned char) (value >> (--size * 8));
    }

    return p;
}


static uint64_t
ngx_http_cache_purge_job_decode(const unsigned char *p, size_t size)
{
    uint64_t  value;

    value = 0;
    while (size--) {
        value = (value << 8) | *p++;
    }

    return value;
}


static int
ngx_http_cache_purge_job_valid(const ngx_http_cache_purge_job_t *job)
{
    const ngx_http_cache_purge_select_t  *s;
    ngx_http_cache_rocksdb_bytes_t        text[4];
    size_t                                i, maximum;

    s = &job->select;
    if (job->issued == 0 || job->action > NGX_HTTP_CACHE_PURGE_INVALIDATE
        || job->done > 1 || s->type > NGX_HTTP_CACHE_PURGE_SELECT_TAG
        || s->host_scope > 1 || s->value.len == 0
        || (s->host_scope && s->type != NGX_HTTP_CACHE_PURGE_SELECT_TAG)
        || (s->type == NGX_HTTP_CACHE_PURGE_SELECT_TAG && s->value.len > 1024))
    {
        return 0;
    }

    text[0] = s->value;
    text[1] = s->namespace_value;
    text[2] = s->misc;
    text[3] = job->bookmark;
    for (i = 0; i < 4; i++) {
        maximum = i == 3 ? NGX_HTTP_CACHE_PURGE_JOB_TOKEN
                         : NGX_HTTP_CACHE_PURGE_JOB_TEXT;
        if (text[i].len > maximum
            || (text[i].len != 0 && text[i].data == NULL))
        {
            return 0;
        }
    }

    return 1;
}


static size_t
ngx_http_cache_purge_job_encode(ngx_http_cache_purge_jobs_t *jobs,
    const ngx_http_cache_purge_job_t *job)
{
    unsigned char                  *p;
    ngx_http_cache_rocksdb_bytes_t  text[4];
    size_t                          i;

    p = jobs->output;
    memcpy(p, "PJ1", 4);
    p += 4;
    *p++ = (unsigned char) job->select.type;
    *p++ = (unsigned char) job->select.host_scope;
    *p++ = (unsigned char) job->action;
    *p++ = (unsigned char) job->done;
    p = ngx_http_cache_purge_job_number(p, job->issued, 8);
    p = ngx_http_cache_purge_job_number(p, job->retain_until, 8);
    p = ngx_http_cache_purge_job_number(p, job->visited, 8);
    p = ngx_http_cache_purge_job_number(p, job->affected, 8);
    p = ngx_http_cache_purge_job_number(p, job->skipped, 8);
    p = ngx_http_cache_purge_job_number(p, job->error, 4);
    text[0] = job->select.value;
    text[1] = job->select.namespace_value;
    text[2] = job->select.misc;
    text[3] = job->bookmark;
    for (i = 0; i < 4; i++) {
        p = ngx_http_cache_purge_job_number(p, text[i].len, 4);
    }

    for (i = 0; i < 4; i++) {
        if (text[i].len != 0) {
            memcpy(p, text[i].data, text[i].len);
            p += text[i].len;
        }
    }

    return (size_t) (p - jobs->output);
}


static int
ngx_http_cache_purge_job_save(ngx_http_cache_purge_jobs_t *jobs,
    const ngx_http_cache_purge_job_t  *job, int durable,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_rocksdb_mutation_t  rows[2];
    unsigned char                      keys[2][17];
    size_t                             length;

    length = ngx_http_cache_purge_job_encode(jobs, job);
    memset(rows, 0, sizeof(rows));
    keys[0][0] = 'J';
    keys[1][0] = 'Q';
    memcpy(keys[0] + 1, job->id, 16);
    memcpy(keys[1] + 1, job->id, 16);
    rows[0].key.data = keys[0];
    rows[0].key.len = sizeof(keys[0]);
    rows[0].value.data = jobs->output;
    rows[0].value.len = length;
    rows[1].key.data = keys[1];
    rows[1].key.len = sizeof(keys[1]);
    rows[1].type = job->done ? NGX_HTTP_CACHE_ROCKSDB_DELETE
                             : NGX_HTTP_CACHE_ROCKSDB_PUT;

    if (durable) {
        return ngx_http_cache_rocksdb_write(jobs->store, rows, 2, NULL, 0,
                                            error);
    }

    return ngx_http_cache_rocksdb_write_deferred(jobs->store, rows, 1,
                                                 NULL, 0, error);
}


int
ngx_http_cache_purge_jobs_open(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_purge_catalog_t  *catalog,
    ngx_http_cache_purge_jobs_t    **out, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_jobs_t  *jobs;

    if (out == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }

    *out = NULL;
    if (store == NULL || catalog == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }

    jobs = calloc(1, sizeof(*jobs));
    if (jobs == NULL) {
        return ngx_http_cache_purge_job_error(error,
                  NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cannot allocate purge jobs");
    }

    jobs->store = store;
    jobs->catalog = catalog;
    *out = jobs;

    return 0;
}


void
ngx_http_cache_purge_jobs_close(ngx_http_cache_purge_jobs_t *jobs)
{
    free(jobs);
}


int
ngx_http_cache_purge_job_get(ngx_http_cache_purge_jobs_t *jobs,
    const unsigned char  id[16], ngx_http_cache_purge_job_t *job,
    ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char                    raw[17];
    const unsigned char             *p;
    ngx_http_cache_rocksdb_bytes_t   key, text[4];
    ngx_http_cache_rocksdb_buffer_t  value;
    size_t                           i, length, offset;
    int                              rc;

    raw[0] = 'J';
    memcpy(raw + 1, id, 16);
    key.data = raw;
    key.len = sizeof(raw);
    value.data = jobs->input;
    value.capacity = sizeof(jobs->input);
    rc = ngx_http_cache_rocksdb_get(jobs->store,
                       NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, &value, error);
    if (rc != 0) {
        return rc;
    }

    if (value.length < NGX_HTTP_CACHE_PURGE_JOB_HEADER
        || memcmp(value.data, "PJ1", 4) != 0)
    {
        goto corrupt;
    }

    memset(job, 0, sizeof(*job));
    memcpy(job->id, raw + 1, 16);
    p = value.data;
    job->select.type = p[4];
    job->select.host_scope = p[5];
    job->action = p[6];
    job->done = p[7];
    job->issued = ngx_http_cache_purge_job_decode(p + 8, 8);
    job->retain_until = ngx_http_cache_purge_job_decode(p + 16, 8);
    job->visited = ngx_http_cache_purge_job_decode(p + 24, 8);
    job->affected = ngx_http_cache_purge_job_decode(p + 32, 8);
    job->skipped = ngx_http_cache_purge_job_decode(p + 40, 8);
    job->error = (uint32_t) ngx_http_cache_purge_job_decode(p + 48, 4);
    offset = NGX_HTTP_CACHE_PURGE_JOB_HEADER;
    for (i = 0; i < 4; i++) {
        length = (size_t) ngx_http_cache_purge_job_decode(p + 52 + i * 4, 4);
        if (length > value.length - offset) {
            goto corrupt;
        }

        text[i].data = p + offset;
        text[i].len = length;
        offset += length;
    }

    job->select.value = text[0];
    job->select.namespace_value = text[1];
    job->select.misc = text[2];
    job->bookmark = text[3];
    if (offset == value.length && ngx_http_cache_purge_job_valid(job)) {
        return 0;
    }

corrupt:

    return ngx_http_cache_purge_job_error(error,
                         NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid purge job");
}


static int
ngx_http_cache_purge_job_equal(ngx_http_cache_rocksdb_bytes_t a,
    ngx_http_cache_rocksdb_bytes_t b)
{
    return a.len == b.len && (a.len == 0 || memcmp(a.data, b.data, a.len) == 0);
}


int
ngx_http_cache_purge_job_submit(ngx_http_cache_purge_jobs_t *jobs,
    const ngx_http_cache_purge_job_t  *input, int *duplicate,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_job_t  job, old;
    int                         rc;

    *duplicate = 0;
    job = *input;
    job.done = 0;
    job.visited = job.affected = job.skipped = job.error = 0;
    job.bookmark = ngx_http_cache_purge_job_empty;
    if (!ngx_http_cache_purge_job_valid(&job)) {
        return ngx_http_cache_purge_job_error(error,
                     NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid purge selector");
    }

    rc = ngx_http_cache_purge_job_get(jobs, job.id, &old, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        return ngx_http_cache_purge_job_save(jobs, &job, 1, error);
    }

    if (rc != 0) {
        return rc;
    }

    if (job.issued != old.issued || job.action != old.action
        || job.select.type != old.select.type
        || job.select.host_scope != old.select.host_scope
        || !ngx_http_cache_purge_job_equal(job.select.value, old.select.value)
        || !ngx_http_cache_purge_job_equal(job.select.namespace_value,
                                            old.select.namespace_value)
        || !ngx_http_cache_purge_job_equal(job.select.misc, old.select.misc))
    {
        return ngx_http_cache_purge_job_error(error,
              NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "purge operation id is reused");
    }

    *duplicate = 1;

    return 0;
}


int
ngx_http_cache_purge_job_next(ngx_http_cache_purge_jobs_t *jobs,
    const unsigned char  *after, ngx_http_cache_purge_job_t *job,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_rocksdb_cursor_t  *cursor;
    ngx_http_cache_rocksdb_bytes_t    prefix, resume;
    ngx_http_cache_rocksdb_buffer_t   key, value;
    unsigned char                     raw[17], last[17];
    int                               rc;

    prefix.data = (const unsigned char *) "Q";
    prefix.len = 1;
    resume = ngx_http_cache_purge_job_empty;
    if (after != NULL) {
        last[0] = 'Q';
        memcpy(last + 1, after, 16);
        resume.data = last;
        resume.len = sizeof(last);
    }

    cursor = NULL;
    rc = ngx_http_cache_rocksdb_cursor_open(jobs->store,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix, resume, &cursor, error);
    if (rc != 0) {
        return rc;
    }

    key.data = raw;
    key.capacity = sizeof(raw);
    memset(&value, 0, sizeof(value));
    rc = ngx_http_cache_rocksdb_cursor_next(cursor, &key, &value, error);
    ngx_http_cache_rocksdb_cursor_close(&cursor, NULL);
    if (rc != 0) {
        return rc;
    }

    if (key.length != sizeof(raw) || value.length != 0) {
        return NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
    }

    rc = ngx_http_cache_purge_job_get(jobs, raw + 1, job, error);
    if (rc == 0 && job->done) {
        return NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
    }

    return rc;
}


int
ngx_http_cache_purge_job_step(ngx_http_cache_purge_jobs_t *jobs,
    const unsigned char            id[16], uint64_t now, size_t limit,
    ngx_http_cache_purge_apply_pt  apply, void *data,
    ngx_http_cache_purge_job_t    *job, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_scan_t    *scan;
    ngx_http_cache_purge_entry_t    entry;
    ngx_http_cache_rocksdb_bytes_t  bookmark;
    ngx_http_cache_rocksdb_error_t  saved;
    size_t                          i;
    int                             rc, result, applied;

    if (limit == 0 || limit > 64 || apply == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }

    rc = ngx_http_cache_purge_job_get(jobs, id, job, error);
    if (rc != 0 || job->done) {
        return rc;
    }

    if (job->bookmark.len != 0) {
        memcpy(jobs->token, job->bookmark.data, job->bookmark.len);
    }

    job->bookmark.data = jobs->token;
    job->error = 0;
    scan = NULL;
    result = ngx_http_cache_purge_scan_open(jobs->catalog, &job->select,
                       job->bookmark,
                       job->select.type == NGX_HTTP_CACHE_PURGE_SELECT_TAG,
                       now, &scan, error);
    if (result != 0) {
        return result;
    }

    for (i = 0; i < limit; i++) {
        result = ngx_http_cache_purge_scan_next(scan, &entry, error);
        if (result == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            job->done = 1;
            job->bookmark.len = 0;
            result = 0;
            break;
        }

        if (result == NGX_HTTP_CACHE_ROCKSDB_EXPIRED) {
            result = 0;
            break;
        }

        if (result == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            bookmark = ngx_http_cache_purge_scan_bookmark(scan);
            memcpy(jobs->token, bookmark.data, bookmark.len);
            job->bookmark.len = bookmark.len;
            result = 0;
            break;
        }

        if (result != 0) {
            break;
        }

        if (entry.created >= job->issued) {
            job->skipped++;

        } else if (entry.retain_until != 0) {
            if (job->retain_until > entry.retain_until) {
                result = ngx_http_cache_purge_catalog_retain(jobs->catalog,
                    entry.id, entry.uniq, job->retain_until, error);
                if (result != 0) {
                    break;
                }
            }
            job->skipped++;

        } else {
            applied = apply(data, &entry, job->action, error);
            if (applied != 0 && applied != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
                && applied != NGX_HTTP_CACHE_ROCKSDB_CONFLICT)
            {
                result = applied;
                break;
            }

            if (job->action == NGX_HTTP_CACHE_PURGE_DELETE || applied != 0) {
                if (applied != NGX_HTTP_CACHE_ROCKSDB_CONFLICT
                    && job->retain_until > now)
                {
                    result = ngx_http_cache_purge_catalog_retain(
                        jobs->catalog, entry.id, entry.uniq,
                        job->retain_until, error);
                } else {
                    result = ngx_http_cache_purge_catalog_remove(
                                  jobs->catalog, entry.id, entry.uniq, error);
                }

                if (result != 0) {
                    break;
                }
            }

            if (applied == 0) {
                job->affected++;
            } else {
                job->skipped++;
            }
        }

        job->visited++;
        bookmark = ngx_http_cache_purge_scan_bookmark(scan);
        memcpy(jobs->token, bookmark.data, bookmark.len);
        job->bookmark.len = bookmark.len;
    }

    ngx_http_cache_purge_scan_close(scan);
    memset(&saved, 0, sizeof(saved));
    if (result != 0) {
        job->error = (uint32_t) result;
        if (error != NULL) {
            saved = *error;
        }
    }

    rc = ngx_http_cache_purge_job_save(jobs, job, job->done, error);
    if (rc != 0) {
        return rc;
    }

    if (result != 0 && error != NULL) {
        *error = saved;
        error->code = result;
    }

    return result;
}
