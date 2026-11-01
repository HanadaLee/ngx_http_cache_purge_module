
/*
 * Copyright (C) Hanada
 */

#if (NGX_HTTP_CACHE_PURGE_NATIVE_TEST)
#include "ngx_http_cache_purge.h"
#else
#include "ngx_http_cache_purge_internal.h"
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>


static ngx_http_cache_rocksdb_bytes_t ngx_http_cache_purge_text(
    const char *text);
static int ngx_http_cache_purge_apply(void *data,
    const ngx_http_cache_purge_entry_t *entry, unsigned action,
    ngx_http_cache_rocksdb_error_t *error);
static void ngx_http_cache_purge_entry(ngx_http_cache_purge_entry_t *entry,
    unsigned number, const char *root);
static ngx_http_cache_purge_job_t ngx_http_cache_purge_job(unsigned id,
    unsigned type, const char *value, unsigned action);
static void ngx_http_cache_purge_open(const char *path,
    ngx_http_cache_rocksdb_t **store, ngx_http_cache_purge_catalog_t **catalog,
    ngx_http_cache_purge_jobs_t **jobs);
static void ngx_http_cache_purge_close(ngx_http_cache_rocksdb_t **store,
    ngx_http_cache_purge_catalog_t *catalog, ngx_http_cache_purge_jobs_t *jobs);


static unsigned  ngx_http_cache_purge_checks;

#define ngx_expect(test)                                                      \
    do {                                                                      \
        if (!(test)) {                                                        \
            fprintf(stderr, "line %d: %s\n", __LINE__, #test);                \
            exit(1);                                                          \
        }                                                                     \
        ngx_http_cache_purge_checks++;                                        \
    } while (0)


typedef struct {
    unsigned  calls;
    unsigned  first;
    unsigned  failure;
    unsigned  missing;
    unsigned  replaced;
} ngx_http_cache_purge_test_t;


static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_text(const char *text)
{
    ngx_http_cache_rocksdb_bytes_t  value;

    value.data = (const unsigned char *) text;
    value.len = strlen(text);

    return value;
}


static int
ngx_http_cache_purge_apply(void *data,
    const ngx_http_cache_purge_entry_t *entry, unsigned action,
    ngx_http_cache_rocksdb_error_t *error)
{
    unsigned                      number;
    ngx_http_cache_purge_test_t  *test;
#if (NGX_HTTP_CACHE_PURGE_NATIVE_TEST)
    ngx_int_t                           rc;
    ngx_http_cache_purge_native_file_t  result;
    char                                name[512];

#endif

    test = data;
    number = entry->hash[15];
    test->calls++;

    if (test->first == 0) {
        test->first = number;
    }

    ngx_expect(action <= NGX_HTTP_CACHE_PURGE_INVALIDATE);

    if (number == test->failure) {
        error->code = NGX_HTTP_CACHE_ROCKSDB_IO;
        strcpy(error->message, "injected file error");

        return error->code;
    }

    if (number == test->missing) {
        return NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
    }

    if (number == test->replaced) {
        return NGX_HTTP_CACHE_ROCKSDB_CONFLICT;
    }

#if (NGX_HTTP_CACHE_PURGE_NATIVE_TEST)
    snprintf(name, sizeof(name), "%.*s/%.*s", (int) entry->root.len,
             entry->root.data, (int) entry->path.len, entry->path.data);
    rc = ngx_http_cache_purge_native_file(
        name, action == NGX_HTTP_CACHE_PURGE_INVALIDATE, entry->uniq, &result);

    if (rc == NGX_DECLINED) {
        return result.error == NGX_ENOENT ? NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND :
                                            NGX_HTTP_CACHE_ROCKSDB_CONFLICT;
    }

    if (rc != NGX_OK) {
        return NGX_HTTP_CACHE_ROCKSDB_IO;
    }
#endif

    return 0;
}


static void
ngx_http_cache_purge_entry(ngx_http_cache_purge_entry_t *entry,
    unsigned number, const char *root)
{
    static char  path[64];
#if (NGX_HTTP_CACHE_PURGE_NATIVE_TEST)
    int                           fd;
    struct stat                   info;
    ngx_http_file_cache_header_t  header;
    char                          name[512];

#endif

    snprintf(path, sizeof(path), "file-%03u", number);
    memset(entry, 0, sizeof(*entry));
    entry->hash[15] = (unsigned char) number;
    entry->uniq = number + 100;
    entry->created = 100;
    entry->url = ngx_http_cache_purge_text("http://a.test/docs/a");
    entry->host = ngx_http_cache_purge_text("a.test");
    entry->root = ngx_http_cache_purge_text(root);
    entry->zone = ngx_http_cache_purge_text(number % 2 ? "zone-a" : "zone-b");
    entry->misc = ngx_http_cache_purge_text(number == 10 ? "b" : "a");
    entry->path = ngx_http_cache_purge_text(path);
    entry->tags[0] = ngx_http_cache_purge_text("news");
    entry->tag_count = 1;
#if (NGX_HTTP_CACHE_PURGE_NATIVE_TEST)
    snprintf(name, sizeof(name), "%s/%s", root, path);
    memset(&header, 0, sizeof(header));
    header.version = NGX_HTTP_CACHE_VERSION;
    header.valid_sec = 3600;
    header.date = 100;
    header.header_start = sizeof(header);
    header.body_start = sizeof(header);
    fd = open(name, O_CREAT | O_WRONLY | O_TRUNC, 0600);
    ngx_expect(fd != -1);
    ngx_expect(write(fd, &header, sizeof(header)) == sizeof(header));
    ngx_expect(write(fd, "body", 4) == 4);
    ngx_expect(fstat(fd, &info) == 0);
    ngx_expect(close(fd) == 0);
    entry->uniq = info.st_ino;
#endif
}


static ngx_http_cache_purge_job_t
ngx_http_cache_purge_job(unsigned id, unsigned type, const char *value,
    unsigned action)
{
    ngx_http_cache_purge_job_t  job;

    memset(&job, 0, sizeof(job));
    job.id[15] = (unsigned char) id;
    job.issued = 200;
    job.retain_until = 320;
    job.action = action;
    job.select.type = type;
    job.select.value = ngx_http_cache_purge_text(value);

    return job;
}


static void
ngx_http_cache_purge_open(const char *path, ngx_http_cache_rocksdb_t **store,
    ngx_http_cache_purge_catalog_t **catalog,
    ngx_http_cache_purge_jobs_t **jobs)
{
    ngx_http_cache_rocksdb_error_t    error;
    ngx_http_cache_rocksdb_options_t  options;

    ngx_http_cache_rocksdb_init_options(&options);
    ngx_expect(ngx_http_cache_rocksdb_open(path, &options, store, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_catalog_open(*store, catalog, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_jobs_open(*store, *catalog, jobs, &error)
               == 0);
}


static void
ngx_http_cache_purge_close(ngx_http_cache_rocksdb_t **store,
    ngx_http_cache_purge_catalog_t *catalog, ngx_http_cache_purge_jobs_t *jobs)
{
    ngx_http_cache_rocksdb_error_t  error;

    ngx_http_cache_purge_jobs_close(jobs);
    ngx_http_cache_purge_catalog_close(catalog);
    ngx_expect(ngx_http_cache_rocksdb_close(store, &error) == 0);
}


int
ngx_http_cache_purge_test_main(void)
{
    int                              duplicate, rc, status;
    pid_t                            child;
    size_t                           removed;
    uint64_t                         written, synced;
    unsigned                         i;
    ngx_http_cache_rocksdb_t        *store;
    ngx_http_cache_purge_job_t       input, output, saved;
    ngx_http_cache_purge_jobs_t     *jobs;
    ngx_http_cache_purge_scan_t     *scan;
    ngx_http_cache_purge_test_t      test;
    ngx_http_cache_purge_entry_t     entry;
    ngx_http_cache_purge_catalog_t  *catalog;
    ngx_http_cache_rocksdb_bytes_t   empty;
    ngx_http_cache_rocksdb_error_t   error;
    ngx_http_cache_rocksdb_stats_t   stats;
    char                             root[] = "/tmp/cache-jobs-XXXXXX";
    char                             crash[] = "/tmp/cache-jobs-crash-XXXXXX";
    char                             text[32769];
    char                             binary[] = { 'a', 0, 'b' };
    unsigned char                    id[24];
#if (NGX_HTTP_CACHE_PURGE_NATIVE_TEST)
    int                           fd;
    ngx_http_file_cache_header_t  header;
    char                          name[512];

#endif

    ngx_expect(mkdtemp(root) != NULL);
    ngx_http_cache_purge_open(root, &store, &catalog, &jobs);
    memset(&test, 0, sizeof(test));
    empty = ngx_http_cache_purge_text("");

    for (i = 1; i <= 10; i++) {
        ngx_http_cache_purge_entry(&entry, i, root);

        if (i == 9) {
            entry.created = 200;
        } else if (i == 10) {
            entry.created = 201;
        }

        ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error)
                   == 0);
    }

    input = ngx_http_cache_purge_job(1, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX,
                                     "http://a.test/docs/",
                                     NGX_HTTP_CACHE_PURGE_DELETE);
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error) == 0
        && !duplicate);
    ngx_expect(
        ngx_http_cache_rocksdb_durability(store, &written, &synced, &error)
        == 0
        && written == synced && written != 0);
    input.retain_until = 999;
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error) == 0
        && duplicate);
    ngx_expect(ngx_http_cache_purge_job_get(jobs, input.id, &output, &error)
               == 0
               && output.retain_until == 320);
    input.action = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error)
        == NGX_HTTP_CACHE_ROCKSDB_CONFLICT);
    input.action = NGX_HTTP_CACHE_PURGE_DELETE;
    ngx_expect(ngx_http_cache_purge_job_next(jobs, NULL, &output, &error) == 0
               && output.id[15] == 1 && !output.done);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 201, 3,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0);
    ngx_expect(test.calls == 3 && output.visited == 3 && output.affected == 3
               && !output.done && output.bookmark.len != 0);
    ngx_expect(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0
               && stats.active_cursors == 0 && stats.cursor_handles == 0);
    ngx_http_cache_purge_close(&store, catalog, jobs);

    /* A reopened owner resumes at its bounded persisted bookmark. */
    ngx_http_cache_purge_open(root, &store, &catalog, &jobs);
    test.failure = 5;
    test.first = 0;
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 202, 3,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == NGX_HTTP_CACHE_ROCKSDB_IO);
    ngx_expect(output.visited == 4 && output.affected == 4 && !output.done
               && output.error == NGX_HTTP_CACHE_ROCKSDB_IO && test.first == 4
               && strcmp(error.message, "injected file error") == 0);
    test.failure = 0;
    test.first = 0;
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 203, 3,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0);
    ngx_expect(output.visited == 7 && test.first == 5 && output.error == 0);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 204, 64,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0);
    ngx_expect(output.done && output.visited == 10 && output.affected == 8
               && output.skipped == 2);
    ngx_expect(ngx_http_cache_purge_job_next(jobs, NULL, &saved, &error)
               == NGX_HTTP_CACHE_ROCKSDB_DONE);
    ngx_expect(
        ngx_http_cache_rocksdb_durability(store, &written, &synced, &error)
        == 0
        && written == synced);
    i = test.calls;
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 205, 64,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0
               && test.calls == i && output.done);
    ngx_expect(ngx_http_cache_purge_catalog_collect(catalog, 319, 64, &removed,
                                                    &error)
               == 0
               && removed == 0);
    ngx_expect(ngx_http_cache_purge_catalog_collect(catalog, 320, 64, &removed,
                                                    &error)
               == 0
               && removed == 8);
#if (NGX_HTTP_CACHE_PURGE_NATIVE_TEST)
    for (i = 1; i <= 10; i++) {
        snprintf(name, sizeof(name), "%s/file-%03u", root, i);
        ngx_expect((access(name, F_OK) == 0) == (i >= 9));
    }
#endif

    /* Only the selected misc is invalidated; same-second objects survive. */
    input = ngx_http_cache_purge_job(2, NGX_HTTP_CACHE_PURGE_SELECT_KEY,
                                     "http://a.test/docs/a",
                                     NGX_HTTP_CACHE_PURGE_INVALIDATE);
    input.issued = 300;
    input.select.misc = ngx_http_cache_purge_text("a");
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error) == 0
        && !duplicate);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 301, 64,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0);
    ngx_expect(output.done && output.affected == 1 && output.visited == 1);
    scan = NULL;
    ngx_expect(ngx_http_cache_purge_scan_open(catalog, &input.select, empty, 0,
                                              301, &scan, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_scan_next(scan, &entry, &error) == 0
               && entry.hash[15] == 9 && entry.retain_until == 0);
    memcpy(id, entry.id, 24);
    ngx_http_cache_purge_scan_close(scan);
#if (NGX_HTTP_CACHE_PURGE_NATIVE_TEST)
    snprintf(name, sizeof(name), "%s/file-009", root);
    fd = open(name, O_RDONLY);
    ngx_expect(fd != -1
               && read(fd, &header, sizeof(header)) == sizeof(header));
    ngx_expect(close(fd) == 0 && header.valid_sec == 0 && header.date == 100);
#endif

    /* Absent and replaced files cannot wedge the pending queue. */
    input = ngx_http_cache_purge_job(3, NGX_HTTP_CACHE_PURGE_SELECT_TAG,
                                     "news", NGX_HTTP_CACHE_PURGE_DELETE);
    input.issued = 300;
    input.retain_until = 400;
    test.missing = 9;
    test.replaced = 10;
    ngx_expect(ngx_http_cache_purge_catalog_retain(catalog, id, entry.uniq,
                                                   310, &error)
               == 0);
    i = test.calls;
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error)
        == 0);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 301, 64,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0);
    ngx_expect(output.done && output.affected == 0 && output.skipped == 2);
    ngx_expect(test.calls == i + 1);
    ngx_expect(ngx_http_cache_purge_catalog_get(catalog, id, &entry, &error)
               == 0
               && entry.retain_until == 400);
    ngx_expect(ngx_http_cache_purge_catalog_collect(catalog, 310, 64, &removed,
                                                    &error)
               == 0
               && removed == 0);
    ngx_expect(ngx_http_cache_purge_catalog_collect(catalog, 400, 64, &removed,
                                                    &error)
               == 0
               && removed == 1);

    /* Large expressions and binary misc are stored without a native key. */
    memset(text, 'x', sizeof(text) - 1);
    text[sizeof(text) - 1] = 0;
    input = ngx_http_cache_purge_job(4, NGX_HTTP_CACHE_PURGE_SELECT_KEY, text,
                                     NGX_HTTP_CACHE_PURGE_DELETE);
    input.select.namespace_value = ngx_http_cache_purge_text(text);
    input.select.misc.data = (const unsigned char *) binary;
    input.select.misc.len = sizeof(binary);
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error)
        == 0);
    ngx_expect(ngx_http_cache_purge_job_get(jobs, input.id, &output, &error)
               == 0
               && output.select.value.len == 32768
               && output.select.namespace_value.len == 32768
               && output.select.misc.len == 3
               && memcmp(output.select.misc.data, binary, 3) == 0);
    input.select.value.len++;
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error)
        == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    input.select.value.len--;
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 300, 0,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 300, 64,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0
               && output.done && output.visited == 0);

    /* Queue continuation gives each pending operation a bounded turn. */

    for (i = 5; i < 8; i++) {
        input = ngx_http_cache_purge_job(i, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                                         "http://empty.test/",
                                         NGX_HTTP_CACHE_PURGE_DELETE);
        ngx_expect(
            ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error)
            == 0);
    }

    ngx_expect(ngx_http_cache_purge_job_next(jobs, NULL, &output, &error) == 0
               && output.id[15] == 5);
    ngx_expect(ngx_http_cache_purge_job_next(jobs, output.id, &output, &error)
               == 0
               && output.id[15] == 6);
    ngx_expect(ngx_http_cache_purge_job_next(jobs, output.id, &output, &error)
               == 0
               && output.id[15] == 7);
    ngx_expect(ngx_http_cache_purge_job_next(jobs, output.id, &output, &error)
               == NGX_HTTP_CACHE_ROCKSDB_DONE);

    /* Retained rows yield after 64 candidates without a file callback. */

    for (i = 11; i <= 80; i++) {
        ngx_http_cache_purge_entry(&entry, i, root);
        entry.url = ngx_http_cache_purge_text("http://a.test/retained/a");
        ngx_expect(ngx_http_cache_purge_apply(
                       &test, &entry, NGX_HTTP_CACHE_PURGE_DELETE, &error)
                   == 0);
        ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error)
                   == 0);
    }

    input = ngx_http_cache_purge_job(9, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX,
                                     "http://a.test/retained/",
                                     NGX_HTTP_CACHE_PURGE_DELETE);
    ngx_expect(ngx_http_cache_purge_scan_open(catalog, &input.select, empty, 0,
                                              301, &scan, &error)
               == 0);

    while ((rc = ngx_http_cache_purge_scan_next(scan, &entry, &error)) == 0) {
        ngx_expect(ngx_http_cache_purge_catalog_retain(catalog, entry.id,
                                                       entry.uniq, 350, &error)
                   == 0);
    }

    ngx_expect(rc == NGX_HTTP_CACHE_ROCKSDB_DONE);
    ngx_http_cache_purge_scan_close(scan);
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error)
        == 0);
    i = test.calls;
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 301, 3,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0
               && !output.done && output.visited == 0
               && output.bookmark.len != 0 && test.calls == i);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 301, 3,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0
               && output.done && output.visited == 0 && test.calls == i);

    /* The longest URL bookmark survives dictionary deletion and reopening. */

    for (i = 81; i <= 82; i++) {
        ngx_http_cache_purge_entry(&entry, i, root);
        entry.url = ngx_http_cache_purge_text(text);
        ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error)
                   == 0);
    }

    input = ngx_http_cache_purge_job(10, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX,
                                     text, NGX_HTTP_CACHE_PURGE_DELETE);
    ngx_expect(
        ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error)
        == 0);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 330, 1,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0
               && output.visited == 1 && output.bookmark.len == 32822);
    ngx_http_cache_purge_close(&store, catalog, jobs);
    ngx_http_cache_purge_open(root, &store, &catalog, &jobs);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 330, 1,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0
               && output.visited == 2 && !output.done);
    ngx_expect(ngx_http_cache_purge_job_step(jobs, input.id, 330, 1,
                                             ngx_http_cache_purge_apply, &test,
                                             &output, &error)
               == 0
               && output.done && output.affected == 2);
    ngx_expect(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0
               && stats.active_cursors == 0 && stats.cursor_handles == 0);
    ngx_http_cache_purge_close(&store, catalog, jobs);

    /* Accepted tasks survive abrupt owner exit, without a file-proof log. */
    ngx_expect(mkdtemp(crash) != NULL);
    child = fork();
    ngx_expect(child >= 0);

    if (child == 0) {
        ngx_http_cache_purge_open(crash, &store, &catalog, &jobs);
        input =
            ngx_http_cache_purge_job(8, NGX_HTTP_CACHE_PURGE_SELECT_HOST,
                                     "a.test", NGX_HTTP_CACHE_PURGE_DELETE);
        rc = ngx_http_cache_purge_job_submit(jobs, &input, &duplicate, &error);

        if (rc != 0) {
            _exit(2);
        }

        kill(getpid(), SIGKILL);
        _exit(3);
    }

    ngx_expect(waitpid(child, &status, 0) == child && WIFSIGNALED(status)
               && WTERMSIG(status) == SIGKILL);
    ngx_http_cache_purge_open(crash, &store, &catalog, &jobs);
    ngx_expect(ngx_http_cache_purge_job_next(jobs, NULL, &output, &error) == 0
               && output.id[15] == 8 && !output.done);
    ngx_http_cache_purge_close(&store, catalog, jobs);
    printf("%u background job checks passed; files: %s\n",
           ngx_http_cache_purge_checks, root);

    return 0;
}
