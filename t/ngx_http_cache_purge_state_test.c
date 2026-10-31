/* Copyright (C) Hanada */

#include "ngx_http_cache_purge.h"
#include <sys/wait.h>


ngx_pid_t             ngx_pid;
ngx_int_t             ngx_ncpu = 2;
volatile ngx_cycle_t *ngx_cycle;

static unsigned       ngx_http_cache_purge_checks;


#define ngx_expect(test)                                                     \
    do {                                                                     \
        if (!(test)) {                                                       \
            fprintf(stderr, "line %d: %s\n", __LINE__, #test);                \
            exit(1);                                                         \
        }                                                                    \
        ngx_http_cache_purge_checks++;                                       \
    } while (0)


/* Use the actual nginx slab and mutex; replace logging and debug traps. */
void
ngx_debug_point(void)
{
    abort();
}


void
ngx_log_error_core(ngx_uint_t level, ngx_log_t *log, ngx_err_t err,
    const char  *fmt, ...)
{
    if (level <= NGX_LOG_ALERT) {
        fprintf(stderr, "unexpected nginx allocator/mutex error: %s\n", fmt);
        abort();
    }
}


static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_text(const char *text)
{
    ngx_http_cache_rocksdb_bytes_t  value;

    value.data = (const u_char *) text;
    value.len = strlen(text);
    return value;
}


static ngx_http_cache_purge_job_t
ngx_http_cache_purge_job(unsigned number, unsigned type, const char *value)
{
    ngx_http_cache_purge_job_t  job;

    memset(&job, 0, sizeof(job));
    job.id[14] = (u_char) (number >> 8);
    job.id[15] = (u_char) number;
    job.issued = 200;
    job.retain_until = 320;
    job.select.type = type;
    job.select.value = ngx_http_cache_purge_text(value);
    return job;
}


static void
ngx_http_cache_purge_test_urls(ngx_http_cache_purge_state_t *state)
{
    ngx_http_cache_purge_job_t    jobs[128];
    ngx_http_cache_purge_entry_t  entry;
    ngx_http_cache_purge_match_t  fast, full;
    ngx_int_t                     a, b;
    ngx_uint_t                    i, j, k, pass;
    u_char                        values[128][8], query[8];

    memset(&entry, 0, sizeof(entry));
    for (i = 0; i < 128; i++) {
        jobs[i] = ngx_http_cache_purge_job(i,
                     i % 3 == 0 ? NGX_HTTP_CACHE_PURGE_SELECT_URL
                                : NGX_HTTP_CACHE_PURGE_SELECT_PREFIX, "x");
        jobs[i].issued = 600 + i;
        jobs[i].retain_until = 1200;
        jobs[i].action = i % 2;
        jobs[i].select.value.data = values[i];
        jobs[i].select.value.len = i % 7 + 1;
        for (j = 0; j < jobs[i].select.value.len; j++) {
            values[i][j] = ((i / 7) >> j) & 1 ? 255 : 0;
        }
    }

    /* Permuted installation includes duplicate, nested and binary selectors. */
    for (i = 0; i < 128; i++) {
        ngx_expect(ngx_http_cache_purge_state_install(state,
                   &jobs[(i * 37) % 128]) == NGX_OK);
    }

    for (pass = 0; pass < 2; pass++) {
        if (pass) {
            for (i = 0; i < 128; i += 3) {
                ngx_http_cache_purge_state_finish(state, jobs[i].id);
            }
        }
        for (i = 1; i < 512; i++) {
            for (j = 0, k = i; k > 1; k >>= 1) {
                query[j++] = k & 1 ? 255 : 0;
            }
            entry.url.data = query;
            entry.url.len = j;
            a = ngx_http_cache_purge_state_match_url(state, &entry, 1000,
                                                      &fast);
            b = ngx_http_cache_purge_state_match_local(state, &entry, 1000,
                                                        &full);
            ngx_expect(a == b && memcmp(&fast, &full, sizeof(fast)) == 0);
        }
    }

    /* Collection must not leave references to removed ancestor rules. */
    ngx_http_cache_purge_state_collect(state, 1201);
    for (i = 0; i < 128; i++) {
        entry.url = jobs[i].select.value;
        a = ngx_http_cache_purge_state_match_url(state, &entry, 1201, &fast);
        b = ngx_http_cache_purge_state_match_local(state, &entry, 1201, &full);
        ngx_expect(a == b && memcmp(&fast, &full, sizeof(fast)) == 0);
        ngx_http_cache_purge_state_finish(state, jobs[i].id);
    }
    ngx_http_cache_purge_state_collect(state, 1202);
}


static void
ngx_http_cache_purge_test_tags(ngx_http_cache_purge_state_t *state)
{
    ngx_http_cache_purge_job_t    job, other;
    ngx_http_cache_purge_entry_t  entry;
    ngx_http_cache_purge_match_t  match;
    ngx_uint_t                    i;

    memset(&entry, 0, sizeof(entry));
    other = ngx_http_cache_purge_job(1, NGX_HTTP_CACHE_PURGE_SELECT_TAG,
                                   "unmatched");
    ngx_expect(ngx_http_cache_purge_state_install(state, &other) == NGX_OK);
    job = ngx_http_cache_purge_job(0, NGX_HTTP_CACHE_PURGE_SELECT_TAG, "Aa");
    ngx_expect(ngx_hash_key((u_char *) "Aa", 2)
               == ngx_hash_key((u_char *) "BB", 2));
    ngx_expect(ngx_http_cache_purge_state_install(state, &job) == NGX_OK);
    for (i = 0; i < 64; i++) {
        entry.tags[i] = ngx_http_cache_purge_text("BB");
    }
    for (i = 0; i <= 64; i++) {
        entry.tag_count = i;
        ngx_expect(ngx_http_cache_purge_state_match(state, &entry, 201, &match)
                   == NGX_DECLINED);
        if (i != 0) {
            entry.tags[i - 1] = ngx_http_cache_purge_text("Aa");
            ngx_expect(ngx_http_cache_purge_state_match(state, &entry, 201,
                                                          &match)
                       == NGX_OK && match.delete_before == 200);
            entry.tags[i - 1] = ngx_http_cache_purge_text("BB");
        }
    }
    entry.tags[63] = ngx_http_cache_purge_text("Aa");
    entry.namespace_value = ngx_http_cache_purge_text("other");
    ngx_expect(ngx_http_cache_purge_state_match(state, &entry, 201, &match)
               == NGX_DECLINED);
    entry.namespace_value = ngx_http_cache_purge_text("");
    ngx_http_cache_purge_state_finish(state, job.id);
    ngx_expect(ngx_http_cache_purge_state_match_local(state, &entry, 201,
                                                        &match)
               == NGX_DECLINED);
    ngx_expect(ngx_http_cache_purge_state_match(state, &entry, 201, &match)
               == NGX_OK && !match.delete_before
               && match.propagate_delete == 200);
    ngx_http_cache_purge_state_finish(state, other.id);
    ngx_http_cache_purge_state_collect(state, 321);
}


static void
ngx_http_cache_purge_empty(ngx_http_cache_purge_state_t *state)
{
    ngx_uint_t  rules, messages;
    size_t      bytes;

    ngx_http_cache_purge_state_stats(state, &rules, &messages, &bytes);
    ngx_expect(rules == 0 && messages == 0 && bytes == 0);
}


int
ngx_http_cache_purge_test_main(void)
{
    ngx_shm_zone_t                   zone;
    ngx_http_cache_purge_state_t     state, reloaded;
    ngx_http_cache_purge_job_t       job, result;
    ngx_http_cache_purge_entry_t     entry;
    ngx_http_cache_purge_match_t     match;
    ngx_http_cache_purge_message_t  *message, *taken, *pending[256];
    ngx_http_cache_purge_work_t     *work;
    ngx_slab_pool_t                 *pool;
    ngx_cycle_t                      cycle;
    ngx_log_t                        log;
    ngx_uint_t                       i, baseline, rules, messages;
    size_t                           bytes;
    int                              code, status, ready[2], done[2];
    char                             byte, url[] = "http://a.test/one";
    char                             misc[] = { 'a', 0, 'b' };
    char                            *large;
    pid_t                            child;
#if (NGX_HTTP_CACHE_PURGE_BACKEND_TEST)
    ngx_http_cache_rocksdb_t         *store;
    ngx_http_cache_purge_catalog_t   *catalog;
    ngx_http_cache_purge_jobs_t      *jobs;
    ngx_http_cache_rocksdb_options_t  options;
    ngx_http_cache_rocksdb_error_t    error;
    char                              database[] = "/tmp/cache-state-XXXXXX";
    int                               duplicate;
#endif

    memset(&cycle, 0, sizeof(cycle));
    memset(&log, 0, sizeof(log));
    log.log_level = NGX_LOG_WARN;
    cycle.log = &log;
    ngx_cycle = &cycle;
    ngx_pid = getpid();
    ngx_pagesize = (ngx_uint_t) getpagesize();
    for (i = ngx_pagesize; i >>= 1; ngx_pagesize_shift++) { /* void */ }
    ngx_slab_sizes_init();
    memset(&zone, 0, sizeof(zone));
    zone.shm.size = 1024 * 1024;
    zone.shm.addr = mmap(NULL, zone.shm.size, PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    ngx_expect(zone.shm.addr != MAP_FAILED);
    pool = (ngx_slab_pool_t *) zone.shm.addr;
    pool->end = zone.shm.addr + zone.shm.size;
    pool->min_shift = 3;
    pool->addr = zone.shm.addr;
    ngx_slab_init(pool);
    ngx_expect(ngx_shmtx_create(&pool->mutex, &pool->lock, NULL) == NGX_OK);
    memset(&state, 0, sizeof(state));
    zone.data = &state;
    ngx_expect(ngx_http_cache_purge_state_init(&zone, NULL) == NGX_OK);
    baseline = pool->pfree;
    work = malloc(NGX_HTTP_CACHE_PURGE_MESSAGE_MAX);
    ngx_expect(work != NULL);
    memset(&entry, 0, sizeof(entry));
    entry.url = ngx_http_cache_purge_text("http://a.test/one");
    entry.host = ngx_http_cache_purge_text("a.test");

    /* Empty-table lookup must not acquire even the native shared mutex. */
    ngx_shmtx_lock(&pool->mutex);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_DECLINED);
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 201, &match)
               == NGX_DECLINED);
    ngx_shmtx_unlock(&pool->mutex);

    job = ngx_http_cache_purge_job(1, NGX_HTTP_CACHE_PURGE_SELECT_URL, url);
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_SUBMIT, &job, &message) == NGX_OK);
    /* Mutate the caller's buffer to verify that the queue owns its copy. */
    /* cppcheck-suppress unreadVariable */
    url[7] = 'b';
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_DECLINED);
    ngx_expect(ngx_http_cache_purge_state_poll(&state, &message, &code, &result)
               == NGX_AGAIN);
    ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken) == NGX_OK);
    ngx_expect(taken == message && work->type == NGX_HTTP_CACHE_PURGE_SUBMIT
               && work->data.job.select.value.data[7] == 'a'
               && work->data.job.select.value.data >= (u_char *) (work + 1)
               && work->data.job.select.value.data
                  < (u_char *) work + work->size);
#if (NGX_HTTP_CACHE_PURGE_BACKEND_TEST)
    ngx_expect(mkdtemp(database) != NULL);
    ngx_http_cache_rocksdb_options(&options);
    ngx_expect(ngx_http_cache_rocksdb_open(database, &options, &store, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_catalog_open(store, &catalog, &error) == 0);
    ngx_expect(ngx_http_cache_purge_jobs_open(store, catalog, &jobs, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_job_submit(jobs, &work->data.job,
                                              &duplicate, &error) == 0);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_DECLINED);
    ngx_expect(ngx_http_cache_purge_state_poll(&state, &message, &code, &result)
               == NGX_AGAIN);
    ngx_expect(ngx_http_cache_purge_job_get(jobs, work->data.job.id,
                                           &result, &error) == 0);
    ngx_http_cache_purge_state_complete(&state, taken, 0, &result);
    ngx_http_cache_purge_jobs_close(jobs);
    ngx_http_cache_purge_catalog_close(catalog);
    ngx_expect(ngx_http_cache_rocksdb_close(&store, &error) == 0);
#else
    ngx_http_cache_purge_state_complete(&state, taken, 0, &work->data.job);
#endif
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_OK && match.delete_before == 200
               && match.propagate_delete == 200 && !match.invalidate_before);
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 201, &match)
               == NGX_OK && match.delete_before == 200
               && match.propagate_delete == 200 && !match.invalidate_before);
    ngx_expect(ngx_http_cache_purge_state_poll(&state, &message, &code, &result)
               == NGX_OK && code == 0 && message == NULL
               && result.select.value.data == NULL);
    ngx_expect(ngx_http_cache_purge_state_filtering(&state) == 1);
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 0);
    ngx_http_cache_purge_state_finish(&state, job.id);
    ngx_expect(ngx_http_cache_purge_state_filtering(&state) == 0);
    ngx_expect(ngx_http_cache_purge_state_active(&state) == 1);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 319, &match)
               == NGX_OK && !match.delete_before && match.propagate_delete);
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 319, &match)
               == NGX_DECLINED);
    ngx_expect(ngx_http_cache_purge_state_match_local(&state, &entry, 319,
                                                     &match) == NGX_DECLINED);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 320, &match)
               == NGX_DECLINED);
    ngx_http_cache_purge_state_collect(&state, 320);
    ngx_http_cache_purge_empty(&state);
    ngx_expect(pool->pfree == baseline);

    /* Failed durable acceptance must not publish a rule. */
    job = ngx_http_cache_purge_job(2, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                                                       "http://a.test/one");
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_SUBMIT, &job, &message) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken) == NGX_OK);
    ngx_http_cache_purge_state_complete(&state, taken,
                                       NGX_HTTP_CACHE_ROCKSDB_IO, NULL);
    ngx_expect(ngx_http_cache_purge_state_poll(&state, &message, &code, &result)
               == NGX_OK && code == NGX_HTTP_CACHE_ROCKSDB_IO);
    ngx_http_cache_purge_empty(&state);

    /* Disconnecting the requester does not cancel the accepted operation. */
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_SUBMIT, &job, &message) == NGX_OK);
    ngx_http_cache_purge_state_detach(&state, &message);
    ngx_expect(message == NULL);
    ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken) == NGX_OK);
    ngx_http_cache_purge_state_complete(&state, taken, 0, &work->data.job);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_OK);
    ngx_http_cache_purge_state_finish(&state, job.id);
    ngx_http_cache_purge_state_collect(&state, 321);
    ngx_http_cache_purge_empty(&state);

    /* Key scope is the explicit URL/namespace/misc tuple, including NUL. */
    job = ngx_http_cache_purge_job(3, NGX_HTTP_CACHE_PURGE_SELECT_KEY,
                                                       "http://a.test/one");
    job.select.misc.data = (u_char *) misc;
    job.select.misc.len = sizeof(misc);
    job.action = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 1);
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 1);
    job.done = 1;
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 0);
    job.done = 0;
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 1);
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 201, &match)
               == NGX_AGAIN && !match.delete_before
               && !match.propagate_invalidate);
    entry.url = ngx_http_cache_purge_text("http://a.test/other");
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 201, &match)
               == NGX_AGAIN);
    entry.url = ngx_http_cache_purge_text("http://a.test/one");
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_DECLINED);
    entry.misc = job.select.misc;
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_OK && match.invalidate_before == 200
               && match.propagate_invalidate == 200);
    entry.namespace_value = ngx_http_cache_purge_text("a.test");
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_DECLINED);
    entry.namespace_value = ngx_http_cache_purge_text("");
    ngx_http_cache_purge_state_finish(&state, job.id);
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 0);
    ngx_http_cache_purge_state_collect(&state, 322);

    /* Namespace TAGs and host TAGs have distinct scopes. */
    job = ngx_http_cache_purge_job(4, NGX_HTTP_CACHE_PURGE_SELECT_TAG, "news");
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 201, &match)
               == NGX_AGAIN);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_DECLINED);
    entry.tags[0] = ngx_http_cache_purge_text("news");
    entry.tag_count = 1;
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_OK);
    job.select.host_scope = 1;
    job.select.namespace_value = ngx_http_cache_purge_text("a.test");
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job)
               == NGX_DECLINED);
    ngx_http_cache_purge_state_finish(&state, job.id);
    ngx_http_cache_purge_state_collect(&state, 323);
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    entry.namespace_value = ngx_http_cache_purge_text("other");
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_OK);
    entry.host = ngx_http_cache_purge_text("b.test");
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
               == NGX_DECLINED);
    ngx_http_cache_purge_state_finish(&state, job.id);
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 0);
    /* Retained TAG propagation must not force full HIT identity evaluation. */
    job = ngx_http_cache_purge_job(7, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX,
                                                          "http://a.test/");
    job.action = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    entry.host = ngx_http_cache_purge_text("a.test");
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 0);
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 211, &match)
               == NGX_OK && match.invalidate_before == 200
               && !match.propagate_delete);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 211, &match)
               == NGX_OK && match.propagate_delete == 200);
    ngx_http_cache_purge_state_finish(&state, job.id);
    entry.host = ngx_http_cache_purge_text("b.test");
    ngx_http_cache_purge_state_collect(&state, 324);
    ngx_http_cache_purge_empty(&state);

    job = ngx_http_cache_purge_job(5, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX,
                                                          "http://a.test/");
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 500, &match)
               == NGX_OK && match.delete_before == 200
               && match.propagate_delete == 0);
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 500, &match)
               == NGX_OK && match.delete_before == 200
               && match.propagate_delete == 0);
    job = ngx_http_cache_purge_job(6, NGX_HTTP_CACHE_PURGE_SELECT_HOST,
                                   "b.test");
    job.action = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    job.issued = 210;
    ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    /* A later host rule requires a complete retry, not partial timestamps. */
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 1);
    ngx_expect(ngx_http_cache_purge_state_match_url(&state, &entry, 211, &match)
               == NGX_AGAIN && !match.delete_before && !match.invalidate_before
               && !match.propagate_delete && !match.propagate_invalidate);
    ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 211, &match)
               == NGX_OK && match.delete_before == 200
               && match.invalidate_before == 210);
    memset(&reloaded, 0, sizeof(reloaded));
    zone.data = &reloaded;
    ngx_expect(ngx_http_cache_purge_state_init(&zone, &state) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_complex(&reloaded) == 1);
    ngx_expect(ngx_http_cache_purge_state_match(&reloaded, &entry, 211, &match)
               == NGX_OK && match.invalidate_before == 210);
    for (i = 5; i <= 6; i++) {
        job.id[15] = (u_char) i;
        ngx_http_cache_purge_state_finish(&state, job.id);
    }
    ngx_http_cache_purge_state_collect(&state, 501);
    ngx_expect(ngx_http_cache_purge_state_complex(&state) == 0);
    ngx_http_cache_purge_empty(&state);

    /* Rule limits reject before admission and release reserved queue space. */
    for (i = 1; i <= 256; i++) {
        job = ngx_http_cache_purge_job(i, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                                                        "http://a.test/one");
        ngx_expect(ngx_http_cache_purge_state_install(&state, &job) == NGX_OK);
    }
    job = ngx_http_cache_purge_job(257, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                                                       "http://a.test/one");
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_SUBMIT, &job, &message) == NGX_BUSY);
    ngx_http_cache_purge_state_stats(&state, &rules, &messages, &bytes);
    ngx_expect(rules == 256 && messages == 0 && bytes == 0);
    for (i = 1; i <= 256; i++) {
        job = ngx_http_cache_purge_job(i, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                                                        "http://a.test/one");
        ngx_http_cache_purge_state_finish(&state, job.id);
    }
    ngx_http_cache_purge_state_collect(&state, 502);
    ngx_http_cache_purge_empty(&state);
    ngx_expect(pool->pfree == baseline);

    /* Queue count and byte limits are independent of the object count. */
    for (i = 0; i < 256; i++) {
        ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
                   NGX_HTTP_CACHE_PURGE_STATUS, &job, &pending[i]) == NGX_OK);
    }
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_STATUS, &job, &message) == NGX_BUSY);
    for (i = 0; i < 256; i++) {
        ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken)
                   == NGX_OK);
        ngx_http_cache_purge_state_complete(&state, taken,
                                            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
                                            NULL);
        ngx_expect(ngx_http_cache_purge_state_poll(&state, &pending[i],
                   &code, &result) == NGX_OK
                   && code == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
    }
    ngx_http_cache_purge_empty(&state);

    large = malloc(32768);
    ngx_expect(large != NULL);
    memset(large, 'x', 32768);
    entry.url.data = (u_char *) large;
    entry.url.len = 32768;
    entry.host = entry.namespace_value = entry.misc = entry.root
               = entry.zone = entry.path = entry.url;
    entry.tag_count = 0;
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_RECORD, &entry, &message) == NGX_OK
               && message == NULL);
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_RECORD, &entry, &message) == NGX_OK);
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_RECORD, &entry, &message) == NGX_BUSY);
    free(large);
    for (i = 0; i < 2; i++) {
        ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken)
                   == NGX_OK && work->data.entry.url.data[100] == 'x');
        ngx_http_cache_purge_state_complete(&state, taken, 0, NULL);
    }
    ngx_http_cache_purge_empty(&state);

    /* An independent requester observes publication before its completion. */
    memset(&entry, 0, sizeof(entry));
    entry.url = ngx_http_cache_purge_text("http://a.test/one");
    job = ngx_http_cache_purge_job(7, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                                                       "http://a.test/one");
    ngx_expect(pipe(ready) == 0 && pipe(done) == 0);
    child = fork();
    ngx_expect(child >= 0);
    if (child == 0) {
        ngx_pid = getpid();
        ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
                   NGX_HTTP_CACHE_PURGE_SUBMIT, &job, &message) == NGX_OK);
        ngx_expect(write(ready[1], "r", 1) == 1);
        ngx_expect(read(done[0], &byte, 1) == 1);
        ngx_expect(ngx_http_cache_purge_state_match(&state, &entry, 201, &match)
                   == NGX_OK);
        ngx_expect(ngx_http_cache_purge_state_poll(&state, &message,
                   &code, &result) == NGX_OK && code == 0);
        _exit(0);
    }
    ngx_expect(read(ready[0], &byte, 1) == 1);
    ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken) == NGX_OK);
    ngx_http_cache_purge_state_complete(&state, taken, 0, &work->data.job);
    ngx_expect(write(done[1], "d", 1) == 1);
    ngx_expect(waitpid(child, &status, 0) == child
               && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    close(ready[0]); close(ready[1]); close(done[0]); close(done[1]);
    ngx_http_cache_purge_state_finish(&state, job.id);
    ngx_http_cache_purge_state_collect(&state, 503);
    ngx_http_cache_purge_empty(&state);

    /* A dead consumer's bounded packet is returned to the pending queue. */
    ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
               NGX_HTTP_CACHE_PURGE_SUBMIT, &job, &message) == NGX_OK);
    child = fork();
    ngx_expect(child >= 0);
    if (child == 0) {
        ngx_pid = getpid();
        ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken)
                   == NGX_OK);
        _exit(0);
    }
    ngx_expect(waitpid(child, &status, 0) == child
               && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    ngx_http_cache_purge_state_collect(&state, 504);
    ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken) == NGX_OK);
    ngx_http_cache_purge_state_complete(&state, taken, 0, &work->data.job);
    ngx_expect(ngx_http_cache_purge_state_poll(&state, &message, &code, &result)
               == NGX_OK && code == 0);
    ngx_http_cache_purge_state_finish(&state, job.id);
    ngx_http_cache_purge_state_collect(&state, 505);
    ngx_http_cache_purge_empty(&state);

    /* A dead requester leaves the task active, but not a retained reply. */
    job = ngx_http_cache_purge_job(8, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                                                       "http://a.test/one");
    child = fork();
    ngx_expect(child >= 0);
    if (child == 0) {
        ngx_pid = getpid();
        ngx_expect(ngx_http_cache_purge_state_enqueue(&state,
                   NGX_HTTP_CACHE_PURGE_SUBMIT, &job, &message) == NGX_OK);
        _exit(0);
    }
    ngx_expect(waitpid(child, &status, 0) == child
               && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    ngx_expect(ngx_http_cache_purge_state_take(&state, work, &taken) == NGX_OK);
    ngx_http_cache_purge_state_complete(&state, taken, 0, &work->data.job);
    ngx_http_cache_purge_state_collect(&state, 506);
    ngx_http_cache_purge_state_stats(&state, &rules, &messages, &bytes);
    ngx_expect(rules == 1 && messages == 0 && bytes == 0);
    ngx_http_cache_purge_state_finish(&state, job.id);
    ngx_http_cache_purge_state_collect(&state, 507);
    ngx_http_cache_purge_empty(&state);
    ngx_http_cache_purge_test_urls(&state);
    ngx_http_cache_purge_empty(&state);
    ngx_http_cache_purge_test_tags(&state);
    ngx_http_cache_purge_empty(&state);
    ngx_expect(pool->pfree == baseline);
    ngx_shmtx_destroy(&pool->mutex);
    ngx_expect(munmap(zone.shm.addr, zone.shm.size) == 0);
    free(work);
    printf("%u shared-state checks passed\n", ngx_http_cache_purge_checks);
    return 0;
}
