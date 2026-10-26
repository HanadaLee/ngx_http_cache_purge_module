/* Copyright (C) Hanada */
#include "ngx_http_cache_purge_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ngx_test_index(check)                                                  \
    do {                                                                      \
        if (!(check)) {                                                       \
            fprintf(stderr, "C index check failed at %d: %s\n", __LINE__,      \
                    error.message);                                           \
            abort();                                                          \
        }                                                                     \
        checks++;                                                             \
    } while (0)

unsigned
ngx_http_cache_purge_test_query_api(ngx_http_cache_rocksdb_t *store,
    const unsigned char *key, size_t length, const unsigned char *id)
{
    ngx_http_cache_purge_selector_t        selector;
    ngx_http_cache_purge_query_t          *query;
    ngx_http_cache_purge_index_match_t     match;
    ngx_http_cache_rocksdb_error_t         error;
    ngx_http_cache_rocksdb_bytes_t         empty, bookmark;
    ngx_http_cache_rocksdb_stats_t         before, after;
    ngx_http_cache_rocksdb_buffer_t        saved;
    ngx_http_cache_rocksdb_mutation_t      mutation;
    ngx_http_cache_purge_index_expected_t  expected[2];
    ngx_http_cache_purge_index_match_t     results[2];
    unsigned char                         *input;
    unsigned char                          token[33024], object[17], raw[65536];
    unsigned                               checks;
    int                                    rc;

    checks = 0;
    memset(&error, 0, sizeof(error));
    memset(&selector, 0, sizeof(selector));
    memset(&empty, 0, sizeof(empty));
    query = NULL;
    selector.kind = NGX_HTTP_CACHE_PURGE_KEY;
    selector.value.data = key;
    selector.value.len = length;
    ngx_test_index(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0);
    rc = ngx_http_cache_purge_query_open(NULL, &selector, 0, empty, &query,
                                         &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID && query == NULL);
    rc = ngx_http_cache_purge_query_open(store, &selector, 0, empty, NULL,
                                         &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    selector.value.data = NULL;
    rc = ngx_http_cache_purge_query_open(store, &selector, 0, empty, &query,
                                         &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID && query == NULL);
    input = malloc(length);
    ngx_test_index(input != NULL);
    memcpy(input, key, length);
    selector.value.data = input;
    rc = ngx_http_cache_purge_query_open(store, &selector, 0, empty, &query,
                                         &error);
    ngx_test_index(rc == 0 && query != NULL);
    memset(input, '!', length);
    free(input);
    selector.value.data = key;
    rc = ngx_http_cache_purge_query_next(query, NULL, &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    rc = ngx_http_cache_purge_query_next(query, &match, &error);
    ngx_test_index(rc == 0 && match.revision == 2 && match.state == 2
                   && memcmp(match.id, id, 16) == 0);
    bookmark = ngx_http_cache_purge_query_bookmark(query);
    ngx_test_index(bookmark.len != 0 && bookmark.len <= sizeof(token));
    memcpy(token, bookmark.data, bookmark.len);
    bookmark.data = token;
    ngx_test_index(ngx_http_cache_purge_query_close(&query, &error) == 0
                   && query == NULL);
    ngx_test_index(ngx_http_cache_purge_query_close(&query, &error) == 0);
    rc = ngx_http_cache_purge_query_open(store, &selector, 0, bookmark, &query,
                                         &error);
    ngx_test_index(rc == 0);
    memset(&match, 0xff, sizeof(match));
    rc = ngx_http_cache_purge_query_next(query, &match, &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_DONE && match.revision == 0);
    ngx_test_index(ngx_http_cache_purge_query_close(&query, &error) == 0);
    selector.kind = NGX_HTTP_CACHE_PURGE_URL;
    rc = ngx_http_cache_purge_query_open(store, &selector, 0, bookmark, &query,
                                         &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID && query == NULL);
    selector.kind = NGX_HTTP_CACHE_PURGE_KEY;
    bookmark.len = 1;
    rc = ngx_http_cache_purge_query_open(store, &selector, 0, bookmark, &query,
                                         &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT && query == NULL);
    ngx_test_index(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                   && after.writes == before.writes
                   && after.cursor_handles == before.cursor_handles);

    memset(expected, 0, sizeof(expected));
    memcpy(expected[0].id, id, 16);
    expected[0].revision = 2;
    expected[1] = expected[0];
    rc = ngx_http_cache_purge_index_init(NULL, &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    ngx_test_index(ngx_http_cache_purge_index_init(store, &error) == 0);
    rc = ngx_http_cache_purge_index_apply(
        store, NGX_HTTP_CACHE_PURGE_INDEX_RETIRE, expected, 1, NULL, &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    memset(results, 0xff, sizeof(results));
    rc = ngx_http_cache_purge_index_apply(
        store, (ngx_http_cache_purge_index_action_e) 99, expected, 1, results,
        &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
                   && results[0].revision == 0);
    rc = ngx_http_cache_purge_index_apply(
        store, NGX_HTTP_CACHE_PURGE_INDEX_COMMIT_PENDING, expected, 2, results,
        &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID);

    /*
     * The second reference sees the first staged retirement, then rolls it
     * back.
     */
    memset(results, 0xff, sizeof(results));
    rc = ngx_http_cache_purge_index_apply(
        store, NGX_HTTP_CACHE_PURGE_INDEX_RETIRE, expected, 2, results, &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
                   && results[0].revision == 0 && results[1].revision == 0);
    expected[1].id[0] = 0xff;
    rc = ngx_http_cache_purge_index_apply(
        store, NGX_HTTP_CACHE_PURGE_INDEX_RETIRE, expected, 2, results, &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
                   && results[0].revision == 0 && results[1].revision == 0);
    rc = ngx_http_cache_purge_index_apply(store,
                                          NGX_HTTP_CACHE_PURGE_INDEX_PUBLISH,
                                          expected, 1, results, &error);
    ngx_test_index(rc == 0 && results[0].revision == 2
                   && results[0].state == 2);
    ngx_test_index(ngx_http_cache_purge_query_open(store, &selector, 0, empty,
                                                   &query, &error)
                   == 0);
    rc = ngx_http_cache_purge_query_next(query, &match, &error);
    ngx_test_index(rc == 0 && match.revision == 2 && match.state == 2);
    ngx_test_index(ngx_http_cache_purge_query_close(&query, &error) == 0);
    ngx_test_index(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                   && after.writes == before.writes
                   && after.cursor_handles == before.cursor_handles);

    /*
     * Corrupt an existing writer-produced object, then restore its exact bytes.
     */
    object[0] = 'O';
    memcpy(object + 1, id, 16);
    mutation.cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
    mutation.type = NGX_HTTP_CACHE_ROCKSDB_PUT;
    mutation.key.data = object;
    mutation.key.len = sizeof(object);
    saved.data = raw;
    saved.capacity = sizeof(raw);
    saved.length = 0;
    ngx_test_index(ngx_http_cache_rocksdb_get(store, mutation.cf, mutation.key,
                                              &saved, &error)
                   == 0);
    mutation.value.data = (const unsigned char *) "bad";
    mutation.value.len = 3;
    ngx_test_index(
        ngx_http_cache_rocksdb_write(store, &mutation, 1, NULL, 0, &error)
        == 0);
    ngx_test_index(ngx_http_cache_purge_query_open(store, &selector, 0, empty,
                                                   &query, &error)
                   == 0);
    rc = ngx_http_cache_purge_query_next(query, &match, &error);
    ngx_test_index(rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT && match.revision == 0);
    ngx_test_index(ngx_http_cache_purge_query_bookmark(query).len == 0);
    ngx_test_index(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                   && after.cursor_handles == before.cursor_handles);
    ngx_test_index(ngx_http_cache_purge_query_close(&query, &error) == 0);
    mutation.value.data = raw;
    mutation.value.len = saved.length;
    ngx_test_index(
        ngx_http_cache_rocksdb_write(store, &mutation, 1, NULL, 0, &error)
        == 0);
    return checks;
}
