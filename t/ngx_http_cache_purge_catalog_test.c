
/*
 * Copyright (C) Hanada
 */


#include "ngx_http_cache_purge_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>


static ngx_http_cache_rocksdb_bytes_t ngx_http_cache_purge_text(
    const char *text);
static void ngx_http_cache_purge_test_entry(
    ngx_http_cache_purge_entry_t *entry, unsigned number);
static size_t ngx_http_cache_purge_test_scan(
    ngx_http_cache_purge_catalog_t *catalog, unsigned type, const char *value,
    const char *scope, const char *misc, int host_scope, int retained,
    uint64_t now, unsigned char ids[32][24]);
static size_t ngx_http_cache_purge_test_rows(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, const char *prefix, size_t key_length);
static int ngx_http_cache_purge_test_tag(void *data,
    ngx_http_cache_rocksdb_bytes_t tag);
static int ngx_http_cache_purge_test_visit(void *data,
    const ngx_http_cache_purge_entry_t *entry,
    ngx_http_cache_rocksdb_error_t *error);
static void ngx_http_cache_purge_test_walk(
    ngx_http_cache_purge_catalog_t *catalog);


static unsigned  ngx_http_cache_purge_checks;

#define ngx_expect(test)                                                      \
    do {                                                                      \
        if (!(test)) {                                                        \
            fprintf(stderr, "line %d: %s\n", __LINE__, #test);                \
            exit(1);                                                          \
        }                                                                     \
        ngx_http_cache_purge_checks++;                                        \
    } while (0)


static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_text(const char *text)
{
    ngx_http_cache_rocksdb_bytes_t  value;

    value.data = (const unsigned char *) text;
    value.len = strlen(text);

    return value;
}


static void
ngx_http_cache_purge_test_entry(ngx_http_cache_purge_entry_t *entry,
    unsigned number)
{
    static char  paths[128][48];

    ngx_expect(number < 128);
    snprintf(paths[number], sizeof(paths[number]), "cache/%032x", number);
    memset(entry, 0, sizeof(*entry));
    entry->hash[15] = (unsigned char) number;
    entry->uniq = number + 100;
    entry->created = 1000;
    entry->url = ngx_http_cache_purge_text("http://a.test/docs/a");
    entry->host = ngx_http_cache_purge_text("a.test");
    entry->root =
        ngx_http_cache_purge_text(number % 2 ? "/cache/a" : "/cache/b");
    entry->zone = ngx_http_cache_purge_text(number % 2 ? "zone-a" : "zone-b");
    entry->namespace_value = ngx_http_cache_purge_text("");
    entry->misc = ngx_http_cache_purge_text("");
    entry->path = ngx_http_cache_purge_text(paths[number]);
    entry->tags[0] = ngx_http_cache_purge_text("news");
    entry->tags[1] = ngx_http_cache_purge_text("css");
    entry->tags[2] = ngx_http_cache_purge_text("news");
    entry->tag_count = 3;

    if (number == 5 || number == 6) {
        entry->misc = ngx_http_cache_purge_text("language=en");
    }

    if (number == 7) {
        entry->namespace_value = ngx_http_cache_purge_text("scope");
    }

    if (number == 8 || number == 9) {
        entry->url = ngx_http_cache_purge_text("http://a.test/docs/b");
    }

    if (number == 10) {
        entry->url = ngx_http_cache_purge_text("http://b.test/docs/a");
        entry->host = ngx_http_cache_purge_text("b.test");
    }
}


static size_t
ngx_http_cache_purge_test_scan(ngx_http_cache_purge_catalog_t *catalog,
    unsigned type, const char *value, const char *scope, const char *misc,
    int host_scope, int retained, uint64_t now, unsigned char ids[32][24])
{
    int                             rc;
    size_t                          count;
    ngx_http_cache_purge_scan_t    *scan;
    ngx_http_cache_purge_entry_t    entry;
    ngx_http_cache_purge_select_t   select;
    ngx_http_cache_rocksdb_bytes_t  after;
    ngx_http_cache_rocksdb_error_t  error;

    memset(&select, 0, sizeof(select));
    select.type = type;
    select.value = ngx_http_cache_purge_text(value);
    select.namespace_value = ngx_http_cache_purge_text(scope);
    select.misc = ngx_http_cache_purge_text(misc);
    select.host_scope = host_scope;

    if (host_scope) {
        select.host = select.namespace_value;
        select.namespace_value = ngx_http_cache_purge_text("");
    }

    after = ngx_http_cache_purge_text("");
    scan = NULL;
    ngx_expect(ngx_http_cache_purge_scan_open(catalog, &select, after,
                                              retained, now, &scan, &error)
               == 0);
    count = 0;

    for ( ;; ) {
        rc = ngx_http_cache_purge_scan_next(scan, &entry, &error);

        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            continue;
        }

        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            break;
        }

        if (rc != 0) {
            fprintf(stderr, "scan: %d %s\n", rc, error.message);
        }

        ngx_expect(rc == 0 && count < 32 && entry.tag_count == 0);
        ngx_expect(entry.url.data == NULL && entry.host.data == NULL
                   && entry.namespace_value.data == NULL
                   && entry.misc.data == NULL);
        ngx_expect(entry.root.len != 0 && entry.zone.len != 0
                   && entry.path.len != 0);

        if (ids != NULL) {
            memcpy(ids[count], entry.id, 24);
        }

        count++;
    }

    ngx_http_cache_purge_scan_close(scan);

    return count;
}


static size_t
ngx_http_cache_purge_test_rows(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, const char *prefix, size_t key_length)
{
    int                               rc;
    size_t                            count;
    ngx_http_cache_rocksdb_error_t    error;
    ngx_http_cache_rocksdb_buffer_t   key, value;
    ngx_http_cache_rocksdb_cursor_t  *cursor;
    unsigned char                     keys[65536], values[65536];

    cursor = NULL;
    ngx_expect(ngx_http_cache_rocksdb_cursor_open(
                   store, cf, ngx_http_cache_purge_text(prefix),
                   ngx_http_cache_purge_text(""), &cursor, &error)
               == 0);
    key.data = keys;
    key.capacity = sizeof(keys);
    value.data = values;
    value.capacity = sizeof(values);
    count = 0;

    for ( ;; ) {
        rc = ngx_http_cache_rocksdb_cursor_next(cursor, &key, &value, &error);

        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            break;
        }

        ngx_expect(rc == 0);
        ngx_expect(key_length == 0 || key.length == key_length);
        count++;
    }

    ngx_expect(ngx_http_cache_rocksdb_cursor_close(&cursor, &error) == 0);

    return count;
}


static int
ngx_http_cache_purge_test_tag(void *data, ngx_http_cache_rocksdb_bytes_t tag)
{
    size_t  *count = data;

    ngx_expect(tag.len > 0);
    (*count)++;

    return 0;
}


typedef struct {
    ngx_http_cache_purge_catalog_t  *catalog;
    const char                      *root;
    size_t                           count;
    int                              pause;
    int                              remove;
    unsigned char                    ids[8][24];
} ngx_http_cache_purge_walk_test_t;


static int
ngx_http_cache_purge_test_visit(void *data,
    const ngx_http_cache_purge_entry_t *entry,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_walk_test_t  *test = data;

    ngx_expect(entry->root.len == strlen(test->root)
               && memcmp(entry->root.data, test->root, entry->root.len) == 0);
    ngx_expect(entry->url.data == NULL && entry->tag_count == 0);

    if (test->pause) {
        test->pause = 0;

        return NGX_HTTP_CACHE_ROCKSDB_BUSY;
    }

    ngx_expect(test->count < 8);
    memcpy(test->ids[test->count++], entry->id, 24);

    if (test->remove) {
        return ngx_http_cache_purge_catalog_remove(test->catalog, entry->id,
                                                   entry->uniq, error);
    }

    return ngx_http_cache_purge_catalog_retain(test->catalog, entry->id,
                                               entry->uniq, 2000, error);
}


static void
ngx_http_cache_purge_test_walk(ngx_http_cache_purge_catalog_t *catalog)
{
    size_t                            i, count;
    ngx_http_cache_purge_entry_t      entry;
    ngx_http_cache_rocksdb_bytes_t    root;
    ngx_http_cache_rocksdb_error_t    error;
    ngx_http_cache_purge_walk_test_t  test;
    unsigned char                     after[24], empty[24];

    memset(&test, 0, sizeof(test));
    memset(after, 0, sizeof(after));
    memset(empty, 0, sizeof(empty));
    test.catalog = catalog;
    test.root = "/walk/a";
    root = ngx_http_cache_purge_text(test.root);
    ngx_expect(ngx_http_cache_purge_catalog_walk(
                   catalog, root, after, 2, ngx_http_cache_purge_test_visit,
                   &test, &error)
               == NGX_HTTP_CACHE_ROCKSDB_DONE
               && test.count == 0);

    for (i = 1; i <= 6; i++) {
        ngx_http_cache_purge_test_entry(&entry, (unsigned) i);
        entry.root = ngx_http_cache_purge_text(i % 2 ? "/walk/a" : "/walk/b");
        ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error)
                   == 0);
    }

    test.pause = 1;
    ngx_expect(ngx_http_cache_purge_catalog_walk(
                   catalog, root, after, 2, ngx_http_cache_purge_test_visit,
                   &test, &error)
               == NGX_HTTP_CACHE_ROCKSDB_BUSY
               && test.count == 0);
    ngx_expect(memcmp(after, empty, sizeof(after)) == 0);
    ngx_expect(ngx_http_cache_purge_catalog_walk(
                   catalog, root, after, 2, ngx_http_cache_purge_test_visit,
                   &test, &error)
               == 0
               && test.count == 2);
    ngx_expect(ngx_http_cache_purge_catalog_walk(
                   catalog, root, after, 2, ngx_http_cache_purge_test_visit,
                   &test, &error)
               == NGX_HTTP_CACHE_ROCKSDB_DONE
               && test.count == 3);

    for (i = 0; i < 3; i++) {
        ngx_expect(test.ids[i][23] == i * 2 + 1);
        ngx_expect(ngx_http_cache_purge_catalog_get(catalog, test.ids[i],
                                                    &entry, &error)
                   == 0);
        ngx_expect(entry.retain_until == 2000 && entry.tag_count == 2);
    }

    test.root = "/walk/b";
    test.count = 0;
    test.remove = 1;
    root = ngx_http_cache_purge_text(test.root);
    ngx_expect(ngx_http_cache_purge_catalog_walk(
                   catalog, root, after, 8, ngx_http_cache_purge_test_visit,
                   &test, &error)
               == NGX_HTTP_CACHE_ROCKSDB_DONE
               && test.count == 3);

    for (i = 0; i < 3; i++) {
        ngx_expect(test.ids[i][23] == i * 2 + 2);
    }

    ngx_http_cache_purge_test_entry(&entry, 8);
    entry.root = root;
    ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error) == 0);
    test.count = 0;
    ngx_expect(ngx_http_cache_purge_catalog_walk(
                   catalog, root, after, 8, ngx_http_cache_purge_test_visit,
                   &test, &error)
               == NGX_HTTP_CACHE_ROCKSDB_DONE
               && test.count == 1);
    ngx_expect(test.ids[0][23] == 8);
    ngx_expect(
        ngx_http_cache_purge_catalog_collect(catalog, 2000, 64, &count, &error)
        == 0
        && count == 3);
}


int
ngx_http_cache_purge_test_main(void)
{
    ngx_http_cache_rocksdb_t          *store;
    ngx_http_cache_purge_scan_t       *scan;
    ngx_http_cache_purge_entry_t       entry, current;
    ngx_http_cache_purge_select_t      select;
    ngx_http_cache_purge_catalog_t    *catalog;
    ngx_http_cache_rocksdb_bytes_t     bookmark;
    ngx_http_cache_rocksdb_error_t     error;
    ngx_http_cache_rocksdb_options_t   options;
    ngx_http_cache_rocksdb_mutation_t  legacy;
    unsigned char                      ids[32][24], token[32830], saved[24];
    char                               path[] = "/tmp/cache-catalog-XXXXXX";
    char                               old_path[128];
    char                               tag_values[128][16], long_url[4097];
    unsigned char                      misc[] = { 'a', 0, 'b' };
    int                                rc;
    size_t                             i, count, removed;

    ngx_expect(mkdtemp(path) != NULL);
    ngx_http_cache_rocksdb_init_options(&options);
    options.cache_bytes = 8 * 1024 * 1024;
    options.write_buffer_bytes = 2 * 1024 * 1024;
    store = NULL;
    catalog = NULL;
    ngx_expect(ngx_http_cache_rocksdb_open(path, &options, &store, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_catalog_open(store, &catalog, &error)
               == 0);

    ngx_http_cache_purge_test_entry(&entry, 99);
    memset(long_url, 'x', sizeof(long_url) - 1);
    memcpy(long_url, "http://a.test/", 14);
    long_url[sizeof(long_url) - 1] = 0;
    entry.url = ngx_http_cache_purge_text(long_url);
    entry.misc.data = misc;
    entry.misc.len = sizeof(misc);
    entry.tag_count = 128;

    for (i = 0; i < 128; i++) {
        snprintf(tag_values[i], sizeof(tag_values[i]), "tag-%02u",
                 (unsigned) i);
        entry.tags[i] = ngx_http_cache_purge_text(tag_values[i]);
    }

    ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error) == 0);
    count = 0;
    ngx_expect(ngx_http_cache_purge_catalog_tags(catalog, &entry,
                                                 ngx_http_cache_purge_test_tag,
                                                 &count, &error)
               == 0);
    ngx_expect(count == 128);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_TAG,
                   "tag-00,absent,tag-127", "", "", 0, 0, 0, ids)
               == 1);

    /* Replace a full TAG set without exceeding the bounded mutation arena. */

    for (i = 0; i < 128; i++) {
        snprintf(tag_values[i], sizeof(tag_values[i]), "new-%03u",
                 (unsigned) i);
        entry.tags[i] = ngx_http_cache_purge_text(tag_values[i]);
    }

    ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error) == 0);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_TAG,
                   "tag-00,new-000,new-127", "", "", 0, 0, 0, ids)
               == 1);
    memset(&select, 0, sizeof(select));
    select.type = NGX_HTTP_CACHE_PURGE_SELECT_KEY;
    select.value = entry.url;
    select.misc = entry.misc;
    ngx_expect(ngx_http_cache_purge_scan_open(catalog, &select,
                                              ngx_http_cache_purge_text(""), 0,
                                              0, &scan, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_scan_next(scan, &current, &error) == 0);
    ngx_expect(current.tag_count == 0 && current.url.data == NULL
               && current.misc.data == NULL);
    memcpy(saved, current.id, 24);
    ngx_http_cache_purge_scan_close(scan);
    entry.tag_count = 129;
    ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error)
               == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    entry.tag_count = 128;
    entry.misc.len = 32769;
    ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error)
               == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    entry.misc.len = sizeof(misc);
    entry.path = ngx_http_cache_purge_text("../outside");
    ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error)
               == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    ngx_expect(
        ngx_http_cache_purge_catalog_get(catalog, saved, &current, &error)
        == 0);
    ngx_expect(current.tag_count == 128 && current.url.len == 4096);
    ngx_expect(current.misc.len == sizeof(misc)
               && memcmp(current.misc.data, misc, sizeof(misc)) == 0);
    ngx_expect(ngx_http_cache_purge_catalog_remove(catalog, saved, 0, &error)
               == 0);

    for (i = 1; i <= 10; i++) {
        ngx_http_cache_purge_test_entry(&entry, (unsigned) i);
        rc = ngx_http_cache_purge_catalog_put(catalog, &entry, &error);

        if (rc != 0) {
            fprintf(stderr, "put: %d %s\n", rc, error.message);
        }

        ngx_expect(rc == 0);
    }

    ngx_http_cache_purge_test_entry(&entry, 1);
    count = 0;
    ngx_expect(ngx_http_cache_purge_catalog_tags(catalog, &entry,
                                                 ngx_http_cache_purge_test_tag,
                                                 &count, &error)
               == 0);
    ngx_expect(count == 2); /* Four physical files, two distinct TAGs. */

    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                   "http://a.test/docs/a", "", "", 0, 0, 0, ids)
               == 6);

    for (i = 0; i < 6; i++) {
        if (ids[i][23] == 1) { /* Select fixture file 1. */
            break;
        }
    }

    ngx_expect(i < 6);
    memcpy(saved, ids[i], 24);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_KEY,
                   "http://a.test/docs/a", "", "", 0, 0, 0, NULL)
               == 4);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_KEY,
                   "http://a.test/docs/a", "", "language=en", 0, 0, 0, NULL)
               == 2);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_KEY,
                   "http://a.test/docs/a", "scope", "", 0, 0, 0, NULL)
               == 1);
    ngx_expect(ngx_http_cache_purge_test_scan(catalog,
                                              NGX_HTTP_CACHE_PURGE_SELECT_HOST,
                                              "a.test", "", "", 0, 0, 0, NULL)
               == 8);
    ngx_expect(ngx_http_cache_purge_test_scan(catalog,
                                              NGX_HTTP_CACHE_PURGE_SELECT_TAG,
                                              "news", "", "", 0, 0, 0, NULL)
               == 9);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_TAG, "news", "scope",
                   "", 0, 0, 0, NULL)
               == 1);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_TAG, "news", "a.test",
                   "", 1, 0, 0, NULL)
               == 8);
    ngx_expect(ngx_http_cache_purge_test_scan(catalog,
                                              NGX_HTTP_CACHE_PURGE_SELECT_TAG,
                                              "absent", "", "", 0, 0, 0, NULL)
               == 0);

    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_URL, "A", 41)
               == 10);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_URL, "dU", 0)
               == 4);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_TAG, "N", 41)
               == 20);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_TAG, "H", 41)
               == 20);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_TAG, "K", 25)
               == 10);

    memset(&select, 0, sizeof(select));
    select.type = NGX_HTTP_CACHE_PURGE_SELECT_PREFIX;
    select.value = ngx_http_cache_purge_text("http://a.test/docs/");
    bookmark = ngx_http_cache_purge_text("");
    count = 0;

    for ( ;; ) {
        scan = NULL;
        ngx_expect(ngx_http_cache_purge_scan_open(catalog, &select, bookmark,
                                                  0, 0, &scan, &error)
                   == 0);
        rc = ngx_http_cache_purge_scan_next(scan, &current, &error);

        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            ngx_http_cache_purge_scan_close(scan);
            break;
        }

        ngx_expect(rc == 0 && count < 8);

        for (i = 0; i < count; i++) {
            ngx_expect(memcmp(ids[i], current.id, 24) != 0);
        }

        memcpy(ids[count++], current.id, 24);
        bookmark = ngx_http_cache_purge_scan_bookmark(scan);
        memcpy(token, bookmark.data, bookmark.len);
        bookmark.data = token;
        ngx_http_cache_purge_scan_close(scan);
    }

    ngx_expect(count == 8);
    ngx_expect(
        ngx_http_cache_purge_catalog_get(catalog, saved, &current, &error)
        == 0);
    ngx_expect(ngx_http_cache_purge_catalog_remove(catalog, saved,
                                                   current.uniq + 1000, &error)
               == NGX_HTTP_CACHE_ROCKSDB_CONFLICT);
    ngx_expect(ngx_http_cache_purge_catalog_retain(catalog, saved,
                                                   current.uniq, 3000, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX, "", "", "", 0,
                   0, 2000, NULL)
               == 8);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX, "", "", "", 0,
                   1, 2000, NULL)
               == 9);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX, "", "", "", 0,
                   1, 3000, NULL)
               == 8);
    ngx_expect(ngx_http_cache_purge_catalog_collect(catalog, 2999, 32,
                                                    &removed, &error)
               == 0);
    ngx_expect(removed == 0);
    ngx_expect(ngx_http_cache_purge_catalog_collect(catalog, 3000, 32,
                                                    &removed, &error)
               == 0);
    ngx_expect(removed == 1);

    ngx_http_cache_purge_test_entry(&entry, 1);
    entry.url = ngx_http_cache_purge_text("http://a.test/docs/aa");
    ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error) == 0);
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_URL,
                   "http://a.test/docs/aa", "", "", 0, 0, 0, NULL)
               == 1);
    count = ngx_http_cache_purge_test_scan(
        catalog, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX, "http://a.test/docs/a",
        "", "", 0, 0, 0, NULL);
    ngx_expect(count == 6);

    ngx_http_cache_purge_catalog_close(catalog);
    ngx_expect(ngx_http_cache_rocksdb_close(&store, &error) == 0);
    ngx_expect(ngx_http_cache_rocksdb_open(path, &options, &store, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_catalog_open(store, &catalog, &error)
               == 0);
    count = ngx_http_cache_purge_test_scan(
        catalog, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX, "", "", "", 0, 0, 0, ids);
    ngx_expect(count == 9);

    select.value = ngx_http_cache_purge_text("");
    ngx_expect(ngx_http_cache_purge_scan_open(catalog, &select,
                                              ngx_http_cache_purge_text(""), 0,
                                              0, &scan, &error)
               == 0);
    removed = 0;

    while ((rc = ngx_http_cache_purge_scan_next(scan, &current, &error)) == 0)
    {
        ngx_expect(ngx_http_cache_purge_catalog_remove(catalog, current.id,
                                                       current.uniq, &error)
                   == 0);
        removed++;
    }

    ngx_expect(rc == NGX_HTTP_CACHE_ROCKSDB_DONE && removed == count);
    ngx_http_cache_purge_scan_close(scan);

    /* Empty scope deletion leaves other namespaces intact. */
    ngx_expect(ngx_http_cache_purge_test_scan(
                   catalog, NGX_HTTP_CACHE_PURGE_SELECT_PREFIX, "", "scope",
                   "", 0, 0, 0, ids) == 1);
    ngx_expect(ngx_http_cache_purge_catalog_remove(catalog, ids[0], 0, &error)
               == 0);

    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_URL, "", 0)
               == 0);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_TAG, "", 0)
               == 0);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "d", 0)
               == 0);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "D", 9)
               == 0);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "F", 25)
               == 0);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "G", 33)
               == 0);

    for (i = 16; i < 86; i++) {
        ngx_http_cache_purge_test_entry(&entry, (unsigned) i);
        ngx_expect(ngx_http_cache_purge_catalog_put(catalog, &entry, &error)
                   == 0);
    }

    ngx_expect(ngx_http_cache_purge_scan_open(catalog, &select,
                                              ngx_http_cache_purge_text(""), 0,
                                              0, &scan, &error)
               == 0);
    count = 0;

    while ((rc = ngx_http_cache_purge_scan_next(scan, &current, &error)) == 0)
    {
        ngx_expect(ngx_http_cache_purge_catalog_retain(
                       catalog, current.id, current.uniq, 2000, &error)
                   == 0);
        count++;
    }

    ngx_expect(rc == NGX_HTTP_CACHE_ROCKSDB_DONE && count == 70);
    ngx_http_cache_purge_scan_close(scan);
    ngx_expect(ngx_http_cache_purge_scan_open(catalog, &select,
                                              ngx_http_cache_purge_text(""), 0,
                                              1000, &scan, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_scan_next(scan, &current, &error)
               == NGX_HTTP_CACHE_ROCKSDB_BUSY);
    bookmark = ngx_http_cache_purge_scan_bookmark(scan);
    memcpy(token, bookmark.data, bookmark.len);
    bookmark.data = token;
    ngx_http_cache_purge_scan_close(scan);
    ngx_expect(ngx_http_cache_purge_scan_open(catalog, &select, bookmark, 0,
                                              1000, &scan, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_scan_next(scan, &current, &error)
               == NGX_HTTP_CACHE_ROCKSDB_DONE);
    ngx_http_cache_purge_scan_close(scan);

    for (i = 0; i < 3; i++) {
        ngx_expect(ngx_http_cache_purge_catalog_collect(catalog, 2000, 32,
                                                        &removed, &error)
                   == 0);
        ngx_expect(removed == (i < 2 ? 32 : 6));
    }

    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_URL, "", 0)
               == 0);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_TAG, "", 0)
               == 0);
    ngx_http_cache_purge_test_walk(catalog);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_URL, "", 0)
               == 0);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_TAG, "", 0)
               == 0);
    ngx_http_cache_purge_catalog_close(catalog);
    ngx_expect(ngx_http_cache_rocksdb_close(&store, &error) == 0);

    snprintf(old_path, sizeof(old_path), "%s/legacy", path);
    ngx_expect(mkdir(old_path, 0700) == 0);
    ngx_expect(ngx_http_cache_rocksdb_open(old_path, &options, &store, &error)
               == 0);
    memset(&legacy, 0, sizeof(legacy));
    legacy.key = ngx_http_cache_purge_text("M:index");
    legacy.value = ngx_http_cache_purge_text("14");
    ngx_expect(ngx_http_cache_rocksdb_write(store, &legacy, 1, NULL, 0, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_catalog_open(store, &catalog, &error)
               == NGX_HTTP_CACHE_ROCKSDB_SCHEMA);
    ngx_expect(catalog == NULL);
    ngx_expect(ngx_http_cache_purge_test_rows(
                   store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "M:catalog", 0)
               == 0);
    ngx_expect(ngx_http_cache_rocksdb_close(&store, &error) == 0);

    snprintf(old_path, sizeof(old_path), "%s/version-one", path);
    ngx_expect(mkdir(old_path, 0700) == 0);
    ngx_expect(ngx_http_cache_rocksdb_open(old_path, &options, &store, &error)
               == 0);
    memset(&legacy, 0, sizeof(legacy));
    legacy.key = ngx_http_cache_purge_text("M:catalog");
    legacy.value = ngx_http_cache_purge_text("1");
    ngx_expect(ngx_http_cache_rocksdb_write(store, &legacy, 1, NULL, 0, &error)
               == 0);
    ngx_expect(ngx_http_cache_purge_catalog_open(store, &catalog, &error)
               == NGX_HTTP_CACHE_ROCKSDB_SCHEMA);
    ngx_expect(catalog == NULL);
    ngx_expect(ngx_http_cache_rocksdb_close(&store, &error) == 0);

    printf("{\"status\":\"passed\",\"checks\":%u,\"root\":\"%s\"}\n",
           ngx_http_cache_purge_checks, path);

    return 0;
}
