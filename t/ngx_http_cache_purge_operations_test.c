/*
 * Copyright (C) Hanada
 */

#include "ngx_http_cache_purge_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned                       ngx_http_cache_purge_test_count;
static ngx_http_cache_rocksdb_error_t ngx_http_cache_purge_test_error;

static void
ngx_http_cache_purge_test_check(int ok, const char *message)
{
    if (!ok) {
        fprintf(stderr, "FAIL %s: %s\n", message,
                ngx_http_cache_purge_test_error.message);
        exit(1);
    }
    ngx_http_cache_purge_test_count++;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_test_bytes(const char *value)
{
    ngx_http_cache_rocksdb_bytes_t bytes;

    bytes.data = (const unsigned char *) value;
    bytes.len = strlen(value);
    return bytes;
}

static ngx_http_cache_rocksdb_t *
ngx_http_cache_purge_test_open(const char *path)
{
    ngx_http_cache_rocksdb_options_t  options;
    ngx_http_cache_rocksdb_t         *store;

    ngx_http_cache_rocksdb_options(&options);
    store = NULL;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_rocksdb_open(path, &options, &store,
                                    &ngx_http_cache_purge_test_error)
            == 0,
        "open database");
    return store;
}

static void
ngx_http_cache_purge_test_write(ngx_http_cache_rocksdb_t *store,
    const char *key, const unsigned char *data, size_t size)
{
    ngx_http_cache_rocksdb_mutation_t mutation;

    mutation.cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
    mutation.type = NGX_HTTP_CACHE_ROCKSDB_PUT;
    mutation.key = ngx_http_cache_purge_test_bytes(key);
    mutation.value.data = data;
    mutation.value.len = size;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_rocksdb_write(store, &mutation, 1, NULL, 0,
                                     &ngx_http_cache_purge_test_error)
            == 0,
        "write fixture");
}

static uint64_t
ngx_http_cache_purge_test_writes(ngx_http_cache_rocksdb_t *store)
{
    ngx_http_cache_rocksdb_stats_t stats;

    ngx_http_cache_purge_test_check(
        ngx_http_cache_rocksdb_stats(store, &stats,
                                     &ngx_http_cache_purge_test_error)
            == 0,
        "read write counter");
    return stats.writes;
}

static void
ngx_http_cache_purge_test_hex(const unsigned char *data, size_t size)
{
    size_t             i;
    static const char  digits[] = "0123456789abcdef";

    for (i = 0; i < size; i++) {
        putchar(digits[data[i] >> 4]);
        putchar(digits[data[i] & 15]);
    }
}

static void
ngx_http_cache_purge_test_dump(ngx_http_cache_rocksdb_t *store)
{
    ngx_http_cache_rocksdb_cursor_t  *cursor;
    ngx_http_cache_rocksdb_buffer_t   key, value;
    ngx_http_cache_rocksdb_bytes_t    empty;
    int                               rc;

    empty = ngx_http_cache_purge_test_bytes("");
    cursor = NULL;
    key.capacity = 65536;
    value.capacity = 262144;
    key.data = malloc(key.capacity);
    value.data = malloc(value.capacity);
    ngx_http_cache_purge_test_check(key.data != NULL && value.data != NULL,
                                    "dump buffers");
    ngx_http_cache_purge_test_check(ngx_http_cache_rocksdb_cursor_open(
                                        store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                        empty, empty, &cursor,
                                        &ngx_http_cache_purge_test_error)
                                        == 0,
                                    "dump cursor");
    for ( ;; ) {
        rc = ngx_http_cache_rocksdb_cursor_next(
            cursor, &key, &value, &ngx_http_cache_purge_test_error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            break;
        }
        ngx_http_cache_purge_test_check(rc == 0, "dump row");
        ngx_http_cache_purge_test_hex(key.data, key.length);
        putchar('\t');
        ngx_http_cache_purge_test_hex(value.data, value.length);
        putchar('\n');
    }
    ngx_http_cache_purge_test_check(
        ngx_http_cache_rocksdb_cursor_close(&cursor,
                                            &ngx_http_cache_purge_test_error)
            == 0,
        "close dump cursor");
    free(key.data);
    free(value.data);
}

int
main(int argc, char **argv)
{
    ngx_http_cache_rocksdb_t               *store;
    ngx_http_cache_purge_operations_t       ops;
    ngx_http_cache_purge_operation_input_t  input;
    ngx_http_cache_purge_operation_t        op;
    ngx_http_cache_purge_operation_page_t   page;
    ngx_http_cache_purge_operation_usage_t  usage;
    ngx_http_cache_purge_checkpoint_t       point;
    ngx_http_cache_rocksdb_buffer_t         saved;
    ngx_http_cache_rocksdb_bytes_t          empty;
    uint64_t                                writes, revision, sequence;
    size_t                                  expired;
    int                                     rc;

    unsigned char  raw[1024];
    char           path[] = "/tmp/ngx-cache-ledger-c-XXXXXX";
    const char     binding[] = { 'a', '\0', 'b' };

    if (argc == 3 && strcmp(argv[1], "--dump") == 0) {
        store = ngx_http_cache_purge_test_open(argv[2]);
        ngx_http_cache_purge_test_dump(store);
        ngx_http_cache_purge_test_check(
            ngx_http_cache_rocksdb_close(&store,
                                         &ngx_http_cache_purge_test_error)
                == 0,
            "close dump database");
        return 0;
    }
    ngx_http_cache_purge_test_check(argc == 1, "test arguments");
    ngx_http_cache_purge_test_check(mkdtemp(path) != NULL, "test directory");
    store = ngx_http_cache_purge_test_open(path);
    /* Only the ledger is tested here; the file index has its own suite. */
    ngx_http_cache_purge_test_write(store, "M:index", (unsigned char *) "2", 1);
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_operations_init(&ops, store, NULL,
                                             &ngx_http_cache_purge_test_error)
            == 0,
        "initialize C ledger");
    memset(&input, 0, sizeof(input));
    input.id = ngx_http_cache_purge_test_bytes("c-api");
    input.selector.kind = NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG;
    input.selector.value = ngx_http_cache_purge_test_bytes("photo");
    input.selector.scope = ngx_http_cache_purge_test_bytes("team");
    input.mode = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    input.issued_at_ms = 1000;
    input.binding.data = (const unsigned char *) binding;
    input.binding.len = sizeof(binding);
    writes = ngx_http_cache_purge_test_writes(store);
    rc = ngx_http_cache_purge_operations_accept(
        &ops, &input, 1000, 300000, NULL, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                    "reject missing C output before writing");
    rc = ngx_http_cache_purge_operations_accept(
        &ops, NULL, 1000, 300000, &op, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
                                        && op.storage == NULL,
                                    "reject missing C input");
    input.selector.value.data = NULL;
    rc = ngx_http_cache_purge_operations_accept(
        &ops, &input, 1000, 300000, &op, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                    "reject nonempty null selector bytes");
    ngx_http_cache_purge_test_check(ngx_http_cache_purge_test_writes(store)
                                        == writes,
                                    "invalid C arguments do not mutate ledger");
    input.selector.value = ngx_http_cache_purge_test_bytes("photo");
    rc = ngx_http_cache_purge_operations_accept(
        &ops, &input, 1000, 300000, &op, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(
        rc == 0 && op.storage != NULL && op.input.binding.len == sizeof(binding)
            && op.input.binding.data != input.binding.data
            && memcmp(op.input.binding.data, binding, sizeof(binding)) == 0,
        "owned C result preserves embedded zero bytes");
    ngx_http_cache_purge_test_check(op.stage == NGX_HTTP_CACHE_PURGE_PREPARING
                                        && op.effective_at_ms == 0
                                        && op.revision == 1,
                                    "acceptance is not activation");
    sequence = op.sequence;
    ngx_http_cache_purge_operation_free(&op);
    ngx_http_cache_purge_operation_free(&op);
    writes = ngx_http_cache_purge_test_writes(store);
    rc = ngx_http_cache_purge_operations_accept(
        &ops, &input, 999999, 1, &op, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(
        rc == 0 && op.propagation_ms == 300000
            && ngx_http_cache_purge_test_writes(store) == writes,
        "C replay preserves original propagation policy without a write");
    ngx_http_cache_purge_operation_free(&op);
    rc = ngx_http_cache_purge_operations_activate(
        &ops, input.id, 1, 1100, &op, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(
        rc == 0 && op.propagation_until_ms == 301100, "C activation deadline");
    revision = op.revision;
    ngx_http_cache_purge_operation_free(&op);
    memset(&point, 0, sizeof(point));
    point.id = 1;
    point.scan_done = 2;
    rc = ngx_http_cache_purge_operations_checkpoint(
        &ops, input.id, revision, &point, &op,
        &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                    "reject invalid C boolean");
    point.scan_done = 1;
    point.pending = 1;
    point.next_run_ms = 1200;
    point.visits_delta = 1;
    rc = ngx_http_cache_purge_operations_checkpoint(
        &ops, input.id, revision, &point, &op,
        &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(
        rc == 0 && op.stage == NGX_HTTP_CACHE_PURGE_DRAINING,
        "C checkpoint enters draining");
    revision = op.revision;
    ngx_http_cache_purge_operation_free(&op);
    rc = ngx_http_cache_purge_operations_expire(
        &ops, 301100, 32, &expired, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == 0 && expired == 1,
                                    "expire C propagation");
    empty = ngx_http_cache_purge_test_bytes("");
    rc = ngx_http_cache_purge_operations_page(&ops, 0, 0, empty, 32, &page,
                                              &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(
        rc == 0 && page.count == 1 && page.items[0].pending == 1
            && !page.items[0].propagate
            && page.items[0].stage == NGX_HTTP_CACHE_PURGE_DRAINING,
        "expired window retains unfinished FENCE");
    revision = page.items[0].revision;
    ngx_http_cache_purge_operation_page_free(&page);
    rc = ngx_http_cache_purge_operations_materialize(
        &ops, input.id, revision, sequence, &op,
        &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
                                        && op.storage == NULL,
                                    "pending files prevent materialization");
    point.id = 2;
    point.pending = 0;
    rc = ngx_http_cache_purge_operations_checkpoint(
        &ops, input.id, revision, &point, &op,
        &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == 0, "complete C file checkpoint");
    revision = op.revision;
    ngx_http_cache_purge_operation_free(&op);
    rc = ngx_http_cache_purge_operations_materialize(
        &ops, input.id, revision, sequence, &op,
        &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(
        rc == 0 && op.stage == NGX_HTTP_CACHE_PURGE_MATERIALIZED,
        "verified drain materializes operation");
    ngx_http_cache_purge_operation_free(&op);
    rc = ngx_http_cache_purge_operations_usage(
        &ops, &usage, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == 0 && usage.live_records == 0
                                        && usage.live_selector_bytes == 0,
                                    "release C resident budget");
    saved.data = raw;
    saved.capacity = sizeof(raw);
    saved.length = 0;
    rc = ngx_http_cache_rocksdb_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                    ngx_http_cache_purge_test_bytes("Dc-api"),
                                    &saved, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(rc == 0, "read durable C receipt bytes");
    ngx_http_cache_purge_test_write(store, "Dc-api", (unsigned char *) "bad",
                                    3);
    rc = ngx_http_cache_purge_operations_get(&ops, input.id, &op,
                                             &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(
        rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT && op.storage == NULL,
        "truncated record is rejected without a partial result");
    ngx_http_cache_purge_test_write(store, "Dc-api", raw, saved.length);
    rc = ngx_http_cache_purge_operations_accept(
        &ops, &input, 900000, 1, &op, &ngx_http_cache_purge_test_error);
    ngx_http_cache_purge_test_check(
        rc == 0 && op.stage == NGX_HTTP_CACHE_PURGE_MATERIALIZED,
        "completed C receipt cannot resurrect work");
    ngx_http_cache_purge_operation_free(&op);
    ngx_http_cache_purge_test_check(
        ngx_http_cache_rocksdb_close(&store, &ngx_http_cache_purge_test_error)
            == 0,
        "C calls release all cursors");
    printf("PASS %u pure C ledger checks\n", ngx_http_cache_purge_test_count);
    return 0;
}
