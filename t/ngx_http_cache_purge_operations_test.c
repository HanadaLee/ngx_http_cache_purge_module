/*
 * Copyright (C) Hanada
 */

#include "ngx_http_cache_purge_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

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
    size_t            i;
    static const char digits[] = "0123456789abcdef";

    for (i = 0; i < size; i++) {
        putchar(digits[data[i] >> 4]);
        putchar(digits[data[i] & 15]);
    }
}

static void
ngx_http_cache_purge_test_dump(ngx_http_cache_rocksdb_t *store)
{
    ngx_http_cache_rocksdb_cursor_t *cursor;
    ngx_http_cache_rocksdb_buffer_t  key, value;
    ngx_http_cache_rocksdb_bytes_t  empty;
    int                             rc;

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

static int
ngx_http_cache_purge_test_visit(void *data,
    const ngx_http_cache_purge_control_match_t *match)
{
    unsigned *count;

    count = data;
    ngx_http_cache_purge_test_check(
        match->value.len == 3 && memcmp(match->value.data, "one", 3) == 0,
        "prepared image owns selector bytes");
    (*count)++;
    return 0;
}

static int
ngx_http_cache_purge_test_reject(void *data,
    const ngx_http_cache_purge_control_match_t *match)
{
    (void) data;
    (void) match;
    return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
}

static void
ngx_http_cache_purge_test_control(ngx_http_cache_purge_operations_t *ops)
{
    ngx_http_cache_purge_control_writer_t  *writer;
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_purge_control_image_t  *image, *other;
    ngx_http_cache_purge_coordinator_t     *coordinator;
    ngx_http_cache_purge_control_context_t  context;
    ngx_http_cache_purge_operation_t       source[2], accepted, replay;
    ngx_http_cache_rocksdb_error_t  error;
    void                           *memory;
    size_t                          size;
    uint64_t                               version, current;
    uint32_t                               state;
    unsigned                               calls, steps;
    int                                    rc;
    char                                   value[] = "one";

    memory = NULL;
    size = ngx_http_cache_purge_control_size(4096, 8);
    ngx_http_cache_purge_test_check(posix_memalign(&memory, 64, size) == 0,
                                    "allocate C controls");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_init(memory, size, 4096, 8, &error) == 0,
        "initialize C controls");
    writer = NULL;
    reader = NULL;
    image = NULL;
    coordinator = NULL;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_writer_open(memory, size, &writer, &error)
            == 0,
        "open C publisher");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_reader_open(memory, size, &reader, &error)
            == 0,
        "open C reader");

    memset(source, 0, sizeof(source));
    source[0].input.id = ngx_http_cache_purge_test_bytes("c-control");
    source[0].input.selector.kind = NGX_HTTP_CACHE_PURGE_KEY;
    source[0].input.selector.value = ngx_http_cache_purge_test_bytes(value);
    source[0].input.mode = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    source[0].input.issued_at_ms = 10;
    source[0].sequence = 1;
    source[0].stage = NGX_HTTP_CACHE_PURGE_PREPARING;
    rc = ngx_http_cache_purge_control_prepare(writer, NULL, 1, &image, &error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
                                        && image == NULL,
                                    "reject null C operations");
    rc = ngx_http_cache_purge_control_prepare(writer, source, 65537, &image,
                                              &error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT
                                        && image == NULL,
                                    "bound C operation count");
    source[1] = source[0];
    rc =
        ngx_http_cache_purge_control_prepare(writer, source, 2, &image, &error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
                                        && image == NULL,
                                    "reject duplicate C IDs");
    source[1].input.id = ngx_http_cache_purge_test_bytes("other-control");
    rc =
        ngx_http_cache_purge_control_prepare(writer, source, 2, &image, &error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
                                        && image == NULL,
                                    "reject duplicate C sequences");
    source[1] = source[0];
    source[1].input.selector.value.data = NULL;
    rc = ngx_http_cache_purge_control_prepare(writer, source + 1, 1, &image,
                                              &error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                    "reject null nonempty selector");

    rc =
        ngx_http_cache_purge_control_prepare(writer, source, 1, &image, &error);
    ngx_http_cache_purge_test_check(rc == 0, "prepare C image");
    value[0] = 'X';
    rc = ngx_http_cache_purge_control_publish(writer, image, &version, &error);
    ngx_http_cache_purge_test_check(rc == 0, "publish copied C image");
    ngx_http_cache_purge_control_image_free(&image);
    ngx_http_cache_purge_control_image_free(&image);
    ngx_http_cache_purge_test_check(image == NULL,
                                    "C image free is repeatable");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_gate_open(writer, &error) == 0,
        "open C admission gate");

    memset(&context, 0, sizeof(context));
    context.key = ngx_http_cache_purge_test_bytes("one");
    calls = 0;
    rc = ngx_http_cache_purge_control_match(reader, &context, 8, 4096,
                                            ngx_http_cache_purge_test_visit,
                                            &calls, &current, &error);
    ngx_http_cache_purge_test_check(rc == 0 && calls == 1 && current == version,
                                    "C match copied input");
    rc = ngx_http_cache_purge_control_match(reader, &context, 8, 4096,
                                            ngx_http_cache_purge_test_reject,
                                            NULL, &current, &error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT
                                        && current == 0,
                                    "C callback error clears result");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_guard_begin(reader, version, &error) == 0,
        "callback error released C pin");
    rc = ngx_http_cache_purge_control_reader_close(&reader, &error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
                                        && reader != NULL,
                                    "pinned reader retained");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_guard_end(reader, &error) == 0,
        "release C guard");

    other = NULL;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_prepare(writer, source, 1, &other, &error)
            == 0,
        "prepare C stale image");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_gate_close(writer, &error) == 0,
        "close C admission gate");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_prepare(writer, NULL, 0, &image, &error)
            == 0,
        "prepare C empty image");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_publish(writer, image, &current, &error)
            == 0,
        "publish C empty image");
    version = current;
    rc = ngx_http_cache_purge_control_publish(writer, other, &current, &error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
                                        && current == 0,
                                    "reject C stale image");
    ngx_http_cache_purge_control_image_free(&image);
    ngx_http_cache_purge_control_image_free(&other);
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_writer_close(&writer, &error) == 0,
        "release C publisher");

    ngx_http_cache_purge_test_check(ngx_http_cache_purge_coordinator_open(
                                        ops, memory, size, &coordinator, &error)
                                        == 0,
                                    "open C coordinator");
    for (steps = 0; steps < 10; steps++) {
        ngx_http_cache_purge_test_check(
            ngx_http_cache_purge_coordinator_step(coordinator, &error) == 0,
            "recover C coordinator");
        ngx_http_cache_purge_test_check(
            ngx_http_cache_purge_coordinator_status(coordinator, &state,
                                                    &current, &error)
                == 0,
            "C coordinator status");
        if (state == NGX_HTTP_CACHE_PURGE_COORD_READY) {
            break;
        }
    }
    ngx_http_cache_purge_test_check(steps < 10 && current > version,
                                    "C recovery rebuilds publication");
    value[0] = 'o';
    rc = ngx_http_cache_purge_coordinator_accept(
        coordinator, &source[0].input, 100, 300000, &accepted, &error);
    ngx_http_cache_purge_test_check(
        rc == 0 && accepted.stage == NGX_HTTP_CACHE_PURGE_PREPARING,
        "C acceptance remains PREPARING");
    rc = ngx_http_cache_purge_coordinator_accept(coordinator, &source[0].input,
                                                 101, 1, &replay, &error);
    ngx_http_cache_purge_test_check(rc == 0
                                        && replay.sequence == accepted.sequence,
                                    "replay while C publication pending");
    ngx_http_cache_purge_operation_free(&accepted);
    ngx_http_cache_purge_operation_free(&replay);
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_coordinator_step(coordinator, &error) == 0,
        "publish accepted C control");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_coordinator_step(coordinator, &error) == 0,
        "drain C publication");
    calls = 0;
    rc = ngx_http_cache_purge_control_match(reader, &context, 8, 4096,
                                            ngx_http_cache_purge_test_visit,
                                            &calls, &current, &error);
    ngx_http_cache_purge_test_check(rc == 0 && calls == 1,
                                    "accepted C selector is published");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_coordinator_close(&coordinator, &error) == 0
            && coordinator == NULL,
        "close C coordinator");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_control_reader_close(&reader, &error) == 0,
        "close C control reader");
    free(memory);
}

static void
ngx_http_cache_purge_test_publication(void)
{
    ngx_http_cache_purge_publications_t       *publications;
    ngx_http_cache_purge_publication_target_t  target;
    ngx_http_cache_purge_index_fill_t          fill;
    ngx_http_cache_purge_index_match_t         match;
    ngx_http_cache_purge_file_marker_t         marker;
    ngx_http_cache_purge_root_t                root;
    ngx_http_cache_rocksdb_t                  *store;
    ngx_http_cache_rocksdb_error_t            *error;
    unsigned char                              digest[16];
    uint64_t                                   offset;
    int                                       anchor, lease, fd, rc;
    char parent[] = "/tmp/cache-publication-c-XXXXXX";
    char cache[128], database[128], temporary[128], final[128], locks[128];
    char configured[128];

    error = &ngx_http_cache_purge_test_error;
    ngx_http_cache_purge_test_check(mkdtemp(parent) != NULL,
                                    "C publication directory");
    snprintf(cache, sizeof(cache), "%s/cache", parent);
    snprintf(database, sizeof(database), "%s/db", parent);
    snprintf(temporary, sizeof(temporary), "%s/cache/temp", parent);
    snprintf(final, sizeof(final), "%s/cache/final", parent);
    snprintf(locks, sizeof(locks), "%s/locks", parent);
    snprintf(configured, sizeof(configured), "%s", cache);
    ngx_http_cache_purge_test_check(mkdir(cache, 0700) == 0,
                                    "C cache directory");
    ngx_http_cache_purge_test_check(mkdir(database, 0700) == 0,
                                    "C index directory");
    store = ngx_http_cache_purge_test_open(database);
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_index_init(store, error) == 0,
        "C publication index");
    root.path = ngx_http_cache_purge_test_bytes(configured);
    root.zone = ngx_http_cache_purge_test_bytes("cache");
    root.kind = NGX_HTTP_CACHE_PURGE_ROOT_CACHE;
    publications = NULL;
    rc = ngx_http_cache_purge_publications_open(&root, 1, database,
                                                &publications, error);
    ngx_http_cache_purge_test_check(rc == 0, "open C publication roots");
    configured[0] = 'X';
    rc = ngx_http_cache_purge_publications_target(
        publications, store, ngx_http_cache_purge_test_bytes(cache), root.zone,
        ngx_http_cache_purge_test_bytes("../outside"), &target, error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
                                        && target.storage == NULL,
                                    "reject C path traversal");
    rc = ngx_http_cache_purge_publications_target(
        publications, store, ngx_http_cache_purge_test_bytes(cache), root.zone,
        ngx_http_cache_purge_test_bytes("final"), &target, error);
    ngx_http_cache_purge_test_check(
        rc == 0 && target.root.len == strlen(cache)
            && target.address.len == strlen(final)
            && memcmp(target.address.data, final, target.address.len) == 0,
        "root config copied; target owns canonical path");
    ngx_http_cache_purge_publication_target_free(&target);
    ngx_http_cache_purge_publication_target_free(&target);
    ngx_http_cache_purge_test_check(target.storage == NULL,
                                    "repeat C target free");

    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_file_locks_open(locks, getuid(), 1, &anchor, error)
            == 0,
        "C publication lock anchor");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_file_address(
            ngx_http_cache_purge_test_bytes(final), digest, &offset, error)
            == 0,
        "C publication address");
    ngx_http_cache_purge_test_check(ngx_http_cache_purge_file_lease_acquire(
                                        locks, anchor, offset, &lease, error)
                                        == 0,
                                    "C publication lease");
    fd = open(temporary, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
    ngx_http_cache_purge_test_check(fd >= 0 && write(fd, "entity", 6) == 6,
                                    "C temporary entity");
    memset(&fill, 0, sizeof(fill));
    fill.file.id[15] = 1;
    fill.file.state = NGX_HTTP_CACHE_PURGE_INDEX_PREPARED;
    fill.file.key = ngx_http_cache_purge_test_bytes("key");
    fill.file.url =
        ngx_http_cache_purge_test_bytes("https://example.test/file");
    fill.file.host = ngx_http_cache_purge_test_bytes("example.test");
    fill.file.tag_namespace = ngx_http_cache_purge_test_bytes("tenant");
    fill.file.root = ngx_http_cache_purge_test_bytes(cache);
    fill.file.zone = root.zone;
    fill.file.path = ngx_http_cache_purge_test_bytes("final");
    fill.file.metadata.len = 1;
    memset(&match, 0xff, sizeof(match));
    rc = ngx_http_cache_purge_publications_prepare(
        publications, store, &fill, ngx_http_cache_purge_test_bytes(temporary),
        anchor, lease, fd, getuid(), &match, error);
    ngx_http_cache_purge_test_check(
        rc == NGX_HTTP_CACHE_ROCKSDB_INVALID && match.revision == 0
            && match.id[15] == 0,
        "C prepare rejects invalid bytes and clears output");
    fill.file.metadata = ngx_http_cache_purge_test_bytes("policy");
    rc = ngx_http_cache_purge_publications_prepare(
        publications, store, &fill, ngx_http_cache_purge_test_bytes(temporary),
        anchor, lease, fd, getuid(), &match, error);
    ngx_http_cache_purge_test_check(
        rc == 0 && match.revision == 1
            && match.state == NGX_HTTP_CACHE_PURGE_INDEX_PREPARED
            && fill.file.metadata.len == 6,
        "C PREPARE keeps borrowed input unchanged");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_file_marker_read(fd, &marker, error) == 0
            && marker.state == NGX_HTTP_CACHE_PURGE_FILE_PREPARED,
        "C durable intent precedes marker");
    ngx_http_cache_purge_test_check(rename(temporary, final) == 0,
                                    "native rename before C FINISH");
    rc = ngx_http_cache_purge_publications_finish(
        publications, store, fill.file.id, 1, anchor, lease, getuid(), &match,
        error);
    ngx_http_cache_purge_test_check(
        rc == 0 && match.state == NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED,
        "C FINISH commits physical identity");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_file_marker_read(fd, &marker, error) == 0
            && marker.state == NGX_HTTP_CACHE_PURGE_FILE_READY,
        "C FINISH durable readable marker");
    rc = ngx_http_cache_purge_publications_target(
        publications, store, fill.file.root, fill.file.zone, fill.file.path,
        &target, error);
    ngx_http_cache_purge_test_check(
        rc == 0 && target.previous[15] == 1,
        "C target returns current physical instance");
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_publications_close(&publications, error) == 0
            && publications == NULL,
        "close C publication roots");
    ngx_http_cache_purge_test_check(
        target.root.len == strlen(cache)
            && memcmp(target.root.data, cache, strlen(cache)) == 0,
        "owned target outlives publication context");
    ngx_http_cache_purge_publication_target_free(&target);
    close(fd);
    close(lease);
    close(anchor);
    ngx_http_cache_purge_test_check(ngx_http_cache_rocksdb_close(&store, error)
                                        == 0,
                                    "close C publication store");
}

static void
ngx_http_cache_purge_test_fence(void)
{
    ngx_http_cache_purge_fence_t        fence, decoded;
    ngx_http_cache_purge_requirement_t  item[64], parsed[64], temporary;
    ngx_http_cache_purge_ack_t          ack, response;
    ngx_http_cache_rocksdb_bytes_t      input;
    ngx_http_cache_rocksdb_error_t     *error;
    unsigned char                      raw[65537], copy[65537], digest[32];
    unsigned char                      reply[NGX_HTTP_CACHE_PURGE_ACK_SIZE + 1];
    char                               ids[64][16];
    size_t                             length, needed, i, bit, roundtrip;
    int                                rc;
    static const unsigned char expected[32] = {
        0xe7, 0xf8, 0xa9, 0xf5, 0x10, 0x27, 0x18, 0x88,
        0x93, 0x8e, 0x9e, 0x77, 0x40, 0x54, 0x4e, 0x23,
        0x1d, 0xb0, 0x2d, 0x51, 0x3a, 0x1c, 0x0d, 0xab,
        0x77, 0x41, 0x67, 0x82, 0x68, 0x04, 0xc6, 0xf5
    };
    static const unsigned char group[] = {
        2, 0, 4, 'b', 'l', 'u', 'e', 0, 3, 'r', 'e', 'd'
    };

    error = &ngx_http_cache_purge_test_error;
    memset(&fence, 0, sizeof(fence));
    memset(item, 0, sizeof(item));
    for (i = 0; i < 16; i++) {
        fence.nonce[i] = (unsigned char) i;
        item[0].ledger[i] = (unsigned char) (i + 32);
    }
    for (i = 0; i < 32; i++) {
        fence.identity[i] = (unsigned char) (i + 1);
    }
    item[0].sequence = UINT64_C(0x0102030405060708);
    item[0].issued_at_ms = UINT64_MAX - 1;
    item[0].mode = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    item[0].id = ngx_http_cache_purge_test_bytes("op1");
    item[0].selector.kind = NGX_HTTP_CACHE_PURGE_URL;
    item[0].selector.value.data = (const unsigned char *) "https://a/x\0y";
    item[0].selector.value.len = 13;
    fence.count = 1;
    fence.requirements = item;
    rc = ngx_http_cache_purge_fence_encode(&fence, NULL, 0, &needed,
                                            error);
    ngx_http_cache_purge_test_check(rc == 0 && needed == 118,
        "propagation sizing matches independent wire layout");
    memset(raw, 0xa5, sizeof(raw));
    rc = ngx_http_cache_purge_fence_encode(&fence, raw, needed - 1, &length,
                                            error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT
                                        && length == 0 && raw[0] == 0xa5,
        "short encode buffer is not partially written");
    rc = ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error);
    ngx_http_cache_purge_test_check(rc == 0 && length == needed,
        "encode binary URL and full-width event time");
    input.data = raw;
    input.len = length;
    rc = ngx_http_cache_purge_fence_digest(input, digest,
                                            error);
    ngx_http_cache_purge_test_check(
        rc == 0 && memcmp(digest, expected, 32) == 0,
        "envelope digest matches independent Python struct/SHA256 vector");
    rc = ngx_http_cache_purge_fence_decode(input, &decoded, parsed, 64,
                                            error);
    ngx_http_cache_purge_test_check(rc == 0 && decoded.count == 1
        && parsed[0].sequence == item[0].sequence
        && parsed[0].issued_at_ms == UINT64_MAX - 1
        && parsed[0].selector.value.len == 13
        && parsed[0].selector.value.data[11] == 0
        && parsed[0].selector.value.data >= raw
        && parsed[0].selector.value.data + 13 <= raw + length,
        "decoder borrows bounded bytes and does not interpret a window");
    for (i = 0; i < length; i++) {
        input.len = i;
        rc = ngx_http_cache_purge_fence_decode(input, &decoded, parsed, 64,
                                            error);
        ngx_http_cache_purge_test_check(rc != 0 && decoded.count == 0
            && decoded.requirements == NULL,
            "every truncated frame leaves no usable partial envelope");
    }
    input.len = length + 1;
    rc = ngx_http_cache_purge_fence_decode(input, &decoded, parsed, 64,
                                            error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID,
        "trailing frame bytes rejected");
    input.len = length;
    rc = ngx_http_cache_purge_fence_decode(input, &decoded, parsed, 0,
                                            error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT
                                        && decoded.count == 0,
        "decode descriptor capacity is enforced");
    for (i = 0; i < length; i++) {
        for (bit = 1; bit <= 128; bit <<= 1) {
            raw[i] ^= (unsigned char) bit;
            rc = ngx_http_cache_purge_fence_decode(input, &decoded, parsed, 64,
                                            error);
            if (rc == 0) {
                rc = ngx_http_cache_purge_fence_encode(&decoded, copy,
                    sizeof(copy), &roundtrip, error);
                ngx_http_cache_purge_test_check(
                    rc == 0 && roundtrip == length
                        && memcmp(raw, copy, length) == 0,
                    "accepted bit mutation retains one canonical encoding");
            } else {
                ngx_http_cache_purge_test_check(decoded.count == 0,
                    "rejected bit mutation exposes no partial request");
            }
            raw[i] ^= (unsigned char) bit;
        }
    }
    memset(&ack, 0, sizeof(ack));
    memcpy(ack.nonce, fence.nonce, 16);
    memcpy(ack.digest, digest, 32);
    ack.outcome = NGX_HTTP_CACHE_PURGE_ACK_FRESH;
    ack.stale_policy = NGX_HTTP_CACHE_PURGE_STALE_STRICT;
    rc = ngx_http_cache_purge_ack_encode(&ack, reply,
                                            error);
    ngx_http_cache_purge_test_check(rc == 0, "encode strict fresh ack");
    input.data = reply;
    input.len = NGX_HTTP_CACHE_PURGE_ACK_SIZE;
    rc = ngx_http_cache_purge_ack_check(input, fence.nonce, digest, &response,
                                            error);
    ngx_http_cache_purge_test_check(rc == 0
        && response.outcome == NGX_HTTP_CACHE_PURGE_ACK_FRESH,
        "matching request-bound ack");
    reply[4] ^= 1;
    rc = ngx_http_cache_purge_ack_check(input, fence.nonce, digest, &response,
                                            error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
                                        && response.outcome == 0,
        "another request nonce cannot satisfy the current request");
    reply[4] ^= 1;
    reply[20] ^= 1;
    rc = ngx_http_cache_purge_ack_check(input, fence.nonce, digest, &response,
                                            error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
        "same nonce with a different requirement set is rejected");
    reply[20] ^= 1;
    for (i = 0; i <= sizeof(reply); i++) {
        if (i == NGX_HTTP_CACHE_PURGE_ACK_SIZE) {
            continue;
        }
        input.len = i;
        rc = ngx_http_cache_purge_ack_check(input, fence.nonce, digest,
            &response, error);
        ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
                                            && response.outcome == 0,
            "truncated or extended ack rejected");
    }
    ack.outcome = NGX_HTTP_CACHE_PURGE_ACK_STALE;
    rc = ngx_http_cache_purge_ack_encode(&ack, reply,
                                            error);
    ngx_http_cache_purge_test_check(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID,
        "strict stale outcome is contradictory");
    ack.stale_policy = NGX_HTTP_CACHE_PURGE_STALE_NATIVE;
    rc = ngx_http_cache_purge_ack_encode(&ack, reply,
                                            error);
    input.len = NGX_HTTP_CACHE_PURGE_ACK_SIZE;
    ngx_http_cache_purge_test_check(rc == 0
        && ngx_http_cache_purge_ack_check(input, fence.nonce, digest, &response,
                                            error) == 0
        && response.outcome == NGX_HTTP_CACHE_PURGE_ACK_STALE,
        "native stale outcome remains explicitly distinguishable");
    reply[54] = 1;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_ack_check(input, fence.nonce, digest, &response,
                                            error)
            == NGX_HTTP_CACHE_ROCKSDB_INVALID,
        "unknown ack flags are not silently accepted");

    item[1] = item[0];
    item[1].sequence++;
    item[1].id = ngx_http_cache_purge_test_bytes("op2");
    item[1].selector.kind = NGX_HTTP_CACHE_PURGE_NAMESPACE_TAGS;
    item[1].selector.value.data = group;
    item[1].selector.value.len = sizeof(group);
    item[1].selector.scope = ngx_http_cache_purge_test_bytes("scope");
    item[1].mode = NGX_HTTP_CACHE_PURGE_DELETE;
    item[1].candidate = 1;
    fence.count = 2;
    rc = ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error);
    input.data = raw;
    input.len = length;
    ngx_http_cache_purge_test_check(rc == 0
        && ngx_http_cache_purge_fence_decode(input, &decoded, parsed, 64,
                                            error) == 0
        && parsed[1].candidate == 1
        && parsed[1].selector.value.len == sizeof(group)
        && memcmp(parsed[1].selector.value.data, group, sizeof(group)) == 0,
        "complete scoped candidate group survives encoding");
    item[1].id = item[0].id;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error)
            == NGX_HTTP_CACHE_ROCKSDB_INVALID,
        "same ledger and ID cannot carry different sequences");
    item[1].ledger[0]++;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error) == 0,
        "independent ledgers may reuse an operation ID");
    temporary = item[0];
    item[0] = item[1];
    item[1] = temporary;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error)
            == NGX_HTTP_CACHE_ROCKSDB_INVALID,
        "unordered ledger set rejected");
    item[0] = temporary;
    item[0].candidate = 1;
    fence.count = 1;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error)
            == NGX_HTTP_CACHE_ROCKSDB_INVALID,
        "candidate flag is reserved for TAG selectors");
    item[0] = temporary;
    for (i = 0; i < 64; i++) {
        item[i] = temporary;
        item[i].sequence = i + 1;
        (void) snprintf(ids[i], sizeof(ids[i]), "op-%u", (unsigned) i);
        item[i].id = ngx_http_cache_purge_test_bytes(ids[i]);
    }
    fence.count = 64;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error) == 0,
        "maximum operation count supported");
    input.data = raw;
    input.len = length;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_fence_decode(input, &decoded, parsed, 64,
                                            error) == 0
        && decoded.count == 64,
        "maximum operation set decodes without dropping members");
    fence.count = 65;
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error)
            == NGX_HTTP_CACHE_ROCKSDB_INVALID,
        "count overflow rejected before reading descriptors");
    fence.count = 2;
    memset(copy, 'a', sizeof(copy));
    item[0].selector.value.data = copy;
    item[0].selector.value.len = 32768;
    item[1].selector.value = item[0].selector.value;
    memset(raw, 0xa5, sizeof(raw));
    ngx_http_cache_purge_test_check(
        ngx_http_cache_purge_fence_encode(&fence, raw, sizeof(raw), &length,
                                            error)
            == NGX_HTTP_CACHE_ROCKSDB_LIMIT
        && length == 0 && raw[0] == 0xa5,
        "aggregate byte overflow does not silently truncate or write");
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
    uint64_t                               writes, revision, sequence;
    size_t  expired;
    int     rc;

    unsigned char raw[1024];
    char          path[] = "/tmp/ngx-cache-ledger-c-XXXXXX";
    const char    binding[] = { 'a', '\0', 'b' };

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
    ngx_http_cache_purge_test_write(store, "M:index", (unsigned char *) "6", 1);
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
    ngx_http_cache_purge_test_control(&ops);
    ngx_http_cache_purge_test_publication();
    ngx_http_cache_purge_test_fence();
    ngx_http_cache_purge_test_check(
        ngx_http_cache_rocksdb_close(&store, &ngx_http_cache_purge_test_error)
            == 0,
        "C calls release all cursors");
    printf("PASS %u pure C ledger/control checks\n",
           ngx_http_cache_purge_test_count);
    return 0;
}
