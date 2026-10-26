/*
 * Copyright (C) Hanada
 */

#include "ngx_http_cache_purge_internal.h"

#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NGX_HTTP_CACHE_PURGE_OP_RECORD  131072
#define NGX_HTTP_CACHE_PURGE_OP_MEMORY  (1024 * 1024)
#define NGX_HTTP_CACHE_PURGE_OP_ROWS    256
#define NGX_HTTP_CACHE_PURGE_OP_PAGE    262144

typedef struct ngx_http_cache_purge_op_block_s  ngx_http_cache_purge_op_block_t;

struct ngx_http_cache_purge_op_block_s {
    ngx_http_cache_purge_op_block_t  *next;
    unsigned char                     data[1];
};

typedef struct {
    ngx_http_cache_rocksdb_bytes_t  bytes;
    int                             exists;
} ngx_http_cache_purge_op_value_t;

typedef struct {
    ngx_http_cache_rocksdb_bytes_t   key;
    ngx_http_cache_purge_op_value_t  before;
    ngx_http_cache_purge_op_value_t  after;
    int                              observed;
    int                              changed;
} ngx_http_cache_purge_op_row_t;

typedef struct {
    ngx_http_cache_rocksdb_t         *store;
    ngx_http_cache_purge_op_block_t  *blocks;
    ngx_http_cache_rocksdb_error_t    error;
    int                               status;
    size_t                            bytes;
    size_t                            count;
    ngx_http_cache_purge_op_row_t     rows[NGX_HTTP_CACHE_PURGE_OP_ROWS];
} ngx_http_cache_purge_op_tx_t;

typedef struct {
    ngx_http_cache_purge_op_tx_t  *tx;
    unsigned char                 *data;
    size_t                         size;
    size_t                         at;
} ngx_http_cache_purge_op_codec_t;

typedef struct {
    ngx_http_cache_purge_operation_t  op;
    size_t                            digest_len;
    unsigned char                     digest[32];
} ngx_http_cache_purge_op_stored_t;

static const ngx_http_cache_rocksdb_bytes_t ngx_http_cache_purge_op_empty = {
    NULL, 0
};

static int
ngx_http_cache_purge_op_fail(ngx_http_cache_purge_op_tx_t *tx, int code,
    const char *message)
{
    if (tx->status == 0) {
        tx->status = code;
        tx->error.code = code;
        snprintf(tx->error.message, sizeof(tx->error.message), "%s", message);
    }
    return tx->status;
}

static int
ngx_http_cache_purge_op_require(ngx_http_cache_purge_op_tx_t *tx, int ok,
    int code, const char *message)
{
    if (!ok) {
        ngx_http_cache_purge_op_fail(tx, code, message);
    }
    return tx->status == 0;
}

static void
ngx_http_cache_purge_op_begin(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_purge_operations_t *ops, void *output)
{
    memset(tx, 0, sizeof(*tx));
    if (ops == NULL || ops->store == NULL || output == NULL) {
        ngx_http_cache_purge_op_fail(tx, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                     "invalid operation handle/output");
        return;
    }
    tx->store = ops->store;
}

static int
ngx_http_cache_purge_op_end(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_block_t *block, *next;

    for (block = tx->blocks; block != NULL; block = next) {
        next = block->next;
        free(block);
    }
    if (error != NULL) {
        *error = tx->error;
        error->code = tx->status;
    }
    return tx->status;
}

static void *
ngx_http_cache_purge_op_alloc(ngx_http_cache_purge_op_tx_t *tx, size_t size)
{
    ngx_http_cache_purge_op_block_t *block;

    if (tx->status != 0) {
        return NULL;
    }
    if (size > NGX_HTTP_CACHE_PURGE_OP_MEMORY - sizeof(*block)
        || tx->bytes > NGX_HTTP_CACHE_PURGE_OP_MEMORY - sizeof(*block) - size)
    {
        ngx_http_cache_purge_op_fail(tx, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                     "operation transaction memory limit");
        return NULL;
    }
    block = malloc(sizeof(*block) + size);
    if (block == NULL) {
        ngx_http_cache_purge_op_fail(tx, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                     "operation allocation failed");
        return NULL;
    }
    block->next = tx->blocks;
    tx->blocks = block;
    tx->bytes += sizeof(*block) + size;
    return block->data;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_op_literal(const char *value)
{
    ngx_http_cache_rocksdb_bytes_t result;

    result.data = (const unsigned char *) value;
    result.len = strlen(value);
    return result;
}

static int
ngx_http_cache_purge_op_equal(ngx_http_cache_rocksdb_bytes_t a,
    ngx_http_cache_rocksdb_bytes_t b)
{
    return a.len == b.len && (a.len == 0 || memcmp(a.data, b.data, a.len) == 0);
}

static int
ngx_http_cache_purge_op_bytes_valid(ngx_http_cache_rocksdb_bytes_t value,
    size_t limit)
{
    return value.len <= limit && (value.len == 0 || value.data != NULL);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_op_copy(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t source)
{
    ngx_http_cache_rocksdb_bytes_t  result;
    unsigned char                  *data;

    data = ngx_http_cache_purge_op_alloc(tx, source.len);
    if (data == NULL) {
        return ngx_http_cache_purge_op_empty;
    }
    if (source.len != 0) {
        memcpy(data, source.data, source.len);
    }
    result.data = data;
    result.len = source.len;
    return result;
}

static void
ngx_http_cache_purge_op_put_number(ngx_http_cache_purge_op_codec_t *codec,
    uint64_t value, unsigned width)
{
    if (codec->tx->status != 0) {
        return;
    }
    if (width > codec->size - codec->at) {
        ngx_http_cache_purge_op_fail(codec->tx, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                     "operation encoding size mismatch");
        return;
    }
    while (width != 0) {
        width--;
        codec->data[codec->at++] = (unsigned char) (value >> (width * 8));
    }
}

static void
ngx_http_cache_purge_op_put_bytes(ngx_http_cache_purge_op_codec_t *codec,
    ngx_http_cache_rocksdb_bytes_t value)
{
    ngx_http_cache_purge_op_put_number(codec, value.len, 4);
    if (codec->tx->status != 0) {
        return;
    }
    if (value.len > codec->size - codec->at) {
        ngx_http_cache_purge_op_fail(codec->tx, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                     "operation encoding size mismatch");
        return;
    }
    if (value.len != 0) {
        memcpy(codec->data + codec->at, value.data, value.len);
    }
    codec->at += value.len;
}

static uint64_t
ngx_http_cache_purge_op_get_number(ngx_http_cache_purge_op_codec_t *codec,
    unsigned width)
{
    uint64_t value;

    value = 0;
    if (codec->tx->status != 0) {
        return 0;
    }
    if (width > codec->size - codec->at) {
        ngx_http_cache_purge_op_fail(codec->tx, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                     "truncated operation record");
        return 0;
    }
    while (width != 0) {
        value = (value << 8) | codec->data[codec->at++];
        width--;
    }
    return value;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_op_get_bytes(ngx_http_cache_purge_op_codec_t *codec,
    size_t limit)
{
    ngx_http_cache_rocksdb_bytes_t  value;
    uint64_t                        size;

    size = ngx_http_cache_purge_op_get_number(codec, 4);
    if (codec->tx->status != 0) {
        return ngx_http_cache_purge_op_empty;
    }
    if (size > limit || size > codec->size - codec->at) {
        ngx_http_cache_purge_op_fail(codec->tx, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                     "invalid operation string length");
        return ngx_http_cache_purge_op_empty;
    }
    value.data = codec->data + codec->at;
    value.len = (size_t) size;
    codec->at += value.len;
    return value;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_op_number(ngx_http_cache_purge_op_tx_t *tx, uint64_t value)
{
    ngx_http_cache_purge_op_codec_t  codec;
    ngx_http_cache_rocksdb_bytes_t   result;

    codec.tx = tx;
    codec.data = ngx_http_cache_purge_op_alloc(tx, 8);
    codec.size = 8;
    codec.at = 0;
    ngx_http_cache_purge_op_put_number(&codec, value, 8);
    result.data = codec.data;
    result.len = tx->status == 0 ? 8 : 0;
    return result;
}

static uint64_t
ngx_http_cache_purge_op_integer(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t value)
{
    ngx_http_cache_purge_op_codec_t codec;

    if (!ngx_http_cache_purge_op_require(tx, value.len == 8,
                                         NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                         "invalid operation integer"))
    {
        return 0;
    }
    codec.tx = tx;
    codec.data = (unsigned char *) value.data;
    codec.size = value.len;
    codec.at = 0;
    return ngx_http_cache_purge_op_get_number(&codec, 8);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_op_key(ngx_http_cache_purge_op_tx_t *tx,
    unsigned char kind, const ngx_http_cache_purge_operation_t *op)
{
    ngx_http_cache_purge_op_codec_t  codec;
    ngx_http_cache_rocksdb_bytes_t   key;

    codec.tx = tx;
    codec.size = kind == 'D' ? 1 + op->input.id.len : kind == 'L' ? 9 : 17;
    codec.data = ngx_http_cache_purge_op_alloc(tx, codec.size);
    codec.at = 0;
    ngx_http_cache_purge_op_put_number(&codec, kind, 1);
    if (tx->status != 0) {
        return ngx_http_cache_purge_op_empty;
    }
    if (kind == 'D') {
        if (op->input.id.len != 0) {
            memcpy(codec.data + 1, op->input.id.data, op->input.id.len);
        }
    } else {
        if (kind != 'L') {
            ngx_http_cache_purge_op_put_number(
                &codec,
                kind == 'Q' ? op->next_run_ms : op->propagation_until_ms, 8);
        }
        ngx_http_cache_purge_op_put_number(&codec, op->sequence, 8);
    }
    key.data = codec.data;
    key.len = codec.size;
    return key;
}

static int
ngx_http_cache_purge_op_id(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t id, int code)
{
    size_t i;

    if (!ngx_http_cache_purge_op_require(
            tx, id.len != 0 && ngx_http_cache_purge_op_bytes_valid(id, 128),
            code, "invalid operation ID length"))
    {
        return 0;
    }
    for (i = 0; i < id.len; i++) {
        if (!ngx_http_cache_purge_op_require(
                tx, id.data[i] > 32 && id.data[i] < 127, code,
                "invalid operation ID byte"))
        {
            return 0;
        }
    }
    return 1;
}

static int
ngx_http_cache_purge_op_validate(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_operation_input_t *input, int stored)
{
    uint32_t  kind;
    int       code, tag;

    code = stored ? NGX_HTTP_CACHE_ROCKSDB_CORRUPT :
                    NGX_HTTP_CACHE_ROCKSDB_INVALID;
    if (!ngx_http_cache_purge_op_require(tx, input != NULL, code,
                                         "missing operation input")
        || !ngx_http_cache_purge_op_id(tx, input->id, code))
    {
        return 0;
    }
    kind = input->selector.kind;
    tag = kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG
          || kind == NGX_HTTP_CACHE_PURGE_HOST_TAG;
    if (!ngx_http_cache_purge_op_require(
            tx,
            input->mode == NGX_HTTP_CACHE_PURGE_INVALIDATE
                || input->mode == NGX_HTTP_CACHE_PURGE_DELETE,
            code, "invalid purge mode")
        || !ngx_http_cache_purge_op_require(
            tx,
            input->issued_at_ms != 0
                && ngx_http_cache_purge_op_bytes_valid(input->binding, 256),
            code, "invalid operation timestamp/binding")
        || !ngx_http_cache_purge_op_require(
            tx, kind <= NGX_HTTP_CACHE_PURGE_HOST_TAG, code,
            "invalid operation selector")
        || !ngx_http_cache_purge_op_require(
            tx,
            (input->selector.value.len == 0
             || input->selector.value.data != NULL)
                && (input->selector.scope.len == 0
                    || input->selector.scope.data != NULL),
            code, "invalid selector bytes")
        || !ngx_http_cache_purge_op_require(
            tx,
            ngx_http_cache_purge_op_bytes_valid(input->selector.value,
                                                tag ? 1024 : 32768)
                && ngx_http_cache_purge_op_bytes_valid(input->selector.scope,
                                                       32768),
            stored ? NGX_HTTP_CACHE_ROCKSDB_CORRUPT :
                     NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "operation selector exceeds limit")
        || !ngx_http_cache_purge_op_require(tx,
                                            tag ?
                                                input->selector.scope.len != 0 :
                                                input->selector.scope.len == 0,
                                            code, "invalid selector scope")
        || !ngx_http_cache_purge_op_require(
            tx,
            input->selector.value.len != 0
                || kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX
                || kind == NGX_HTTP_CACHE_PURGE_URL_PREFIX,
            code, "empty operation selector"))
    {
        return 0;
    }
    return 1;
}

uint64_t
ngx_http_cache_purge_operation_charge(
    const ngx_http_cache_purge_operation_input_t *input)
{
    uint64_t  size;
    size_t    i;
    size_t    lengths[4];

    if (input == NULL) {
        return UINT64_MAX;
    }
    lengths[0] = input->id.len;
    lengths[1] = input->selector.value.len;
    lengths[2] = input->selector.scope.len;
    lengths[3] = input->binding.len;
    size = 256;
    for (i = 0; i < 4; i++) {
        if (lengths[i] > UINT64_MAX - size) {
            return UINT64_MAX;
        }
        size += lengths[i];
    }
    return size;
}

static int
ngx_http_cache_purge_op_scheduled(const ngx_http_cache_purge_operation_t *op)
{
    return op->stage == NGX_HTTP_CACHE_PURGE_ACTIVE
           || op->stage == NGX_HTTP_CACHE_PURGE_DRAINING;
}

static int
ngx_http_cache_purge_op_resident(const ngx_http_cache_purge_operation_t *op)
{
    return op->stage != NGX_HTTP_CACHE_PURGE_MATERIALIZED || op->propagate;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_op_encode(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_op_stored_t *stored)
{
    const ngx_http_cache_purge_operation_t  *op;
    ngx_http_cache_purge_op_codec_t          codec;
    ngx_http_cache_rocksdb_bytes_t           result, digest;

    op = &stored->op;
    codec.tx = tx;
    codec.size = 126 + op->input.id.len + op->input.selector.value.len
                 + op->input.selector.scope.len + op->input.binding.len
                 + op->bookmark.len + op->last_error.len + stored->digest_len;
    if (!ngx_http_cache_purge_op_require(
            tx, codec.size <= NGX_HTTP_CACHE_PURGE_OP_RECORD,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation record exceeds limit"))
    {
        return ngx_http_cache_purge_op_empty;
    }
    codec.data = ngx_http_cache_purge_op_alloc(tx, codec.size);
    codec.at = 0;
    ngx_http_cache_purge_op_put_number(&codec, 1, 1);
    ngx_http_cache_purge_op_put_bytes(&codec, op->input.id);
    ngx_http_cache_purge_op_put_number(&codec, op->input.mode, 1);
    ngx_http_cache_purge_op_put_number(&codec, op->input.issued_at_ms, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->input.selector.kind, 1);
    ngx_http_cache_purge_op_put_bytes(&codec, op->input.selector.value);
    ngx_http_cache_purge_op_put_bytes(&codec, op->input.selector.scope);
    ngx_http_cache_purge_op_put_bytes(&codec, op->input.binding);
    ngx_http_cache_purge_op_put_number(&codec, op->sequence, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->revision, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->accepted_at_ms, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->effective_at_ms, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->propagation_until_ms, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->stage, 1);
    ngx_http_cache_purge_op_put_number(&codec, op->propagation_ms, 4);
    ngx_http_cache_purge_op_put_number(&codec, op->propagate, 1);
    ngx_http_cache_purge_op_put_bytes(&codec, op->bookmark);
    ngx_http_cache_purge_op_put_number(&codec, op->scan_done, 1);
    ngx_http_cache_purge_op_put_number(&codec, op->next_run_ms, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->visits, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->pending, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->failed, 8);
    ngx_http_cache_purge_op_put_number(&codec, op->checkpoint_id, 8);
    ngx_http_cache_purge_op_put_bytes(&codec, op->last_error);
    digest.data = stored->digest;
    digest.len = stored->digest_len;
    ngx_http_cache_purge_op_put_bytes(&codec, digest);
    if (!ngx_http_cache_purge_op_require(tx, codec.at == codec.size,
                                         NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                         "operation encoding size mismatch"))
    {
        return ngx_http_cache_purge_op_empty;
    }
    result.data = codec.data;
    result.len = codec.size;
    return result;
}

static int
ngx_http_cache_purge_op_decode(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t raw,
    ngx_http_cache_purge_op_stored_t *stored)
{
    ngx_http_cache_purge_op_codec_t    codec;
    ngx_http_cache_purge_operation_t  *op;
    ngx_http_cache_rocksdb_bytes_t     digest;

    memset(stored, 0, sizeof(*stored));
    op = &stored->op;
    codec.tx = tx;
    codec.data = (unsigned char *) raw.data;
    codec.size = raw.len;
    codec.at = 0;
    if (!ngx_http_cache_purge_op_require(
            tx, ngx_http_cache_purge_op_get_number(&codec, 1) == 1,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid operation record version"))
    {
        return tx->status;
    }
    op->input.id = ngx_http_cache_purge_op_get_bytes(&codec, 128);
    op->input.mode = ngx_http_cache_purge_op_get_number(&codec, 1);
    op->input.issued_at_ms = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->input.selector.kind = ngx_http_cache_purge_op_get_number(&codec, 1);
    op->input.selector.value = ngx_http_cache_purge_op_get_bytes(&codec, 32768);
    op->input.selector.scope = ngx_http_cache_purge_op_get_bytes(&codec, 32768);
    op->input.binding = ngx_http_cache_purge_op_get_bytes(&codec, 256);
    op->sequence = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->revision = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->accepted_at_ms = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->effective_at_ms = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->propagation_until_ms = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->stage = ngx_http_cache_purge_op_get_number(&codec, 1);
    op->propagation_ms = ngx_http_cache_purge_op_get_number(&codec, 4);
    op->propagate = ngx_http_cache_purge_op_get_number(&codec, 1);
    op->bookmark = ngx_http_cache_purge_op_get_bytes(&codec, 33024);
    op->scan_done = ngx_http_cache_purge_op_get_number(&codec, 1);
    op->next_run_ms = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->visits = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->pending = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->failed = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->checkpoint_id = ngx_http_cache_purge_op_get_number(&codec, 8);
    op->last_error = ngx_http_cache_purge_op_get_bytes(&codec, 512);
    digest = ngx_http_cache_purge_op_get_bytes(&codec, 32);
    if (digest.len != 0) {
        memcpy(stored->digest, digest.data, digest.len);
    }
    stored->digest_len = digest.len;
    if (!ngx_http_cache_purge_op_validate(tx, &op->input, 1)
        || !ngx_http_cache_purge_op_require(
            tx,
            codec.at == codec.size && op->sequence != 0 && op->revision != 0
                && op->accepted_at_ms != 0
                && op->stage >= NGX_HTTP_CACHE_PURGE_PREPARING
                && op->stage <= NGX_HTTP_CACHE_PURGE_MATERIALIZED
                && op->propagate <= 1 && op->scan_done <= 1
                && (op->checkpoint_id ? digest.len == 32 : digest.len == 0),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent operation record")
        || !ngx_http_cache_purge_op_require(
            tx,
            op->stage == NGX_HTTP_CACHE_PURGE_PREPARING ?
                op->effective_at_ms == 0 && !op->propagate :
                op->effective_at_ms != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent operation activation")
        || !ngx_http_cache_purge_op_require(
            tx,
            op->stage == NGX_HTTP_CACHE_PURGE_PREPARING ?
                !op->propagation_until_ms && !op->scan_done
                    && !op->checkpoint_id && !op->next_run_ms :
                op->effective_at_ms <= UINT64_MAX - op->propagation_ms
                    && op->propagation_until_ms
                           == op->effective_at_ms + op->propagation_ms,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "inconsistent operation clock/progress")
        || !ngx_http_cache_purge_op_require(
            tx,
            (!op->propagate || op->propagation_ms != 0)
                && (op->stage != NGX_HTTP_CACHE_PURGE_ACTIVE || !op->scan_done)
                && (op->stage != NGX_HTTP_CACHE_PURGE_DRAINING || op->scan_done)
                && (op->stage != NGX_HTTP_CACHE_PURGE_MATERIALIZED
                    || (op->scan_done && !op->pending && !op->failed)),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent operation progress"))
    {
        return tx->status;
    }
    return 0;
}

static ngx_http_cache_purge_op_value_t
ngx_http_cache_purge_op_read(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t key, ngx_http_cache_rocksdb_cursor_t *view)
{
    ngx_http_cache_purge_op_value_t  value;
    ngx_http_cache_rocksdb_buffer_t  buffer;
    int                              rc;

    memset(&value, 0, sizeof(value));
    buffer.capacity = 512;
    buffer.length = 0;
    buffer.data = ngx_http_cache_purge_op_alloc(tx, buffer.capacity);
    while (tx->status == 0) {
        rc = view != NULL ?
                 ngx_http_cache_rocksdb_cursor_get(
                     view, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, &buffer,
                     &tx->error) :
                 ngx_http_cache_rocksdb_get(tx->store,
                                            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key,
                                            &buffer, &tx->error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            memset(&tx->error, 0, sizeof(tx->error));
            return value;
        }
        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUFFER) {
            if (!ngx_http_cache_purge_op_require(
                    tx,
                    buffer.length > buffer.capacity
                        && buffer.length <= NGX_HTTP_CACHE_PURGE_OP_RECORD,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "oversized operation data"))
            {
                return value;
            }
            buffer.capacity = buffer.length;
            buffer.data = ngx_http_cache_purge_op_alloc(tx, buffer.capacity);
            continue;
        }
        tx->status = rc;
        if (rc == 0) {
            value.exists = 1;
            value.bytes.data = buffer.data;
            value.bytes.len = buffer.length;
        }
        return value;
    }
    return value;
}

static ngx_http_cache_purge_op_row_t *
ngx_http_cache_purge_op_row(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t key)
{
    ngx_http_cache_purge_op_row_t  *row;
    size_t                          i;

    if (tx->status != 0) {
        return NULL;
    }
    for (i = 0; i < tx->count; i++) {
        if (ngx_http_cache_purge_op_equal(tx->rows[i].key, key)) {
            return &tx->rows[i];
        }
    }
    if (!ngx_http_cache_purge_op_require(
            tx, tx->count < NGX_HTTP_CACHE_PURGE_OP_ROWS,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation transaction row limit")) {
        return NULL;
    }
    row = &tx->rows[tx->count++];
    row->key = ngx_http_cache_purge_op_copy(tx, key);
    return tx->status == 0 ? row : NULL;
}

static ngx_http_cache_purge_op_value_t
ngx_http_cache_purge_op_get(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t key)
{
    ngx_http_cache_purge_op_row_t   *row;
    ngx_http_cache_purge_op_value_t  empty;

    memset(&empty, 0, sizeof(empty));
    row = ngx_http_cache_purge_op_row(tx, key);
    if (row == NULL) {
        return empty;
    }
    if (row->changed) {
        return row->after;
    }
    if (!row->observed) {
        row->before = ngx_http_cache_purge_op_read(tx, key, NULL);
        row->observed = 1;
    }
    return row->before;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_op_need(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t key)
{
    ngx_http_cache_purge_op_value_t value;

    value = ngx_http_cache_purge_op_get(tx, key);
    ngx_http_cache_purge_op_require(tx, value.exists,
                                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                    "missing operation metadata");
    return value.bytes;
}

static void
ngx_http_cache_purge_op_put(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t key, ngx_http_cache_rocksdb_bytes_t value,
    int exists)
{
    ngx_http_cache_purge_op_row_t *row;

    row = ngx_http_cache_purge_op_row(tx, key);
    if (row != NULL) {
        row->changed = 1;
        row->after.bytes = value;
        row->after.exists = exists;
    }
}

static uint64_t
ngx_http_cache_purge_op_counter(ngx_http_cache_purge_op_tx_t *tx,
    const char *name)
{
    ngx_http_cache_rocksdb_bytes_t value;

    value =
        ngx_http_cache_purge_op_need(tx, ngx_http_cache_purge_op_literal(name));
    return ngx_http_cache_purge_op_integer(tx, value);
}

static void
ngx_http_cache_purge_op_set_counter(ngx_http_cache_purge_op_tx_t *tx,
    const char *name, uint64_t value)
{
    ngx_http_cache_rocksdb_bytes_t encoded;

    encoded = ngx_http_cache_purge_op_number(tx, value);
    ngx_http_cache_purge_op_put(tx, ngx_http_cache_purge_op_literal(name),
                                encoded, 1);
}

static void
ngx_http_cache_purge_op_commit(ngx_http_cache_purge_op_tx_t *tx)
{
    ngx_http_cache_rocksdb_condition_t  *conditions;
    ngx_http_cache_rocksdb_mutation_t   *mutations;
    ngx_http_cache_purge_op_row_t       *row;
    size_t                               i, nc, nm;

    if (tx->status != 0) {
        return;
    }
    nc = 0;
    nm = 0;
    for (i = 0; i < tx->count; i++) {
        nc += tx->rows[i].observed;
        nm += tx->rows[i].changed;
    }
    if (nm == 0) {
        return;
    }
    conditions = ngx_http_cache_purge_op_alloc(tx, nc * sizeof(*conditions));
    mutations = ngx_http_cache_purge_op_alloc(tx, nm * sizeof(*mutations));
    if (tx->status != 0) {
        return;
    }
    nc = 0;
    nm = 0;
    for (i = 0; i < tx->count; i++) {
        row = &tx->rows[i];
        if (row->observed) {
            conditions[nc].cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
            conditions[nc].key = row->key;
            conditions[nc].value = row->before.bytes;
            conditions[nc++].exists = row->before.exists;
        }
        if (row->changed) {
            mutations[nm].cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
            mutations[nm].key = row->key;
            mutations[nm].type = row->after.exists ?
                                     NGX_HTTP_CACHE_ROCKSDB_PUT :
                                     NGX_HTTP_CACHE_ROCKSDB_DELETE;
            mutations[nm++].value = row->after.bytes;
        }
    }
    tx->status = ngx_http_cache_rocksdb_write(tx->store, mutations, nm,
                                              conditions, nc, &tx->error);
}

static int
ngx_http_cache_purge_op_load(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t id, ngx_http_cache_purge_op_stored_t *stored)
{
    ngx_http_cache_purge_op_value_t    value;
    ngx_http_cache_purge_operation_t  *op;
    ngx_http_cache_rocksdb_bytes_t     link;

    memset(stored, 0, sizeof(*stored));
    op = &stored->op;
    if (!ngx_http_cache_purge_op_id(tx, id, NGX_HTTP_CACHE_ROCKSDB_INVALID)) {
        return tx->status;
    }
    op->input.id = id;
    value = ngx_http_cache_purge_op_get(
        tx, ngx_http_cache_purge_op_key(tx, 'D', op));
    if (!ngx_http_cache_purge_op_require(tx, value.exists,
                                         NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
                                         "operation not found")
        || ngx_http_cache_purge_op_decode(tx, value.bytes, stored) != 0)
    {
        return tx->status;
    }
    ngx_http_cache_purge_op_require(
        tx, ngx_http_cache_purge_op_equal(op->input.id, id),
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "operation ID mismatch");
    if (ngx_http_cache_purge_op_resident(op)) {
        link = ngx_http_cache_purge_op_need(
            tx, ngx_http_cache_purge_op_key(tx, 'L', op));
        ngx_http_cache_purge_op_require(
            tx, ngx_http_cache_purge_op_equal(link, id),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing resident operation link");
    }
    if (ngx_http_cache_purge_op_scheduled(op)) {
        link = ngx_http_cache_purge_op_need(
            tx, ngx_http_cache_purge_op_key(tx, 'Q', op));
        ngx_http_cache_purge_op_require(
            tx, ngx_http_cache_purge_op_equal(link, id),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing scheduled operation link");
    }
    if (op->propagate) {
        link = ngx_http_cache_purge_op_need(
            tx, ngx_http_cache_purge_op_key(tx, 'E', op));
        ngx_http_cache_purge_op_require(
            tx, ngx_http_cache_purge_op_equal(link, id),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing propagation expiry link");
    }
    return tx->status;
}

static void
ngx_http_cache_purge_op_save(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_op_stored_t *before,
    const ngx_http_cache_purge_op_stored_t *after)
{
    const ngx_http_cache_purge_operation_t  *a, *b;
    ngx_http_cache_rocksdb_bytes_t           raw;
    uint64_t                                 count, size, charge;

    a = &before->op;
    b = &after->op;
    if (ngx_http_cache_purge_op_scheduled(a)) {
        ngx_http_cache_purge_op_put(tx, ngx_http_cache_purge_op_key(tx, 'Q', a),
                                    ngx_http_cache_purge_op_empty, 0);
    }
    if (ngx_http_cache_purge_op_scheduled(b)) {
        ngx_http_cache_purge_op_put(tx, ngx_http_cache_purge_op_key(tx, 'Q', b),
                                    b->input.id, 1);
    }
    if (a->propagate && !b->propagate) {
        ngx_http_cache_purge_op_put(tx, ngx_http_cache_purge_op_key(tx, 'E', a),
                                    ngx_http_cache_purge_op_empty, 0);
    }
    if (!a->propagate && b->propagate) {
        ngx_http_cache_purge_op_put(tx, ngx_http_cache_purge_op_key(tx, 'E', b),
                                    b->input.id, 1);
    }
    if (ngx_http_cache_purge_op_resident(a)
        && !ngx_http_cache_purge_op_resident(b))
    {
        count = ngx_http_cache_purge_op_counter(tx, "M:op-live");
        size = ngx_http_cache_purge_op_counter(tx, "M:op-bytes");
        charge = ngx_http_cache_purge_operation_charge(&a->input);
        if (!ngx_http_cache_purge_op_require(tx, count != 0 && size >= charge,
                                             NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                             "operation budget underflow"))
        {
            return;
        }
        ngx_http_cache_purge_op_set_counter(tx, "M:op-live", count - 1);
        ngx_http_cache_purge_op_set_counter(tx, "M:op-bytes", size - charge);
        ngx_http_cache_purge_op_put(tx, ngx_http_cache_purge_op_key(tx, 'L', a),
                                    ngx_http_cache_purge_op_empty, 0);
    }
    raw = ngx_http_cache_purge_op_encode(tx, after);
    ngx_http_cache_purge_op_put(tx, ngx_http_cache_purge_op_key(tx, 'D', b),
                                raw, 1);
}

static void
ngx_http_cache_purge_op_clone(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_operation_t *input,
    ngx_http_cache_purge_operation_t *output)
{
    ngx_http_cache_rocksdb_bytes_t  *fields[6];
    unsigned char                   *data;
    size_t                           i, size;

    if (tx->status != 0) {
        return;
    }
    *output = *input;
    fields[0] = &output->input.id;
    fields[1] = &output->input.selector.value;
    fields[2] = &output->input.selector.scope;
    fields[3] = &output->input.binding;
    fields[4] = &output->bookmark;
    fields[5] = &output->last_error;
    size = 0;
    for (i = 0; i < 6; i++) {
        size += fields[i]->len;
    }
    data = malloc(size != 0 ? size : 1);
    if (data == NULL) {
        memset(output, 0, sizeof(*output));
        ngx_http_cache_purge_op_fail(tx, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                     "operation result allocation failed");
        return;
    }
    output->storage = data;
    for (i = 0; i < 6; i++) {
        if (fields[i]->len != 0) {
            memcpy(data, fields[i]->data, fields[i]->len);
        }
        fields[i]->data = data;
        data += fields[i]->len;
    }
}

void
ngx_http_cache_purge_operation_free(ngx_http_cache_purge_operation_t *op)
{
    if (op != NULL) {
        free(op->storage);
        memset(op, 0, sizeof(*op));
    }
}

void
ngx_http_cache_purge_operation_page_free(
    ngx_http_cache_purge_operation_page_t *page)
{
    size_t i;

    if (page != NULL) {
        for (i = 0; i < page->count; i++) {
            ngx_http_cache_purge_operation_free(&page->items[i]);
        }
        free(page->items);
        memset(page, 0, sizeof(*page));
    }
}

static int
ngx_http_cache_purge_op_result(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (tx->status != 0) {
        ngx_http_cache_purge_operation_free(out);
    }
    return ngx_http_cache_purge_op_end(tx, error);
}

typedef struct {
    ngx_http_cache_rocksdb_cursor_t  *handle;
    ngx_http_cache_rocksdb_bytes_t    key;
    ngx_http_cache_rocksdb_bytes_t    value;
    unsigned char                     keys[256];
    unsigned char                     values[256];
} ngx_http_cache_purge_op_cursor_t;

static void
ngx_http_cache_purge_op_cursor_open(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_purge_op_cursor_t *cursor, const char *prefix,
    ngx_http_cache_rocksdb_bytes_t after)
{
    memset(cursor, 0, sizeof(*cursor));
    if (tx->status == 0) {
        tx->status = ngx_http_cache_rocksdb_cursor_open(
            tx->store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_op_literal(prefix), after, &cursor->handle,
            &tx->error);
    }
}

static void
ngx_http_cache_purge_op_cursor_close(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_purge_op_cursor_t *cursor)
{
    ngx_http_cache_rocksdb_error_t  error;
    int                             rc;

    if (cursor->handle != NULL) {
        rc = ngx_http_cache_rocksdb_cursor_close(&cursor->handle, &error);
        if (rc != 0) {
            ngx_http_cache_purge_op_fail(tx, rc, error.message);
        }
    }
}

static int
ngx_http_cache_purge_op_cursor_next(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_purge_op_cursor_t *cursor)
{
    ngx_http_cache_rocksdb_buffer_t  key, value;
    int                              rc;

    if (tx->status != 0) {
        return 0;
    }
    key.data = cursor->keys;
    key.capacity = sizeof(cursor->keys);
    key.length = 0;
    value.data = cursor->values;
    value.capacity = sizeof(cursor->values);
    value.length = 0;
    rc = ngx_http_cache_rocksdb_cursor_next(cursor->handle, &key, &value,
                                            &tx->error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
        memset(&tx->error, 0, sizeof(tx->error));
        return 0;
    }
    if (rc == NGX_HTTP_CACHE_ROCKSDB_BUFFER) {
        ngx_http_cache_purge_op_fail(tx, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                     "oversized operation queue row");
        return 0;
    }
    tx->status = rc;
    cursor->key.data = key.data;
    cursor->key.len = key.length;
    cursor->value.data = value.data;
    cursor->value.len = value.length;
    return rc == 0;
}

int
ngx_http_cache_purge_operations_init(ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_operation_limits_t *limits,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operations_t  candidate;
    ngx_http_cache_purge_op_tx_t       tx;
    ngx_http_cache_purge_op_cursor_t   cursor;
    ngx_http_cache_purge_op_value_t    schema, value;
    ngx_http_cache_rocksdb_bytes_t     index;
    size_t                             i;
    const char                        *prefixes[] = { "D", "L", "Q", "E" };
    const char                        *keys[] =
        { "M:op-next", "M:op-live", "M:op-bytes" };

    candidate.store = store;
    candidate.limits.live_records = 4096;
    candidate.limits.live_selector_bytes = 4 * 1024 * 1024;
    if (limits != NULL) {
        candidate.limits = *limits;
    }
    if (ops != NULL) {
        memset(ops, 0, sizeof(*ops));
    }
    ngx_http_cache_purge_op_begin(&tx, &candidate, ops);
    if (!ngx_http_cache_purge_op_require(
            &tx,
            candidate.limits.live_records != 0
                && candidate.limits.live_records <= 65536
                && candidate.limits.live_selector_bytes >= 1024
                && candidate.limits.live_selector_bytes <= 64 * 1024 * 1024,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation capacity"))
    {
        return ngx_http_cache_purge_op_end(&tx, error);
    }
    index = ngx_http_cache_purge_op_need(
        &tx, ngx_http_cache_purge_op_literal("M:index"));
    ngx_http_cache_purge_op_require(
        &tx,
        ngx_http_cache_purge_op_equal(index,
                                      ngx_http_cache_purge_op_literal("2")),
        NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
        "operation ledger requires supported file index");
    schema = ngx_http_cache_purge_op_get(
        &tx, ngx_http_cache_purge_op_literal("M:operations"));
    if (!schema.exists && tx.status == 0) {
        for (i = 0; i < 4 && tx.status == 0; i++) {
            ngx_http_cache_purge_op_cursor_open(&tx, &cursor, prefixes[i],
                                                ngx_http_cache_purge_op_empty);
            ngx_http_cache_purge_op_require(
                &tx, !ngx_http_cache_purge_op_cursor_next(&tx, &cursor),
                NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unversioned operation ledger");
            ngx_http_cache_purge_op_cursor_close(&tx, &cursor);
        }
        for (i = 0; i < 3 && tx.status == 0; i++) {
            value = ngx_http_cache_purge_op_get(
                &tx, ngx_http_cache_purge_op_literal(keys[i]));
            ngx_http_cache_purge_op_require(&tx, !value.exists,
                                            NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
                                            "unversioned operation metadata");
        }
        ngx_http_cache_purge_op_put(
            &tx, ngx_http_cache_purge_op_literal("M:operations"),
            ngx_http_cache_purge_op_literal("1"), 1);
        ngx_http_cache_purge_op_set_counter(&tx, "M:op-next", 1);
        ngx_http_cache_purge_op_set_counter(&tx, "M:op-live", 0);
        ngx_http_cache_purge_op_set_counter(&tx, "M:op-bytes", 0);
        ngx_http_cache_purge_op_commit(&tx);

    } else if (tx.status == 0) {
        ngx_http_cache_purge_op_require(
            &tx,
            ngx_http_cache_purge_op_equal(schema.bytes,
                                          ngx_http_cache_purge_op_literal("1")),
            NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
            "unsupported operation ledger schema");
        ngx_http_cache_purge_op_require(
            &tx, ngx_http_cache_purge_op_counter(&tx, "M:op-next") != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "invalid operation sequence allocator");
        (void) ngx_http_cache_purge_op_counter(&tx, "M:op-live");
        (void) ngx_http_cache_purge_op_counter(&tx, "M:op-bytes");
    }
    if (tx.status == 0) {
        *ops = candidate;
    }
    return ngx_http_cache_purge_op_end(&tx, error);
}

int
ngx_http_cache_purge_operations_get(ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_rocksdb_bytes_t id, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_stored_t  stored;

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (ngx_http_cache_purge_op_load(&tx, id, &stored) == 0) {
        ngx_http_cache_purge_op_clone(&tx, &stored.op, out);
    }
    return ngx_http_cache_purge_op_result(&tx, out, error);
}

int
ngx_http_cache_purge_operations_usage(ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_purge_operation_usage_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t            tx;
    ngx_http_cache_purge_operation_usage_t  usage;
    uint64_t                                next;

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    next = ngx_http_cache_purge_op_counter(&tx, "M:op-next");
    ngx_http_cache_purge_op_require(&tx, next != 0,
                                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                    "invalid operation sequence allocator");
    usage.last_sequence = next - 1;
    usage.live_records = ngx_http_cache_purge_op_counter(&tx, "M:op-live");
    usage.live_selector_bytes =
        ngx_http_cache_purge_op_counter(&tx, "M:op-bytes");
    if (tx.status == 0) {
        *out = usage;
    }
    return ngx_http_cache_purge_op_end(&tx, error);
}

static int
ngx_http_cache_purge_op_same_input(
    const ngx_http_cache_purge_operation_input_t *a,
    const ngx_http_cache_purge_operation_input_t *b)
{
    return a->mode == b->mode && a->issued_at_ms == b->issued_at_ms
           && a->selector.kind == b->selector.kind
           && ngx_http_cache_purge_op_equal(a->id, b->id)
           && ngx_http_cache_purge_op_equal(a->selector.value,
                                            b->selector.value)
           && ngx_http_cache_purge_op_equal(a->selector.scope,
                                            b->selector.scope)
           && ngx_http_cache_purge_op_equal(a->binding, b->binding);
}

static void
ngx_http_cache_purge_op_preview(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms,
    const ngx_http_cache_purge_operation_limits_t *limits,
    ngx_http_cache_purge_op_stored_t *stored, int *replay)
{
    ngx_http_cache_purge_op_value_t  old;
    uint64_t                         count, size, charge, sequence;

    memset(stored, 0, sizeof(*stored));
    *replay = 0;
    if (!ngx_http_cache_purge_op_validate(tx, input, 0)
        || !ngx_http_cache_purge_op_require(tx, now != 0,
                                            NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                            "invalid operation clock"))
    {
        return;
    }
    stored->op.input = *input;
    old = ngx_http_cache_purge_op_get(
        tx, ngx_http_cache_purge_op_key(tx, 'D', &stored->op));
    if (old.exists) {
        ngx_http_cache_purge_op_load(tx, input->id, stored);
        ngx_http_cache_purge_op_require(
            tx, ngx_http_cache_purge_op_same_input(&stored->op.input, input),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "operation ID reused with different input");
        *replay = 1;
        return;
    }
    count = ngx_http_cache_purge_op_counter(tx, "M:op-live");
    size = ngx_http_cache_purge_op_counter(tx, "M:op-bytes");
    charge = ngx_http_cache_purge_operation_charge(input);
    if (!ngx_http_cache_purge_op_require(
            tx,
            count < limits->live_records && size <= limits->live_selector_bytes
                && charge <= limits->live_selector_bytes - size,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "resident operation capacity exhausted"))
    {
        return;
    }
    sequence = ngx_http_cache_purge_op_counter(tx, "M:op-next");
    if (!ngx_http_cache_purge_op_require(
            tx, sequence != 0 && sequence != UINT64_MAX,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation sequence exhausted"))
    {
        return;
    }
    stored->op.sequence = sequence;
    stored->op.revision = 1;
    stored->op.stage = NGX_HTTP_CACHE_PURGE_PREPARING;
    stored->op.accepted_at_ms = now;
    stored->op.propagation_ms = propagation_ms;
}

static int
ngx_http_cache_purge_op_accept(ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, int commit, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_stored_t  stored;
    ngx_http_cache_rocksdb_bytes_t    raw;
    uint64_t                          count, size;
    int                               replay;

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (tx.status == 0) {
        ngx_http_cache_purge_op_preview(&tx, input, now, propagation_ms,
                                        &ops->limits, &stored, &replay);
    }
    if (tx.status == 0) {
        ngx_http_cache_purge_op_clone(&tx, &stored.op, out);
        if (commit && !replay) {
            count = ngx_http_cache_purge_op_counter(&tx, "M:op-live");
            size = ngx_http_cache_purge_op_counter(&tx, "M:op-bytes");
            ngx_http_cache_purge_op_set_counter(&tx, "M:op-next",
                                                stored.op.sequence + 1);
            ngx_http_cache_purge_op_set_counter(&tx, "M:op-live", count + 1);
            ngx_http_cache_purge_op_set_counter(
                &tx, "M:op-bytes",
                size + ngx_http_cache_purge_operation_charge(input));
            raw = ngx_http_cache_purge_op_encode(&tx, &stored);
            ngx_http_cache_purge_op_put(
                &tx, ngx_http_cache_purge_op_key(&tx, 'D', &stored.op), raw, 1);
            ngx_http_cache_purge_op_put(
                &tx, ngx_http_cache_purge_op_key(&tx, 'L', &stored.op),
                input->id, 1);
            ngx_http_cache_purge_op_commit(&tx);
        }
    }
    return ngx_http_cache_purge_op_result(&tx, out, error);
}

int
ngx_http_cache_purge_operations_preview(ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_op_accept(ops, input, now, propagation_ms, 0,
                                          out, error);
}

int
ngx_http_cache_purge_operations_accept(ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_op_accept(ops, input, now, propagation_ms, 1,
                                          out, error);
}

static int
ngx_http_cache_purge_op_revision(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_operation_t *op, uint64_t expected)
{
    return ngx_http_cache_purge_op_require(tx, op->revision == expected,
                                           NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                           "operation revision changed");
}

static void
ngx_http_cache_purge_op_advance(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_purge_operation_t *op)
{
    if (ngx_http_cache_purge_op_require(tx, op->revision != UINT64_MAX,
                                        NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                        "operation revision exhausted"))
    {
        op->revision++;
    }
}

static int
ngx_http_cache_purge_op_update(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_op_stored_t *before,
    ngx_http_cache_purge_op_stored_t *after,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_advance(tx, &after->op);
    ngx_http_cache_purge_op_clone(tx, &after->op, out);
    ngx_http_cache_purge_op_save(tx, before, after);
    ngx_http_cache_purge_op_commit(tx);
    return ngx_http_cache_purge_op_result(tx, out, error);
}

int
ngx_http_cache_purge_operations_activate(ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_rocksdb_bytes_t id, uint64_t revision, uint64_t now,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_stored_t  before, after;

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (!ngx_http_cache_purge_op_require(&tx, now != 0,
                                         NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                         "invalid activation clock")
        || ngx_http_cache_purge_op_load(&tx, id, &before) != 0
        || !ngx_http_cache_purge_op_revision(&tx, &before.op, revision))
    {
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    if (before.op.stage != NGX_HTTP_CACHE_PURGE_PREPARING) {
        ngx_http_cache_purge_op_clone(&tx, &before.op, out);
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    if (!ngx_http_cache_purge_op_require(
            &tx, now <= UINT64_MAX - before.op.propagation_ms,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "propagation deadline overflow"))
    {
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    after = before;
    after.op.stage = NGX_HTTP_CACHE_PURGE_ACTIVE;
    after.op.effective_at_ms = now;
    after.op.propagation_until_ms = now + after.op.propagation_ms;
    after.op.propagate = after.op.propagation_ms != 0;
    after.op.next_run_ms = now;
    return ngx_http_cache_purge_op_update(&tx, &before, &after, out, error);
}

static void
ngx_http_cache_purge_op_digest(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_checkpoint_t *point, unsigned char digest[32])
{
    ngx_http_cache_purge_op_codec_t codec;

    codec.tx = tx;
    codec.size = 49 + point->bookmark.len + point->last_error.len;
    codec.data = ngx_http_cache_purge_op_alloc(tx, codec.size);
    codec.at = 0;
    ngx_http_cache_purge_op_put_number(&codec, point->id, 8);
    ngx_http_cache_purge_op_put_bytes(&codec, point->bookmark);
    ngx_http_cache_purge_op_put_number(&codec, point->scan_done, 1);
    ngx_http_cache_purge_op_put_number(&codec, point->next_run_ms, 8);
    ngx_http_cache_purge_op_put_number(&codec, point->visits_delta, 8);
    ngx_http_cache_purge_op_put_number(&codec, point->pending, 8);
    ngx_http_cache_purge_op_put_number(&codec, point->failed, 8);
    ngx_http_cache_purge_op_put_bytes(&codec, point->last_error);
    if (tx->status == 0) {
        ngx_http_cache_purge_op_require(
            tx,
            codec.at == codec.size
                && SHA256(codec.data, codec.size, digest) != NULL,
            NGX_HTTP_CACHE_ROCKSDB_IO, "operation digest failed");
    }
}

int
ngx_http_cache_purge_operations_checkpoint(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, const ngx_http_cache_purge_checkpoint_t *point,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_stored_t  before, after;
    unsigned char                     digest[32];

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (!ngx_http_cache_purge_op_require(
            &tx,
            point != NULL && point->id != 0
                && ngx_http_cache_purge_op_bytes_valid(point->bookmark, 33024)
                && ngx_http_cache_purge_op_bytes_valid(point->last_error, 512)
                && (point->scan_done == 0 || point->scan_done == 1),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation checkpoint")
        || ngx_http_cache_purge_op_load(&tx, id, &before) != 0)
    {
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    ngx_http_cache_purge_op_digest(&tx, point, digest);
    if (tx.status != 0) {
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    if (point->id == before.op.checkpoint_id) {
        ngx_http_cache_purge_op_require(
            &tx,
            before.digest_len == 32 && memcmp(before.digest, digest, 32) == 0,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "checkpoint ID reused with different progress");
        ngx_http_cache_purge_op_clone(&tx, &before.op, out);
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    if (!ngx_http_cache_purge_op_revision(&tx, &before.op, revision)
        || !ngx_http_cache_purge_op_require(
            &tx,
            ngx_http_cache_purge_op_scheduled(&before.op)
                && point->id > before.op.checkpoint_id,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "operation cannot accept this checkpoint")
        || !ngx_http_cache_purge_op_require(
            &tx, !before.op.scan_done || point->scan_done,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "completed enumeration cannot regress")
        || !ngx_http_cache_purge_op_require(
            &tx, point->visits_delta <= UINT64_MAX - before.op.visits,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation visit counter overflow"))
    {
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    after = before;
    after.op.bookmark = point->bookmark;
    after.op.scan_done = point->scan_done;
    after.op.next_run_ms = point->next_run_ms;
    after.op.visits += point->visits_delta;
    after.op.pending = point->pending;
    after.op.failed = point->failed;
    after.op.last_error = point->last_error;
    after.op.checkpoint_id = point->id;
    memcpy(after.digest, digest, 32);
    after.digest_len = 32;
    if (after.op.scan_done) {
        after.op.stage = NGX_HTTP_CACHE_PURGE_DRAINING;
    }
    return ngx_http_cache_purge_op_update(&tx, &before, &after, out, error);
}

int
ngx_http_cache_purge_operations_materialize(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, uint64_t quiescent_sequence,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_stored_t  before, after;

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (ngx_http_cache_purge_op_load(&tx, id, &before) != 0
        || !ngx_http_cache_purge_op_revision(&tx, &before.op, revision))
    {
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    if (before.op.stage == NGX_HTTP_CACHE_PURGE_MATERIALIZED) {
        ngx_http_cache_purge_op_clone(&tx, &before.op, out);
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    if (!ngx_http_cache_purge_op_require(
            &tx,
            before.op.stage == NGX_HTTP_CACHE_PURGE_DRAINING
                && before.op.scan_done && !before.op.pending
                && !before.op.failed
                && quiescent_sequence >= before.op.sequence,
            NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "operation still has files/readers/writers to drain"))
    {
        return ngx_http_cache_purge_op_result(&tx, out, error);
    }
    after = before;
    after.op.stage = NGX_HTTP_CACHE_PURGE_MATERIALIZED;
    return ngx_http_cache_purge_op_update(&tx, &before, &after, out, error);
}

int
ngx_http_cache_purge_operations_page(ngx_http_cache_purge_operations_t *ops,
    int due, uint64_t now, ngx_http_cache_rocksdb_bytes_t after, size_t limit,
    ngx_http_cache_purge_operation_page_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_cursor_t  cursor;
    ngx_http_cache_purge_op_stored_t  stored;
    ngx_http_cache_purge_op_value_t   raw;
    ngx_http_cache_rocksdb_bytes_t    clock, expected;
    size_t                            remaining, key_size;

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    key_size = due ? 17 : 9;
    if (!ngx_http_cache_purge_op_require(
            &tx, (due == 0 || due == 1) && limit != 0 && limit <= 32,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation page size")
        || !ngx_http_cache_purge_op_require(
            &tx,
            ngx_http_cache_purge_op_bytes_valid(after, key_size)
                && (after.len == 0
                    || (after.len == key_size
                        && after.data[0] == (due ? 'Q' : 'L'))),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation queue bookmark"))
    {
        return ngx_http_cache_purge_op_end(&tx, error);
    }
    out->items = calloc(limit, sizeof(*out->items));
    if (!ngx_http_cache_purge_op_require(&tx, out->items != NULL,
                                         NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                         "operation page allocation failed"))
    {
        return ngx_http_cache_purge_op_end(&tx, error);
    }
    if (after.len != 0) {
        memcpy(out->after, after.data, after.len);
    }
    out->after_len = after.len;
    remaining = NGX_HTTP_CACHE_PURGE_OP_PAGE;
    ngx_http_cache_purge_op_cursor_open(&tx, &cursor, due ? "Q" : "L", after);
    while (out->count < limit && tx.status == 0) {
        if (!ngx_http_cache_purge_op_cursor_next(&tx, &cursor)) {
            out->done = 1;
            break;
        }
        if (!ngx_http_cache_purge_op_require(&tx, cursor.key.len == key_size,
                                             NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                             "invalid operation queue key"))
        {
            break;
        }
        clock.data = cursor.key.data + 1;
        clock.len = 8;
        if (due && ngx_http_cache_purge_op_integer(&tx, clock) > now) {
            out->done = 1;
            break;
        }
        memset(&stored, 0, sizeof(stored));
        stored.op.input.id = cursor.value;
        raw = ngx_http_cache_purge_op_read(
            &tx, ngx_http_cache_purge_op_key(&tx, 'D', &stored.op),
            cursor.handle);
        if (!ngx_http_cache_purge_op_require(&tx, raw.exists,
                                             NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                             "queue has no operation"))
        {
            break;
        }
        if (raw.bytes.len > remaining && out->count != 0) {
            break;
        }
        remaining -= raw.bytes.len;
        if (ngx_http_cache_purge_op_decode(&tx, raw.bytes, &stored) != 0) {
            break;
        }
        expected =
            ngx_http_cache_purge_op_key(&tx, due ? 'Q' : 'L', &stored.op);
        if (!ngx_http_cache_purge_op_require(
                &tx,
                ngx_http_cache_purge_op_equal(stored.op.input.id, cursor.value)
                    && ngx_http_cache_purge_op_equal(expected, cursor.key)
                    && (due ? ngx_http_cache_purge_op_scheduled(&stored.op) :
                              ngx_http_cache_purge_op_resident(&stored.op)),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "queue operation mismatch"))
        {
            break;
        }
        ngx_http_cache_purge_op_clone(&tx, &stored.op, &out->items[out->count]);
        if (tx.status != 0) {
            break;
        }
        out->count++;
        memcpy(out->after, cursor.key.data, cursor.key.len);
        out->after_len = cursor.key.len;
    }
    ngx_http_cache_purge_op_cursor_close(&tx, &cursor);
    if (tx.status != 0) {
        ngx_http_cache_purge_operation_page_free(out);
    }
    return ngx_http_cache_purge_op_end(&tx, error);
}

int
ngx_http_cache_purge_operations_expire(ngx_http_cache_purge_operations_t *ops,
    uint64_t now, size_t limit, size_t *expired,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_cursor_t  cursor;
    ngx_http_cache_purge_op_stored_t  before, after;
    ngx_http_cache_rocksdb_bytes_t    clock, encoded, expected;
    size_t                            count, remaining, cost;

    ngx_http_cache_purge_op_begin(&tx, ops, expired);
    if (expired != NULL) {
        *expired = 0;
    }
    if (!ngx_http_cache_purge_op_require(&tx, limit != 0 && limit <= 32,
                                         NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                         "invalid expiration batch size"))
    {
        return ngx_http_cache_purge_op_end(&tx, error);
    }
    count = 0;
    remaining = NGX_HTTP_CACHE_PURGE_OP_PAGE;
    ngx_http_cache_purge_op_cursor_open(&tx, &cursor, "E",
                                        ngx_http_cache_purge_op_empty);
    while (count < limit && ngx_http_cache_purge_op_cursor_next(&tx, &cursor)) {
        if (!ngx_http_cache_purge_op_require(&tx, cursor.key.len == 17,
                                             NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                             "invalid propagation expiry key"))
        {
            break;
        }
        clock.data = cursor.key.data + 1;
        clock.len = 8;
        if (ngx_http_cache_purge_op_integer(&tx, clock) > now) {
            break;
        }
        if (ngx_http_cache_purge_op_load(&tx, cursor.value, &before) != 0) {
            break;
        }
        encoded = ngx_http_cache_purge_op_encode(&tx, &before);
        cost = encoded.len * 2 + 512;
        if (cost > remaining && count != 0) {
            break;
        }
        if (!ngx_http_cache_purge_op_require(
                &tx, cost <= remaining, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "operation expiration batch budget"))
        {
            break;
        }
        remaining -= cost;
        expected = ngx_http_cache_purge_op_key(&tx, 'E', &before.op);
        if (!ngx_http_cache_purge_op_require(
                &tx,
                before.op.propagate
                    && ngx_http_cache_purge_op_equal(cursor.key, expected),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "stale propagation expiry index"))
        {
            break;
        }
        after = before;
        after.op.propagate = 0;
        ngx_http_cache_purge_op_advance(&tx, &after.op);
        ngx_http_cache_purge_op_save(&tx, &before, &after);
        count++;
    }
    ngx_http_cache_purge_op_cursor_close(&tx, &cursor);
    ngx_http_cache_purge_op_commit(&tx);
    if (tx.status == 0) {
        *expired = count;
    }
    return ngx_http_cache_purge_op_end(&tx, error);
}


/* Bounded snapshot queries over the file index and its ordered postings. */

#define NGX_HTTP_CACHE_PURGE_QUERY_MEMORY  (1024 * 1024)
#define NGX_HTTP_CACHE_PURGE_INDEX_STRING  32768
#define NGX_HTTP_CACHE_PURGE_INDEX_RECORD  524288

typedef struct {
    unsigned char  *data;
    size_t          len;
    size_t          capacity;
} ngx_http_cache_purge_index_buffer_t;

typedef struct {
    ngx_http_cache_rocksdb_cursor_t     *handle;
    ngx_http_cache_purge_index_buffer_t  key;
    ngx_http_cache_purge_index_buffer_t  value;
} ngx_http_cache_purge_index_cursor_t;

typedef struct {
    ngx_http_cache_purge_query_t   *query;
    ngx_http_cache_rocksdb_bytes_t  data;
    size_t                          at;
} ngx_http_cache_purge_index_reader_t;

typedef struct {
    unsigned char                   id[16];
    unsigned char                   previous[16];
    uint32_t                        state;
    uint64_t                        revision;
    uint64_t                        refs[8];
    ngx_http_cache_rocksdb_bytes_t  path;
    ngx_http_cache_rocksdb_bytes_t  metadata;
    ngx_http_cache_rocksdb_bytes_t  digest;
    uint64_t                        tags[64];
    size_t                          tag_count;
} ngx_http_cache_purge_index_record_t;

struct ngx_http_cache_purge_query_s {
    ngx_http_cache_rocksdb_t       *store;
    ngx_http_cache_rocksdb_error_t  error;
    int                             status;
    size_t                          bytes;
    unsigned                        kind;
    int                             empty;
    unsigned char                   recovery;
    unsigned char                   signature[32];
    ngx_http_cache_rocksdb_cf_e         outer_cf;
    ngx_http_cache_purge_index_cursor_t  outer;
    ngx_http_cache_purge_index_cursor_t  links;
    ngx_http_cache_purge_index_cursor_t  files;
    ngx_http_cache_purge_index_buffer_t  prefix;
    ngx_http_cache_purge_index_buffer_t  current_outer;
    ngx_http_cache_purge_index_buffer_t  current_link;
    ngx_http_cache_purge_index_buffer_t  token;
    ngx_http_cache_purge_index_buffer_t  scratch;
    ngx_http_cache_purge_index_buffer_t  lookup;
    ngx_http_cache_purge_index_buffer_t  key;
};

static int
ngx_http_cache_purge_index_fail(ngx_http_cache_purge_query_t *q, int code,
    const char *message)
{
    if (q->status == 0) {
        q->status = code;
        q->error.code = code;
        snprintf(q->error.message, sizeof(q->error.message), "%s", message);
    }
    return q->status;
}

static int
ngx_http_cache_purge_index_require(ngx_http_cache_purge_query_t *q, int ok,
    int code, const char *message)
{
    if (!ok) {
        ngx_http_cache_purge_index_fail(q, code, message);
    }
    return q->status == 0;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_bytes(const ngx_http_cache_purge_index_buffer_t *b)
{
    ngx_http_cache_rocksdb_bytes_t result;

    result.data = b->data;
    result.len = b->len;
    return result;
}

static int
ngx_http_cache_purge_index_reserve(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_buffer_t *b, size_t size)
{
    unsigned char  *data;
    size_t          capacity;

    if (q->status != 0) {
        return 0;
    }
    if (size <= b->capacity) {
        return 1;
    }
    capacity = b->capacity != 0 ? b->capacity : 256;
    if (!ngx_http_cache_purge_index_require(
            q, size <= NGX_HTTP_CACHE_PURGE_INDEX_RECORD,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "index buffer exceeds record limit"))
    {
        return 0;
    }
    while (capacity < size) {
        capacity *= 2;
    }
    if (!ngx_http_cache_purge_index_require(
            q,
            capacity
                <= NGX_HTTP_CACHE_PURGE_QUERY_MEMORY - q->bytes + b->capacity,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "index query memory budget"))
    {
        return 0;
    }
    data = realloc(b->data, capacity);
    if (!ngx_http_cache_purge_index_require(q, data != NULL,
                                            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                            "index query allocation failed"))
    {
        return 0;
    }
    q->bytes += capacity - b->capacity;
    b->data = data;
    b->capacity = capacity;
    return 1;
}

static void
ngx_http_cache_purge_index_set(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_buffer_t *b,
    ngx_http_cache_rocksdb_bytes_t value)
{
    if (ngx_http_cache_purge_index_reserve(q, b, value.len)) {
        if (value.len != 0) {
            memcpy(b->data, value.data, value.len);
        }
        b->len = value.len;
    }
}

static void
ngx_http_cache_purge_index_append(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_buffer_t *b, const void *data, size_t size)
{
    if (!ngx_http_cache_purge_index_require(
            q, size <= NGX_HTTP_CACHE_PURGE_INDEX_RECORD - b->len,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "index encoding exceeds limit"))
    {
        return;
    }
    if (ngx_http_cache_purge_index_reserve(q, b, b->len + size)) {
        if (size != 0) {
            memcpy(b->data + b->len, data, size);
        }
        b->len += size;
    }
}

static void
ngx_http_cache_purge_index_number(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_buffer_t *b, uint64_t value)
{
    unsigned char  raw[8];
    unsigned       i;

    for (i = 0; i < 8; i++) {
        raw[7 - i] = (unsigned char) value;
        value >>= 8;
    }
    ngx_http_cache_purge_index_append(q, b, raw, sizeof(raw));
}

static void
ngx_http_cache_purge_index_string(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_buffer_t *b,
    ngx_http_cache_rocksdb_bytes_t value)
{
    ngx_http_cache_purge_index_number(q, b, value.len);
    ngx_http_cache_purge_index_append(q, b, value.data, value.len);
}

static void
ngx_http_cache_purge_index_row(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_buffer_t *b, unsigned char kind, uint64_t id)
{
    b->len = 0;
    ngx_http_cache_purge_index_append(q, b, &kind, 1);
    ngx_http_cache_purge_index_number(q, b, id);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_take(ngx_http_cache_purge_index_reader_t *r,
    size_t size)
{
    ngx_http_cache_rocksdb_bytes_t value;

    if (!ngx_http_cache_purge_index_require(
            r->query, size <= r->data.len - r->at,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "truncated index record"))
    {
        return ngx_http_cache_purge_op_empty;
    }
    value.data = r->data.data + r->at;
    value.len = size;
    r->at += size;
    return value;
}

static uint64_t
ngx_http_cache_purge_index_read_number(ngx_http_cache_purge_index_reader_t *r,
    size_t size)
{
    ngx_http_cache_rocksdb_bytes_t  raw;
    uint64_t                        value;
    size_t                          i;

    raw = ngx_http_cache_purge_index_take(r, size);
    value = 0;
    for (i = 0; i < raw.len; i++) {
        value = (value << 8) | raw.data[i];
    }
    return value;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_read_string(ngx_http_cache_purge_index_reader_t *r)
{
    uint64_t size;

    size = ngx_http_cache_purge_index_read_number(r, 8);
    if (!ngx_http_cache_purge_index_require(
            r->query, size <= r->data.len - r->at,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid index field size"))
    {
        return ngx_http_cache_purge_op_empty;
    }
    return ngx_http_cache_purge_index_take(r, (size_t) size);
}

static uint64_t
ngx_http_cache_purge_index_integer(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t value)
{
    ngx_http_cache_purge_index_reader_t r;

    if (!ngx_http_cache_purge_index_require(q, value.len == 8,
                                            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                            "invalid index integer"))
    {
        return 0;
    }
    r.query = q;
    r.data = value;
    r.at = 0;
    return ngx_http_cache_purge_index_read_number(&r, 8);
}

static int
ngx_http_cache_purge_index_cursor_close(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor)
{
    ngx_http_cache_rocksdb_error_t  error;
    int                             rc;

    if (cursor->handle == NULL) {
        return 0;
    }
    rc = ngx_http_cache_rocksdb_cursor_close(&cursor->handle, &error);
    if (rc != 0) {
        ngx_http_cache_purge_index_fail(q, rc, error.message);
    }
    return rc;
}

static void
ngx_http_cache_purge_index_cursor_open(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor, ngx_http_cache_rocksdb_cf_e cf,
    ngx_http_cache_rocksdb_bytes_t prefix, ngx_http_cache_rocksdb_bytes_t after,
    ngx_http_cache_rocksdb_cursor_t *view)
{
    ngx_http_cache_rocksdb_error_t  error;
    int                             rc;

    ngx_http_cache_purge_index_cursor_close(q, cursor);
    if (q->status != 0) {
        return;
    }
    rc = view != NULL ?
             ngx_http_cache_rocksdb_cursor_fork(view, cf, prefix, after,
                                                &cursor->handle, &error) :
             ngx_http_cache_rocksdb_cursor_open(q->store, cf, prefix, after,
                                                &cursor->handle, &error);
    if (rc != 0) {
        ngx_http_cache_purge_index_fail(q, rc, error.message);
    }
}

static int
ngx_http_cache_purge_index_cursor_next(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor)
{
    ngx_http_cache_rocksdb_buffer_t key, value;
    ngx_http_cache_rocksdb_error_t  error;
    int                             rc;

    if (cursor->handle == NULL) {
        return 0;
    }
    ngx_http_cache_purge_index_reserve(q, &cursor->key, 256);
    ngx_http_cache_purge_index_reserve(q, &cursor->value, 256);
    while (q->status == 0) {
        key.data = cursor->key.data;
        key.capacity = cursor->key.capacity;
        key.length = 0;
        value.data = cursor->value.data;
        value.capacity = cursor->value.capacity;
        value.length = 0;
        rc = ngx_http_cache_rocksdb_cursor_next(cursor->handle, &key, &value,
                                                &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            return 0;
        }
        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUFFER) {
            if (!ngx_http_cache_purge_index_require(
                    q,
                    key.length <= NGX_HTTP_CACHE_PURGE_INDEX_RECORD
                        && value.length
                               <= NGX_HTTP_CACHE_PURGE_INDEX_RECORD - key.length
                        && (key.length > key.capacity
                            || value.length > value.capacity),
                    NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                    "index cursor row exceeds budget"))
            {
                return 0;
            }
            ngx_http_cache_purge_index_reserve(q, &cursor->key, key.length);
            ngx_http_cache_purge_index_reserve(q, &cursor->value, value.length);
            continue;
        }
        if (rc != 0) {
            ngx_http_cache_purge_index_fail(q, rc, error.message);
            return 0;
        }
        cursor->key.len = key.length;
        cursor->value.len = value.length;
        return 1;
    }
    return 0;
}

/* The returned value is in q->scratch and survives only until the next get. */
static int
ngx_http_cache_purge_index_get(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_rocksdb_cursor_t *view)
{
    ngx_http_cache_rocksdb_buffer_t  value;
    ngx_http_cache_rocksdb_error_t   error;
    int                              rc;

    q->scratch.len = 0;
    ngx_http_cache_purge_index_reserve(q, &q->scratch, 256);
    while (q->status == 0) {
        value.data = q->scratch.data;
        value.capacity = q->scratch.capacity;
        value.length = 0;
        rc = view != NULL ?
                 ngx_http_cache_rocksdb_cursor_get(view, cf, key, &value,
                                                   &error) :
                 ngx_http_cache_rocksdb_get(q->store, cf, key, &value, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            return 0;
        }
        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUFFER) {
            if (!ngx_http_cache_purge_index_require(
                    q, value.length > value.capacity,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid index read size"))
            {
                return 0;
            }
            ngx_http_cache_purge_index_reserve(q, &q->scratch, value.length);
            continue;
        }
        if (rc != 0) {
            ngx_http_cache_purge_index_fail(q, rc, error.message);
            return 0;
        }
        q->scratch.len = value.length;
        return 1;
    }
    return 0;
}

static int
ngx_http_cache_purge_index_value(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_cursor_t *view, uint64_t id, int leaf)
{
    ngx_http_cache_purge_index_reader_t  r;
    ngx_http_cache_rocksdb_bytes_t       raw;
    uint64_t                             parts[5];
    unsigned                            format, count, i;
    size_t                              j;

    ngx_http_cache_purge_index_row(q, &q->key, 'V', id);
    if (!ngx_http_cache_purge_index_get(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->key), view))
    {
        if (leaf) {
            ngx_http_cache_purge_index_fail(q, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                            "missing value component");
        }
        return 0;
    }
    r.query = q;
    r.data = ngx_http_cache_purge_index_bytes(&q->scratch);
    r.at = 0;
    ngx_http_cache_purge_index_require(
        q, ngx_http_cache_purge_index_read_number(&r, 1) == 1,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid value record version");
    (void) ngx_http_cache_purge_index_read_number(&r, 8);
    format = ngx_http_cache_purge_index_read_number(&r, 1);
    if (format == 0) {
        raw = ngx_http_cache_purge_index_read_string(&r);
        ngx_http_cache_purge_index_require(
            q,
            raw.len <= NGX_HTTP_CACHE_PURGE_INDEX_STRING && r.at == r.data.len,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid dictionary value");
        if (!leaf) {
            ngx_http_cache_purge_index_set(q, &q->lookup, raw);
        }
        return q->status == 0;
    }
    if (!ngx_http_cache_purge_index_require(
            q, !leaf && (format == 1 || format == 2),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid recursive value format"))
    {
        return 0;
    }
    count = ngx_http_cache_purge_index_read_number(&r, 1);
    if (!ngx_http_cache_purge_index_require(q, count == (format == 1 ? 5U : 3U),
                                            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                            "invalid value component count"))
    {
        return 0;
    }
    for (i = 0; i < count; i++) {
        parts[i] = ngx_http_cache_purge_index_read_number(&r, 8);
    }
    ngx_http_cache_purge_index_require(q, r.at == r.data.len,
                                       NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                       "unexpected value record suffix");
    q->lookup.len = 0;
    for (i = 0; i < count && q->status == 0; i++) {
        if (!ngx_http_cache_purge_index_value(q, view, parts[i], 1)) {
            break;
        }
        /* A leaf record is version/refcount/format/length followed by bytes. */
        raw.data = q->scratch.data + 18;
        raw.len = q->scratch.len - 18;
        if (format == 1 && i != 0) {
            ngx_http_cache_purge_index_append(q, &q->lookup, "\x1f", 1);
        }
        if (format == 2 && i == 1) {
            ngx_http_cache_purge_index_append(q, &q->lookup, "://", 3);
        }
        for (j = 0; j < raw.len && q->status == 0; j++) {
            if (format == 1 && raw.data[j] == '\\') {
                ngx_http_cache_purge_index_append(q, &q->lookup, "\\\\", 2);
            } else if (format == 1 && raw.data[j] == 31) {
                ngx_http_cache_purge_index_append(q, &q->lookup, "\\x1F", 4);
            } else {
                ngx_http_cache_purge_index_append(q, &q->lookup, raw.data + j,
                                                  1);
            }
            ngx_http_cache_purge_index_require(
                q, q->lookup.len <= NGX_HTTP_CACHE_PURGE_INDEX_STRING,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "reconstructed index value too long");
        }
    }
    return q->status == 0;
}

static uint64_t
ngx_http_cache_purge_index_find(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t text)
{
    unsigned char                        prefix[33];
    ngx_http_cache_rocksdb_bytes_t       range, number;
    ngx_http_cache_purge_index_cursor_t  *cursor;
    uint64_t                              id;
    unsigned                              candidates;

    prefix[0] = 'S';
    if (!ngx_http_cache_purge_index_require(
            q, SHA256(text.data, text.len, prefix + 1) != NULL,
            NGX_HTTP_CACHE_ROCKSDB_IO, "index digest failed"))
    {
        return 0;
    }
    range.data = prefix;
    range.len = sizeof(prefix);
    cursor = &q->files;
    ngx_http_cache_purge_index_cursor_open(
        q, cursor, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, range,
        ngx_http_cache_purge_op_empty, NULL);
    id = 0;
    candidates = 0;
    while (ngx_http_cache_purge_index_cursor_next(q, cursor)) {
        if (!ngx_http_cache_purge_index_require(q, ++candidates <= 16,
                                                NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                                "index digest bucket limit")
            || !ngx_http_cache_purge_index_require(
                q, cursor->key.len == 41, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid digest entry"))
        {
            break;
        }
        number.data = cursor->key.data + 33;
        number.len = 8;
        id = ngx_http_cache_purge_index_integer(q, number);
        if (!ngx_http_cache_purge_index_value(q, cursor->handle, id, 0)) {
            if (q->status != 0) {
                break;
            }
            id = 0;
            continue;
        }
        if (ngx_http_cache_purge_op_equal(
                text, ngx_http_cache_purge_index_bytes(&q->lookup)))
        {
            break;
        }
        id = 0;
    }
    ngx_http_cache_purge_index_cursor_close(q, cursor);
    return q->status == 0 ? id : 0;
}

static int
ngx_http_cache_purge_index_decode(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t data,
    ngx_http_cache_purge_index_record_t *record)
{
    ngx_http_cache_purge_index_reader_t r;
    ngx_http_cache_rocksdb_bytes_t      id;
    size_t                              i, j;
    unsigned                            nonzero;

    memset(record, 0, sizeof(*record));
    r.query = q;
    r.data = data;
    r.at = 0;
    ngx_http_cache_purge_index_require(
        q, ngx_http_cache_purge_index_read_number(&r, 1) == 1,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid file record version");
    id = ngx_http_cache_purge_index_take(&r, 16);
    if (id.len != 0) {
        memcpy(record->id, id.data, 16);
    }
    id = ngx_http_cache_purge_index_take(&r, 16);
    if (id.len != 0) {
        memcpy(record->previous, id.data, 16);
    }
    record->state = ngx_http_cache_purge_index_read_number(&r, 1);
    record->revision = ngx_http_cache_purge_index_read_number(&r, 8);
    nonzero = 0;
    for (i = 0; i < 16; i++) {
        nonzero |= record->id[i];
    }
    ngx_http_cache_purge_index_require(q,
                                       record->state >= 1 && record->state <= 3
                                           && record->revision != 0 && nonzero,
                                       NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                       "invalid file identity/state/revision");
    for (i = 0; i < 8; i++) {
        record->refs[i] = ngx_http_cache_purge_index_read_number(&r, 8);
        ngx_http_cache_purge_index_require(q, record->refs[i] != 0,
                                           NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                           "invalid value reference");
    }
    record->path = ngx_http_cache_purge_index_read_string(&r);
    record->metadata = ngx_http_cache_purge_index_read_string(&r);
    record->digest = ngx_http_cache_purge_index_read_string(&r);
    ngx_http_cache_purge_index_require(
        q,
        record->path.len != 0
            && record->path.len <= NGX_HTTP_CACHE_PURGE_INDEX_STRING
            && record->metadata.len <= 16384 && record->digest.len == 32,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid file record fields");
    record->tag_count = ngx_http_cache_purge_index_read_number(&r, 1);
    if (!ngx_http_cache_purge_index_require(q, record->tag_count <= 64,
                                            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                            "invalid TAG count"))
    {
        return 0;
    }
    for (i = 0; i < record->tag_count && q->status == 0; i++) {
        record->tags[i] = ngx_http_cache_purge_index_read_number(&r, 8);
        ngx_http_cache_purge_index_require(q, record->tags[i] != 0,
                                           NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                           "invalid TAG reference");
        for (j = 0; j < i; j++) {
            ngx_http_cache_purge_index_require(
                q, record->tags[i] != record->tags[j],
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "duplicate TAG reference");
        }
    }
    return ngx_http_cache_purge_index_require(q, r.at == r.data.len,
                                              NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                              "unexpected file record suffix");
}

static int
ngx_http_cache_purge_index_record(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_record_t *record)
{
    return ngx_http_cache_purge_index_decode(q,
        ngx_http_cache_purge_index_bytes(&q->scratch), record);
}

static int
ngx_http_cache_purge_index_compatible(ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_rocksdb_bytes_t prefix, size_t suffix)
{
    return key.len == prefix.len + suffix
           && memcmp(key.data, prefix.data, prefix.len) == 0;
}

static void
ngx_http_cache_purge_index_open_files(ngx_http_cache_purge_query_t *q,
    uint64_t key, ngx_http_cache_rocksdb_bytes_t after)
{
    ngx_http_cache_rocksdb_bytes_t range;

    ngx_http_cache_purge_index_row(q, &q->key, 'k', key);
    if (!ngx_http_cache_purge_index_require(
            q,
            ngx_http_cache_purge_index_get(
                q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_bytes(&q->key), q->outer.handle),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "URL/key posting has no key record"))
    {
        return;
    }
    ngx_http_cache_purge_index_row(q, &q->key, 'F', key);
    range = ngx_http_cache_purge_index_bytes(&q->key);
    if (!ngx_http_cache_purge_index_compatible(after, range, 16)) {
        after = ngx_http_cache_purge_op_empty;
    }
    ngx_http_cache_purge_index_cursor_open(q, &q->files,
                                           NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                           range, after, q->outer.handle);
}

static void
ngx_http_cache_purge_index_open_links(ngx_http_cache_purge_query_t *q,
    uint64_t url, ngx_http_cache_rocksdb_bytes_t after,
    ngx_http_cache_rocksdb_bytes_t file)
{
    ngx_http_cache_rocksdb_bytes_t range, number;

    ngx_http_cache_purge_index_row(q, &q->key, 'A', url);
    range = ngx_http_cache_purge_index_bytes(&q->key);
    if (!ngx_http_cache_purge_index_compatible(after, range, 8)) {
        after = ngx_http_cache_purge_op_empty;
    }
    ngx_http_cache_purge_index_cursor_open(q, &q->links,
                                           NGX_HTTP_CACHE_ROCKSDB_URL, range,
                                           after, q->outer.handle);
    if (after.len != 0
        && ngx_http_cache_purge_index_get(q, NGX_HTTP_CACHE_ROCKSDB_URL, after,
                                          q->outer.handle))
    {
        ngx_http_cache_purge_index_set(q, &q->current_link, after);
        number.data = after.data + 9;
        number.len = 8;
        ngx_http_cache_purge_index_open_files(
            q, ngx_http_cache_purge_index_integer(q, number), file);
    }
}

static void
ngx_http_cache_purge_index_bookmark(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t file)
{
    size_t         size;
    unsigned char  version;

    size = 57 + q->current_outer.len + q->current_link.len + file.len;
    if (!ngx_http_cache_purge_index_reserve(q, &q->token, size)) {
        return;
    }
    q->token.len = 0;
    version = 1;
    ngx_http_cache_purge_index_append(q, &q->token, &version, 1);
    ngx_http_cache_purge_index_append(q, &q->token, q->signature, 32);
    ngx_http_cache_purge_index_string(
        q, &q->token, ngx_http_cache_purge_index_bytes(&q->current_outer));
    ngx_http_cache_purge_index_string(
        q, &q->token, ngx_http_cache_purge_index_bytes(&q->current_link));
    ngx_http_cache_purge_index_string(q, &q->token, file);
}

static int
ngx_http_cache_purge_index_candidate(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor, ngx_http_cache_rocksdb_cf_e cf,
    ngx_http_cache_purge_index_match_t *out)
{
    ngx_http_cache_purge_index_record_t record;
    ngx_http_cache_rocksdb_bytes_t      key, number;
    unsigned char                       object[17];
    uint64_t                            scope, tag;
    size_t                              i;
    int                                 tagged, found;

    tagged = cf == NGX_HTTP_CACHE_ROCKSDB_TAG;
    key = ngx_http_cache_purge_index_bytes(&cursor->key);
    if (!ngx_http_cache_purge_index_require(q, key.len == (tagged ? 33U : 25U),
                                            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                            "invalid file posting"))
    {
        return 0;
    }
    object[0] = 'O';
    memcpy(object + 1, key.data + key.len - 16, 16);
    number.data = object;
    number.len = sizeof(object);
    if (!ngx_http_cache_purge_index_require(
            q,
            ngx_http_cache_purge_index_get(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                           number, cursor->handle),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "file posting has no object")
        || !ngx_http_cache_purge_index_record(q, &record))
    {
        return 0;
    }
    ngx_http_cache_purge_index_require(
        q, memcmp(record.id, object + 1, 16) == 0,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "file posting identity mismatch");
    number.data = key.data + 1;
    number.len = 8;
    scope = ngx_http_cache_purge_index_integer(q, number);
    if (tagged) {
        ngx_http_cache_purge_index_require(
            q, record.refs[key.data[0] == 'N' ? 3 : 2] == scope,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "TAG posting scope mismatch");
        number.data = key.data + 9;
        tag = ngx_http_cache_purge_index_integer(q, number);
        found = 0;
        for (i = 0; i < record.tag_count; i++) {
            found |= record.tags[i] == tag;
        }
        ngx_http_cache_purge_index_require(q, found,
                                           NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                           "TAG posting is absent from file");
    } else {
        ngx_http_cache_purge_index_require(
            q, record.refs[key.data[0] == 'F' ? 0 : 2] == scope,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "file posting key/host mismatch");
    }
    if (q->status != 0 || record.state == 3) {
        return 0;
    }
    ngx_http_cache_purge_index_bookmark(q, key);
    if (q->status != 0) {
        return 0;
    }
    memcpy(out->id, record.id, 16);
    out->state = record.state;
    out->revision = record.revision;
    return 1;
}

static int
ngx_http_cache_purge_index_recover(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_match_t *out)
{
    ngx_http_cache_purge_index_record_t record;
    ngx_http_cache_rocksdb_bytes_t      key, value, object_key;
    unsigned char                       object[17];

    if (!ngx_http_cache_purge_index_cursor_next(q, &q->outer)) {
        ngx_http_cache_purge_index_cursor_close(q, &q->outer);
        q->empty = 1;
        return 0;
    }
    key = ngx_http_cache_purge_index_bytes(&q->outer.key);
    value = ngx_http_cache_purge_index_bytes(&q->outer.value);
    if (!ngx_http_cache_purge_index_require(
            q, q->recovery == 'I' ? value.len == 16 : key.len == 17,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid recovery queue row"))
    {
        return 0;
    }
    object[0] = 'O';
    memcpy(object + 1, q->recovery == 'I' ? value.data : key.data + 1, 16);
    object_key.data = object;
    object_key.len = sizeof(object);
    if (!ngx_http_cache_purge_index_require(
            q,
            ngx_http_cache_purge_index_get(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                           object_key, q->outer.handle),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "pending fill has no file record")
        || !ngx_http_cache_purge_index_record(q, &record))
    {
        return 0;
    }
    ngx_http_cache_purge_index_require(
        q, memcmp(record.id, object + 1, 16) == 0,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "recovery file ID mismatch");
    if (q->recovery == 'I') {
        ngx_http_cache_purge_index_row(q, &q->key, 'I', record.refs[4]);
        ngx_http_cache_purge_index_append(q, &q->key, record.path.data,
                                          record.path.len);
        ngx_http_cache_purge_index_require(
            q,
            (record.state == 1 || record.state == 2)
                && ngx_http_cache_purge_op_equal(
                    key, ngx_http_cache_purge_index_bytes(&q->key)),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "invalid pending-fill relationship");
        if (q->status == 0 && record.state == 2) {
            q->key.data[0] = 'P';
            ngx_http_cache_purge_index_require(
                q,
                ngx_http_cache_purge_index_get(
                    q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                    ngx_http_cache_purge_index_bytes(&q->key), q->outer.handle)
                    && q->scratch.len == 16
                    && memcmp(q->scratch.data, record.id, 16) == 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "unpublished file has no matching current path");
        }
    } else {
        ngx_http_cache_purge_index_require(
            q,
            record.state == 3
                && record.revision
                       == ngx_http_cache_purge_index_integer(q, value),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "invalid retired-file relationship");
    }
    if (q->status != 0) {
        return 0;
    }
    ngx_http_cache_purge_index_bookmark(q, key);
    if (q->status != 0) {
        return 0;
    }
    memcpy(out->id, record.id, 16);
    out->state = record.state;
    out->revision = record.revision;
    return 1;
}

static void
ngx_http_cache_purge_index_select(ngx_http_cache_purge_query_t *q,
    const ngx_http_cache_purge_selector_t *selector,
    ngx_http_cache_rocksdb_bytes_t resume)
{
    ngx_http_cache_purge_index_reader_t r;
    ngx_http_cache_rocksdb_bytes_t      saved[3], signature, range, number;
    uint64_t                            id, scope;
    unsigned char                       kind;
    int                                 tagged, url;

    memset(saved, 0, sizeof(saved));
    if (q->recovery) {
        ngx_http_cache_purge_index_append(q, &q->key, "index-recovery-v1:", 18);
        ngx_http_cache_purge_index_append(q, &q->key, &q->recovery, 1);
        ngx_http_cache_purge_index_append(q, &q->prefix, &q->recovery, 1);
    } else {
        if (!ngx_http_cache_purge_index_require(
                q,
                selector != NULL
                    && selector->kind <= NGX_HTTP_CACHE_PURGE_HOST_TAG,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid index selector"))
        {
            return;
        }
        q->kind = selector->kind;
        tagged = q->kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG
                 || q->kind == NGX_HTTP_CACHE_PURGE_HOST_TAG;
        if (!ngx_http_cache_purge_index_require(
                q,
                (selector->value.len == 0 || selector->value.data != NULL)
                    && (selector->scope.len == 0
                        || selector->scope.data != NULL),
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid selector bytes")
            || !ngx_http_cache_purge_index_require(
                q,
                selector->value.len <= NGX_HTTP_CACHE_PURGE_INDEX_STRING
                    && selector->scope.len <= NGX_HTTP_CACHE_PURGE_INDEX_STRING,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "index selector too long")
            || !ngx_http_cache_purge_index_require(
                q,
                (!tagged || selector->value.len <= 1024)
                    && (tagged ? selector->scope.len != 0 :
                                 selector->scope.len == 0)
                    && (selector->value.len != 0
                        || q->kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX
                        || q->kind == NGX_HTTP_CACHE_PURGE_URL_PREFIX),
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid selector scope/value"))
        {
            return;
        }
        kind = (unsigned char) q->kind;
        ngx_http_cache_purge_index_append(q, &q->key, &kind, 1);
        ngx_http_cache_purge_index_string(q, &q->key, selector->value);
        ngx_http_cache_purge_index_string(q, &q->key, selector->scope);
    }
    if (!ngx_http_cache_purge_index_require(
            q,
            q->status == 0
                && SHA256(q->key.data, q->key.len, q->signature) != NULL,
            NGX_HTTP_CACHE_ROCKSDB_IO, "index query signature failed"))
    {
        return;
    }
    if (resume.len != 0) {
        r.query = q;
        r.data = resume;
        r.at = 0;
        kind = ngx_http_cache_purge_index_read_number(&r, 1);
        signature = ngx_http_cache_purge_index_take(&r, 32);
        if (!ngx_http_cache_purge_index_require(
                q,
                kind == 1 && signature.len == 32
                    && memcmp(signature.data, q->signature, 32) == 0,
                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "bookmark belongs to another selector"))
        {
            return;
        }
        saved[0] = ngx_http_cache_purge_index_read_string(&r);
        saved[1] = ngx_http_cache_purge_index_read_string(&r);
        saved[2] = ngx_http_cache_purge_index_read_string(&r);
        if (!ngx_http_cache_purge_index_require(q, r.at == r.data.len,
                                                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                                "unexpected bookmark suffix"))
        {
            return;
        }
        ngx_http_cache_purge_index_set(q, &q->token, resume);
    }
    q->outer_cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
    if (q->recovery) {
        if (resume.len != 0
            && !ngx_http_cache_purge_index_require(
                q,
                saved[0].len == 0 && saved[1].len == 0
                    && (q->recovery == 'I' ? saved[2].len > 9 :
                                             saved[2].len == 17)
                    && saved[2].data[0] == q->recovery,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid recovery position"))
        {
            return;
        }
        ngx_http_cache_purge_index_cursor_open(
            q, &q->outer, q->outer_cf,
            ngx_http_cache_purge_index_bytes(&q->prefix), saved[2], NULL);
        return;
    }
    switch (q->kind) {
    case NGX_HTTP_CACHE_PURGE_KEY:
    case NGX_HTTP_CACHE_PURGE_URL:
        url = q->kind == NGX_HTTP_CACHE_PURGE_URL;
        q->outer_cf =
            url ? NGX_HTTP_CACHE_ROCKSDB_URL : NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
        q->key.len = 0;
        kind = url ? 'U' : 'K';
        ngx_http_cache_purge_index_append(q, &q->key, &kind, 1);
        ngx_http_cache_purge_index_append(q, &q->key, selector->value.data,
                                          selector->value.len);
        if (!ngx_http_cache_purge_index_get(
                q, q->outer_cf, ngx_http_cache_purge_index_bytes(&q->key),
                NULL))
        {
            q->empty = 1;
            return;
        }
        id = ngx_http_cache_purge_index_integer(
            q, ngx_http_cache_purge_index_bytes(&q->scratch));
        ngx_http_cache_purge_index_row(q, &q->prefix, url ? 'A' : 'F', id);
        break;
    case NGX_HTTP_CACHE_PURGE_KEY_PREFIX:
    case NGX_HTTP_CACHE_PURGE_URL_PREFIX:
        url = q->kind == NGX_HTTP_CACHE_PURGE_URL_PREFIX;
        kind = url ? 'U' : 'K';
        q->outer_cf =
            url ? NGX_HTTP_CACHE_ROCKSDB_URL : NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
        ngx_http_cache_purge_index_append(q, &q->prefix, &kind, 1);
        ngx_http_cache_purge_index_append(q, &q->prefix, selector->value.data,
                                          selector->value.len);
        break;
    case NGX_HTTP_CACHE_PURGE_HOST:
        id = ngx_http_cache_purge_index_find(q, selector->value);
        if (id == 0) {
            q->empty = 1;
            return;
        }
        ngx_http_cache_purge_index_row(q, &q->prefix, 'H', id);
        break;
    default:
        scope = ngx_http_cache_purge_index_find(q, selector->scope);
        id = ngx_http_cache_purge_index_find(q, selector->value);
        if (scope == 0 || id == 0) {
            q->empty = 1;
            return;
        }
        ngx_http_cache_purge_index_row(
            q, &q->prefix,
            q->kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG ? 'N' : 'H', scope);
        ngx_http_cache_purge_index_number(q, &q->prefix, id);
        q->outer_cf = NGX_HTTP_CACHE_ROCKSDB_TAG;
        break;
    }
    range = ngx_http_cache_purge_index_bytes(&q->prefix);
    if (q->kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX
        || q->kind == NGX_HTTP_CACHE_PURGE_URL_PREFIX)
    {
        if (!ngx_http_cache_purge_index_require(
                q,
                saved[0].len == 0
                    || (saved[0].len >= range.len
                        && memcmp(saved[0].data, range.data, range.len) == 0),
                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "bookmark is outside selector prefix"))
        {
            return;
        }
        ngx_http_cache_purge_index_cursor_open(q, &q->outer, q->outer_cf, range,
                                               saved[0], NULL);
        if (saved[0].len != 0
            && ngx_http_cache_purge_index_get(q, q->outer_cf, saved[0],
                                              q->outer.handle))
        {
            ngx_http_cache_purge_index_set(q, &q->current_outer, saved[0]);
            id = ngx_http_cache_purge_index_integer(
                q, ngx_http_cache_purge_index_bytes(&q->scratch));
            if (q->kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX) {
                ngx_http_cache_purge_index_open_files(q, id, saved[2]);
            } else {
                ngx_http_cache_purge_index_open_links(q, id, saved[1],
                                                      saved[2]);
            }
        }
    } else if (q->kind == NGX_HTTP_CACHE_PURGE_URL) {
        if (!ngx_http_cache_purge_index_require(
                q, saved[0].len == 0, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "unexpected URL bookmark outer key"))
        {
            return;
        }
        if (!ngx_http_cache_purge_index_compatible(saved[1], range, 8)) {
            saved[1] = ngx_http_cache_purge_op_empty;
        }
        ngx_http_cache_purge_index_cursor_open(q, &q->outer, q->outer_cf, range,
                                               saved[1], NULL);
        if (saved[1].len != 0
            && ngx_http_cache_purge_index_get(q, q->outer_cf, saved[1],
                                              q->outer.handle))
        {
            ngx_http_cache_purge_index_set(q, &q->current_link, saved[1]);
            number.data = saved[1].data + 9;
            number.len = 8;
            ngx_http_cache_purge_index_open_files(
                q, ngx_http_cache_purge_index_integer(q, number), saved[2]);
        }
    } else {
        if (!ngx_http_cache_purge_index_require(
                q, saved[0].len == 0 && saved[1].len == 0,
                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "unexpected direct bookmark join"))
        {
            return;
        }
        if (!ngx_http_cache_purge_index_compatible(saved[2], range, 16)) {
            saved[2] = ngx_http_cache_purge_op_empty;
        }
        ngx_http_cache_purge_index_cursor_open(q, &q->outer, q->outer_cf, range,
                                               saved[2], NULL);
    }
}

int
ngx_http_cache_purge_query_close(ngx_http_cache_purge_query_t **query,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t         *q;
    ngx_http_cache_purge_index_buffer_t  *buffers[13];
    ngx_http_cache_rocksdb_cursor_t     **handles[3];
    ngx_http_cache_rocksdb_error_t        failure;
    size_t                                i;
    int                                   rc;

    if (error != NULL) {
        memset(error, 0, sizeof(*error));
    }
    if (query == NULL || *query == NULL) {
        return 0;
    }
    q = *query;
    handles[0] = &q->files.handle;
    handles[1] = &q->links.handle;
    handles[2] = &q->outer.handle;
    for (i = 0; i < 3; i++) {
        if (*handles[i] == NULL) {
            continue;
        }
        rc = ngx_http_cache_rocksdb_cursor_close(handles[i], &failure);
        if (rc != 0) {
            if (error != NULL) {
                *error = failure;
            }
            return rc;
        }
    }
    buffers[0] = &q->outer.key;
    buffers[1] = &q->outer.value;
    buffers[2] = &q->links.key;
    buffers[3] = &q->links.value;
    buffers[4] = &q->files.key;
    buffers[5] = &q->files.value;
    buffers[6] = &q->prefix;
    buffers[7] = &q->current_outer;
    buffers[8] = &q->current_link;
    buffers[9] = &q->token;
    buffers[10] = &q->scratch;
    buffers[11] = &q->lookup;
    buffers[12] = &q->key;
    for (i = 0; i < 13; i++) {
        free(buffers[i]->data);
    }
    free(q);
    *query = NULL;
    return 0;
}

int
ngx_http_cache_purge_query_open(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_selector_t *selector, unsigned mode,
    ngx_http_cache_rocksdb_bytes_t bookmark, ngx_http_cache_purge_query_t **out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t  *q;
    int                            rc;

    if (out != NULL) {
        *out = NULL;
    }
    if (error != NULL) {
        memset(error, 0, sizeof(*error));
    }
    rc = NGX_HTTP_CACHE_ROCKSDB_INVALID;
    if (store == NULL || out == NULL
        || mode > NGX_HTTP_CACHE_PURGE_QUERY_RETIRED
        || (mode != 0 && selector != NULL)
        || !ngx_http_cache_purge_op_bytes_valid(bookmark, 33024))
    {
        if (error != NULL) {
            error->code = rc;
            snprintf(error->message, sizeof(error->message),
                     "invalid index query arguments");
        }
        return rc;
    }
    q = calloc(1, sizeof(*q));
    if (q == NULL) {
        if (error != NULL) {
            error->code = NGX_HTTP_CACHE_ROCKSDB_LIMIT;
            snprintf(error->message, sizeof(error->message),
                     "index query allocation failed");
        }
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q->store = store;
    q->bytes = sizeof(*q);
    q->recovery = mode == NGX_HTTP_CACHE_PURGE_QUERY_PENDING ? 'I' :
                  mode == NGX_HTTP_CACHE_PURGE_QUERY_RETIRED ? 'g' :
                                                               0;
    ngx_http_cache_purge_index_select(q, selector, bookmark);
    rc = q->status;
    if (rc != 0) {
        if (error != NULL) {
            *error = q->error;
        }
        ngx_http_cache_purge_query_close(&q, NULL);
        return rc;
    }
    *out = q;
    return 0;
}

int
ngx_http_cache_purge_query_next(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_match_t *match,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_rocksdb_bytes_t  number;
    uint64_t                        id;
    int                             found;

    if (match != NULL) {
        memset(match, 0, sizeof(*match));
    }
    if (error != NULL) {
        memset(error, 0, sizeof(*error));
    }
    if (q == NULL || match == NULL) {
        if (error != NULL) {
            error->code = NGX_HTTP_CACHE_ROCKSDB_INVALID;
            snprintf(error->message, sizeof(error->message),
                     "invalid index query output");
        }
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }
    found = 0;
    if (q->recovery && !q->empty && q->status == 0) {
        found = ngx_http_cache_purge_index_recover(q, match);
    }
    while (!q->recovery && !q->empty && q->status == 0) {
        if (q->files.handle != NULL) {
            while (ngx_http_cache_purge_index_cursor_next(q, &q->files)) {
                found = ngx_http_cache_purge_index_candidate(
                    q, &q->files, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, match);
                if (found) {
                    break;
                }
            }
            if (found || q->status != 0) {
                break;
            }
            ngx_http_cache_purge_index_cursor_close(q, &q->files);
        }
        if (q->links.handle != NULL) {
            if (ngx_http_cache_purge_index_cursor_next(q, &q->links)) {
                if (!ngx_http_cache_purge_index_require(
                        q, q->links.key.len == 17,
                        NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                        "invalid URL/key posting"))
                {
                    break;
                }
                ngx_http_cache_purge_index_set(
                    q, &q->current_link,
                    ngx_http_cache_purge_index_bytes(&q->links.key));
                number.data = q->links.key.data + 9;
                number.len = 8;
                ngx_http_cache_purge_index_open_files(
                    q, ngx_http_cache_purge_index_integer(q, number),
                    ngx_http_cache_purge_op_empty);
                continue;
            }
            ngx_http_cache_purge_index_cursor_close(q, &q->links);
            q->current_link.len = 0;
        }
        if (!ngx_http_cache_purge_index_cursor_next(q, &q->outer)) {
            ngx_http_cache_purge_index_cursor_close(q, &q->outer);
            q->empty = 1;
            break;
        }
        switch (q->kind) {
        case NGX_HTTP_CACHE_PURGE_KEY_PREFIX:
        case NGX_HTTP_CACHE_PURGE_URL_PREFIX:
            ngx_http_cache_purge_index_set(
                q, &q->current_outer,
                ngx_http_cache_purge_index_bytes(&q->outer.key));
            q->current_link.len = 0;
            id = ngx_http_cache_purge_index_integer(
                q, ngx_http_cache_purge_index_bytes(&q->outer.value));
            if (q->kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX) {
                ngx_http_cache_purge_index_open_files(
                    q, id, ngx_http_cache_purge_op_empty);
            } else {
                ngx_http_cache_purge_index_open_links(
                    q, id, ngx_http_cache_purge_op_empty,
                    ngx_http_cache_purge_op_empty);
            }
            break;
        case NGX_HTTP_CACHE_PURGE_URL:
            if (!ngx_http_cache_purge_index_require(
                    q, q->outer.key.len == 17, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "invalid URL/key posting"))
            {
                break;
            }
            ngx_http_cache_purge_index_set(
                q, &q->current_link,
                ngx_http_cache_purge_index_bytes(&q->outer.key));
            number.data = q->outer.key.data + 9;
            number.len = 8;
            ngx_http_cache_purge_index_open_files(
                q, ngx_http_cache_purge_index_integer(q, number),
                ngx_http_cache_purge_op_empty);
            break;
        default:
            found = ngx_http_cache_purge_index_candidate(q, &q->outer,
                                                         q->outer_cf, match);
            break;
        }
        if (found) {
            break;
        }
    }
    if (q->status != 0) {
        ngx_http_cache_purge_index_cursor_close(q, &q->files);
        ngx_http_cache_purge_index_cursor_close(q, &q->links);
        ngx_http_cache_purge_index_cursor_close(q, &q->outer);
        if (error != NULL) {
            *error = q->error;
        }
        memset(match, 0, sizeof(*match));
        return q->status;
    }
    return found ? NGX_HTTP_CACHE_ROCKSDB_OK : NGX_HTTP_CACHE_ROCKSDB_DONE;
}

ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_query_bookmark(const ngx_http_cache_purge_query_t *q)
{
    return q != NULL ? ngx_http_cache_purge_index_bytes(&q->token) :
                       ngx_http_cache_purge_op_empty;
}


/*
 * Conditional, bounded write transactions share the index codec with readers.
 */

#define NGX_HTTP_CACHE_PURGE_TX_BUCKETS  1024
#define NGX_HTTP_CACHE_PURGE_TX_BYTES    524288

typedef struct ngx_http_cache_purge_index_row_s
    ngx_http_cache_purge_index_row_t;

typedef struct {
    ngx_http_cache_rocksdb_bytes_t  bytes;
    int                             exists;
} ngx_http_cache_purge_index_value_t;

struct ngx_http_cache_purge_index_row_s {
    ngx_http_cache_purge_index_row_t  *next;
    ngx_http_cache_rocksdb_cf_e         cf;
    ngx_http_cache_purge_index_buffer_t  key;
    ngx_http_cache_purge_index_buffer_t  original;
    ngx_http_cache_purge_index_buffer_t  current;
    int                                  observed;
    int                                  existed;
    int                                  changed;
    int                                  exists;
};

typedef struct {
    ngx_http_cache_purge_query_t      *context;
    size_t                             charged;
    ngx_http_cache_purge_index_row_t  *buckets[NGX_HTTP_CACHE_PURGE_TX_BUCKETS];
} ngx_http_cache_purge_index_tx_t;

static int
ngx_http_cache_purge_index_error(ngx_http_cache_rocksdb_error_t *error,
    int code, const char *message)
{
    if (error != NULL) {
        error->code = code;
        snprintf(error->message, sizeof(error->message), "%s", message);
    }
    return code;
}

static ngx_http_cache_purge_index_tx_t *
ngx_http_cache_purge_index_tx_begin(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t  *tx;
    ngx_http_cache_purge_query_t     *q;

    if (error != NULL) {
        memset(error, 0, sizeof(*error));
    }
    if (store == NULL) {
        ngx_http_cache_purge_index_error(error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                         "missing index store");
        return NULL;
    }
    tx = calloc(1, sizeof(*tx));
    q = calloc(1, sizeof(*q));
    if (tx == NULL || q == NULL) {
        free(tx);
        free(q);
        ngx_http_cache_purge_index_error(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                         "index transaction allocation failed");
        return NULL;
    }
    q->store = store;
    q->bytes = sizeof(*tx) + sizeof(*q);
    tx->context = q;
    return tx;
}

static void *
ngx_http_cache_purge_index_tx_alloc(ngx_http_cache_purge_index_tx_t *tx,
    size_t size)
{
    ngx_http_cache_purge_query_t  *q;
    void                          *data;

    q = tx->context;
    if (!ngx_http_cache_purge_index_require(
            q, size <= NGX_HTTP_CACHE_PURGE_QUERY_MEMORY - q->bytes,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "index transaction allocation budget"))
    {
        return NULL;
    }
    data = malloc(size);
    if (!ngx_http_cache_purge_index_require(
            q, data != NULL, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "index transaction allocation failed"))
    {
        return NULL;
    }
    q->bytes += size;
    return data;
}

static void
ngx_http_cache_purge_index_tx_copy(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_purge_index_buffer_t *out,
    ngx_http_cache_rocksdb_bytes_t value)
{
    unsigned char  *data;

    if (tx->context->status != 0) {
        return;
    }
    data = NULL;
    if (value.len != 0) {
        data = ngx_http_cache_purge_index_tx_alloc(tx, value.len);
        if (data == NULL) {
            return;
        }
        memcpy(data, value.data, value.len);
    }
    tx->context->bytes -= out->capacity;
    free(out->data);
    out->data = data;
    out->len = value.len;
    out->capacity = value.len;
}

static int
ngx_http_cache_purge_index_tx_charge(ngx_http_cache_purge_index_tx_t *tx,
    size_t size)
{
    if (!ngx_http_cache_purge_index_require(
            tx->context, size <= NGX_HTTP_CACHE_PURGE_TX_BYTES - tx->charged,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "index transaction memory budget"))
    {
        return 0;
    }
    tx->charged += size;
    return 1;
}

static ngx_http_cache_purge_index_row_t *
ngx_http_cache_purge_index_tx_row(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key)
{
    ngx_http_cache_purge_index_row_t  *row;
    uint32_t                           hash;
    size_t                             i, bucket;

    if (tx->context->status != 0) {
        return NULL;
    }
    hash = 2166136261U ^ (uint32_t) cf;
    for (i = 0; i < key.len; i++) {
        hash = (hash ^ key.data[i]) * 16777619U;
    }
    bucket = hash & (NGX_HTTP_CACHE_PURGE_TX_BUCKETS - 1);
    for (row = tx->buckets[bucket]; row != NULL; row = row->next) {
        if (row->cf == cf
            && ngx_http_cache_purge_op_equal(
                key, ngx_http_cache_purge_index_bytes(&row->key)))
        {
            return row;
        }
    }
    row = ngx_http_cache_purge_index_tx_alloc(tx, sizeof(*row));
    if (row == NULL) {
        return NULL;
    }
    memset(row, 0, sizeof(*row));
    row->cf = cf;
    row->next = tx->buckets[bucket];
    tx->buckets[bucket] = row;
    ngx_http_cache_purge_index_tx_copy(tx, &row->key, key);
    return tx->context->status == 0 ? row : NULL;
}

static ngx_http_cache_purge_index_value_t
ngx_http_cache_purge_index_tx_get(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key)
{
    ngx_http_cache_purge_index_row_t   *row;
    ngx_http_cache_purge_index_value_t  value;
    ngx_http_cache_purge_query_t       *q;
    int                                 exists;

    memset(&value, 0, sizeof(value));
    row = ngx_http_cache_purge_index_tx_row(tx, cf, key);
    if (row == NULL) {
        return value;
    }
    if (row->changed) {
        value.exists = row->exists;
        value.bytes = ngx_http_cache_purge_index_bytes(&row->current);
        return value;
    }
    q = tx->context;
    if (!row->observed) {
        exists = ngx_http_cache_purge_index_get(q, cf, key, NULL);
        if (!ngx_http_cache_purge_index_tx_charge(tx, 64 + key.len
                                                          + q->scratch.len))
        {
            return value;
        }
        ngx_http_cache_purge_index_tx_copy(
            tx, &row->original, ngx_http_cache_purge_index_bytes(&q->scratch));
        row->existed = exists;
        row->observed = 1;
    }
    value.exists = row->existed;
    value.bytes = ngx_http_cache_purge_index_bytes(&row->original);
    return value;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_tx_need(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key)
{
    ngx_http_cache_purge_index_value_t  value;

    value = ngx_http_cache_purge_index_tx_get(tx, cf, key);
    ngx_http_cache_purge_index_require(tx->context, value.exists,
                                       NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                       "missing referenced index record");
    return value.bytes;
}

static void
ngx_http_cache_purge_index_tx_set(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_rocksdb_bytes_t value, int exists)
{
    ngx_http_cache_purge_index_row_t  *row;

    row = ngx_http_cache_purge_index_tx_row(tx, cf, key);
    if (row == NULL) {
        return;
    }
    if (row->changed) {
        tx->charged -= 64 + key.len + row->current.len;
    }
    if (!ngx_http_cache_purge_index_tx_charge(tx, 64 + key.len + value.len)) {
        return;
    }
    ngx_http_cache_purge_index_tx_copy(tx, &row->current, value);
    row->changed = 1;
    row->exists = exists;
}

static void
ngx_http_cache_purge_index_tx_commit(ngx_http_cache_purge_index_tx_t *tx)
{
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_row_t    *row;
    ngx_http_cache_rocksdb_mutation_t   *mutations;
    ngx_http_cache_rocksdb_condition_t  *conditions;
    size_t                               i, nc, nm;

    q = tx->context;
    if (q->status != 0) {
        return;
    }
    nc = 0;
    nm = 0;
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_TX_BUCKETS; i++) {
        for (row = tx->buckets[i]; row != NULL; row = row->next) {
            nc += row->observed;
            nm += row->changed;
        }
    }
    if (nm == 0) {
        return;
    }
    mutations =
        ngx_http_cache_purge_index_tx_alloc(tx, nm * sizeof(*mutations));
    conditions =
        nc != 0 ?
            ngx_http_cache_purge_index_tx_alloc(tx, nc * sizeof(*conditions)) :
            NULL;
    if (q->status == 0) {
        nc = 0;
        nm = 0;
        for (i = 0; i < NGX_HTTP_CACHE_PURGE_TX_BUCKETS; i++) {
            for (row = tx->buckets[i]; row != NULL; row = row->next) {
                if (row->observed) {
                    conditions[nc].cf = row->cf;
                    conditions[nc].key =
                        ngx_http_cache_purge_index_bytes(&row->key);
                    conditions[nc].value =
                        ngx_http_cache_purge_index_bytes(&row->original);
                    conditions[nc++].exists = row->existed;
                }
                if (row->changed) {
                    mutations[nm].cf = row->cf;
                    mutations[nm].type = row->exists ?
                                             NGX_HTTP_CACHE_ROCKSDB_PUT :
                                             NGX_HTTP_CACHE_ROCKSDB_DELETE;
                    mutations[nm].key =
                        ngx_http_cache_purge_index_bytes(&row->key);
                    mutations[nm++].value =
                        ngx_http_cache_purge_index_bytes(&row->current);
                }
            }
        }
        q->status = ngx_http_cache_rocksdb_write(q->store, mutations, nm,
                                                 conditions, nc, &q->error);
    }
    if (mutations != NULL) {
        q->bytes -= nm * sizeof(*mutations);
    }
    if (conditions != NULL) {
        q->bytes -= nc * sizeof(*conditions);
    }
    free(mutations);
    free(conditions);
}

static int
ngx_http_cache_purge_index_tx_end(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t      *q;
    ngx_http_cache_purge_index_row_t  *row, *next;
    size_t                             i;
    int                                rc;

    q = tx->context;
    ngx_http_cache_purge_index_cursor_close(q, &q->outer);
    ngx_http_cache_purge_index_cursor_close(q, &q->links);
    ngx_http_cache_purge_index_cursor_close(q, &q->files);
    rc = q->status;
    if (error != NULL) {
        *error = q->error;
        error->code = rc;
    }
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_TX_BUCKETS; i++) {
        for (row = tx->buckets[i]; row != NULL; row = next) {
            next = row->next;
            free(row->key.data);
            free(row->original.data);
            free(row->current.data);
            free(row);
        }
    }
    free(tx);
    ngx_http_cache_purge_query_close(&q, NULL);
    return rc;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_id(const unsigned char id[16])
{
    ngx_http_cache_rocksdb_bytes_t  value;

    value.data = id;
    value.len = 16;
    return value;
}

static int
ngx_http_cache_purge_index_zero(const unsigned char id[16])
{
    unsigned  i, value;

    value = 0;
    for (i = 0; i < 16; i++) {
        value |= id[i];
    }
    return value == 0;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_object_key(ngx_http_cache_purge_query_t *q,
    unsigned char kind, const unsigned char id[16])
{
    q->key.len = 0;
    ngx_http_cache_purge_index_append(q, &q->key, &kind, 1);
    ngx_http_cache_purge_index_append(q, &q->key, id, 16);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_path_key(ngx_http_cache_purge_query_t *q,
    unsigned char kind, const ngx_http_cache_purge_index_record_t *record)
{
    ngx_http_cache_purge_index_row(q, &q->key, kind, record->refs[4]);
    ngx_http_cache_purge_index_append(q, &q->key, record->path.data,
                                      record->path.len);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static int
ngx_http_cache_purge_index_load(ngx_http_cache_purge_index_tx_t *tx,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_purge_index_record_t *record)
{
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_value_t  value;

    q = tx->context;
    value = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'O', file->id));
    if (!ngx_http_cache_purge_index_require(q, value.exists,
                                            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
                                            "file instance not indexed")
        || !ngx_http_cache_purge_index_decode(q, value.bytes, record))
    {
        return 0;
    }
    return ngx_http_cache_purge_index_require(
               q, memcmp(record->id, file->id, 16) == 0,
               NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "file record identity mismatch")
           && ngx_http_cache_purge_index_require(
               q, record->revision == file->revision,
               NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "file revision changed");
}

static void
ngx_http_cache_purge_index_advance(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_record_t *record)
{
    if (ngx_http_cache_purge_index_require(q, record->revision != UINT64_MAX,
                                           NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                           "file revision exhausted"))
    {
        record->revision++;
    }
}

static void
ngx_http_cache_purge_index_save(ngx_http_cache_purge_index_tx_t *tx,
    const ngx_http_cache_purge_index_record_t *record)
{
    ngx_http_cache_purge_query_t  *q;
    unsigned char                  byte;
    size_t                         i;

    q = tx->context;
    q->lookup.len = 0;
    byte = 1;
    ngx_http_cache_purge_index_append(q, &q->lookup, &byte, 1);
    ngx_http_cache_purge_index_append(q, &q->lookup, record->id, 16);
    ngx_http_cache_purge_index_append(q, &q->lookup, record->previous, 16);
    byte = (unsigned char) record->state;
    ngx_http_cache_purge_index_append(q, &q->lookup, &byte, 1);
    ngx_http_cache_purge_index_number(q, &q->lookup, record->revision);
    for (i = 0; i < 8; i++) {
        ngx_http_cache_purge_index_number(q, &q->lookup, record->refs[i]);
    }
    ngx_http_cache_purge_index_string(q, &q->lookup, record->path);
    ngx_http_cache_purge_index_string(q, &q->lookup, record->metadata);
    ngx_http_cache_purge_index_string(q, &q->lookup, record->digest);
    byte = (unsigned char) record->tag_count;
    ngx_http_cache_purge_index_append(q, &q->lookup, &byte, 1);
    for (i = 0; i < record->tag_count; i++) {
        ngx_http_cache_purge_index_number(q, &q->lookup, record->tags[i]);
    }
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'O', record->id),
        ngx_http_cache_purge_index_bytes(&q->lookup), 1);
}

static void
ngx_http_cache_purge_index_retired(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_purge_index_record_t *record)
{
    ngx_http_cache_purge_query_t  *q;

    q = tx->context;
    record->state = 3;
    ngx_http_cache_purge_index_advance(q, record);
    q->lookup.len = 0;
    ngx_http_cache_purge_index_number(q, &q->lookup, record->revision);
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'g', record->id),
        ngx_http_cache_purge_index_bytes(&q->lookup), 1);
    ngx_http_cache_purge_index_save(tx, record);
}

static int
ngx_http_cache_purge_index_path_matches(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_value_t value, const unsigned char id[16])
{
    if (!value.exists) {
        return ngx_http_cache_purge_index_zero(id);
    }
    return ngx_http_cache_purge_index_require(q, value.bytes.len == 16,
                                              NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                              "invalid file instance ID")
           && memcmp(value.bytes.data, id, 16) == 0;
}

static void
ngx_http_cache_purge_index_apply_one(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_purge_index_action_e action,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_purge_index_match_t *out)
{
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_record_t  record, previous;
    ngx_http_cache_purge_index_value_t   intent, current;
    ngx_http_cache_rocksdb_bytes_t       value, id;

    q = tx->context;
    if (!ngx_http_cache_purge_index_load(tx, file, &record)) {
        return;
    }
    id = ngx_http_cache_purge_index_id(file->id);
    switch (action) {
    case NGX_HTTP_CACHE_PURGE_INDEX_COMMIT:
    case NGX_HTTP_CACHE_PURGE_INDEX_COMMIT_PENDING:
        if (!ngx_http_cache_purge_index_require(q, record.state == 1,
                                                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                                "file is not prepared"))
        {
            return;
        }
        value = ngx_http_cache_purge_index_tx_need(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'I', &record));
        ngx_http_cache_purge_index_require(
            q, ngx_http_cache_purge_op_equal(value, id),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "fill lease changed");
        current = ngx_http_cache_purge_index_tx_get(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'P', &record));
        if (!ngx_http_cache_purge_index_require(
                q,
                ngx_http_cache_purge_index_path_matches(q, current,
                                                        record.previous),
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "cache path changed before commit"))
        {
            return;
        }
        if (!ngx_http_cache_purge_index_zero(record.previous)) {
            value = ngx_http_cache_purge_index_tx_need(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_object_key(q, 'O', record.previous));
            if (!ngx_http_cache_purge_index_decode(q, value, &previous)
                || !ngx_http_cache_purge_index_require(
                    q,
                    memcmp(previous.id, record.previous, 16) == 0
                        && previous.state == 2
                        && previous.refs[4] == record.refs[4]
                        && ngx_http_cache_purge_op_equal(previous.path,
                                                         record.path),
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "previous path record is inconsistent"))
            {
                return;
            }
            ngx_http_cache_purge_index_retired(tx, &previous);
        }
        record.state = 2;
        ngx_http_cache_purge_index_advance(q, &record);
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'P', &record), id, 1);
        if (action == NGX_HTTP_CACHE_PURGE_INDEX_COMMIT) {
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_path_key(q, 'I', &record),
                ngx_http_cache_purge_op_empty, 0);
        }
        ngx_http_cache_purge_index_save(tx, &record);
        break;

    case NGX_HTTP_CACHE_PURGE_INDEX_PUBLISH:
    case NGX_HTTP_CACHE_PURGE_INDEX_DISCARD:
        if (!ngx_http_cache_purge_index_require(q, record.state == 2,
                                                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                                "file is not committed"))
        {
            return;
        }
        intent = ngx_http_cache_purge_index_tx_get(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'I', &record));
        if (action == NGX_HTTP_CACHE_PURGE_INDEX_DISCARD
            && !ngx_http_cache_purge_index_require(
                q,
                intent.exists
                    && ngx_http_cache_purge_op_equal(intent.bytes, id),
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "publication lease changed"))
        {
            return;
        }
        value = ngx_http_cache_purge_index_tx_need(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'P', &record));
        if (!ngx_http_cache_purge_index_require(
                q, ngx_http_cache_purge_op_equal(value, id),
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "cache path changed before publication")
            || !ngx_http_cache_purge_index_require(
                q,
                !intent.exists
                    || ngx_http_cache_purge_op_equal(intent.bytes, id),
                NGX_HTTP_CACHE_ROCKSDB_BUSY,
                "another fill owns the path lease"))
        {
            return;
        }
        if (intent.exists) {
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_path_key(q, 'I', &record),
                ngx_http_cache_purge_op_empty, 0);
        }
        if (action == NGX_HTTP_CACHE_PURGE_INDEX_DISCARD) {
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_path_key(q, 'P', &record),
                ngx_http_cache_purge_op_empty, 0);
            ngx_http_cache_purge_index_retired(tx, &record);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_INDEX_ABORT:
        if (!ngx_http_cache_purge_index_require(q, record.state == 1,
                                                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                                "file is not prepared"))
        {
            return;
        }
        value = ngx_http_cache_purge_index_tx_need(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'I', &record));
        if (!ngx_http_cache_purge_index_require(
                q, ngx_http_cache_purge_op_equal(value, id),
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "fill lease changed"))
        {
            return;
        }
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'I', &record),
            ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_retired(tx, &record);
        break;

    case NGX_HTTP_CACHE_PURGE_INDEX_RETIRE:
        if (record.state == 3) {
            break;
        }
        if (!ngx_http_cache_purge_index_require(
                q, record.state == 2, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "prepared file must be drained and aborted"))
        {
            return;
        }
        intent = ngx_http_cache_purge_index_tx_get(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'I', &record));
        if (!ngx_http_cache_purge_index_require(
                q, !intent.exists, NGX_HTTP_CACHE_ROCKSDB_BUSY,
                "cache path has an unfinished fill"))
        {
            return;
        }
        current = ngx_http_cache_purge_index_tx_get(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'P', &record));
        if (current.exists
            && ngx_http_cache_purge_index_path_matches(q, current, file->id))
        {
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_path_key(q, 'P', &record),
                ngx_http_cache_purge_op_empty, 0);
        }
        ngx_http_cache_purge_index_retired(tx, &record);
        break;
    }
    if (q->status == 0) {
        memcpy(out->id, record.id, 16);
        out->revision = record.revision;
        out->state = record.state;
    }
}

int
ngx_http_cache_purge_index_init(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t    *tx;
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_value_t  schema;
    ngx_http_cache_rocksdb_bytes_t      next;
    ngx_http_cache_rocksdb_cf_e        cf;

    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return store == NULL ? NGX_HTTP_CACHE_ROCKSDB_INVALID :
                               NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    schema = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_op_literal("M:index"));
    if (schema.exists) {
        if (!ngx_http_cache_purge_index_require(
                q,
                schema.bytes.len == 1
                    && (schema.bytes.data[0] == '1'
                        || schema.bytes.data[0] == '2'),
                NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unsupported file index schema"))
        {
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
        next = ngx_http_cache_purge_index_tx_need(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_op_literal("M:next"));
        ngx_http_cache_purge_index_require(
            q, ngx_http_cache_purge_index_integer(q, next) != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid index ID allocator");
        if (schema.bytes.data[0] == '1') {
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_op_literal("M:index"),
                ngx_http_cache_purge_op_literal("2"), 1);
        }
    } else if (q->status == 0) {
        for (cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
             cf < NGX_HTTP_CACHE_ROCKSDB_CF_COUNT && q->status == 0; cf++)
        {
            ngx_http_cache_purge_index_cursor_open(
                q, &q->outer, cf, ngx_http_cache_purge_op_empty,
                ngx_http_cache_purge_op_empty, NULL);
            while (ngx_http_cache_purge_index_cursor_next(q, &q->outer)) {
                if (!ngx_http_cache_purge_index_require(
                        q,
                        cf == NGX_HTTP_CACHE_ROCKSDB_DEFAULT
                            && q->outer.key.len != 0
                            && q->outer.key.data[0] == 0,
                        NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
                        "file index schema missing from non-empty store"))
                {
                    break;
                }
            }
            ngx_http_cache_purge_index_cursor_close(q, &q->outer);
        }
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_op_literal("M:index"),
            ngx_http_cache_purge_op_literal("2"), 1);
        q->lookup.len = 0;
        ngx_http_cache_purge_index_number(q, &q->lookup, 1);
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_op_literal("M:next"),
            ngx_http_cache_purge_index_bytes(&q->lookup), 1);
    }
    ngx_http_cache_purge_index_tx_commit(tx);
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_apply(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_purge_index_action_e action,
    const ngx_http_cache_purge_index_expected_t *files, size_t count,
    ngx_http_cache_purge_index_match_t *matches,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t    *tx;
    ngx_http_cache_purge_index_match_t  result[32];
    size_t                              i;

    if (matches != NULL && count <= 32) {
        memset(matches, 0, count * sizeof(*matches));
    }
    if (store == NULL || files == NULL || matches == NULL
        || (unsigned) action > NGX_HTTP_CACHE_PURGE_INDEX_RETIRE)
    {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid index transition arguments");
    }
    if (count == 0 || count > 32) {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "index batch must contain 1-32 files");
    }
    if (action != NGX_HTTP_CACHE_PURGE_INDEX_COMMIT
        && action != NGX_HTTP_CACHE_PURGE_INDEX_RETIRE && count != 1)
    {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "index action requires one file");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    memset(result, 0, sizeof(result));
    for (i = 0; i < count && tx->context->status == 0; i++) {
        ngx_http_cache_purge_index_apply_one(tx, action, &files[i], &result[i]);
    }
    ngx_http_cache_purge_index_tx_commit(tx);
    if (tx->context->status == 0) {
        memcpy(matches, result, count * sizeof(*matches));
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}
