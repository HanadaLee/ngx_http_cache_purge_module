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
