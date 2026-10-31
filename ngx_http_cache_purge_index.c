/*
 * Copyright (C) Hanada
 */

#include "ngx_http_cache_purge_internal.h"

#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NGX_HTTP_CACHE_PURGE_OP_RECORD  131072
#define NGX_HTTP_CACHE_PURGE_OP_MEMORY  (1024 * 1024)
#define NGX_HTTP_CACHE_PURGE_OP_ROWS    256
#define NGX_HTTP_CACHE_PURGE_OP_PAGE    262144

int
ngx_http_cache_purge_tags_decode(ngx_http_cache_rocksdb_bytes_t value,
    ngx_http_cache_rocksdb_bytes_t tags[64], size_t *count)
{
    size_t  at, i, len, common;
    int     order;

    if (count == NULL || tags == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }
    *count = 0;
    if (value.data == NULL || value.len < 7 || value.len > 32768
        || value.data[0] < 2 || value.data[0] > 64)
    {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }
    at = 1;
    for (i = 0; i < value.data[0]; i++) {
        if (value.len - at < 2) {
            return NGX_HTTP_CACHE_ROCKSDB_INVALID;
        }
        len = ((size_t) value.data[at] << 8) | value.data[at + 1];
        at += 2;
        if (len == 0 || len > 1024 || len > value.len - at) {
            return NGX_HTTP_CACHE_ROCKSDB_INVALID;
        }
        tags[i].data = value.data + at;
        tags[i].len = len;
        at += len;
        if (i != 0) {
            common = tags[i - 1].len < len ? tags[i - 1].len : len;
            order = memcmp(tags[i - 1].data, tags[i].data, common);
            if (order > 0 || (order == 0 && tags[i - 1].len >= len)) {
                return NGX_HTTP_CACHE_ROCKSDB_INVALID;
            }
        }
    }
    if (at != value.len) {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }
    *count = i;
    return 0;
}

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
    int                               deferred;
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

static int ngx_http_cache_purge_op_rules_migrate(
    ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_rocksdb_error_t *error);

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
    codec.size = kind == 'D' ? 1 + op->input.id.len :
                 kind == 'L' || kind == 'J' ? 9 : 17;
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
        if (kind != 'L' && kind != 'J') {
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
    ngx_http_cache_rocksdb_bytes_t  tags[64];
    size_t                          count;
    uint32_t                        kind;
    int                             code, tag, scope_valid;

    code = stored ? NGX_HTTP_CACHE_ROCKSDB_CORRUPT :
                    NGX_HTTP_CACHE_ROCKSDB_INVALID;
    if (!ngx_http_cache_purge_op_require(tx, input != NULL, code,
                                         "missing operation input")
        || !ngx_http_cache_purge_op_id(tx, input->id, code))
    {
        return 0;
    }
    kind = input->selector.kind;
    tag = kind >= NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG;
    scope_valid = tag
        ? (kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG
           || kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAGS
           || input->selector.scope.len != 0)
        : input->selector.scope.len == 0;
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
            tx, kind <= NGX_HTTP_CACHE_PURGE_HOST_TAGS, code,
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
                tag && kind <= NGX_HTTP_CACHE_PURGE_HOST_TAG ? 1024 : 32768)
                && ngx_http_cache_purge_op_bytes_valid(input->selector.scope,
                                                       32768),
            stored ? NGX_HTTP_CACHE_ROCKSDB_CORRUPT :
                     NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "operation selector exceeds limit")
        || !ngx_http_cache_purge_op_require(tx, scope_valid, code,
                                            "invalid selector scope")
        || !ngx_http_cache_purge_op_require(
            tx,
            input->selector.value.len != 0
                || kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX
                || kind == NGX_HTTP_CACHE_PURGE_URL_PREFIX,
            code, "empty operation selector"))
    {
        return 0;
    }
    return kind <= NGX_HTTP_CACHE_PURGE_HOST_TAG
           || ngx_http_cache_purge_op_require(tx,
                ngx_http_cache_purge_tags_decode(input->selector.value,
                                                  tags, &count) == 0,
                code, "invalid canonical TAG list");
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
    ngx_http_cache_rocksdb_bytes_t          result, digest;

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
    size_t                              i, nc, nm;

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
    if (tx->deferred) {
        tx->status = ngx_http_cache_rocksdb_write_deferred(tx->store,
            mutations, nm, conditions, nc, &tx->error);
    } else {
        tx->status = ngx_http_cache_rocksdb_write(tx->store, mutations, nm,
                                                  conditions, nc, &tx->error);
    }
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
    const ngx_http_cache_purge_operation_t *a, *b;
    ngx_http_cache_rocksdb_bytes_t          raw;
    uint64_t                                count, size, charge;

    a = &before->op;
    b = &after->op;
    if (a->stage == NGX_HTTP_CACHE_PURGE_PREPARING
        && b->stage != NGX_HTTP_CACHE_PURGE_PREPARING)
    {
        ngx_http_cache_purge_op_put(tx, ngx_http_cache_purge_op_key(tx, 'J', a),
                                    ngx_http_cache_purge_op_empty, 0);
    }
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
    size_t                          i, size;

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
    ngx_http_cache_rocksdb_buffer_t key, value;
    int                             rc;

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

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_op_rule_key(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_selector_t *selector)
{
    ngx_http_cache_purge_op_codec_t  codec;
    ngx_http_cache_rocksdb_bytes_t   key;

    codec.tx = tx;
    codec.size = 6 + selector->scope.len + selector->value.len;
    codec.at = 0;
    codec.data = ngx_http_cache_purge_op_alloc(tx, codec.size);
    ngx_http_cache_purge_op_put_number(&codec, 'T', 1);
    ngx_http_cache_purge_op_put_number(&codec, selector->kind, 1);
    ngx_http_cache_purge_op_put_bytes(&codec, selector->scope);
    if (tx->status != 0) {
        return ngx_http_cache_purge_op_empty;
    }
    if (selector->value.len != 0) {
        memcpy(codec.data + codec.at, selector->value.data,
               selector->value.len);
    }
    key.data = codec.data;
    key.len = codec.size;
    return key;
}

static void
ngx_http_cache_purge_op_rule_decode(ngx_http_cache_purge_op_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t raw, uint64_t requirements[2])
{
    ngx_http_cache_purge_op_codec_t  codec;

    if (!ngx_http_cache_purge_op_require(tx, raw.len == 16,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid selector requirement"))
    {
        return;
    }
    codec.tx = tx;
    codec.data = (unsigned char *) raw.data;
    codec.size = raw.len;
    codec.at = 0;
    requirements[0] = ngx_http_cache_purge_op_get_number(&codec, 8);
    requirements[1] = ngx_http_cache_purge_op_get_number(&codec, 8);
    ngx_http_cache_purge_op_require(tx, requirements[0] || requirements[1],
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "empty selector requirement");
}

static void
ngx_http_cache_purge_op_rule_member(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_operation_t *op,
    const ngx_http_cache_purge_selector_t *selector)
{
    ngx_http_cache_rocksdb_bytes_t   key, raw;
    ngx_http_cache_purge_op_value_t  previous;
    ngx_http_cache_purge_op_codec_t  codec;
    uint64_t                         requirements[2];
    unsigned                         slot;

    key = ngx_http_cache_purge_op_rule_key(tx, selector);
    previous = ngx_http_cache_purge_op_get(tx, key);
    memset(requirements, 0, sizeof(requirements));
    if (previous.exists) {
        ngx_http_cache_purge_op_rule_decode(tx, previous.bytes, requirements);
    }
    slot = op->input.mode == NGX_HTTP_CACHE_PURGE_DELETE;
    if (tx->status != 0 || requirements[slot] >= op->sequence) {
        return;
    }
    requirements[slot] = op->sequence;
    codec.tx = tx;
    codec.size = 16;
    codec.data = ngx_http_cache_purge_op_alloc(tx, codec.size);
    codec.at = 0;
    ngx_http_cache_purge_op_put_number(&codec, requirements[0], 8);
    ngx_http_cache_purge_op_put_number(&codec, requirements[1], 8);
    raw.data = codec.data;
    raw.len = codec.size;
    ngx_http_cache_purge_op_put(tx, key, raw, 1);
}

static void
ngx_http_cache_purge_op_rule_put(ngx_http_cache_purge_op_tx_t *tx,
    const ngx_http_cache_purge_operation_t *op)
{
    ngx_http_cache_purge_selector_t  selector;
    ngx_http_cache_rocksdb_bytes_t   tags[64];
    size_t                          count, i;

    selector = op->input.selector;
    if (selector.kind <= NGX_HTTP_CACHE_PURGE_HOST_TAG) {
        ngx_http_cache_purge_op_rule_member(tx, op, &selector);
        return;
    }
    if (!ngx_http_cache_purge_op_require(tx,
            ngx_http_cache_purge_tags_decode(selector.value, tags, &count) == 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid canonical TAG list"))
    {
        return;
    }
    selector.kind -= 2;
    for (i = 0; i < count && tx->status == 0; i++) {
        selector.value = tags[i];
        ngx_http_cache_purge_op_rule_member(tx, op, &selector);
    }
}

int
ngx_http_cache_purge_operations_requirements(
    ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_selector_t *selector, size_t *budget,
    uint64_t *invalidate, uint64_t *remove,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t            tx;
    ngx_http_cache_purge_operation_input_t  input;
    ngx_http_cache_purge_selector_t         prefix_selector;
    ngx_http_cache_rocksdb_cursor_t        *cursor;
    ngx_http_cache_rocksdb_bytes_t          key, prefix, raw, tags[64];
    ngx_http_cache_rocksdb_buffer_t         found, value;
    uint64_t                                requirements[2], result[2];
    unsigned char                           encoded[16];
    size_t                                  common, length, i, count;
    int                                     rc, is_prefix;

    if (invalidate != NULL) {
        *invalidate = 0;
    }
    if (remove != NULL) {
        *remove = 0;
    }
    ngx_http_cache_purge_op_begin(&tx, ops, invalidate);
    if (!ngx_http_cache_purge_op_require(&tx,
            selector != NULL && budget != NULL && remove != NULL,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid requirement lookup"))
    {
        return ngx_http_cache_purge_op_end(&tx, error);
    }
    memset(&input, 0, sizeof(input));
    input.id = ngx_http_cache_purge_op_literal("lookup");
    input.mode = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    input.issued_at_ms = 1;
    input.selector = *selector;
    if (!ngx_http_cache_purge_op_validate(&tx, &input, 0)) {
        return ngx_http_cache_purge_op_end(&tx, error);
    }
    if (selector->kind > NGX_HTTP_CACHE_PURGE_HOST_TAG) {
        rc = ngx_http_cache_purge_op_end(&tx, error);
        if (rc != 0) {
            return rc;
        }
        rc = ngx_http_cache_purge_tags_decode(selector->value, tags, &count);
        if (rc != 0) {
            return rc;
        }
        prefix_selector = *selector;
        prefix_selector.kind -= 2;
        for (i = 0; i < count; i++) {
            prefix_selector.value = tags[i];
            rc = ngx_http_cache_purge_operations_requirements(ops,
                &prefix_selector, budget, &requirements[0], &requirements[1],
                error);
            if (rc != 0) {
                *invalidate = 0;
                *remove = 0;
                return rc;
            }
            if (requirements[0] > *invalidate) {
                *invalidate = requirements[0];
            }
            if (requirements[1] > *remove) {
                *remove = requirements[1];
            }
        }
        return 0;
    }
    memset(result, 0, sizeof(result));
    key = ngx_http_cache_purge_op_rule_key(&tx, selector);
    value.data = encoded;
    value.capacity = sizeof(encoded);
    value.length = 0;
    cursor = NULL;
    is_prefix = selector->kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX
                || selector->kind == NGX_HTTP_CACHE_PURGE_URL_PREFIX;
    if (!is_prefix && ngx_http_cache_purge_op_require(&tx, *budget != 0,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "selector lookup budget exhausted"))
    {
        (*budget)--;
        rc = ngx_http_cache_rocksdb_get(tx.store,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, &value, &tx.error);
        if (rc == 0) {
            raw.data = encoded;
            raw.len = value.length;
            ngx_http_cache_purge_op_rule_decode(&tx, raw, result);
        } else if (rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            ngx_http_cache_purge_op_fail(&tx,
                rc == NGX_HTTP_CACHE_ROCKSDB_BUFFER
                    ? NGX_HTTP_CACHE_ROCKSDB_CORRUPT : rc,
                "cannot read selector requirement");
        }
    }
    if (tx.status == 0
        && (is_prefix || selector->kind <= NGX_HTTP_CACHE_PURGE_URL))
    {
        prefix_selector = *selector;
        if (!is_prefix) {
            prefix_selector.kind += 2;
        }
        key = ngx_http_cache_purge_op_rule_key(&tx, &prefix_selector);
        prefix = key;
        prefix.len = 6;
        found.data = ngx_http_cache_purge_op_alloc(&tx, 32774);
        found.capacity = 32774;
        found.length = 0;
        if (tx.status == 0) {
            tx.status = ngx_http_cache_rocksdb_cursor_open(tx.store,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix,
                ngx_http_cache_purge_op_empty, &cursor, &tx.error);
        }
        while (tx.status == 0) {
            if (!ngx_http_cache_purge_op_require(&tx, *budget != 0,
                    NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                    "selector lookup budget exhausted"))
            {
                break;
            }
            (*budget)--;
            rc = ngx_http_cache_rocksdb_cursor_seek_le(cursor,
                key, &found, &value, &tx.error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
                break;
            }
            if (rc != 0) {
                ngx_http_cache_purge_op_fail(&tx,
                    rc == NGX_HTTP_CACHE_ROCKSDB_BUFFER
                        ? NGX_HTTP_CACHE_ROCKSDB_CORRUPT : rc,
                    "cannot seek selector prefix requirement");
                break;
            }
            length = found.length - prefix.len;
            common = 0;
            while (common < length && common < key.len - prefix.len
                   && found.data[prefix.len + common]
                      == key.data[prefix.len + common])
            {
                common++;
            }
            if (common == length) {
                raw.data = value.data;
                raw.len = value.length;
                ngx_http_cache_purge_op_rule_decode(&tx, raw, requirements);
                for (i = 0; i < 2 && tx.status == 0; i++) {
                    if (requirements[i] > result[i]) {
                        result[i] = requirements[i];
                    }
                }
                if (length == 0) {
                    break;
                }
                common--;
            }
            /* No prefix longer than this common part can precede target:
             * it would sort after the predecessor we just obtained. */
            key.len = prefix.len + common;
        }
    }
    if (cursor != NULL) {
        ngx_http_cache_rocksdb_cursor_close(&cursor, NULL);
    }
    if (tx.status == 0) {
        *invalidate = result[0];
        *remove = result[1];
    }
    return ngx_http_cache_purge_op_end(&tx, error);
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
    ngx_http_cache_purge_op_value_t   schema, value;
    ngx_http_cache_rocksdb_bytes_t  index, ledger;
    size_t                          i;
    uint64_t                        next, retired;
    int                             changed, rc;
    const char                 *prefixes[] = { "D", "L", "Q", "E", "T", "J" };
    const char *keys[] =
        { "M:op-next", "M:op-live", "M:op-bytes",
          "M:rule-replay", "M:rule-cursor" };
    static const unsigned char zero[16] = { 0 };

    memset(&candidate, 0, sizeof(candidate));
    changed = 0;
    next = 1;
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
                                      ngx_http_cache_purge_op_literal("14")),
        NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
        "operation ledger requires supported file index");
    schema = ngx_http_cache_purge_op_get(
        &tx, ngx_http_cache_purge_op_literal("M:operations"));
    if (!schema.exists && tx.status == 0) {
        for (i = 0; i < 6 && tx.status == 0; i++) {
            ngx_http_cache_purge_op_cursor_open(&tx, &cursor, prefixes[i],
                                                ngx_http_cache_purge_op_empty);
            ngx_http_cache_purge_op_require(
                &tx, !ngx_http_cache_purge_op_cursor_next(&tx, &cursor),
                NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unversioned operation ledger");
            ngx_http_cache_purge_op_cursor_close(&tx, &cursor);
        }
        for (i = 0; i < 5 && tx.status == 0; i++) {
            value = ngx_http_cache_purge_op_get(
                &tx, ngx_http_cache_purge_op_literal(keys[i]));
            ngx_http_cache_purge_op_require(&tx, !value.exists,
                                            NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
                                            "unversioned operation metadata");
        }
        ngx_http_cache_purge_op_put(
            &tx, ngx_http_cache_purge_op_literal("M:operations"),
            ngx_http_cache_purge_op_literal("4"), 1);
        ngx_http_cache_purge_op_set_counter(&tx, "M:op-next", 1);
        ngx_http_cache_purge_op_set_counter(&tx, "M:op-live", 0);
        ngx_http_cache_purge_op_set_counter(&tx, "M:op-bytes", 0);
        ngx_http_cache_purge_op_set_counter(&tx, "M:rule-replay", 0);
        changed = 1;
    } else if (tx.status == 0) {
        ngx_http_cache_purge_op_require(
            &tx,
            ngx_http_cache_purge_op_equal(schema.bytes,
                                          ngx_http_cache_purge_op_literal("1"))
                || ngx_http_cache_purge_op_equal(schema.bytes,
                                          ngx_http_cache_purge_op_literal("2"))
                || ngx_http_cache_purge_op_equal(schema.bytes,
                                          ngx_http_cache_purge_op_literal("3"))
                || ngx_http_cache_purge_op_equal(schema.bytes,
                                          ngx_http_cache_purge_op_literal("4")),
            NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
            "unsupported operation ledger schema");
        next = ngx_http_cache_purge_op_counter(&tx, "M:op-next");
        ngx_http_cache_purge_op_require(
            &tx, next != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "invalid operation sequence allocator");
        (void) ngx_http_cache_purge_op_counter(&tx, "M:op-live");
        (void) ngx_http_cache_purge_op_counter(&tx, "M:op-bytes");
        if (tx.status == 0 && schema.bytes.data[0] == '1') {
            ngx_http_cache_purge_op_set_counter(&tx, "M:rule-replay", 0);
            ngx_http_cache_purge_op_put(&tx,
                ngx_http_cache_purge_op_literal("M:rule-cursor"),
                ngx_http_cache_purge_op_literal("D"), 1);
            changed = 1;
        }
        if (tx.status == 0 && schema.bytes.data[0] != '4') {
            ngx_http_cache_purge_op_put(&tx,
                ngx_http_cache_purge_op_literal("M:operations"),
                ngx_http_cache_purge_op_literal("4"), 1);
            changed = 1;
        }
    }
    value = ngx_http_cache_purge_op_get(&tx,
        ngx_http_cache_purge_op_literal("M:op-ledger"));
    if (tx.status == 0 && !value.exists) {
        if (ngx_http_cache_purge_op_require(&tx,
                RAND_bytes(candidate.ledger, sizeof(candidate.ledger)) == 1
                && memcmp(candidate.ledger, zero, sizeof(zero)) != 0,
                NGX_HTTP_CACHE_ROCKSDB_IO, "cannot create ledger identity"))
        {
            ledger.data = candidate.ledger;
            ledger.len = sizeof(candidate.ledger);
            ngx_http_cache_purge_op_put(&tx,
                ngx_http_cache_purge_op_literal("M:op-ledger"), ledger, 1);
            changed = 1;
        }
    } else if (tx.status == 0
               && ngx_http_cache_purge_op_require(&tx,
                   value.bytes.len == sizeof(candidate.ledger)
                   && memcmp(value.bytes.data, zero, sizeof(zero)) != 0,
                   NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid ledger identity"))
    {
        memcpy(candidate.ledger, value.bytes.data, sizeof(candidate.ledger));
    }
    value = ngx_http_cache_purge_op_get(&tx,
        ngx_http_cache_purge_op_literal("M:op-retired"));
    if (tx.status == 0 && !value.exists) {
        /* A stopped upgrade cannot prove which older operations were removed.
         * Reject old writes without scanning existing file records.
         */
        ngx_http_cache_purge_op_set_counter(&tx, "M:op-retired", next - 1);
        changed = 1;
    } else if (tx.status == 0) {
        retired = ngx_http_cache_purge_op_integer(&tx, value.bytes);
        ngx_http_cache_purge_op_require(&tx, retired < next,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid retired origin boundary");
    }
    if (changed) {
        ngx_http_cache_purge_op_commit(&tx);
    }
    if (tx.status == 0) {
        *ops = candidate;
    }
    rc = ngx_http_cache_purge_op_end(&tx, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_op_rules_migrate(ops, error);
        if (rc != 0) {
            memset(ops, 0, sizeof(*ops));
        }
    }
    return rc;
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

static void
ngx_http_cache_purge_op_boundary(ngx_http_cache_purge_op_tx_t *tx,
    uint64_t *last, uint64_t *retired)
{
    uint64_t  next;

    next = ngx_http_cache_purge_op_counter(tx, "M:op-next");
    ngx_http_cache_purge_op_require(tx, next != 0,
                                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                    "invalid operation sequence allocator");
    *last = next - 1;
    *retired = ngx_http_cache_purge_op_counter(tx, "M:op-retired");
    ngx_http_cache_purge_op_require(tx, *retired < next,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid retired origin boundary");
}

int
ngx_http_cache_purge_operations_usage(ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_purge_operation_usage_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t            tx;
    ngx_http_cache_purge_operation_usage_t  usage;

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    ngx_http_cache_purge_op_boundary(&tx, &usage.last_sequence,
                                     &usage.retired_sequence);
    usage.live_records = ngx_http_cache_purge_op_counter(&tx, "M:op-live");
    usage.live_selector_bytes =
        ngx_http_cache_purge_op_counter(&tx, "M:op-bytes");
    if (tx.status == 0) {
        *out = usage;
    }
    return ngx_http_cache_purge_op_end(&tx, error);
}

int
ngx_http_cache_purge_operations_origin(ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_watermark_t *origin,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t  tx;
    uint64_t                      last, retired;

    ngx_http_cache_purge_op_begin(&tx, ops, &last);
    /* Publication needs the writable interval, not live-operation usage. */
    ngx_http_cache_purge_op_boundary(&tx, &last, &retired);
    if (tx.status == 0
        && ((origin == NULL && retired != 0)
        || (origin != NULL
            && (memcmp(origin->ledger, ops->ledger, 16)
                || origin->sequence < retired || origin->sequence > last))))
    {
        ngx_http_cache_purge_op_fail(&tx, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                     "origin outside writable boundary");
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
    ngx_http_cache_purge_op_value_t old;
    uint64_t                        count, size, charge, sequence;

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
    ngx_http_cache_rocksdb_bytes_t    raw, key;
    ngx_http_cache_purge_op_value_t   queued;
    uint64_t                         count, size;
    int                              replay;

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
            ngx_http_cache_purge_op_rule_put(&tx, &stored.op);
            ngx_http_cache_purge_op_require(&tx,
                ngx_http_cache_purge_op_counter(&tx, "M:rule-replay")
                    == stored.op.sequence - 1,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "selector requirements have not caught up with the ledger");
            ngx_http_cache_purge_op_set_counter(&tx, "M:rule-replay",
                                                stored.op.sequence);
        }
        if (commit == 2 && stored.op.stage == NGX_HTTP_CACHE_PURGE_PREPARING) {
            key = ngx_http_cache_purge_op_key(&tx, 'J', &stored.op);
            queued = ngx_http_cache_purge_op_get(&tx, key);
            if (queued.exists) {
                ngx_http_cache_purge_op_require(&tx,
                    ngx_http_cache_purge_op_equal(queued.bytes, input->id),
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "activation queue identity changed");
            } else {
                ngx_http_cache_purge_op_put(&tx, key, input->id, 1);
            }
        }
        if (commit) {
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

int
ngx_http_cache_purge_operations_submit(ngx_http_cache_purge_operations_t *ops,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_op_accept(ops, input, now, propagation_ms, 2,
                                          out, error);
}

int
ngx_http_cache_purge_operations_activation(
    ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_cursor_t  cursor;
    ngx_http_cache_purge_op_stored_t  stored;

    ngx_http_cache_purge_op_begin(&tx, ops, out);
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    ngx_http_cache_purge_op_cursor_open(&tx, &cursor, "J",
                                          ngx_http_cache_purge_op_empty);
    if (ngx_http_cache_purge_op_cursor_next(&tx, &cursor)) {
        if (ngx_http_cache_purge_op_require(&tx, cursor.key.len == 9,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid activation queue key")
            && ngx_http_cache_purge_op_load(&tx, cursor.value, &stored) == 0
            && ngx_http_cache_purge_op_require(&tx,
                stored.op.stage == NGX_HTTP_CACHE_PURGE_PREPARING
                && ngx_http_cache_purge_op_equal(cursor.key,
                    ngx_http_cache_purge_op_key(&tx, 'J', &stored.op)),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "activation queue does not match its operation"))
        {
            ngx_http_cache_purge_op_clone(&tx, &stored.op, out);
        }
    } else if (tx.status == 0) {
        ngx_http_cache_purge_op_fail(&tx, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
                                        "no operation awaits activation");
    }
    ngx_http_cache_purge_op_cursor_close(&tx, &cursor);
    return ngx_http_cache_purge_op_result(&tx, out, error);
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
    ngx_http_cache_purge_op_tx_t     tx;
    ngx_http_cache_purge_op_stored_t before, after;

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

static int
ngx_http_cache_purge_operations_checkpoint_internal(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, const ngx_http_cache_purge_checkpoint_t *point,
    ngx_http_cache_purge_operation_t *out, int deferred,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t     tx;
    ngx_http_cache_purge_op_stored_t before, after;
    unsigned char                    digest[32];

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
        if (tx.status == 0 && !deferred) {
            tx.status = ngx_http_cache_rocksdb_sync(ops->store, &tx.error);
        }
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
    tx.deferred = deferred;
    return ngx_http_cache_purge_op_update(&tx, &before, &after, out, error);
}

int
ngx_http_cache_purge_operations_checkpoint(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, const ngx_http_cache_purge_checkpoint_t *point,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_operations_checkpoint_internal(ops, id,
        revision, point, out, 0, error);
}

int
ngx_http_cache_purge_operations_checkpoint_deferred(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, const ngx_http_cache_purge_checkpoint_t *point,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_operations_checkpoint_internal(ops, id,
        revision, point, out, 1, error);
}

int
ngx_http_cache_purge_operations_materialize(
    ngx_http_cache_purge_operations_t *ops, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t revision, uint64_t quiescent_sequence,
    ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t     tx;
    ngx_http_cache_purge_op_stored_t before, after;
    uint64_t                        retired, next;

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
    next = ngx_http_cache_purge_op_counter(&tx, "M:op-next");
    retired = ngx_http_cache_purge_op_counter(&tx, "M:op-retired");
    if (!ngx_http_cache_purge_op_require(&tx, retired < next,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid retired origin boundary")
        || !ngx_http_cache_purge_op_require(&tx, quiescent_sequence < next,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "quiescent boundary is in future"))
    {
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
    after.op.last_error = ngx_http_cache_purge_op_empty;
    after.op.next_run_ms = 0;
    if (before.op.sequence > retired) {
        ngx_http_cache_purge_op_set_counter(&tx, "M:op-retired",
                                              before.op.sequence);
    }
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
    ngx_http_cache_rocksdb_bytes_t   clock, expected;
    size_t                           remaining, key_size;

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

static int
ngx_http_cache_purge_op_rules_migrate(ngx_http_cache_purge_operations_t *ops,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t           tx;
    ngx_http_cache_purge_op_value_t        after;
    ngx_http_cache_purge_op_stored_t       stored;
    ngx_http_cache_rocksdb_cursor_t       *cursor;
    ngx_http_cache_rocksdb_buffer_t        key, value;
    ngx_http_cache_rocksdb_bytes_t         raw;
    uint64_t                               replay, next;
    int                                    rc;

    for ( ;; ) {
        ngx_http_cache_purge_op_begin(&tx, ops, &stored);
        replay = ngx_http_cache_purge_op_counter(&tx, "M:rule-replay");
        next = ngx_http_cache_purge_op_counter(&tx, "M:op-next");
        after = ngx_http_cache_purge_op_get(&tx,
            ngx_http_cache_purge_op_literal("M:rule-cursor"));
        if (!ngx_http_cache_purge_op_require(&tx, next != 0 && replay < next,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid selector requirement replay boundary")
            || (!after.exists && replay == next - 1))
        {
            return ngx_http_cache_purge_op_end(&tx, error);
        }
        if (!ngx_http_cache_purge_op_require(&tx,
                after.exists && after.bytes.len != 0 && after.bytes.len <= 129
                    && after.bytes.data[0] == 'D',
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid selector requirement replay cursor"))
        {
            return ngx_http_cache_purge_op_end(&tx, error);
        }
        cursor = NULL;
        key.capacity = 129;
        key.length = 0;
        key.data = ngx_http_cache_purge_op_alloc(&tx, key.capacity);
        value.capacity = NGX_HTTP_CACHE_PURGE_OP_RECORD;
        value.length = 0;
        value.data = ngx_http_cache_purge_op_alloc(&tx, value.capacity);
        rc = tx.status;
        if (rc == 0) {
            rc = ngx_http_cache_rocksdb_cursor_open(tx.store,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_op_literal("D"), after.bytes,
                &cursor, &tx.error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_rocksdb_cursor_next(cursor,
                &key, &value, &tx.error);
        }
        if (cursor != NULL) {
            ngx_http_cache_rocksdb_cursor_close(&cursor, NULL);
        }
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            if (ngx_http_cache_purge_op_require(&tx, replay == next - 1,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "selector replay has missing operation records"))
            {
                ngx_http_cache_purge_op_put(&tx,
                    ngx_http_cache_purge_op_literal("M:rule-cursor"),
                    ngx_http_cache_purge_op_empty, 0);
                ngx_http_cache_purge_op_commit(&tx);
            }
            return ngx_http_cache_purge_op_end(&tx, error);
        }
        tx.status = rc;
        raw.data = value.data;
        raw.len = value.length;
        if (tx.status == 0
            && ngx_http_cache_purge_op_decode(&tx, raw, &stored) == 0)
        {
            raw.data = key.data;
            raw.len = key.length;
            if (ngx_http_cache_purge_op_require(&tx,
                    replay < next - 1 && stored.op.sequence < next
                        && ngx_http_cache_purge_op_equal(raw,
                            ngx_http_cache_purge_op_key(&tx, 'D', &stored.op)),
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "selector replay has inconsistent operation records"))
            {
                ngx_http_cache_purge_op_rule_put(&tx, &stored.op);
                ngx_http_cache_purge_op_set_counter(&tx,
                    "M:rule-replay", replay + 1);
                ngx_http_cache_purge_op_put(&tx,
                    ngx_http_cache_purge_op_literal("M:rule-cursor"), raw, 1);
                ngx_http_cache_purge_op_commit(&tx);
            }
        }
        rc = ngx_http_cache_purge_op_end(&tx, error);
        if (rc != 0) {
            return rc;
        }
    }
}

int
ngx_http_cache_purge_operations_expire(ngx_http_cache_purge_operations_t *ops,
    uint64_t now, size_t limit, size_t *expired,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_op_tx_t      tx;
    ngx_http_cache_purge_op_cursor_t  cursor;
    ngx_http_cache_purge_op_stored_t before, after;
    ngx_http_cache_rocksdb_bytes_t   clock, encoded, expected;
    size_t                           count, remaining, cost;

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
    ngx_http_cache_purge_index_buffer_t  next;
    uint64_t                             remaining;
    unsigned                             catalog;
    unsigned                             resumed;
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

typedef struct ngx_http_cache_purge_index_tx_s  ngx_http_cache_purge_index_tx_t;

static int ngx_http_cache_purge_index_zero(const unsigned char id[16]);

static int
ngx_http_cache_purge_index_tx_read(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key);

typedef struct ngx_http_cache_purge_index_group_s
    ngx_http_cache_purge_index_group_t;

struct ngx_http_cache_purge_query_s {
    ngx_http_cache_purge_index_tx_t     *transaction;
    ngx_http_cache_rocksdb_t            *store;
    ngx_http_cache_rocksdb_error_t       error;
    int                                  status;
    size_t                               bytes;
    unsigned                             kind;
    unsigned                             key_nibble;
    int                                  joined;
    uint64_t                             host;
    uint64_t                             tag;
    int                                  empty;
    int                                  limited;
    size_t                               advances;
    unsigned char                        recovery;
    unsigned char                        signature[32];
    ngx_http_cache_rocksdb_cf_e          outer_cf;
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
    ngx_http_cache_purge_index_group_t  *group;
    const uint64_t                      *exclude_tags;
    size_t                               exclude_count;
};

struct ngx_http_cache_purge_index_group_s {
    ngx_http_cache_purge_query_t  *child;
    ngx_http_cache_rocksdb_bytes_t scope, tags[64];
    uint64_t                      ids[64];
    size_t                        count, current;
    unsigned char                 storage[1];
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

static int
ngx_http_cache_purge_index_catalog_kind(unsigned kind)
{
    return kind == 'I' || kind == 'g' || kind == 'd' || kind == 'G'
           || kind == 'z';
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_catalog_name(unsigned char name[11], unsigned kind)
{
    ngx_http_cache_rocksdb_bytes_t  key;

    memcpy(name, "M:catalog:", 10);
    name[10] = (unsigned char) kind;
    key.data = name;
    key.len = 11;
    return key;
}

static int
ngx_http_cache_purge_index_catalog_decode(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t raw, unsigned kind, uint64_t *count,
    ngx_http_cache_rocksdb_bytes_t *before,
    ngx_http_cache_rocksdb_bytes_t *after)
{
    ngx_http_cache_purge_index_reader_t  r;

    r.query = q;
    r.data = raw;
    r.at = 0;
    ngx_http_cache_purge_index_require(q,
        ngx_http_cache_purge_index_read_number(&r, 1) == 1,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid work catalog version");
    if (count != NULL) {
        *count = ngx_http_cache_purge_index_read_number(&r, 8);
    }
    *before = ngx_http_cache_purge_index_read_string(&r);
    *after = ngx_http_cache_purge_index_read_string(&r);
    return ngx_http_cache_purge_index_require(q,
        r.at == raw.len
        && (before->len == 0
            || (before->len > 1 && before->len <= 32784
                && before->data[0] == kind))
        && (after->len == 0
            || (after->len > 1 && after->len <= 32784
                && after->data[0] == kind))
        && (count == NULL
            || ((*count == 0) == (before->len == 0)
                && (*count == 0) == (after->len == 0))),
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid work catalog links");
}

static void ngx_http_cache_purge_index_catalog_open(
    ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor, unsigned kind,
    ngx_http_cache_rocksdb_bytes_t after,
    ngx_http_cache_rocksdb_cursor_t *view);
static int ngx_http_cache_purge_index_catalog_next(
    ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor);

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
    cursor->catalog = 0;
    if (cf == NGX_HTTP_CACHE_ROCKSDB_DEFAULT && prefix.len == 1
        && ngx_http_cache_purge_index_catalog_kind(prefix.data[0]))
    {
        ngx_http_cache_purge_index_catalog_open(q, cursor, prefix.data[0],
                                               after, view);
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
    if (q->limited) {
        if (q->advances == 0) {
            ngx_http_cache_purge_index_fail(q, NGX_HTTP_CACHE_ROCKSDB_BUSY,
                                               "index scan yielded");
            return 0;
        }
        q->advances--;
    }
    if (cursor->catalog) {
        return ngx_http_cache_purge_index_catalog_next(q, cursor);
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
ngx_http_cache_purge_index_read(ngx_http_cache_purge_query_t *q,
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
ngx_http_cache_purge_index_get(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_rocksdb_cursor_t *view)
{
    return q->transaction != NULL ?
               ngx_http_cache_purge_index_tx_read(q->transaction, cf, key) :
               ngx_http_cache_purge_index_read(q, cf, key, view);
}

/* Catalog links are read through one snapshot, never a live transaction. */
static void
ngx_http_cache_purge_index_catalog_open(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor, unsigned kind,
    ngx_http_cache_rocksdb_bytes_t after, ngx_http_cache_rocksdb_cursor_t *view)
{
    ngx_http_cache_rocksdb_error_t   error;
    ngx_http_cache_rocksdb_bytes_t   key, first, last;
    unsigned char                   name[11];
    int                             rc;

    cursor->catalog = kind;
    cursor->resumed = 0;
    cursor->remaining = 0;
    cursor->next.len = 0;
    ngx_http_cache_purge_index_set(q, &cursor->value, after);
    cursor->key.len = 0;
    key = ngx_http_cache_purge_index_catalog_name(name, kind);
    rc = view != NULL ?
         ngx_http_cache_rocksdb_cursor_fork(view,
             NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, ngx_http_cache_purge_op_empty,
             &cursor->handle, &error) :
         ngx_http_cache_rocksdb_cursor_open_cached(q->store,
             NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, ngx_http_cache_purge_op_empty,
             &cursor->handle, &error);
    if (rc != 0) {
        ngx_http_cache_purge_index_fail(q, rc, error.message);
        return;
    }
    if (!ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_read(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                            key, cursor->handle),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing work catalog header")
        || !ngx_http_cache_purge_index_catalog_decode(q,
                ngx_http_cache_purge_index_bytes(&q->scratch), kind,
                &cursor->remaining, &first, &last))
    {
        return;
    }
    ngx_http_cache_purge_index_set(q, &cursor->next, first);
    if (cursor->value.len == 0) {
        return;
    }
    q->key.len = 0;
    ngx_http_cache_purge_index_append(q, &q->key, "q", 1);
    ngx_http_cache_purge_index_append(q, &q->key, cursor->value.data,
                                     cursor->value.len);
    if (ngx_http_cache_purge_index_read(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->key), cursor->handle))
    {
        if (ngx_http_cache_purge_index_catalog_decode(q,
                ngx_http_cache_purge_index_bytes(&q->scratch), kind, NULL,
                &first, &last))
        {
            ngx_http_cache_purge_index_set(q, &cursor->next, last);
            ngx_http_cache_purge_index_set(q, &cursor->key,
                ngx_http_cache_purge_index_bytes(&cursor->value));
            cursor->resumed = 1;
        }
    }
    /* A removed bookmark restarts at the current head. Actions remain
     * instance/revision guarded; deleted links cannot hide surviving work. */
}

static int
ngx_http_cache_purge_index_catalog_next(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor)
{
    ngx_http_cache_rocksdb_bytes_t  before, after;

    if (q->status != 0) {
        return 0;
    }
    if (cursor->next.len == 0) {
        ngx_http_cache_purge_index_require(q,
            cursor->resumed || cursor->remaining == 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "truncated work catalog");
        return 0;
    }
    if (!ngx_http_cache_purge_index_require(q, cursor->remaining != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "cyclic work catalog"))
    {
        return 0;
    }
    q->key.len = 0;
    ngx_http_cache_purge_index_append(q, &q->key, "q", 1);
    ngx_http_cache_purge_index_append(q, &q->key, cursor->next.data,
                                     cursor->next.len);
    if (!ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_read(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_bytes(&q->key), cursor->handle),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing work catalog node")
        || !ngx_http_cache_purge_index_catalog_decode(q,
                ngx_http_cache_purge_index_bytes(&q->scratch), cursor->catalog,
                NULL, &before, &after)
        || !ngx_http_cache_purge_index_require(q,
                ngx_http_cache_purge_op_equal(before,
                    ngx_http_cache_purge_index_bytes(&cursor->key)),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "work catalog backlink mismatch"))
    {
        return 0;
    }
    ngx_http_cache_purge_index_set(q, &cursor->key,
        ngx_http_cache_purge_index_bytes(&cursor->next));
    ngx_http_cache_purge_index_set(q, &cursor->next, after);
    if (!ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_read(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_bytes(&cursor->key), cursor->handle),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "work catalog row is missing"))
    {
        return 0;
    }
    ngx_http_cache_purge_index_set(q, &cursor->value,
        ngx_http_cache_purge_index_bytes(&q->scratch));
    cursor->remaining--;
    return q->status == 0;
}

/* K stores the binary internal identity, never a native cache key. */
static int
ngx_http_cache_purge_index_key_projection(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t text)
{
    unsigned char  raw[33], digit;
    size_t         i;

    if (text.len != 64 || text.data == NULL) {
        return 0;
    }
    memset(raw, 0, sizeof(raw));
    raw[0] = 'K';
    for (i = 0; i < 64; i++) {
        digit = text.data[i];
        if (digit >= '0' && digit <= '9') {
            digit -= '0';
        } else if (digit >= 'a' && digit <= 'f') {
            digit = (unsigned char) (digit - 'a' + 10);
        } else {
            return 0;
        }
        raw[1 + i / 2] = (unsigned char) ((raw[1 + i / 2] << 4) | digit);
    }
    q->key.len = 0;
    ngx_http_cache_purge_index_append(q, &q->key, raw, sizeof(raw));
    return q->status == 0;
}

static uint64_t
ngx_http_cache_purge_index_key_find(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t text)
{
    if (!ngx_http_cache_purge_index_key_projection(q, text)
        || !ngx_http_cache_purge_index_get(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->key), NULL))
    {
        return 0;
    }
    return ngx_http_cache_purge_index_integer(q,
                ngx_http_cache_purge_index_bytes(&q->scratch));
}

static int
ngx_http_cache_purge_index_key_prefix(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t text)
{
    ngx_http_cache_rocksdb_bytes_t  padded;
    unsigned char                   raw[64];

    if (text.len > sizeof(raw) || (text.len && text.data == NULL)) {
        return 0;
    }
    memset(raw, '0', sizeof(raw));
    if (text.len) {
        memcpy(raw, text.data, text.len);
    }
    padded.data = raw;
    padded.len = sizeof(raw);
    if (!ngx_http_cache_purge_index_key_projection(q, padded)) {
        return 0;
    }
    if (text.len & 1) {
        q->key_nibble = (q->key.data[1 + text.len / 2] >> 4) + 1;
    }
    ngx_http_cache_purge_index_append(q, &q->prefix, q->key.data,
                                      1 + text.len / 2);
    return q->status == 0;
}

static int
ngx_http_cache_purge_index_key_text(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_cursor_t *view, uint64_t id)
{
    static const unsigned char  hex[] = "0123456789abcdef";
    unsigned char              text[64], *digest;
    size_t                     i;

    ngx_http_cache_purge_index_row(q, &q->key, 'k', id);
    if (!ngx_http_cache_purge_index_get(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->key), view))
    {
        return 0;
    }
    if (!ngx_http_cache_purge_index_require(q, q->scratch.len == 72,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid key identity record"))
    {
        return 0;
    }
    digest = q->scratch.data + 40;
    for (i = 0; i < 32; i++) {
        text[2 * i] = hex[digest[i] >> 4];
        text[2 * i + 1] = hex[digest[i] & 15];
    }
    q->lookup.len = 0;
    ngx_http_cache_purge_index_append(q, &q->lookup, text, sizeof(text));
    return q->status == 0;
}

typedef struct {
    uint64_t                        references;
    unsigned                        format;
    size_t                          count;
    uint64_t                        parts[3];
    ngx_http_cache_rocksdb_bytes_t  raw;
} ngx_http_cache_purge_index_dictionary_t;

static int
ngx_http_cache_purge_index_dict_decode(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t data,
    ngx_http_cache_purge_index_dictionary_t *value)
{
    ngx_http_cache_purge_index_reader_t  r;
    size_t                               i;

    memset(value, 0, sizeof(*value));
    r.query = q;
    r.data = data;
    r.at = 0;
    ngx_http_cache_purge_index_require(
        q, ngx_http_cache_purge_index_read_number(&r, 1) == 1,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid value record version");
    value->references = ngx_http_cache_purge_index_read_number(&r, 8);
    value->format = ngx_http_cache_purge_index_read_number(&r, 1);
    if (value->format == 0) {
        value->raw = ngx_http_cache_purge_index_read_string(&r);
        ngx_http_cache_purge_index_require(
            q, value->raw.len <= NGX_HTTP_CACHE_PURGE_INDEX_STRING,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "oversized dictionary value");
    } else {
        if (!ngx_http_cache_purge_index_require(
                q, value->format == 2,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "unknown value representation"))
        {
            return 0;
        }
        value->count = ngx_http_cache_purge_index_read_number(&r, 1);
        if (!ngx_http_cache_purge_index_require(
                q, value->count == 3,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid component count"))
        {
            return 0;
        }
        for (i = 0; i < value->count; i++) {
            value->parts[i] = ngx_http_cache_purge_index_read_number(&r, 8);
        }
    }
    return ngx_http_cache_purge_index_require(q, r.at == r.data.len,
                                              NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                              "unexpected value record suffix");
}

static int
ngx_http_cache_purge_index_value(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_cursor_t *view, uint64_t id, int leaf)
{
    ngx_http_cache_purge_index_dictionary_t  value;
    ngx_http_cache_rocksdb_bytes_t           raw;
    size_t                                   i;

    ngx_http_cache_purge_index_row(q, &q->key, 'V', id);
    if (!ngx_http_cache_purge_index_get(
            q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->key), view))
    {
        if (leaf) {
            ngx_http_cache_purge_index_fail(q, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                            "missing value component");
        }
        return 0;
    }
    if (!ngx_http_cache_purge_index_dict_decode(
            q, ngx_http_cache_purge_index_bytes(&q->scratch), &value))
    {
        return 0;
    }
    if (value.format == 0) {
        if (!leaf) {
            ngx_http_cache_purge_index_set(q, &q->lookup, value.raw);
        }
        return q->status == 0;
    }
    if (!ngx_http_cache_purge_index_require(
            q, !leaf, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "recursive value components are unsupported"))
    {
        return 0;
    }
    q->lookup.len = 0;
    for (i = 0; i < value.count && q->status == 0; i++) {
        if (!ngx_http_cache_purge_index_value(q, view, value.parts[i], 1)) {
            break;
        }
        raw.data = q->scratch.data + 18;
        raw.len = q->scratch.len - 18;
        if (i == 1) {
            ngx_http_cache_purge_index_append(q, &q->lookup, "://", 3);
        }
        ngx_http_cache_purge_index_append(q, &q->lookup, raw.data, raw.len);
        ngx_http_cache_purge_index_require(q,
            q->lookup.len <= NGX_HTTP_CACHE_PURGE_INDEX_STRING,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "reconstructed URL is too long");
    }
    return q->status == 0;
}

static uint64_t
ngx_http_cache_purge_index_find(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t text)
{
    unsigned char                         prefix[33];
    ngx_http_cache_rocksdb_bytes_t        range, number;
    ngx_http_cache_rocksdb_error_t        error;
    ngx_http_cache_purge_index_cursor_t  *cursor;
    uint64_t                              id;
    unsigned                              candidates;
    int                                   rc;

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
    ngx_http_cache_purge_index_cursor_close(q, cursor);
    if (q->status != 0) {
        return 0;
    }

    /* Digest probes inspect at most 16 candidates; bulk cursors stay cold. */
    rc = ngx_http_cache_rocksdb_cursor_open_cached(q->store,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, range, ngx_http_cache_purge_op_empty,
        &cursor->handle, &error);
    if (rc != 0) {
        ngx_http_cache_purge_index_fail(q, rc, error.message);
        return 0;
    }
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
    ngx_http_cache_purge_index_reader_t  r;
    ngx_http_cache_rocksdb_bytes_t       id;
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
    return ngx_http_cache_purge_index_decode(
        q, ngx_http_cache_purge_index_bytes(&q->scratch), record);
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
    if (q->current_link.len != 0 && q->current_link.data[0] == 'A'
        && !ngx_http_cache_purge_index_require(q,
            q->current_link.len == 25 && q->scratch.len == 72
                && memcmp(q->scratch.data + 8,
                          q->current_link.data + 1, 8) == 0
                && memcmp(q->scratch.data + 24,
                          q->current_link.data + 9, 16) == 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "URL variant posting disagrees with key identity"))
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
    if (!ngx_http_cache_purge_index_compatible(after, range, 16)) {
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
        number = ngx_http_cache_purge_index_bytes(&q->scratch);
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
    size_t                              i, j;
    int                                 tagged, global, found;

    tagged = cf == NGX_HTTP_CACHE_ROCKSDB_TAG;
    key = ngx_http_cache_purge_index_bytes(&cursor->key);
    global = tagged && key.len != 0 && key.data[0] == 'T';
    if (!ngx_http_cache_purge_index_require(q,
            key.len == (tagged && !global ? 33U : 25U),
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
        tag = scope;
        if (!global) {
            ngx_http_cache_purge_index_require(q,
                record.refs[key.data[0] == 'N' ? 3 : 2] == scope,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "TAG posting scope mismatch");
            number.data = key.data + 9;
            tag = ngx_http_cache_purge_index_integer(q, number);
        }
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
    if (q->status != 0) {
        return 0;
    }
    ngx_http_cache_purge_index_bookmark(q, key);
    if (q->status != 0 || record.state == 3) {
        return 0;
    }
    /* Each OR member emits only files not present in an earlier member.
     * The bounded file TAG list replaces a result-sized deduplication set. */
    for (i = 0; i < q->exclude_count; i++) {
        for (j = 0; j < record.tag_count; j++) {
            if (q->exclude_tags[i] != 0
                && q->exclude_tags[i] == record.tags[j])
            {
                return 0;
            }
        }
    }
    if (q->tag != 0 && !tagged) {
        found = 0;
        for (i = 0; i < record.tag_count; i++) {
            found |= record.tags[i] == q->tag;
        }
        if (!found) {
            return 0;
        }
    }
    if (q->host != 0) {
        found = record.refs[2] == q->host;
        if (q->joined) {
            ngx_http_cache_purge_index_require(q, found,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "host/key posting disagrees with file identity");
        }
        if (!found || q->status != 0) {
            return 0;
        }
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
    } else if (q->recovery == 'd') {
        ngx_http_cache_purge_index_require(q,
            (record.state == 2 || record.state == 3) && value.len == 24,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid deferred Delete record");
        if (q->status == 0) {
            ngx_http_cache_purge_index_require(q,
                !ngx_http_cache_purge_index_zero(value.data),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing Delete ledger");
            value.data += 16;
            value.len = 8;
            ngx_http_cache_purge_index_require(q,
                ngx_http_cache_purge_index_integer(q, value) != 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing Delete sequence");
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

static uint64_t
ngx_http_cache_purge_index_references(ngx_http_cache_purge_query_t *q,
    uint64_t id)
{
    ngx_http_cache_purge_index_dictionary_t  value;

    ngx_http_cache_purge_index_row(q, &q->key, 'V', id);
    if (!ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_get(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_bytes(&q->key), NULL),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing selector dictionary value")
        || !ngx_http_cache_purge_index_dict_decode(q,
            ngx_http_cache_purge_index_bytes(&q->scratch), &value))
    {
        return 0;
    }
    return value.references;
}

static void
ngx_http_cache_purge_index_select(ngx_http_cache_purge_query_t *q,
    const ngx_http_cache_purge_selector_t *selector,
    ngx_http_cache_rocksdb_bytes_t resume)
{
    ngx_http_cache_purge_index_reader_t r;
    ngx_http_cache_rocksdb_bytes_t      saved[3], signature, range, number;
    uint64_t                            id, scope, host_refs, tag_refs;
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
                    && (tagged ? (q->kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG
                                   || selector->scope.len != 0) :
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
        q->joined = url;
        q->outer_cf =
            url ? NGX_HTTP_CACHE_ROCKSDB_URL : NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
        q->key.len = 0;
        if (url) {
            ngx_http_cache_purge_index_append(q, &q->key, "U", 1);
            ngx_http_cache_purge_index_append(q, &q->key, selector->value.data,
                                              selector->value.len);
        } else if (!ngx_http_cache_purge_index_key_projection(q,
                                                           selector->value))
        {
            q->empty = 1;
            return;
        }
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
        if (!ngx_http_cache_purge_index_key_prefix(q, selector->value)) {
            q->empty = 1;
            return;
        }
        break;
    case NGX_HTTP_CACHE_PURGE_URL_PREFIX:
        q->outer_cf = NGX_HTTP_CACHE_ROCKSDB_URL;
        ngx_http_cache_purge_index_append(q, &q->prefix, "U", 1);
        ngx_http_cache_purge_index_append(q, &q->prefix, selector->value.data,
                                          selector->value.len);
        break;
    case NGX_HTTP_CACHE_PURGE_HOST:
        id = ngx_http_cache_purge_index_find(q, selector->value);
        if (id == 0) {
            q->empty = 1;
            return;
        }
        q->host = id;
        q->joined = 1;
        ngx_http_cache_purge_index_row(q, &q->prefix, 'B', id);
        break;
    default:
        scope = ngx_http_cache_purge_index_find(q, selector->scope);
        id = ngx_http_cache_purge_index_find(q, selector->value);
        if (scope == 0 || id == 0) {
            q->empty = 1;
            return;
        }
        if (q->kind == NGX_HTTP_CACHE_PURGE_HOST_TAG) {
            q->host = scope;
            q->tag = id;
            host_refs = ngx_http_cache_purge_index_references(q, scope);
            tag_refs = ngx_http_cache_purge_index_references(q, id);
            q->joined = host_refs < tag_refs;
            if (saved[1].len != 0 && saved[1].data[0] == 'B') {
                q->joined = 1;
            } else if (saved[2].len != 0 && saved[2].data[0] == 'T') {
                q->joined = 0;
            }
            ngx_http_cache_purge_index_row(q, &q->prefix,
                q->joined ? 'B' : 'T', q->joined ? scope : id);
            q->outer_cf = q->joined ? NGX_HTTP_CACHE_ROCKSDB_DEFAULT
                                    : NGX_HTTP_CACHE_ROCKSDB_TAG;
        } else {
            ngx_http_cache_purge_index_row(q, &q->prefix, 'N', scope);
            ngx_http_cache_purge_index_number(q, &q->prefix, id);
            q->outer_cf = NGX_HTTP_CACHE_ROCKSDB_TAG;
        }
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
    } else if (q->joined) {
        if (!ngx_http_cache_purge_index_require(
                q, saved[0].len == 0, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "unexpected joined bookmark outer key"))
        {
            return;
        }
        if (!ngx_http_cache_purge_index_compatible(saved[1], range,
                         q->kind == NGX_HTTP_CACHE_PURGE_URL ? 16 : 8))
        {
            saved[1] = ngx_http_cache_purge_op_empty;
        }
        ngx_http_cache_purge_index_cursor_open(q, &q->outer, q->outer_cf, range,
                                               saved[1], NULL);
        if (saved[1].len != 0
            && ngx_http_cache_purge_index_get(q, q->outer_cf, saved[1],
                                              q->outer.handle))
        {
            ngx_http_cache_purge_index_set(q, &q->current_link, saved[1]);
            if (q->kind == NGX_HTTP_CACHE_PURGE_URL) {
                number = ngx_http_cache_purge_index_bytes(&q->scratch);
            } else {
                number.data = saved[1].data + 9;
                number.len = 8;
            }
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

static void
ngx_http_cache_purge_index_group_bookmark(ngx_http_cache_purge_query_t *q)
{
    ngx_http_cache_rocksdb_bytes_t  child;
    unsigned char                   prefix[2];

    prefix[0] = 2;
    prefix[1] = (unsigned char) q->group->current;
    child = ngx_http_cache_purge_query_bookmark(q->group->child);
    if (!ngx_http_cache_purge_index_require(q, child.len <= 256,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "oversized TAG member bookmark"))
    {
        return;
    }
    q->token.len = 0;
    ngx_http_cache_purge_index_append(q, &q->token, prefix, sizeof(prefix));
    ngx_http_cache_purge_index_append(q, &q->token, q->signature, 32);
    ngx_http_cache_purge_index_append(q, &q->token, child.data, child.len);
}

static void
ngx_http_cache_purge_index_group_open(ngx_http_cache_purge_query_t *q,
    const ngx_http_cache_purge_selector_t *selector,
    ngx_http_cache_rocksdb_bytes_t resume)
{
    ngx_http_cache_purge_index_group_t  *group;
    ngx_http_cache_rocksdb_bytes_t       tags[64], value, child;
    size_t                              count, size, i;
    unsigned char                       kind;

    if (!ngx_http_cache_purge_index_require(q,
            selector->kind <= NGX_HTTP_CACHE_PURGE_HOST_TAGS
                && (selector->kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAGS
                    || selector->scope.len != 0)
                && ngx_http_cache_purge_op_bytes_valid(selector->scope, 32768)
                && ngx_http_cache_purge_tags_decode(selector->value,
                                                       tags, &count) == 0,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid TAG group selector"))
    {
        return;
    }
    size = sizeof(*group) + selector->value.len + selector->scope.len;
    group = calloc(1, size);
    if (group == NULL) {
        ngx_http_cache_purge_index_fail(q, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                        "TAG group allocation failed");
        return;
    }
    q->group = group;
    q->bytes += size;
    q->kind = selector->kind;
    if (!ngx_http_cache_purge_index_reserve(q, &q->token, 290)) {
        return;
    }
    value.data = group->storage;
    value.len = selector->value.len;
    memcpy(group->storage, selector->value.data, value.len);
    group->scope.data = group->storage + value.len;
    group->scope.len = selector->scope.len;
    if (group->scope.len) {
        memcpy(group->storage + value.len, selector->scope.data,
               group->scope.len);
    }
    (void) ngx_http_cache_purge_tags_decode(value, group->tags, &group->count);
    kind = (unsigned char) selector->kind;
    ngx_http_cache_purge_index_append(q, &q->key, &kind, 1);
    ngx_http_cache_purge_index_string(q, &q->key, value);
    ngx_http_cache_purge_index_string(q, &q->key, group->scope);
    if (!ngx_http_cache_purge_index_require(q,
            q->status == 0
                && SHA256(q->key.data, q->key.len, q->signature) != NULL,
            NGX_HTTP_CACHE_ROCKSDB_IO, "TAG query digest failed"))
    {
        return;
    }
    if (resume.len != 0) {
        if (!ngx_http_cache_purge_index_require(q,
                resume.len >= 34 && resume.len <= 290 && resume.data[0] == 2
                    && resume.data[1] <= group->count
                    && memcmp(resume.data + 2, q->signature, 32) == 0
                    && (resume.data[1] != group->count || resume.len == 34),
                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "bookmark belongs to another TAG group"))
        {
            return;
        }
        group->current = resume.data[1];
        child.data = resume.data + 34;
        child.len = resume.len - 34;
        ngx_http_cache_purge_index_set(q, &q->current_outer, child);
    }
    for (i = 0; i < group->count && q->status == 0; i++) {
        group->ids[i] = ngx_http_cache_purge_index_find(q, group->tags[i]);
    }
    if (resume.len != 0) {
        ngx_http_cache_purge_index_set(q, &q->token, resume);
    } else {
        ngx_http_cache_purge_index_group_bookmark(q);
    }
}

static int
ngx_http_cache_purge_index_group_next(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_match_t *match,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_group_t  *group;
    ngx_http_cache_purge_selector_t      selector;
    ngx_http_cache_rocksdb_error_t       failure;
    int                                  rc;

    group = q->group;
    while (q->status == 0 && group->current < group->count) {
        if (q->limited && q->advances == 0) {
            return NGX_HTTP_CACHE_ROCKSDB_BUSY;
        }
        if (group->child == NULL) {
            if (q->limited) {
                q->advances--;
            }
            selector.kind = q->kind - 2;
            selector.scope = group->scope;
            selector.value = group->tags[group->current];
            rc = ngx_http_cache_purge_query_open(q->store, &selector, 0,
                ngx_http_cache_purge_index_bytes(&q->current_outer),
                &group->child, &failure);
            if (rc != 0) {
                ngx_http_cache_purge_index_fail(q, rc, failure.message);
                break;
            }
            if (!ngx_http_cache_purge_index_require(q,
                    group->child->bytes
                        <= NGX_HTTP_CACHE_PURGE_QUERY_MEMORY - q->bytes,
                    NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                    "TAG group exceeds shared query memory budget"))
            {
                break;
            }
            /* Charge the parent to its one active member. Parent buffers
             * are fixed before this point; all later growth is in the child. */
            group->child->bytes += q->bytes;
            q->current_outer.len = 0;
            group->child->exclude_tags = group->ids;
            group->child->exclude_count = group->current;
        }
        group->child->limited = q->limited;
        group->child->advances = q->advances;
        rc = ngx_http_cache_purge_query_next(group->child, match, &failure);
        q->advances = group->child->advances;
        group->child->limited = 0;
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            rc = ngx_http_cache_purge_query_close(&group->child, &failure);
            if (rc == 0) {
                group->current++;
                ngx_http_cache_purge_index_group_bookmark(q);
                continue;
            }
        } else if (rc == 0 || rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            ngx_http_cache_purge_index_group_bookmark(q);
            if (q->status == 0) {
                return rc;
            }
            break;
        }
        ngx_http_cache_purge_index_fail(q, rc, failure.message);
    }
    if (error != NULL && q->status != 0) {
        *error = q->error;
    }
    return q->status != 0 ? q->status : NGX_HTTP_CACHE_ROCKSDB_DONE;
}

int
ngx_http_cache_purge_query_close(ngx_http_cache_purge_query_t **query,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t         *q;
    ngx_http_cache_purge_index_buffer_t  *buffers[16];
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
    if (q->group != NULL) {
        rc = ngx_http_cache_purge_query_close(&q->group->child, error);
        if (rc != 0) {
            return rc;
        }
        free(q->group);
        q->group = NULL;
    }
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
    buffers[13] = &q->outer.next;
    buffers[14] = &q->links.next;
    buffers[15] = &q->files.next;
    for (i = 0; i < 16; i++) {
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
        || mode > NGX_HTTP_CACHE_PURGE_QUERY_DELETE
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
                  mode == NGX_HTTP_CACHE_PURGE_QUERY_DELETE ? 'd' :
                                                               0;
    if (selector != NULL && selector->kind > NGX_HTTP_CACHE_PURGE_HOST_TAG) {
        ngx_http_cache_purge_index_group_open(q, selector, bookmark);
    } else {
        ngx_http_cache_purge_index_select(q, selector, bookmark);
    }
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
    if (q->group != NULL) {
        return ngx_http_cache_purge_index_group_next(q, match, error);
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
                        q, q->links.key.len == 25,
                        NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                        "invalid URL/key posting"))
                {
                    break;
                }
                ngx_http_cache_purge_index_set(
                    q, &q->current_link,
                    ngx_http_cache_purge_index_bytes(&q->links.key));
                number = ngx_http_cache_purge_index_bytes(&q->links.value);
                ngx_http_cache_purge_index_open_files(
                    q, ngx_http_cache_purge_index_integer(q, number),
                    ngx_http_cache_purge_op_empty);
                ngx_http_cache_purge_index_bookmark(
                    q, ngx_http_cache_purge_op_empty);
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
        if (q->kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX) {
            if (!ngx_http_cache_purge_index_require(q,
                    q->outer.key.len == 33, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "invalid internal key projection"))
            {
                break;
            }
            if (q->key_nibble
                && (unsigned) (q->outer.key.data[q->prefix.len] >> 4) + 1
                   != q->key_nibble)
            {
                continue;
            }
        }
        if (q->joined) {
            if (!ngx_http_cache_purge_index_require(q,
                    q->outer.key.len
                        == (q->kind == NGX_HTTP_CACHE_PURGE_URL ? 25 : 17),
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "invalid identity/key posting"))
            {
                break;
            }
            ngx_http_cache_purge_index_set(q, &q->current_link,
                ngx_http_cache_purge_index_bytes(&q->outer.key));
            if (q->kind == NGX_HTTP_CACHE_PURGE_URL) {
                number = ngx_http_cache_purge_index_bytes(&q->outer.value);
            } else {
                number.data = q->outer.key.data + 9;
                number.len = 8;
            }
            ngx_http_cache_purge_index_open_files(q,
                ngx_http_cache_purge_index_integer(q, number),
                ngx_http_cache_purge_op_empty);
            ngx_http_cache_purge_index_bookmark(q,
                ngx_http_cache_purge_op_empty);
            continue;
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
            ngx_http_cache_purge_index_bookmark(
                q, ngx_http_cache_purge_op_empty);
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

int
ngx_http_cache_purge_query_step(ngx_http_cache_purge_query_t *q,
    size_t advances, ngx_http_cache_purge_index_match_t *match,
    ngx_http_cache_rocksdb_error_t *error)
{
    int  rc;

    if (q == NULL || advances < 4 || advances > 4096) {
        if (error != NULL) {
            error->code = NGX_HTTP_CACHE_ROCKSDB_INVALID;
            snprintf(error->message, sizeof(error->message),
                     "invalid index scan budget");
        }
        if (match != NULL) {
            memset(match, 0, sizeof(*match));
        }
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }
    q->limited = 1;
    q->advances = advances;
    rc = ngx_http_cache_purge_query_next(q, match, error);
    q->limited = 0;
    return rc;
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

#define NGX_HTTP_CACHE_PURGE_TX_HASH  1024
#define NGX_HTTP_CACHE_PURGE_TX_BYTES    524288

typedef struct ngx_http_cache_purge_index_row_s
    ngx_http_cache_purge_index_row_t;

typedef struct {
    ngx_http_cache_rocksdb_bytes_t  bytes;
    int                             exists;
} ngx_http_cache_purge_index_value_t;

struct ngx_http_cache_purge_index_row_s {
    ngx_http_cache_purge_index_row_t   *next;
    ngx_http_cache_rocksdb_cf_e         cf;
    ngx_http_cache_purge_index_buffer_t  key;
    ngx_http_cache_purge_index_buffer_t  original;
    ngx_http_cache_purge_index_buffer_t  current;
    int                                  observed;
    int                                  existed;
    int                                  changed;
    int                                  exists;
};

typedef struct ngx_http_cache_purge_index_intern_s
    ngx_http_cache_purge_index_intern_t;

struct ngx_http_cache_purge_index_intern_s {
    ngx_http_cache_purge_index_intern_t  *next;
    unsigned char                         digest[32];
    uint64_t                              id;
};

struct ngx_http_cache_purge_index_tx_s {
    ngx_http_cache_purge_query_t         *context;
    size_t                                charged;
    unsigned                              deferred;
    ngx_http_cache_purge_index_row_t     *buckets[NGX_HTTP_CACHE_PURGE_TX_HASH];
    ngx_http_cache_purge_index_intern_t  *interns[256];
};

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

int
ngx_http_cache_purge_identity_digest(ngx_http_cache_rocksdb_bytes_t url,
    ngx_http_cache_rocksdb_bytes_t tag_namespace,
    ngx_http_cache_rocksdb_bytes_t misc,
    unsigned char digest[NGX_HTTP_CACHE_PURGE_IDENTITY_DIGEST_SIZE],
    ngx_http_cache_rocksdb_error_t *error)
{
    EVP_MD_CTX                     *ctx;
    ngx_http_cache_rocksdb_bytes_t   fields[3];
    unsigned char                    length[4];
    unsigned                         size;
    size_t                           i;
    int                              ok;

    if (digest == NULL || !ngx_http_cache_purge_op_bytes_valid(url, 32768)
        || !ngx_http_cache_purge_op_bytes_valid(tag_namespace, 32768)
        || !ngx_http_cache_purge_op_bytes_valid(misc, 32768))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid configured cache identity");
    }
    fields[0] = tag_namespace;
    fields[1] = url;
    fields[2] = misc;
    ctx = EVP_MD_CTX_new();
    if (ctx == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "identity digest allocation failed");
    }
    ok = EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) == 1
         && EVP_DigestUpdate(ctx, "NCI1", 4) == 1;
    for (i = 0; ok && i < 3; i++) {
        length[0] = (unsigned char) (fields[i].len >> 24);
        length[1] = (unsigned char) (fields[i].len >> 16);
        length[2] = (unsigned char) (fields[i].len >> 8);
        length[3] = (unsigned char) fields[i].len;
        ok = EVP_DigestUpdate(ctx, length, sizeof(length)) == 1;
        if (ok && fields[i].len) {
            ok = EVP_DigestUpdate(ctx, fields[i].data, fields[i].len) == 1;
        }
    }
    if (ok) {
        ok = EVP_DigestFinal_ex(ctx, digest, &size) == 1
             && size == NGX_HTTP_CACHE_PURGE_IDENTITY_DIGEST_SIZE;
    }
    EVP_MD_CTX_free(ctx);
    if (!ok) {
        memset(digest, 0, NGX_HTTP_CACHE_PURGE_IDENTITY_DIGEST_SIZE);
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_IO,
            "configured cache identity digest failed");
    }
    return ngx_http_cache_purge_index_error(error, 0, "");
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
    q->transaction = tx;
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
    if (data == NULL) {
        ngx_http_cache_purge_index_fail(q, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                        "index transaction allocation failed");
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
    unsigned char *data;

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
    size_t                            i, bucket;

    if (tx->context->status != 0) {
        return NULL;
    }
    hash = 2166136261U ^ (uint32_t) cf;
    for (i = 0; i < key.len; i++) {
        hash = (hash ^ key.data[i]) * 16777619U;
    }
    bucket = hash & (NGX_HTTP_CACHE_PURGE_TX_HASH - 1);
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
        exists = ngx_http_cache_purge_index_read(q, cf, key, NULL);
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

static int
ngx_http_cache_purge_index_tx_read(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key)
{
    ngx_http_cache_purge_index_value_t value;

    value = ngx_http_cache_purge_index_tx_get(tx, cf, key);
    if (value.exists && tx->context->status == 0) {
        ngx_http_cache_purge_index_set(tx->context, &tx->context->scratch,
                                       value.bytes);
    }
    return value.exists && tx->context->status == 0;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_tx_need(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key)
{
    ngx_http_cache_purge_index_value_t value;

    value = ngx_http_cache_purge_index_tx_get(tx, cf, key);
    ngx_http_cache_purge_index_require(tx->context, value.exists,
                                       NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                       "missing referenced index record");
    return value.bytes;
}

static void
ngx_http_cache_purge_index_tx_store(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_rocksdb_bytes_t value, int exists)
{
    ngx_http_cache_purge_index_row_t *row;

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

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_catalog_node(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t key)
{
    q->key.len = 0;
    ngx_http_cache_purge_index_append(q, &q->key, "q", 1);
    ngx_http_cache_purge_index_append(q, &q->key, key.data, key.len);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static void
ngx_http_cache_purge_index_catalog_store(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t key, const uint64_t *count,
    ngx_http_cache_rocksdb_bytes_t before, ngx_http_cache_rocksdb_bytes_t after)
{
    ngx_http_cache_purge_query_t  *q;

    q = tx->context;
    q->lookup.len = 0;
    ngx_http_cache_purge_index_append(q, &q->lookup, "\1", 1);
    if (count != NULL) {
        ngx_http_cache_purge_index_number(q, &q->lookup, *count);
    }
    ngx_http_cache_purge_index_string(q, &q->lookup, before);
    ngx_http_cache_purge_index_string(q, &q->lookup, after);
    ngx_http_cache_purge_index_tx_store(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        key, ngx_http_cache_purge_index_bytes(&q->lookup), 1);
}

/* The exact row, links and count always share the caller's atomic batch. */
static void
ngx_http_cache_purge_index_catalog_change(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t key, int exists)
{
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_value_t   node;
    ngx_http_cache_rocksdb_bytes_t       meta, raw, first, last, before, after;
    ngx_http_cache_rocksdb_bytes_t       left, right;
    uint64_t                            count;
    unsigned                            kind;
    unsigned char                       name[11];

    q = tx->context;
    kind = key.data[0];
    meta = ngx_http_cache_purge_index_catalog_name(name, kind);
    raw = ngx_http_cache_purge_index_tx_need(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, meta);
    if (!ngx_http_cache_purge_index_catalog_decode(q, raw, kind, &count,
                                                  &first, &last))
    {
        return;
    }
    node = ngx_http_cache_purge_index_tx_get(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_catalog_node(q, key));
    if (!ngx_http_cache_purge_index_require(q, node.exists != exists,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "work catalog membership mismatch"))
    {
        return;
    }
    if (exists) {
        if (!ngx_http_cache_purge_index_require(q, count != UINT64_MAX,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "work catalog count overflow"))
        {
            return;
        }
        if (count != 0) {
            raw = ngx_http_cache_purge_index_tx_need(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_catalog_node(q, last));
            if (!ngx_http_cache_purge_index_catalog_decode(q, raw, kind, NULL,
                                                          &before, &after)
                || !ngx_http_cache_purge_index_require(q, after.len == 0,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "work catalog tail mismatch"))
            {
                return;
            }
            ngx_http_cache_purge_index_catalog_store(tx,
                ngx_http_cache_purge_index_catalog_node(q, last), NULL,
                before, key);
        } else {
            first = key;
        }
        ngx_http_cache_purge_index_catalog_store(tx,
            ngx_http_cache_purge_index_catalog_node(q, key), NULL,
            last, ngx_http_cache_purge_op_empty);
        count++;
        last = key;
    } else {
        if (!ngx_http_cache_purge_index_require(q, count != 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "work catalog count underflow")
            || !ngx_http_cache_purge_index_catalog_decode(q, node.bytes, kind,
                                                        NULL, &before, &after)
            || !ngx_http_cache_purge_index_require(q,
                !ngx_http_cache_purge_op_equal(before, key)
                && !ngx_http_cache_purge_op_equal(after, key),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "cyclic work catalog node"))
        {
            return;
        }
        if (before.len) {
            raw = ngx_http_cache_purge_index_tx_need(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_catalog_node(q, before));
            if (!ngx_http_cache_purge_index_catalog_decode(q, raw, kind, NULL,
                                                          &left, &right)
                || !ngx_http_cache_purge_index_require(q,
                    ngx_http_cache_purge_op_equal(right, key),
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "work catalog previous mismatch"))
            {
                return;
            }
            ngx_http_cache_purge_index_catalog_store(tx,
                ngx_http_cache_purge_index_catalog_node(q, before), NULL,
                left, after);
        } else {
            ngx_http_cache_purge_index_require(q,
                ngx_http_cache_purge_op_equal(first, key),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "work catalog head mismatch");
            first = after;
        }
        if (after.len) {
            raw = ngx_http_cache_purge_index_tx_need(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_catalog_node(q, after));
            if (!ngx_http_cache_purge_index_catalog_decode(q, raw, kind, NULL,
                                                          &left, &right)
                || !ngx_http_cache_purge_index_require(q,
                    ngx_http_cache_purge_op_equal(left, key),
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "work catalog next mismatch"))
            {
                return;
            }
            ngx_http_cache_purge_index_catalog_store(tx,
                ngx_http_cache_purge_index_catalog_node(q, after), NULL,
                before, right);
        } else {
            ngx_http_cache_purge_index_require(q,
                ngx_http_cache_purge_op_equal(last, key),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "work catalog tail mismatch");
            last = before;
        }
        count--;
    }
    ngx_http_cache_purge_index_catalog_store(tx, meta, &count, first, last);
    if (!exists) {
        /* The new endpoints may point into this node's current transaction
         * value, including a link changed earlier in the same batch. */
        ngx_http_cache_purge_index_tx_store(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_catalog_node(q, key),
            ngx_http_cache_purge_op_empty, 0);
    }
}

static void
ngx_http_cache_purge_index_tx_set(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_rocksdb_bytes_t value, int exists)
{
    ngx_http_cache_purge_index_row_t    *row;
    ngx_http_cache_purge_index_value_t   before;
    ngx_http_cache_rocksdb_bytes_t       saved;

    if (cf == NGX_HTTP_CACHE_ROCKSDB_DEFAULT && key.len > 1
        && ngx_http_cache_purge_index_catalog_kind(key.data[0]))
    {
        row = ngx_http_cache_purge_index_tx_row(tx, cf, key);
        if (row == NULL) {
            return;
        }
        key = ngx_http_cache_purge_index_bytes(&row->key);
        /* Link work reuses the query's scratch buffers. Save the caller's
         * value in the transaction before touching any of them. */
        saved.len = value.len;
        saved.data = ngx_http_cache_purge_index_tx_alloc(tx, value.len + 1);
        if (saved.data == NULL) {
            return;
        }
        if (value.len) {
            memcpy((void *) saved.data, value.data, value.len);
        }
        before = ngx_http_cache_purge_index_tx_get(tx, cf, key);
        if (before.exists != exists) {
            ngx_http_cache_purge_index_catalog_change(tx, key, exists);
        }
        ngx_http_cache_purge_index_tx_store(tx, cf, key, saved, exists);
        tx->context->bytes -= saved.len + 1;
        free((void *) saved.data);
        return;
    }
    ngx_http_cache_purge_index_tx_store(tx, cf, key, value, exists);
}

static void
ngx_http_cache_purge_index_tx_commit(ngx_http_cache_purge_index_tx_t *tx)
{
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_row_t    *row;
    ngx_http_cache_rocksdb_mutation_t   *mutations;
    ngx_http_cache_rocksdb_condition_t  *conditions;
    size_t                              i, nc, nm;

    q = tx->context;
    if (q->status != 0) {
        return;
    }
    nc = 0;
    nm = 0;
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_TX_HASH; i++) {
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
        for (i = 0; i < NGX_HTTP_CACHE_PURGE_TX_HASH; i++) {
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
        if (tx->deferred) {
            q->status = ngx_http_cache_rocksdb_write_deferred(q->store,
                mutations, nm, conditions, nc, &q->error);
        } else {
            q->status = ngx_http_cache_rocksdb_write(q->store, mutations, nm,
                                                     conditions, nc, &q->error);
        }
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
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_row_t    *row, *next;
    ngx_http_cache_purge_index_intern_t *intern, *following;
    size_t  i;
    int     rc;

    q = tx->context;
    ngx_http_cache_purge_index_cursor_close(q, &q->outer);
    ngx_http_cache_purge_index_cursor_close(q, &q->links);
    ngx_http_cache_purge_index_cursor_close(q, &q->files);
    rc = q->status;
    if (error != NULL) {
        *error = q->error;
        error->code = rc;
    }
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_TX_HASH; i++) {
        for (row = tx->buckets[i]; row != NULL; row = next) {
            next = row->next;
            free(row->key.data);
            free(row->original.data);
            free(row->current.data);
            free(row);
        }
    }
    for (i = 0; i < 256; i++) {
        for (intern = tx->interns[i]; intern != NULL; intern = following) {
            following = intern->next;
            free(intern);
        }
    }
    free(tx);
    ngx_http_cache_purge_query_close(&q, NULL);
    return rc;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_id(const unsigned char id[16])
{
    ngx_http_cache_rocksdb_bytes_t value;

    value.data = id;
    value.len = 16;
    return value;
}

static int
ngx_http_cache_purge_index_zero(const unsigned char id[16])
{
    unsigned i, value;

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
    ngx_http_cache_purge_query_t *q;

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
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_record_t record, previous;
    ngx_http_cache_purge_index_value_t  intent, current;
    ngx_http_cache_rocksdb_bytes_t      value, id;

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

unsigned
ngx_http_cache_purge_index_schema_14(void)
{
    return 14;
}

/* Upgrade only the small work catalogs, not the URL/TAG or cache-file index.
 * Each batch advances a durable cursor with its links. An interrupted open
 * resumes here before the component accepts any normal writer operations. */
static int
ngx_http_cache_purge_index_catalog_migrate(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t     *tx;
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_rocksdb_bytes_t      saved, prefix, after;
    ngx_http_cache_rocksdb_error_t      failure;
    size_t                              count;
    unsigned                            phase;
    unsigned char                       next;
    int                                 rc, more;

    static const unsigned char         kinds[] = "IgdGz";

    for ( ;; ) {
        tx = ngx_http_cache_purge_index_tx_begin(store, error);
        if (tx == NULL) {
            return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
        }
        q = tx->context;
        saved = ngx_http_cache_purge_index_tx_need(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_op_literal("M:catalog-upgrade"));
        if (!ngx_http_cache_purge_index_require(q, saved.len != 0
                && saved.data[0] <= 5,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid catalog upgrade cursor"))
        {
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
        phase = saved.data[0];
        if (phase == 5) {
            ngx_http_cache_purge_index_tx_set(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_op_literal("M:catalog-upgrade"),
                ngx_http_cache_purge_op_empty, 0);
            ngx_http_cache_purge_index_tx_set(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_op_literal("M:index"),
                ngx_http_cache_purge_op_literal("14"), 1);
        } else {
            prefix.data = &kinds[phase];
            prefix.len = 1;
            after.data = saved.data + 1;
            after.len = saved.len - 1;
            /* Legacy iteration is deliberately limited to this upgrade. */
            rc = ngx_http_cache_rocksdb_cursor_open(store,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix, after,
                &q->outer.handle, &failure);
            if (rc != 0) {
                ngx_http_cache_purge_index_fail(q, rc, failure.message);
            }
            count = 0;
            while (count < 8
                   && ngx_http_cache_purge_index_cursor_next(q, &q->outer))
            {
                ngx_http_cache_purge_index_catalog_change(tx,
                    ngx_http_cache_purge_index_bytes(&q->outer.key), 1);
                ngx_http_cache_purge_index_set(q, &q->token,
                    ngx_http_cache_purge_index_bytes(&q->outer.key));
                count++;
            }
            more = count == 8
                   && ngx_http_cache_purge_index_cursor_next(q, &q->outer);
            next = (unsigned char) (phase + !more);
            q->lookup.len = 0;
            ngx_http_cache_purge_index_append(q, &q->lookup, &next, 1);
            if (more) {
                ngx_http_cache_purge_index_append(q, &q->lookup,
                                                 q->token.data, q->token.len);
            }
            ngx_http_cache_purge_index_tx_set(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_op_literal("M:catalog-upgrade"),
                ngx_http_cache_purge_index_bytes(&q->lookup), 1);
        }
        ngx_http_cache_purge_index_tx_commit(tx);
        rc = ngx_http_cache_purge_index_tx_end(tx, error);
        if (rc != 0 || phase == 5) {
            return rc;
        }
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
    uint64_t                           count;
    size_t                             i;
    unsigned char                      name[11];
    int                                rc, upgrade, initialize;
    static const unsigned char         kinds[] = "IgdGz";

    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return store == NULL ? NGX_HTTP_CACHE_ROCKSDB_INVALID :
                               NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    upgrade = 0;
    initialize = 0;
    schema = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_op_literal("M:index"));
    if (schema.exists) {
        initialize = ngx_http_cache_purge_op_equal(schema.bytes,
                         ngx_http_cache_purge_op_literal("13"));
        upgrade = initialize
                  || ngx_http_cache_purge_op_equal(schema.bytes,
                         ngx_http_cache_purge_op_literal("14:catalog"));
        if (!ngx_http_cache_purge_index_require(q,
                upgrade || ngx_http_cache_purge_op_equal(schema.bytes,
                    ngx_http_cache_purge_op_literal("14")),
                NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
                "file index requires work catalog schema 14"))
        {
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
        next = ngx_http_cache_purge_index_tx_need(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_op_literal("M:next"));
        ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_integer(q, next) != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid index ID allocator");
    } else if (q->status == 0) {
        initialize = 1;
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
            ngx_http_cache_purge_op_literal("14"), 1);
        q->lookup.len = 0;
        ngx_http_cache_purge_index_number(q, &q->lookup, 1);
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_op_literal("M:next"),
            ngx_http_cache_purge_index_bytes(&q->lookup), 1);
    }
    if (initialize) {
        count = 0;
        for (i = 0; i < 5; i++) {
            ngx_http_cache_purge_index_catalog_store(tx,
                ngx_http_cache_purge_index_catalog_name(name, kinds[i]),
                &count, ngx_http_cache_purge_op_empty,
                ngx_http_cache_purge_op_empty);
        }
        if (upgrade) {
            ngx_http_cache_purge_index_tx_set(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_op_literal("M:index"),
                ngx_http_cache_purge_op_literal("14:catalog"), 1);
            next.data = (const unsigned char *) "";
            next.len = 1;
            ngx_http_cache_purge_index_tx_set(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_op_literal("M:catalog-upgrade"),
                next, 1);
        }
    }
    ngx_http_cache_purge_index_tx_commit(tx);
    rc = ngx_http_cache_purge_index_tx_end(tx, error);
    if (rc != 0) {
        return rc;
    }
    return upgrade ? ngx_http_cache_purge_index_catalog_migrate(store, error) :
                     0;
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
    tx->deferred = action == NGX_HTTP_CACHE_PURGE_INDEX_COMMIT
                   || action == NGX_HTTP_CACHE_PURGE_INDEX_COMMIT_PENDING
                   || action == NGX_HTTP_CACHE_PURGE_INDEX_PUBLISH;
    ngx_http_cache_purge_index_tx_commit(tx);
    if (tx->context->status == 0) {
        memcpy(matches, result, count * sizeof(*matches));
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}


/* Shared values, file preparation and bounded reclamation. */

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_row_key(ngx_http_cache_purge_query_t *q,
    unsigned char kind, uint64_t id)
{
    ngx_http_cache_purge_index_row(q, &q->key, kind, id);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_text_key(ngx_http_cache_purge_query_t *q,
    unsigned char kind, ngx_http_cache_rocksdb_bytes_t text)
{
    q->key.len = 0;
    ngx_http_cache_purge_index_append(q, &q->key, &kind, 1);
    ngx_http_cache_purge_index_append(q, &q->key, text.data, text.len);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_relation(ngx_http_cache_purge_query_t *q,
    unsigned char kind, uint64_t first, uint64_t second)
{
    ngx_http_cache_purge_index_row(q, &q->key, kind, first);
    ngx_http_cache_purge_index_number(q, &q->key, second);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_variant_key(ngx_http_cache_purge_query_t *q,
    uint64_t url, uint64_t tag_namespace, uint64_t misc)
{
    ngx_http_cache_purge_index_row(q, &q->key, 'A', url);
    ngx_http_cache_purge_index_number(q, &q->key, tag_namespace);
    ngx_http_cache_purge_index_number(q, &q->key, misc);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_posting(ngx_http_cache_purge_query_t *q,
    unsigned char kind, uint64_t scope, uint64_t tag,
    const unsigned char id[16])
{
    ngx_http_cache_purge_index_row(q, &q->key, kind, scope);
    if (tag != 0) {
        ngx_http_cache_purge_index_number(q, &q->key, tag);
    }
    ngx_http_cache_purge_index_append(q, &q->key, id, 16);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_encoded_number(ngx_http_cache_purge_query_t *q,
    uint64_t value)
{
    q->lookup.len = 0;
    ngx_http_cache_purge_index_number(q, &q->lookup, value);
    return ngx_http_cache_purge_index_bytes(&q->lookup);
}

static uint64_t
ngx_http_cache_purge_index_key_number(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t value, int field)
{
    if (!ngx_http_cache_purge_index_require(q,
            value.len == 72 && field >= 0 && field < 5,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid key identity record"))
    {
        return 0;
    }
    value.data += 8 * field;
    value.len = 8;
    return ngx_http_cache_purge_index_integer(q, value);
}

static void
ngx_http_cache_purge_index_key_counter(ngx_http_cache_purge_index_tx_t *tx,
    uint64_t id, uint64_t files, uint64_t url, uint64_t host,
    uint64_t tag_namespace, uint64_t misc, const unsigned char digest[32])
{
    ngx_http_cache_purge_query_t   *q;
    ngx_http_cache_rocksdb_bytes_t  key;

    q = tx->context;
    if (!ngx_http_cache_purge_index_require(q,
            url != 0 && host != 0 && tag_namespace != 0 && misc != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "empty key identity reference"))
    {
        return;
    }
    key = ngx_http_cache_purge_index_row_key(q, 'k', id);
    q->lookup.len = 0;
    ngx_http_cache_purge_index_number(q, &q->lookup, files);
    ngx_http_cache_purge_index_number(q, &q->lookup, url);
    ngx_http_cache_purge_index_number(q, &q->lookup, host);
    ngx_http_cache_purge_index_number(q, &q->lookup, tag_namespace);
    ngx_http_cache_purge_index_number(q, &q->lookup, misc);
    ngx_http_cache_purge_index_append(q, &q->lookup, digest, 32);
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key,
        ngx_http_cache_purge_index_bytes(&q->lookup), 1);
}

static void
ngx_http_cache_purge_index_counter(ngx_http_cache_purge_index_tx_t *tx,
    unsigned char kind, uint64_t id, uint64_t value)
{
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_rocksdb_bytes_t      key, number;
    ngx_http_cache_purge_index_value_t  previous;
    uint64_t                            url, host, tag_namespace, misc;

    q = tx->context;
    key = ngx_http_cache_purge_index_row_key(q, kind, id);
    if (kind == 'k') {
        previous = ngx_http_cache_purge_index_tx_get(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
        if (!ngx_http_cache_purge_index_require(q, previous.exists,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "key counter has no identity"))
        {
            return;
        }
        url = ngx_http_cache_purge_index_key_number(q, previous.bytes, 1);
        host = ngx_http_cache_purge_index_key_number(q, previous.bytes, 2);
        tag_namespace = ngx_http_cache_purge_index_key_number(q,
                                                            previous.bytes, 3);
        misc = ngx_http_cache_purge_index_key_number(q, previous.bytes, 4);
        ngx_http_cache_purge_index_key_counter(tx, id, value, url, host,
            tag_namespace, misc, previous.bytes.data + 40);
        return;
    }
    number = ngx_http_cache_purge_index_encoded_number(q, value);
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key,
                                      number, 1);
}

static uint64_t
ngx_http_cache_purge_index_allocate(ngx_http_cache_purge_index_tx_t *tx)
{
    ngx_http_cache_purge_query_t   *q;
    ngx_http_cache_rocksdb_bytes_t  raw;
    uint64_t                        next;

    q = tx->context;
    raw = ngx_http_cache_purge_index_tx_need(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_op_literal("M:next"));
    next = ngx_http_cache_purge_index_integer(q, raw);
    if (!ngx_http_cache_purge_index_require(q, next != 0 && next != UINT64_MAX,
                                            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                            "index ID space exhausted"))
    {
        return 0;
    }
    raw = ngx_http_cache_purge_index_encoded_number(q, next + 1);
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                      ngx_http_cache_purge_op_literal("M:next"),
                                      raw, 1);
    return next;
}

static int
ngx_http_cache_purge_index_dict_load(ngx_http_cache_purge_index_tx_t *tx,
    uint64_t id, ngx_http_cache_purge_index_dictionary_t *value)
{
    ngx_http_cache_rocksdb_bytes_t raw;

    raw = ngx_http_cache_purge_index_tx_need(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(tx->context, 'V', id));
    return ngx_http_cache_purge_index_dict_decode(tx->context, raw, value);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_dict_text(ngx_http_cache_purge_index_tx_t *tx,
    uint64_t id)
{
    ngx_http_cache_purge_query_t *q;

    q = tx->context;
    ngx_http_cache_purge_index_require(
        q, ngx_http_cache_purge_index_value(q, NULL, id, 0),
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing dictionary value");
    return ngx_http_cache_purge_index_bytes(&q->lookup);
}

static void
ngx_http_cache_purge_index_dict_save(ngx_http_cache_purge_index_tx_t *tx,
    uint64_t id, const ngx_http_cache_purge_index_dictionary_t *value)
{
    ngx_http_cache_purge_query_t  *q;
    unsigned char                  byte;
    size_t                         i;

    q = tx->context;
    q->lookup.len = 0;
    byte = 1;
    ngx_http_cache_purge_index_append(q, &q->lookup, &byte, 1);
    ngx_http_cache_purge_index_number(q, &q->lookup, value->references);
    byte = (unsigned char) value->format;
    ngx_http_cache_purge_index_append(q, &q->lookup, &byte, 1);
    if (value->format == 0) {
        ngx_http_cache_purge_index_string(q, &q->lookup, value->raw);
    } else {
        byte = (unsigned char) value->count;
        ngx_http_cache_purge_index_append(q, &q->lookup, &byte, 1);
        for (i = 0; i < value->count; i++) {
            ngx_http_cache_purge_index_number(q, &q->lookup, value->parts[i]);
        }
    }
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'V', id),
        ngx_http_cache_purge_index_bytes(&q->lookup), 1);
}

static int
ngx_http_cache_purge_index_digest(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t text, unsigned char hash[32])
{
    return ngx_http_cache_purge_index_require(
        q, q->status == 0 && SHA256(text.data, text.len, hash) != NULL,
        NGX_HTTP_CACHE_ROCKSDB_IO, "index digest failed");
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_digest_key(ngx_http_cache_purge_query_t *q,
    const unsigned char hash[32], uint64_t id)
{
    q->key.len = 0;
    ngx_http_cache_purge_index_append(q, &q->key, "S", 1);
    ngx_http_cache_purge_index_append(q, &q->key, hash, 32);
    ngx_http_cache_purge_index_number(q, &q->key, id);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static void
ngx_http_cache_purge_index_remember(ngx_http_cache_purge_index_tx_t *tx,
    const unsigned char hash[32], uint64_t id)
{
    ngx_http_cache_purge_index_intern_t  *entry;
    unsigned                              bucket;

    if (tx->context->status != 0 || id == 0) {
        return;
    }
    bucket = hash[0];
    for (entry = tx->interns[bucket]; entry != NULL; entry = entry->next) {
        if (entry->id == id) {
            return;
        }
    }
    entry = ngx_http_cache_purge_index_tx_alloc(tx, sizeof(*entry));
    if (entry != NULL) {
        entry->next = tx->interns[bucket];
        entry->id = id;
        memcpy(entry->digest, hash, 32);
        tx->interns[bucket] = entry;
    }
}

static uint64_t
ngx_http_cache_purge_index_dict_find(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t text)
{
    ngx_http_cache_purge_index_intern_t  *entry;
    ngx_http_cache_rocksdb_bytes_t        raw;
    unsigned char                         hash[32];
    uint64_t                              id;

    if (!ngx_http_cache_purge_index_digest(tx->context, text, hash)) {
        return 0;
    }
    for (entry = tx->interns[hash[0]]; entry != NULL; entry = entry->next) {
        if (memcmp(entry->digest, hash, 32) == 0) {
            raw = ngx_http_cache_purge_index_dict_text(tx, entry->id);
            if (ngx_http_cache_purge_op_equal(raw, text)) {
                return tx->context->status == 0 ? entry->id : 0;
            }
        }
    }
    id = ngx_http_cache_purge_index_find(tx->context, text);
    ngx_http_cache_purge_index_remember(tx, hash, id);
    return id;
}

static void
ngx_http_cache_purge_index_dict_retain(ngx_http_cache_purge_index_tx_t *tx,
    uint64_t id)
{
    ngx_http_cache_purge_index_dictionary_t value;

    if (ngx_http_cache_purge_index_dict_load(tx, id, &value)
        && ngx_http_cache_purge_index_require(
            tx->context, value.references != UINT64_MAX,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "index value reference overflow"))
    {
        value.references++;
        ngx_http_cache_purge_index_dict_save(tx, id, &value);
    }
}

static void
ngx_http_cache_purge_index_dict_drop(ngx_http_cache_purge_index_tx_t *tx,
    uint64_t id)
{
    ngx_http_cache_purge_index_dictionary_t value;
    ngx_http_cache_purge_index_intern_t   **link, *entry;
    ngx_http_cache_purge_query_t   *q;
    ngx_http_cache_rocksdb_bytes_t  text;
    unsigned char                   hash[32];
    size_t                          i;

    q = tx->context;
    if (!ngx_http_cache_purge_index_dict_load(tx, id, &value)
        || !ngx_http_cache_purge_index_require(
            q, value.references != 0, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "index value reference underflow"))
    {
        return;
    }
    if (--value.references != 0) {
        ngx_http_cache_purge_index_dict_save(tx, id, &value);
        return;
    }
    text = ngx_http_cache_purge_index_dict_text(tx, id);
    if (!ngx_http_cache_purge_index_digest(q, text, hash)) {
        return;
    }
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_digest_key(q, hash, id),
        ngx_http_cache_purge_op_empty, 0);
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'V', id),
        ngx_http_cache_purge_op_empty, 0);
    link = &tx->interns[hash[0]];
    while (*link != NULL) {
        entry = *link;
        if (entry->id == id) {
            *link = entry->next;
            q->bytes -= sizeof(*entry);
            free(entry);
            break;
        }
        link = &entry->next;
    }
    for (i = 0; i < value.count && q->status == 0; i++) {
        ngx_http_cache_purge_index_dict_drop(tx, value.parts[i]);
    }
}

static uint64_t
ngx_http_cache_purge_index_intern(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t text, unsigned format,
    const ngx_http_cache_rocksdb_bytes_t *parts, size_t count, int leaf)
{
    ngx_http_cache_purge_index_dictionary_t  value;
    ngx_http_cache_purge_query_t            *q;
    unsigned char                            hash[32];
    uint64_t                                id, old[3], part;
    size_t                                  i, old_count;

    q = tx->context;
    id = ngx_http_cache_purge_index_dict_find(tx, text);
    if (q->status != 0) {
        return 0;
    }
    if (id != 0) {
        if (ngx_http_cache_purge_index_dict_load(tx, id, &value) && leaf
            && value.format != 0)
        {
            old_count = value.count;
            memcpy(old, value.parts, old_count * sizeof(old[0]));
            value.format = 0;
            value.count = 0;
            value.raw = text;
            ngx_http_cache_purge_index_dict_save(tx, id, &value);
            for (i = 0; i < old_count && q->status == 0; i++) {
                ngx_http_cache_purge_index_dict_drop(tx, old[i]);
            }
        }
        return id;
    }
    memset(&value, 0, sizeof(value));
    value.format = format;
    if (format == 0) {
        value.raw = text;
    } else {
        value.count = count;
        for (i = 0; i < count && q->status == 0; i++) {
            part =
                ngx_http_cache_purge_index_intern(tx, parts[i], 0, NULL, 0, 1);
            ngx_http_cache_purge_index_dict_retain(tx, part);
            value.parts[i] = part;
        }
    }
    id = ngx_http_cache_purge_index_allocate(tx);
    ngx_http_cache_purge_index_dict_save(tx, id, &value);
    if (ngx_http_cache_purge_index_digest(q, text, hash)) {
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_digest_key(q, hash, id),
            ngx_http_cache_purge_op_empty, 1);
        ngx_http_cache_purge_index_remember(tx, hash, id);
    }
    return id;
}

static void
ngx_http_cache_purge_index_fields(const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_bytes_t fields[11])
{
    fields[0] = file->key;
    fields[1] = file->url;
    fields[2] = file->host;
    fields[3] = file->tag_namespace;
    fields[4] = file->root;
    fields[5] = file->zone;
    fields[6] = file->path;
    fields[7] = file->range;
    fields[8] = file->vary;
    fields[9] = file->metadata;
    fields[10] = file->misc;
}

static int
ngx_http_cache_purge_index_input(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t value, size_t limit)
{
    return ngx_http_cache_purge_index_require(
               q, value.len == 0 || value.data != NULL,
               NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid index input bytes")
           && ngx_http_cache_purge_index_require(
               q, value.len <= limit, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
               "index input field exceeds limit");
}

static int
ngx_http_cache_purge_index_relative(ngx_http_cache_rocksdb_bytes_t path)
{
    size_t start, end, size;

    if (path.len == 0 || path.data[0] == '/' || path.data[path.len - 1] == '/'
        || memchr(path.data, 0, path.len) != NULL)
    {
        return 0;
    }
    for (start = 0; start < path.len; start = end + 1) {
        for (end = start; end < path.len && path.data[end] != '/'; end++) {
            /* void */
        }
        size = end - start;
        if (size == 0 || (size == 1 && path.data[start] == '.')
            || (size == 2 && path.data[start] == '.'
                && path.data[start + 1] == '.'))
        {
            return 0;
        }
    }
    return 1;
}

static int
ngx_http_cache_purge_index_compare(const void *left, const void *right)
{
    const ngx_http_cache_rocksdb_bytes_t *a, *b;
    size_t  size;
    int     rc;

    a = left;
    b = right;
    size = a->len < b->len ? a->len : b->len;
    rc = size != 0 ? memcmp(a->data, b->data, size) : 0;
    return rc != 0 ? rc : a->len < b->len ? -1 : a->len != b->len;
}

static size_t
ngx_http_cache_purge_index_tags(ngx_http_cache_purge_query_t *q,
    const ngx_http_cache_rocksdb_bytes_t *input, size_t count,
    ngx_http_cache_rocksdb_bytes_t tags[64])
{
    size_t i, used;

    if (!ngx_http_cache_purge_index_require(
            q, count <= 64, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "too many TAGs")
        || !ngx_http_cache_purge_index_require(q, count == 0 || input != NULL,
                                               NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                               "missing TAG bytes"))
    {
        return 0;
    }
    for (i = 0; i < count; i++) {
        if (!ngx_http_cache_purge_index_require(
                q,
                input[i].len != 0 && input[i].len <= 1024
                    && input[i].data != NULL,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid TAG length"))
        {
            return 0;
        }
        tags[i] = input[i];
    }
    if (count > 1) {
        qsort(tags, count, sizeof(tags[0]), ngx_http_cache_purge_index_compare);
    }
    used = 0;
    for (i = 0; i < count; i++) {
        if (used == 0
            || !ngx_http_cache_purge_op_equal(tags[used - 1], tags[i]))
        {
            tags[used++] = tags[i];
        }
    }
    return used;
}

static void
ngx_http_cache_purge_index_hash_field(ngx_http_cache_purge_query_t *q,
    EVP_MD_CTX *ctx, ngx_http_cache_rocksdb_bytes_t value)
{
    unsigned char  raw[8];
    uint64_t       size;
    unsigned       i;

    size = value.len;
    for (i = 0; i < 8; i++) {
        raw[7 - i] = (unsigned char) size;
        size >>= 8;
    }
    if (q->status == 0) {
        ngx_http_cache_purge_index_require(
            q,
            EVP_DigestUpdate(ctx, raw, 8) == 1
                && (value.len == 0
                    || EVP_DigestUpdate(ctx, value.data, value.len) == 1),
            NGX_HTTP_CACHE_ROCKSDB_IO, "index fingerprint update failed");
    }
}

static int
ngx_http_cache_purge_index_fingerprint(ngx_http_cache_purge_query_t *q,
    const ngx_http_cache_purge_index_file_t *file,
    const ngx_http_cache_rocksdb_bytes_t *tags, size_t count,
    unsigned char hash[32])
{
    ngx_http_cache_rocksdb_bytes_t  fields[11];
    EVP_MD_CTX                     *ctx;
    size_t                          i, total, length;
    unsigned char                   identity[32];
    ngx_http_cache_rocksdb_error_t  error;
    int                             rc;
    unsigned                        hash_size;

    ngx_http_cache_purge_index_fields(file, fields);
    total = 0;
    for (i = 0; i < 11; i++) {
        if (!ngx_http_cache_purge_index_input(q, fields[i],
                                              i == 9 ? 16384 : 32768))
        {
            return 0;
        }
        total += fields[i].len;
    }
    for (i = 0; i < 6; i++) {
        if (!ngx_http_cache_purge_index_input(q, file->components[i], 32768)) {
            return 0;
        }
        total += file->components[i].len;
    }
    for (i = 0; i < count; i++) {
        total += tags[i].len;
    }
    if (!ngx_http_cache_purge_index_require(
            q,
            !ngx_http_cache_purge_index_zero(file->id) && file->state == 1
                && file->revision == 0
                && file->structured_key == 0
                && (file->structured_url == 0 || file->structured_url == 1),
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "prepare requires a new file descriptor")
        || !ngx_http_cache_purge_index_require(
            q,
            file->key.len && file->url.len && file->host.len
                && file->zone.len && file->root.len
                && file->root.data[0] == '/'
                && memchr(file->root.data, 0, file->root.len) == NULL
                && ngx_http_cache_purge_index_relative(file->path),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid file identity or path")
        || !ngx_http_cache_purge_index_require(
            q, file->tag_namespace.len <= 255, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "TAG namespace too long")
        || !ngx_http_cache_purge_index_require(q, total <= 262144,
                                               NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                               "file index input budget"))
    {
        return 0;
    }
    for (i = 0; i < file->tag_namespace.len; i++) {
        if (!ngx_http_cache_purge_index_require(
                q,
                file->tag_namespace.data[i] > 32
                    && file->tag_namespace.data[i] < 127
                    && file->tag_namespace.data[i] != ',',
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid TAG namespace"))
        {
            return 0;
        }
    }
    rc = ngx_http_cache_purge_identity_digest(file->url, file->tag_namespace,
                                             file->misc, identity, &error);
    if (rc != 0) {
        ngx_http_cache_purge_index_fail(q, rc, error.message);
        return 0;
    }
    if (!ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_key_projection(q, file->key),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid internal key identity"))
    {
        return 0;
    }
    if (!ngx_http_cache_purge_index_require(q,
            memcmp(q->key.data + 1, identity, 32) == 0,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "internal key does not match URL, namespace and misc"))
    {
        return 0;
    }
    if (file->structured_url) {
        length = file->components[1].len;
        total = length + 3 + file->components[2].len + file->components[3].len;
        ngx_http_cache_purge_index_require(
            q,
            total == file->url.len
                && (length == 0
                    || memcmp(file->url.data, file->components[1].data, length)
                           == 0)
                && memcmp(file->url.data + length, "://", 3) == 0
                && (file->components[2].len == 0
                    || memcmp(file->url.data + length + 3,
                              file->components[2].data, file->components[2].len)
                           == 0)
                && (file->components[3].len == 0
                    || memcmp(file->url.data + length + 3
                                  + file->components[2].len,
                              file->components[3].data, file->components[3].len)
                           == 0),
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "URL components do not match final URL bytes");
    }
    if (q->status != 0) {
        return 0;
    }
    ctx = EVP_MD_CTX_create();
    if (!ngx_http_cache_purge_index_require(
            q, ctx != NULL, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "index fingerprint allocation failed"))
    {
        return 0;
    }
    ngx_http_cache_purge_index_require(
        q,
        EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) == 1
            && EVP_DigestUpdate(ctx, file->id, 16) == 1,
        NGX_HTTP_CACHE_ROCKSDB_IO, "index fingerprint initialization failed");
    for (i = 0; i < 11; i++) {
        ngx_http_cache_purge_index_hash_field(q, ctx, fields[i]);
    }
    for (i = 0; i < count; i++) {
        ngx_http_cache_purge_index_hash_field(q, ctx, tags[i]);
    }
    hash_size = 0;
    if (q->status == 0) {
        ngx_http_cache_purge_index_require(
            q,
            EVP_DigestFinal_ex(ctx, hash, &hash_size) == 1 && hash_size == 32,
            NGX_HTTP_CACHE_ROCKSDB_IO, "index fingerprint finalization failed");
    }
    EVP_MD_CTX_destroy(ctx);
    return q->status == 0;
}

static void
ngx_http_cache_purge_index_url_link(ngx_http_cache_purge_index_tx_t *tx,
    uint64_t key, uint64_t url, ngx_http_cache_rocksdb_bytes_t text, int create)
{
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_value_t  count, projection, forward;
    ngx_http_cache_rocksdb_bytes_t      identity;
    uint64_t                            uses, tag_namespace, misc;

    q = tx->context;
    identity = ngx_http_cache_purge_index_tx_need(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'k', key));
    if (!ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_key_number(q, identity, 1) == url,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "key url identity cannot change"))
    {
        return;
    }
    tag_namespace = ngx_http_cache_purge_index_key_number(q, identity, 3);
    misc = ngx_http_cache_purge_index_key_number(q, identity, 4);
    count = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'u', url));
    projection = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_URL,
        ngx_http_cache_purge_index_text_key(q, 'U', text));
    uses =
        count.exists ? ngx_http_cache_purge_index_integer(q, count.bytes) : 0;
    if (!ngx_http_cache_purge_index_require(
            q,
            count.exists ?
                projection.exists && uses != 0
                    && ngx_http_cache_purge_index_integer(q, projection.bytes)
                           == url :
                !projection.exists,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent URL projection"))
    {
        return;
    }
    forward = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_URL,
        ngx_http_cache_purge_index_variant_key(q, url, tag_namespace, misc));
    if (!ngx_http_cache_purge_index_require(
            q,
            forward.exists == !create
                && (!forward.exists
                    || (count.exists
                        && ngx_http_cache_purge_index_integer(q,
                                                      forward.bytes) == key)),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent URL/key relationship")
        || forward.exists)
    {
        return;
    }
    if (!count.exists) {
        ngx_http_cache_purge_index_dict_retain(tx, url);
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_URL,
            ngx_http_cache_purge_index_text_key(q, 'U', text),
            ngx_http_cache_purge_index_encoded_number(q, url), 1);
    }
    if (!ngx_http_cache_purge_index_require(q, uses != UINT64_MAX,
                                            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                            "URL relationship overflow"))
    {
        return;
    }
    ngx_http_cache_purge_index_counter(tx, 'u', url, uses + 1);
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_URL,
        ngx_http_cache_purge_index_variant_key(q, url, tag_namespace, misc),
        ngx_http_cache_purge_index_encoded_number(q, key), 1);
}

static void
ngx_http_cache_purge_index_host_link(ngx_http_cache_purge_index_tx_t *tx,
    uint64_t key, uint64_t host, int create)
{
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_value_t  forward;
    ngx_http_cache_rocksdb_bytes_t      identity;

    q = tx->context;
    identity = ngx_http_cache_purge_index_tx_need(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'k', key));
    if (!ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_key_number(q, identity, 2) == host,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "key host identity cannot change"))
    {
        return;
    }
    forward = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_relation(q, 'B', host, key));
    if (!ngx_http_cache_purge_index_require(q,
            forward.exists == !create
                && (!forward.exists || forward.bytes.len == 0),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "inconsistent host/key relationship")
        || forward.exists)
    {
        return;
    }
    ngx_http_cache_purge_index_dict_retain(tx, host);
    ngx_http_cache_purge_index_tx_set(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_relation(q, 'B', host, key),
        ngx_http_cache_purge_op_empty, 1);
}

static void
ngx_http_cache_purge_index_tag_links(ngx_http_cache_purge_index_tx_t *tx,
    const ngx_http_cache_purge_index_record_t *record, int add)
{
    size_t i;

    for (i = 0; i < record->tag_count && tx->context->status == 0; i++) {
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_TAG,
            ngx_http_cache_purge_index_posting(
                tx->context, 'N', record->refs[3], record->tags[i], record->id),
            ngx_http_cache_purge_op_empty, add);
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_TAG,
            ngx_http_cache_purge_index_posting(
                tx->context, 'T', record->tags[i], 0, record->id),
            ngx_http_cache_purge_op_empty, add);
    }
}

static uint64_t
ngx_http_cache_purge_index_field(ngx_http_cache_purge_index_tx_t *tx,
    ngx_http_cache_rocksdb_bytes_t text)
{
    uint64_t id;

    id = ngx_http_cache_purge_index_intern(tx, text, 0, NULL, 0, 1);
    ngx_http_cache_purge_index_dict_retain(tx, id);
    return id;
}

static void
ngx_http_cache_purge_index_match(
    const ngx_http_cache_purge_index_record_t *record,
    ngx_http_cache_purge_index_match_t *out)
{
    memcpy(out->id, record->id, 16);
    out->revision = record->revision;
    out->state = record->state;
}

static void
ngx_http_cache_purge_index_prepare_one(ngx_http_cache_purge_index_tx_t *tx,
    const ngx_http_cache_purge_index_file_t *file,
    const unsigned char previous[16], ngx_http_cache_purge_index_match_t *out)
{
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_record_t  record;
    ngx_http_cache_purge_index_value_t  existing, current, intent, count,
        projection;
    ngx_http_cache_rocksdb_bytes_t tags[64], hash_bytes, owner;
    unsigned char  hash[32], identity_digest[32];
    uint64_t       files, misc, identity[4];
    size_t                         tag_count, i;

    q = tx->context;
    tag_count =
        ngx_http_cache_purge_index_tags(q, file->tags, file->tag_count, tags);
    if (!ngx_http_cache_purge_index_fingerprint(q, file, tags, tag_count, hash)
        || !ngx_http_cache_purge_index_require(
            q, memcmp(file->id, previous, 16) != 0,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "file cannot replace itself"))
    {
        return;
    }
    hash_bytes.data = hash;
    hash_bytes.len = 32;
    existing = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'O', file->id));
    if (existing.exists) {
        if (!ngx_http_cache_purge_index_decode(q, existing.bytes, &record)
            || !ngx_http_cache_purge_index_require(
                q,
                memcmp(record.id, file->id, 16) == 0 && record.state != 3
                    && memcmp(record.previous, previous, 16) == 0
                    && ngx_http_cache_purge_op_equal(record.digest, hash_bytes),
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "file ID reused with another fill"))
        {
            return;
        }
        if (record.state == 1) {
            owner = ngx_http_cache_purge_index_tx_need(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_path_key(q, 'I', &record));
            ngx_http_cache_purge_index_require(
                q,
                ngx_http_cache_purge_op_equal(
                    owner, ngx_http_cache_purge_index_id(file->id)),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "prepared file has no matching lease");
        }
        if (q->status == 0) {
            ngx_http_cache_purge_index_match(&record, out);
        }
        return;
    }
    memset(&record, 0, sizeof(record));
    memcpy(record.id, file->id, 16);
    memcpy(record.previous, previous, 16);
    record.state = 1;
    record.revision = 1;
    record.path = file->path;
    record.metadata = file->metadata;
    record.digest = hash_bytes;
    record.refs[4] = ngx_http_cache_purge_index_field(tx, file->root);
    current = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'P', &record));
    if (!ngx_http_cache_purge_index_require(
            q, ngx_http_cache_purge_index_path_matches(q, current, previous),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "cache path instance changed"))
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
    if (!ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_key_projection(q, file->key),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid internal key identity"))
    {
        return;
    }
    memcpy(identity_digest, q->key.data + 1, 32);
    projection = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_bytes(&q->key));
    record.refs[0] = projection.exists
        ? ngx_http_cache_purge_index_integer(q, projection.bytes)
        : ngx_http_cache_purge_index_allocate(tx);
    count = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'k', record.refs[0]));
    if (!ngx_http_cache_purge_index_require(q,
            count.exists == projection.exists,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent key projection"))
    {
        return;
    }
    files = count.exists
            ? ngx_http_cache_purge_index_key_number(q, count.bytes, 0) : 0;
    if (!ngx_http_cache_purge_index_require(q,
            !count.exists || (count.bytes.len == 72
                && memcmp(count.bytes.data + 40, identity_digest, 32) == 0),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "key digest disagrees with projection"))
    {
        return;
    }
    for (i = 0; i < 4; i++) {
        identity[i] = count.exists
            ? ngx_http_cache_purge_index_key_number(q, count.bytes, i + 1) : 0;
    }
    if (!count.exists) {
        ngx_http_cache_purge_index_key_projection(q, file->key);
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->key),
            ngx_http_cache_purge_index_encoded_number(q, record.refs[0]), 1);
    }
    record.refs[1] = ngx_http_cache_purge_index_intern(tx, file->url,
        file->structured_url ? 2 : 0, &file->components[1], 3, 0);
    record.refs[2] = ngx_http_cache_purge_index_field(tx, file->host);
    record.refs[3] = ngx_http_cache_purge_index_field(tx, file->tag_namespace);
    misc = ngx_http_cache_purge_index_field(tx, file->misc);
    if (!ngx_http_cache_purge_index_require(q,
            !count.exists || (identity[0] == record.refs[1]
                && identity[1] == record.refs[2]
                && identity[2] == record.refs[3] && identity[3] == misc),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "key URL, host, namespace or misc identity cannot change"))
    {
        return;
    }
    if (!count.exists) {
        ngx_http_cache_purge_index_dict_retain(tx, record.refs[3]);
    }
    if (count.exists) {
        ngx_http_cache_purge_index_dict_drop(tx, misc);
    }
    if (!ngx_http_cache_purge_index_require(q, files != UINT64_MAX,
                                            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                            "key file count overflow"))
    {
        return;
    }
    ngx_http_cache_purge_index_key_counter(tx, record.refs[0], files + 1,
        record.refs[1], record.refs[2], record.refs[3], misc, identity_digest);
    ngx_http_cache_purge_index_url_link(tx, record.refs[0], record.refs[1],
                                       file->url, !count.exists);
    ngx_http_cache_purge_index_host_link(tx, record.refs[0], record.refs[2],
                                       !count.exists);
    record.refs[5] = ngx_http_cache_purge_index_field(tx, file->zone);
    record.refs[6] = ngx_http_cache_purge_index_field(tx, file->range);
    record.refs[7] = ngx_http_cache_purge_index_field(tx, file->vary);
    record.tag_count = tag_count;
    for (i = 0; i < tag_count && q->status == 0; i++) {
        record.tags[i] = ngx_http_cache_purge_index_field(tx, tags[i]);
    }
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_posting(q, 'F', record.refs[0], 0, file->id),
        ngx_http_cache_purge_op_empty, 1);
    ngx_http_cache_purge_index_tag_links(tx, &record, 1);
    ngx_http_cache_purge_index_save(tx, &record);
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'I', &record),
        ngx_http_cache_purge_index_id(file->id), 1);
    if (q->status == 0) {
        ngx_http_cache_purge_index_match(&record, out);
    }
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_native_key(ngx_http_cache_purge_query_t *q,
    const ngx_http_cache_purge_index_record_t *file,
    const unsigned char main[16], const unsigned char variant[16])
{
    ngx_http_cache_purge_index_row(q, &q->key, 'n', file->refs[0]);
    ngx_http_cache_purge_index_append(q, &q->key, main, 16);
    ngx_http_cache_purge_index_number(q, &q->key, file->refs[7]);
    ngx_http_cache_purge_index_append(q, &q->key, variant, 16);
    ngx_http_cache_purge_index_append(q, &q->key, file->id, 16);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static void
ngx_http_cache_purge_index_native_bind(ngx_http_cache_purge_index_tx_t *tx,
    const ngx_http_cache_purge_index_expected_t *expected,
    const unsigned char main[16], const unsigned char variant[16])
{
    ngx_http_cache_purge_index_record_t  file;
    ngx_http_cache_purge_index_value_t   binding, posting, missing;
    ngx_http_cache_purge_query_t         *q;
    ngx_http_cache_rocksdb_bytes_t        key, value;
    unsigned char                         native[32], raw[65];

    q = tx->context;
    if (!ngx_http_cache_purge_index_load(tx, expected, &file)) {
        return;
    }
    memcpy(native, main, 16);
    memcpy(native + 16, variant, 16);
    value.data = native;
    value.len = sizeof(native);
    binding = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'j', file.id));
    key = ngx_http_cache_purge_index_native_key(q, &file, main, variant);
    if (q->status != 0) {
        return;
    }
    memcpy(raw, key.data, sizeof(raw));
    key.data = raw;
    posting = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (!ngx_http_cache_purge_index_require(q,
            !binding.exists || ngx_http_cache_purge_op_equal(binding.bytes,
                value), NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "native file identity changed")
        || !ngx_http_cache_purge_index_require(q,
            binding.exists == posting.exists
            && (!posting.exists || posting.bytes.len == 0),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent native posting"))
    {
        return;
    }
    if (!binding.exists) {
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            key, ngx_http_cache_purge_op_empty, 1);
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_object_key(q, 'j', file.id), value, 1);
    }
    key = ngx_http_cache_purge_index_posting(q, 'z', file.refs[0], 0, file.id);
    missing = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (missing.exists
        && ngx_http_cache_purge_index_require(q, missing.bytes.len == 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid unresolved identity"))
    {
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            key, ngx_http_cache_purge_op_empty, 0);
    }
}

int
ngx_http_cache_purge_index_native_identity(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    unsigned char main[16], unsigned char variant[16],
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t     *tx;
    ngx_http_cache_purge_index_record_t  record;
    ngx_http_cache_purge_index_value_t   value;
    ngx_http_cache_purge_query_t        *q;

    if (main != NULL) {
        memset(main, 0, 16);
    }
    if (variant != NULL) {
        memset(variant, 0, 16);
    }
    if (store == NULL || file == NULL || main == NULL || variant == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native identity query");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    if (ngx_http_cache_purge_index_load(tx, file, &record)) {
        value = ngx_http_cache_purge_index_tx_get(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_object_key(q, 'j', file->id));
        if (ngx_http_cache_purge_index_require(q, value.exists,
                NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "native binding is absent")
            && ngx_http_cache_purge_index_require(q, value.bytes.len == 32,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid native binding"))
        {
            memcpy(main, value.bytes.data, 16);
            memcpy(variant, value.bytes.data + 16, 16);
        }
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_native_ready(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_bytes_t key, int *ready,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t     *tx;
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_value_t   cursor;
    uint64_t                             id;

    if (ready != NULL) {
        *ready = 0;
    }
    if (store == NULL || ready == NULL || !key.len
        || !ngx_http_cache_purge_op_bytes_valid(key, 32768))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native readiness query");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    id = ngx_http_cache_purge_index_key_find(tx->context, key);
    if (!id) {
        if (q->status == 0) {
            *ready = 1;
        }
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    cursor = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_op_literal("M:native-cursor"));
    if (cursor.exists || q->status != 0) {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    if (id != 0) {
        ngx_http_cache_purge_index_cursor_open(q, &q->outer,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_row_key(q, 'z', id),
            ngx_http_cache_purge_op_empty, NULL);
        if (ngx_http_cache_purge_index_cursor_next(q, &q->outer)) {
            ngx_http_cache_purge_index_require(q,
                q->outer.key.len == 25 && q->outer.value.len == 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid unresolved identity");
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
    }
    if (q->status == 0) {
        *ready = 1;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

static void
ngx_http_cache_purge_index_resolve_native(ngx_http_cache_purge_index_tx_t *tx,
    const ngx_http_cache_purge_index_record_t *record,
    ngx_http_cache_purge_native_resolver_pt resolver, void *data)
{
    ngx_http_cache_purge_query_t          *q;
    ngx_http_cache_purge_index_value_t     binding, missing;
    ngx_http_cache_purge_index_file_t      file;
    ngx_http_cache_purge_index_expected_t  expected;
    ngx_http_cache_rocksdb_error_t         failure;
    ngx_http_cache_rocksdb_bytes_t         key;
    unsigned char                          main[16], variant[16];
    unsigned                               state;
    int                                    rc;

    q = tx->context;
    memcpy(expected.id, record->id, 16);
    expected.revision = record->revision;
    binding = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'j', record->id));
    if (binding.exists) {
        if (ngx_http_cache_purge_index_require(q, binding.bytes.len == 32,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid native binding"))
        {
            ngx_http_cache_purge_index_native_bind(tx, &expected,
                binding.bytes.data, binding.bytes.data + 16);
        }
        return;
    }
    if (q->status != 0) {
        return;
    }
    memset(&file, 0, sizeof(file));
    memset(&failure, 0, sizeof(failure));
    memset(main, 0, sizeof(main));
    memset(variant, 0, sizeof(variant));
    state = NGX_HTTP_CACHE_PURGE_NATIVE_MISSING;
    rc = ngx_http_cache_purge_index_get_file(q->store, record->id, &file,
        &failure);
    if (rc == 0) {
        rc = resolver(data, &file, main, variant, &state, &failure);
    }
    ngx_http_cache_purge_index_file_free(&file);
    if (rc != 0) {
        ngx_http_cache_purge_index_fail(q, rc,
            failure.message[0] ? failure.message
                               : "native migration resolver failed");
        return;
    }
    if (!ngx_http_cache_purge_index_require(q,
            state <= NGX_HTTP_CACHE_PURGE_NATIVE_MISSING,
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid native migration resolution"))
    {
        return;
    }
    if (state == NGX_HTTP_CACHE_PURGE_NATIVE_BIND) {
        ngx_http_cache_purge_index_native_bind(tx, &expected, main, variant);
        return;
    }
    key = ngx_http_cache_purge_index_posting(q, 'z', record->refs[0], 0,
        record->id);
    missing = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (!ngx_http_cache_purge_index_require(q,
            !missing.exists || missing.bytes.len == 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid unresolved identity"))
    {
        return;
    }
    if (missing.exists != (state == NGX_HTTP_CACHE_PURGE_NATIVE_MISSING)) {
        ngx_http_cache_purge_index_tx_set(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, ngx_http_cache_purge_op_empty,
            state == NGX_HTTP_CACHE_PURGE_NATIVE_MISSING);
    }
}

int
ngx_http_cache_purge_index_migrate_native(ngx_http_cache_rocksdb_t *store,
    size_t budget, ngx_http_cache_purge_native_resolver_pt resolver, void *data,
    int *done, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t      *tx;
    ngx_http_cache_purge_query_t         *q;
    ngx_http_cache_purge_index_value_t    cursor;
    ngx_http_cache_purge_index_record_t   record;
    size_t                                count;
    int                                   more;

    if (done != NULL) {
        *done = 0;
    }
    if (store == NULL || resolver == NULL || done == NULL
        || !budget || budget > 32)
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native migration budget");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    cursor = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_op_literal("M:native-cursor"));
    if (!cursor.exists) {
        if (q->status == 0) {
            *done = 1;
        }
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    if (!ngx_http_cache_purge_index_require(q,
            (cursor.bytes.len == 1 || cursor.bytes.len == 17)
            && cursor.bytes.data[0] == 'O', NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "invalid native migration cursor"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    ngx_http_cache_purge_index_cursor_open(q, &q->outer,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_op_literal("O"), cursor.bytes, NULL);
    count = 0;
    while (count < budget
           && ngx_http_cache_purge_index_cursor_next(q, &q->outer))
    {
        if (!ngx_http_cache_purge_index_require(q, q->outer.key.len == 17,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid native migration row")
            || !ngx_http_cache_purge_index_decode(q,
                ngx_http_cache_purge_index_bytes(&q->outer.value), &record)
            || !ngx_http_cache_purge_index_require(q,
                memcmp(record.id, q->outer.key.data + 1, 16) == 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "native migration ID mismatch"))
        {
            break;
        }
        ngx_http_cache_purge_index_resolve_native(tx, &record, resolver, data);
        ngx_http_cache_purge_index_set(q, &q->token,
            ngx_http_cache_purge_index_bytes(&q->outer.key));
        count++;
    }
    more = count == budget
           && ngx_http_cache_purge_index_cursor_next(q, &q->outer);
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_op_literal("M:native-cursor"),
        more ? ngx_http_cache_purge_index_bytes(&q->token)
             : ngx_http_cache_purge_op_empty, more);
    ngx_http_cache_purge_index_tx_commit(tx);
    if (q->status == 0) {
        *done = !more;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_retry_native(ngx_http_cache_rocksdb_t *store,
    size_t budget, ngx_http_cache_purge_native_resolver_pt resolver, void *data,
    unsigned char bookmark[25], size_t *length,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t      *tx;
    ngx_http_cache_purge_query_t         *q;
    ngx_http_cache_purge_index_value_t    value;
    ngx_http_cache_purge_index_record_t   record;
    ngx_http_cache_rocksdb_bytes_t        start, number;
    size_t                                count;
    int                                   more;

    if (store == NULL || resolver == NULL || bookmark == NULL || length == NULL
        || !budget || budget > 32
        || (*length && (*length != 25 || bookmark[0] != 'z')))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native retry cursor");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    start.data = bookmark;
    start.len = *length;
    ngx_http_cache_purge_index_cursor_open(q, &q->outer,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, ngx_http_cache_purge_op_literal("z"),
        start, NULL);
    count = 0;
    while (count < budget
           && ngx_http_cache_purge_index_cursor_next(q, &q->outer))
    {
        if (!ngx_http_cache_purge_index_require(q,
                q->outer.key.len == 25 && q->outer.value.len == 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid unresolved identity"))
        {
            break;
        }
        value = ngx_http_cache_purge_index_tx_get(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_object_key(q, 'O',
                q->outer.key.data + 9));
        number.data = q->outer.key.data + 1;
        number.len = 8;
        if (!ngx_http_cache_purge_index_require(q, value.exists,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "unresolved file is absent")
            || !ngx_http_cache_purge_index_decode(q, value.bytes, &record)
            || !ngx_http_cache_purge_index_require(q,
                memcmp(record.id, q->outer.key.data + 9, 16) == 0
                && record.refs[0]
                   == ngx_http_cache_purge_index_integer(q, number),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "unresolved file differs"))
        {
            break;
        }
        ngx_http_cache_purge_index_resolve_native(tx, &record, resolver, data);
        ngx_http_cache_purge_index_set(q, &q->token,
            ngx_http_cache_purge_index_bytes(&q->outer.key));
        count++;
    }
    more = count == budget
           && ngx_http_cache_purge_index_cursor_next(q, &q->outer);
    ngx_http_cache_purge_index_tx_commit(tx);
    if (q->status == 0) {
        *length = more ? q->token.len : 0;
        if (*length) {
            memcpy(bookmark, q->token.data, *length);
        }
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_bind_native(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const unsigned char main[16], const unsigned char variant[16],
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t  *tx;

    if (store == NULL || file == NULL || main == NULL || variant == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native file binding");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    ngx_http_cache_purge_index_native_bind(tx, file, main, variant);
    ngx_http_cache_purge_index_tx_commit(tx);
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

static int
ngx_http_cache_purge_index_prepare_internal(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_file_t *file,
    const unsigned char previous[16], const unsigned char main[16],
    const unsigned char variant[16], ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t       *tx;
    ngx_http_cache_purge_index_match_t     result;
    ngx_http_cache_purge_index_expected_t  expected;
    static const unsigned char            empty[16] = { 0 };

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (store == NULL || file == NULL || out == NULL) {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid file preparation arguments");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    memset(&result, 0, sizeof(result));
    ngx_http_cache_purge_index_prepare_one(
        tx, file, previous != NULL ? previous : empty, &result);
    if (tx->context->status == 0 && main != NULL) {
        memcpy(expected.id, result.id, 16);
        expected.revision = result.revision;
        ngx_http_cache_purge_index_native_bind(tx, &expected, main, variant);
    }
    tx->deferred = 1;
    ngx_http_cache_purge_index_tx_commit(tx);
    if (tx->context->status == 0) {
        *out = result;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_prepare(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_file_t *file,
    const unsigned char previous[16], ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_prepare_internal(store, file, previous,
        NULL, NULL, out, error);
}

int
ngx_http_cache_purge_index_prepare_native(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_file_t *file,
    const unsigned char previous[16], const unsigned char main[16],
    const unsigned char variant[16], ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (main == NULL || variant == NULL) {
        if (out != NULL) {
            memset(out, 0, sizeof(*out));
        }
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing native file identity");
    }
    return ngx_http_cache_purge_index_prepare_internal(store, file, previous,
        main, variant, out, error);
}

int
ngx_http_cache_purge_index_prepare_batch(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_fill_t *fills, size_t count,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t    *tx;
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_match_t  result[32];
    ngx_http_cache_rocksdb_bytes_t      fields[11];
    size_t                             i, j, remaining;

    if (out != NULL && count <= 32) {
        memset(out, 0, count * sizeof(*out));
    }
    if (store == NULL || fills == NULL || out == NULL) {
        return ngx_http_cache_purge_index_error(error,
                                                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                                "invalid fill batch arguments");
    }
    if (count == 0 || count > 32) {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "fill batch must contain 1-32 files");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    remaining = 262144;
    for (i = 0; i < count && q->status == 0; i++) {
        if (!ngx_http_cache_purge_index_require(
                q, fills[i].file.tag_count <= 64, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "too many TAGs"))
        {
            break;
        }
        ngx_http_cache_purge_index_fields(&fills[i].file, fields);
        for (j = 0; j < 11 && q->status == 0; j++) {
            if (ngx_http_cache_purge_index_require(
                    q, fields[j].len <= remaining, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                    "fill batch input budget"))
            {
                remaining -= fields[j].len;
            }
        }
        for (j = 0; j < 6 && q->status == 0; j++) {
            if (ngx_http_cache_purge_index_require(
                    q, fills[i].file.components[j].len <= remaining,
                    NGX_HTTP_CACHE_ROCKSDB_LIMIT, "fill batch input budget"))
            {
                remaining -= fills[i].file.components[j].len;
            }
        }
        for (j = 0; j < fills[i].file.tag_count && q->status == 0; j++) {
            if (ngx_http_cache_purge_index_require(
                    q, fills[i].file.tags[j].len <= remaining,
                    NGX_HTTP_CACHE_ROCKSDB_LIMIT, "fill batch input budget"))
            {
                remaining -= fills[i].file.tags[j].len;
            }
        }
    }
    memset(result, 0, sizeof(result));
    for (i = 0; i < count && q->status == 0; i++) {
        ngx_http_cache_purge_index_prepare_one(tx, &fills[i].file,
                                               fills[i].previous, &result[i]);
    }
    tx->deferred = 1;
    ngx_http_cache_purge_index_tx_commit(tx);
    if (q->status == 0) {
        memcpy(out, result, count * sizeof(*out));
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

static int
ngx_http_cache_purge_index_fetch(ngx_http_cache_purge_index_tx_t *tx,
    const unsigned char id[16], int absent,
    ngx_http_cache_purge_index_record_t *record)
{
    ngx_http_cache_purge_index_value_t  value;
    ngx_http_cache_purge_query_t       *q;

    q = tx->context;
    value = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'O', id));
    return ngx_http_cache_purge_index_require(q, value.exists, absent,
                                              "file instance not indexed")
           && ngx_http_cache_purge_index_decode(q, value.bytes, record)
           && ngx_http_cache_purge_index_require(
               q, memcmp(record->id, id, 16) == 0,
               NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "file identity mismatch");
}

void
ngx_http_cache_purge_index_identity_free(
    ngx_http_cache_purge_index_identity_t *identity)
{
    if (identity != NULL) {
        free(identity->storage);
        memset(identity, 0, sizeof(*identity));
    }
}

int
ngx_http_cache_purge_index_identity(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_bytes_t key,
    ngx_http_cache_purge_index_identity_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t    *tx;
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_value_t  value;
    ngx_http_cache_rocksdb_bytes_t     *fields[4], raw;
    uint64_t                           id, refs[4];
    size_t                             offsets[4], i;
    int                                rc;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (store == NULL || out == NULL || key.len == 0
        || !ngx_http_cache_purge_op_bytes_valid(key, 32768))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid key identity lookup");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    id = ngx_http_cache_purge_index_key_find(tx->context, key);
    value = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'k', id));
    if (!ngx_http_cache_purge_index_require(q, id != 0 && value.exists,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "key identity is absent"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    for (i = 0; i < 4; i++) {
        refs[i] = ngx_http_cache_purge_index_key_number(q, value.bytes, i + 1);
    }
    fields[0] = &out->url;
    fields[1] = &out->host;
    fields[2] = &out->tag_namespace;
    fields[3] = &out->misc;
    for (i = 0; i < 4 && q->status == 0; i++) {
        raw = ngx_http_cache_purge_index_dict_text(tx, refs[i]);
        offsets[i] = q->token.len;
        fields[i]->len = raw.len;
        ngx_http_cache_purge_index_append(q, &q->token, raw.data, raw.len);
    }
    if (q->status == 0) {
        for (i = 0; i < 4; i++) {
            fields[i]->data = q->token.data + offsets[i];
        }
        out->storage = q->token.data;
        q->bytes -= q->token.capacity;
        memset(&q->token, 0, sizeof(q->token));
    }
    rc = ngx_http_cache_purge_index_tx_end(tx, error);
    if (rc != 0) {
        ngx_http_cache_purge_index_identity_free(out);
    }
    return rc;
}

void
ngx_http_cache_purge_index_file_free(ngx_http_cache_purge_index_file_t *file)
{
    if (file != NULL) {
        free(file->storage);
        memset(file, 0, sizeof(*file));
    }
}

int
ngx_http_cache_purge_index_get_file(ngx_http_cache_rocksdb_t *store,
    const unsigned char id[16], ngx_http_cache_purge_index_file_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t     *tx;
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_record_t  record;
    ngx_http_cache_rocksdb_bytes_t     *fields[75], raw;
    size_t                              offsets[75], i, count;
    uint64_t                            misc;
    int                                 rc;
    static const unsigned refs[10] = { 0, 1, 2, 3, 4, 5, 0, 6, 7, 0 };

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (store == NULL || id == NULL || out == NULL) {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid file lookup arguments");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    if (!ngx_http_cache_purge_index_fetch(
            tx, id, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, &record))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    raw = ngx_http_cache_purge_index_tx_need(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'k', record.refs[0]));
    misc = ngx_http_cache_purge_index_key_number(q, raw, 4);
    fields[0] = &out->key;
    fields[1] = &out->url;
    fields[2] = &out->host;
    fields[3] = &out->tag_namespace;
    fields[4] = &out->root;
    fields[5] = &out->zone;
    fields[6] = &out->path;
    fields[7] = &out->range;
    fields[8] = &out->vary;
    fields[9] = &out->metadata;
    fields[10] = &out->misc;
    for (i = 0; i < record.tag_count; i++) {
        fields[11 + i] = &out->tags[i];
    }
    count = 11 + record.tag_count;
    for (i = 0; i < count && q->status == 0; i++) {
        if (i == 0) {
            ngx_http_cache_purge_index_require(q,
                ngx_http_cache_purge_index_key_text(q, NULL, record.refs[0]),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing key identity");
            raw = ngx_http_cache_purge_index_bytes(&q->lookup);
        } else if (i == 6) {
            raw = record.path;
        } else if (i == 9) {
            raw = record.metadata;
        } else if (i == 10) {
            raw = ngx_http_cache_purge_index_dict_text(tx, misc);
        } else {
            raw = ngx_http_cache_purge_index_dict_text(
                tx, i < 10 ? record.refs[refs[i]] : record.tags[i - 11]);
        }
        offsets[i] = q->token.len;
        fields[i]->len = raw.len;
        ngx_http_cache_purge_index_append(q, &q->token, raw.data, raw.len);
    }
    if (q->status == 0) {
        for (i = 0; i < count; i++) {
            fields[i]->data = q->token.data + offsets[i];
        }
        out->storage = q->token.data;
        q->bytes -= q->token.capacity;
        memset(&q->token, 0, sizeof(q->token));
        memcpy(out->id, id, 16);
        memcpy(out->previous, record.previous, 16);
        out->state = record.state;
        out->revision = record.revision;
        out->tag_count = record.tag_count;
    }
    rc = ngx_http_cache_purge_index_tx_end(tx, error);
    if (rc != 0) {
        ngx_http_cache_purge_index_file_free(out);
    }
    return rc;
}

static int
ngx_http_cache_purge_index_file_policy(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const ngx_http_cache_purge_watermark_t *input,
    ngx_http_cache_purge_watermark_t *out, unsigned char kind,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t      *tx;
    ngx_http_cache_purge_query_t         *q;
    ngx_http_cache_purge_index_record_t   record;
    ngx_http_cache_purge_index_value_t    value, intent, current;
    ngx_http_cache_purge_index_reader_t   reader;
    ngx_http_cache_purge_watermark_t      required;

    memset(out, 0, sizeof(*out));
    if (store == NULL || file == NULL
        || (input != NULL && (input->sequence == 0
            || ngx_http_cache_purge_index_zero(input->ledger))))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid file policy");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    if (!ngx_http_cache_purge_index_load(tx, file, &record)) {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    memset(&required, 0, sizeof(required));
    value = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, kind, file->id));
    if (value.exists) {
        if (!ngx_http_cache_purge_index_require(q,
                value.bytes.len == 24,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid file policy length"))
        {
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
        memcpy(required.ledger, value.bytes.data, 16);
        reader.query = q;
        reader.data = value.bytes;
        reader.at = 16;
        required.sequence = ngx_http_cache_purge_index_read_number(&reader, 8);
        ngx_http_cache_purge_index_require(q,
            required.sequence != 0
                && !ngx_http_cache_purge_index_zero(required.ledger),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid file policy boundary");
    }
    if (input != NULL && q->status == 0) {
        intent = ngx_http_cache_purge_index_tx_get(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'I', &record));
        current = ngx_http_cache_purge_index_tx_get(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'P', &record));
        if (!ngx_http_cache_purge_index_require(q, record.state == 2,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "file policy requires a committed file")
            || !ngx_http_cache_purge_index_require(q,
                kind == 'd' || !intent.exists,
                NGX_HTTP_CACHE_ROCKSDB_BUSY,
                "file policy awaits path publication")
            || !ngx_http_cache_purge_index_require(q,
                ngx_http_cache_purge_index_path_matches(q, current, file->id),
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "file policy target is no longer current")
            || !ngx_http_cache_purge_index_require(q,
                !value.exists
                    || memcmp(required.ledger, input->ledger, 16) == 0,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "file policy ledger changed"))
        {
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
        if (input->sequence > required.sequence) {
            required = *input;
            q->lookup.len = 0;
            ngx_http_cache_purge_index_append(q, &q->lookup,
                                              required.ledger, 16);
            ngx_http_cache_purge_index_number(q, &q->lookup, required.sequence);
            ngx_http_cache_purge_index_tx_set(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_object_key(q, kind, file->id),
                ngx_http_cache_purge_index_bytes(&q->lookup), 1);
        }
        ngx_http_cache_purge_index_tx_commit(tx);
    }
    if (q->status == 0) {
        *out = required;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_require_validation(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const ngx_http_cache_purge_watermark_t *required,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_watermark_t  out;

    if (required == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing validation requirement");
    }
    return ngx_http_cache_purge_index_file_policy(store, file, required,
                                                    &out, 'v', error);
}

int
ngx_http_cache_purge_index_validation_requirement(
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_purge_watermark_t *required,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (required == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing validation output");
    }
    return ngx_http_cache_purge_index_file_policy(store, file, NULL,
                                                    required, 'v', error);
}

int
ngx_http_cache_purge_index_require_delete(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const ngx_http_cache_purge_watermark_t *required,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_watermark_t  out;

    if (required == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing Delete requirement");
    }
    return ngx_http_cache_purge_index_file_policy(store, file, required,
                                                    &out, 'd', error);
}

int
ngx_http_cache_purge_index_delete_requirement(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_purge_watermark_t *required,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (required == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing Delete output");
    }
    return ngx_http_cache_purge_index_file_policy(store, file, NULL,
                                                    required, 'd', error);
}

int
ngx_http_cache_purge_index_clear_delete(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const ngx_http_cache_purge_watermark_t *required,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t      *tx;
    ngx_http_cache_purge_query_t         *q;
    ngx_http_cache_purge_index_record_t   record;
    ngx_http_cache_purge_index_value_t    value;

    if (store == NULL || file == NULL || required == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid Delete cancellation");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    if (!ngx_http_cache_purge_index_load(tx, file, &record)) {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    q->lookup.len = 0;
    ngx_http_cache_purge_index_append(q, &q->lookup, required->ledger, 16);
    ngx_http_cache_purge_index_number(q, &q->lookup, required->sequence);
    value = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'd', file->id));
    if (!value.exists) {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    if (ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_op_equal(value.bytes,
                ngx_http_cache_purge_index_bytes(&q->lookup)),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "Delete requirement changed"))
    {
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_object_key(q, 'd', file->id),
            ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_tx_commit(tx);
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_current(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_bytes_t root, ngx_http_cache_rocksdb_bytes_t path,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t    *tx;
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_value_t  current;
    ngx_http_cache_purge_index_record_t record, address;
    uint64_t       root_id;
    unsigned char  id[16];

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (store == NULL || out == NULL) {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid current-path arguments");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    if (!ngx_http_cache_purge_index_input(q, root, 32768)
        || !ngx_http_cache_purge_index_input(q, path, 32768)
        || !ngx_http_cache_purge_index_require(
            q,
            root.len != 0 && root.data[0] == '/'
                && memchr(root.data, 0, root.len) == NULL
                && ngx_http_cache_purge_index_relative(path),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid indexed path"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    root_id = ngx_http_cache_purge_index_dict_find(tx, root);
    if (!ngx_http_cache_purge_index_require(q, root_id != 0,
                                            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
                                            "root is not indexed"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    memset(&address, 0, sizeof(address));
    address.refs[4] = root_id;
    address.path = path;
    current = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'P', &address));
    if (ngx_http_cache_purge_index_require(q, current.exists,
                                           NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
                                           "path is not indexed")
        && ngx_http_cache_purge_index_require(q, current.bytes.len == 16,
                                              NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                              "invalid path identity"))
    {
        memcpy(id, current.bytes.data, 16);
        if (ngx_http_cache_purge_index_fetch(
                tx, id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, &record)
            && ngx_http_cache_purge_index_require(
                q,
                record.state == 2 && record.refs[4] == root_id
                    && ngx_http_cache_purge_op_equal(record.path, path),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "inconsistent current cache path"))
        {
            ngx_http_cache_purge_index_match(&record, out);
        }
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_has_intent(ngx_http_cache_rocksdb_t *store,
    const unsigned char id[16], int *owned,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t     *tx;
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_record_t  record;
    ngx_http_cache_purge_index_value_t   intent;
    ngx_http_cache_rocksdb_bytes_t      current, identity;
    int                                 found;

    if (owned != NULL) {
        *owned = 0;
    }
    if (store == NULL || id == NULL || owned == NULL) {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid intent arguments");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    if (!ngx_http_cache_purge_index_fetch(
            tx, id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, &record))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    identity = ngx_http_cache_purge_index_id(id);
    intent = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'I', &record));
    found =
        intent.exists && ngx_http_cache_purge_op_equal(intent.bytes, identity);
    ngx_http_cache_purge_index_require(
        q, (record.state != 1 || found) && (record.state != 3 || !found),
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent fill intent");
    if (record.state == 2) {
        current = ngx_http_cache_purge_index_tx_need(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'P', &record));
        ngx_http_cache_purge_index_require(
            q, ngx_http_cache_purge_op_equal(current, identity),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "committed path identity mismatch");
    }
    if (q->status == 0) {
        *owned = found;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

static int
ngx_http_cache_purge_index_update_internal(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    const ngx_http_cache_rocksdb_bytes_t *input, size_t count,
    ngx_http_cache_purge_index_match_t *out, int pending, int deferred,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t     *tx;
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_record_t  record;
    ngx_http_cache_purge_index_value_t   intent;
    ngx_http_cache_purge_index_match_t   result;
    ngx_http_cache_rocksdb_bytes_t       tags[64], current;
    uint64_t                            selected[64], id;
    unsigned char                       retained[64];
    size_t                              i, j, n;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (store == NULL || file == NULL || out == NULL) {
        return ngx_http_cache_purge_index_error(error,
                                                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                                "invalid metadata arguments");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    n = ngx_http_cache_purge_index_tags(q, input, count, tags);
    tx->deferred = deferred;
    if (!ngx_http_cache_purge_index_input(q, metadata, 16384)
        || !ngx_http_cache_purge_index_load(tx, file, &record)
        || !ngx_http_cache_purge_index_require(q, record.state == 2,
                                               NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                               "file is not committed"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    intent = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'I', &record));
    if (pending == 2) {
        current = ngx_http_cache_purge_index_tx_need(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'P', &record));
        if (!ngx_http_cache_purge_index_require(q,
                ngx_http_cache_purge_index_path_matches(q, intent, file->id)
                && ngx_http_cache_purge_op_equal(current,
                    ngx_http_cache_purge_index_id(file->id)),
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "updated path identity changed"))
        {
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
    } else if (!ngx_http_cache_purge_index_require(
            q, !intent.exists, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "cache path has an unfinished fill"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    memset(retained, 0, sizeof(retained));
    for (i = 0; i < n && q->status == 0; i++) {
        id = ngx_http_cache_purge_index_dict_find(tx, tags[i]);
        for (j = 0; j < record.tag_count && record.tags[j] != id; j++) {
            /* void */
        }
        if (j != record.tag_count) {
            retained[j] = 1;
        } else {
            id = ngx_http_cache_purge_index_field(tx, tags[i]);
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_TAG,
                ngx_http_cache_purge_index_posting(q, 'N', record.refs[3], id,
                                                   file->id),
                ngx_http_cache_purge_op_empty, 1);
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_TAG,
                ngx_http_cache_purge_index_posting(q, 'T', id, 0,
                                                   file->id),
                ngx_http_cache_purge_op_empty, 1);
        }
        selected[i] = id;
    }
    for (i = 0; i < record.tag_count && q->status == 0; i++) {
        if (retained[i]) {
            continue;
        }
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_TAG,
            ngx_http_cache_purge_index_posting(q, 'N', record.refs[3],
                                               record.tags[i], file->id),
            ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_TAG,
            ngx_http_cache_purge_index_posting(q, 'T', record.tags[i],
                                               0, file->id),
            ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_dict_drop(tx, record.tags[i]);
    }
    if (q->status == 0) {
        record.tag_count = n;
        memcpy(record.tags, selected, n * sizeof(selected[0]));
        record.metadata = metadata;
        ngx_http_cache_purge_index_advance(q, &record);
        ngx_http_cache_purge_index_save(tx, &record);
        if (pending) {
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_path_key(q, 'I', &record),
                pending == 1 ? ngx_http_cache_purge_index_id(file->id)
                             : ngx_http_cache_purge_op_empty,
                pending == 1);
        }
        ngx_http_cache_purge_index_match(&record, &result);
    }
    ngx_http_cache_purge_index_tx_commit(tx);
    if (q->status == 0) {
        *out = result;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_update(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    const ngx_http_cache_rocksdb_bytes_t *tags, size_t count,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_update_internal(
        store, file, metadata, tags, count, out, 0, 0, error);
}

int
ngx_http_cache_purge_index_update_pending(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    const ngx_http_cache_rocksdb_bytes_t *tags, size_t count,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_update_internal(
        store, file, metadata, tags, count, out, 1, 0, error);
}

int
ngx_http_cache_purge_index_update_pending_deferred(
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    const ngx_http_cache_rocksdb_bytes_t *tags, size_t count,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_update_internal(
        store, file, metadata, tags, count, out, 1, 1, error);
}

int
ngx_http_cache_purge_index_publish_tags(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    const ngx_http_cache_rocksdb_bytes_t *tags, size_t count,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_update_internal(
        store, file, metadata, tags, count, out, 2, 0, error);
}

int
ngx_http_cache_purge_index_publish_update(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t     *tx;
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_record_t  record;
    ngx_http_cache_purge_index_match_t   result;
    ngx_http_cache_rocksdb_bytes_t       owner, current, identity;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (store == NULL || file == NULL || out == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid update publication");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    if (!ngx_http_cache_purge_index_input(q, metadata, 16384)
        || !ngx_http_cache_purge_index_load(tx, file, &record)
        || !ngx_http_cache_purge_index_require(q, record.state == 2,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "file is not committed"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    identity = ngx_http_cache_purge_index_id(file->id);
    owner = ngx_http_cache_purge_index_tx_need(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'I', &record));
    current = ngx_http_cache_purge_index_tx_need(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'P', &record));
    ngx_http_cache_purge_index_require(q,
        ngx_http_cache_purge_op_equal(owner, identity)
        && ngx_http_cache_purge_op_equal(current, identity),
        NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "updated path identity changed");
    if (q->status == 0) {
        record.metadata = metadata;
        ngx_http_cache_purge_index_advance(q, &record);
        ngx_http_cache_purge_index_tx_set(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_path_key(q, 'I', &record),
            ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_save(tx, &record);
        ngx_http_cache_purge_index_match(&record, &result);
    }
    ngx_http_cache_purge_index_tx_commit(tx);
    if (q->status == 0) {
        *out = result;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

static void
ngx_http_cache_purge_index_collect_one(ngx_http_cache_purge_index_tx_t *tx,
    const ngx_http_cache_purge_index_expected_t *file)
{
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_record_t  record;
    ngx_http_cache_purge_index_value_t  current, owner, native, posting;
    ngx_http_cache_rocksdb_bytes_t  raw;
    uint64_t                        count;
    size_t                          i;
    static const unsigned               refs[6] = { 4, 2, 3, 5, 6, 7 };

    q = tx->context;
    if (!ngx_http_cache_purge_index_load(tx, file, &record)
        || !ngx_http_cache_purge_index_require(
            q, record.state == 3, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "only retired file records can be collected"))
    {
        return;
    }
    current = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'P', &record));
    owner = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_path_key(q, 'I', &record));
    if (!ngx_http_cache_purge_index_require(
            q,
            !current.exists
                || !ngx_http_cache_purge_index_path_matches(q, current,
                                                            file->id),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "retired file is still the current path instance")
        || !ngx_http_cache_purge_index_require(
            q,
            !owner.exists, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "file path has an unfinished publication"))
    {
        return;
    }
    native = ngx_http_cache_purge_index_tx_get(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'j', file->id));
    if (native.exists) {
        if (!ngx_http_cache_purge_index_require(q, native.bytes.len == 32,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid native binding"))
        {
            return;
        }
        raw = ngx_http_cache_purge_index_native_key(q, &record,
            native.bytes.data, native.bytes.data + 16);
        posting = ngx_http_cache_purge_index_tx_get(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, raw);
        if (!ngx_http_cache_purge_index_require(q,
                posting.exists && posting.bytes.len == 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "native posting is missing"))
        {
            return;
        }
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            raw, ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_object_key(q, 'j', file->id),
            ngx_http_cache_purge_op_empty, 0);
    }
    ngx_http_cache_purge_index_tag_links(tx, &record, 0);
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_posting(q, 'z', record.refs[0], 0, file->id),
        ngx_http_cache_purge_op_empty, 0);
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_posting(q, 'F', record.refs[0], 0, file->id),
        ngx_http_cache_purge_op_empty, 0);
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'O', file->id),
        ngx_http_cache_purge_op_empty, 0);
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'g', file->id),
        ngx_http_cache_purge_op_empty, 0);
    ngx_http_cache_purge_index_tx_set(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'v', file->id),
        ngx_http_cache_purge_op_empty, 0);
    ngx_http_cache_purge_index_tx_set(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_object_key(q, 'd', file->id),
        ngx_http_cache_purge_op_empty, 0);
    for (i = 0; i < 6 && q->status == 0; i++) {
        ngx_http_cache_purge_index_dict_drop(tx, record.refs[refs[i]]);
    }
    for (i = 0; i < record.tag_count && q->status == 0; i++) {
        ngx_http_cache_purge_index_dict_drop(tx, record.tags[i]);
    }
    raw = ngx_http_cache_purge_index_tx_need(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'k', record.refs[0]));
    count = ngx_http_cache_purge_index_key_number(q, raw, 0);
    if (ngx_http_cache_purge_index_require(q, count != 0,
                                           NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                           "key file count underflow"))
    {
        ngx_http_cache_purge_index_counter(tx, 'k', record.refs[0], count - 1);
        if (count == 1) {
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_row_key(q, 'G', record.refs[0]),
                ngx_http_cache_purge_op_empty, 1);
        }
    }
}

static int
ngx_http_cache_purge_index_collect_internal(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *files, size_t count,
    int deferred, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t  *tx;
    size_t                            i;

    if (store == NULL || files == NULL) {
        return ngx_http_cache_purge_index_error(error,
                                                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                                "invalid collection arguments");
    }
    if (count == 0 || count > 32) {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "collect batch must contain 1-32 files");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    tx->deferred = deferred;
    for (i = 0; i < count && tx->context->status == 0; i++) {
        ngx_http_cache_purge_index_collect_one(tx, &files[i]);
    }
    ngx_http_cache_purge_index_tx_commit(tx);
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_collect(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *files, size_t count,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_collect_internal(store, files, count, 0,
                                                      error);
}

int
ngx_http_cache_purge_index_collect_deferred(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *files, size_t count,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_collect_internal(store, files, count, 1,
                                                      error);
}

static int
ngx_http_cache_purge_index_collect_keys_internal(
    ngx_http_cache_rocksdb_t *store, size_t budget, size_t *removed,
    int deferred, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t    *tx;
    ngx_http_cache_purge_query_t       *q;
    ngx_http_cache_purge_index_value_t  count;
    ngx_http_cache_rocksdb_bytes_t     number, raw, text;
    uint64_t                           id, url, host, tag_namespace, misc, uses;

    if (removed != NULL) {
        *removed = 0;
    }
    if (store == NULL || removed == NULL || budget == 0 || budget > 32) {
        return ngx_http_cache_purge_index_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid key collection budget/output");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    tx->deferred = deferred;
    q = tx->context;
    ngx_http_cache_purge_index_cursor_open(q, &q->outer,
                                           NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                           ngx_http_cache_purge_op_literal("G"),
                                           ngx_http_cache_purge_op_empty, NULL);
    if (!ngx_http_cache_purge_index_cursor_next(q, &q->outer)
        || !ngx_http_cache_purge_index_require(q, q->outer.key.len == 9,
                                               NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                               "invalid key collection entry"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    number.data = q->outer.key.data + 1;
    number.len = 8;
    id = ngx_http_cache_purge_index_integer(q, number);
    ngx_http_cache_purge_index_cursor_close(q, &q->outer);
    count = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'k', id));
    if (!count.exists
        || ngx_http_cache_purge_index_key_number(q, count.bytes, 0) != 0)
    {
        ngx_http_cache_purge_index_tx_set(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_row_key(q, 'G', id),
            ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_tx_commit(tx);
        if (q->status == 0) {
            *removed = 1;
        }
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    url = ngx_http_cache_purge_index_key_number(q, count.bytes, 1);
    host = ngx_http_cache_purge_index_key_number(q, count.bytes, 2);
    tag_namespace = ngx_http_cache_purge_index_key_number(q, count.bytes, 3);
    misc = ngx_http_cache_purge_index_key_number(q, count.bytes, 4);
    count = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'h', id));
    if (count.exists) {
        ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_integer(q, count.bytes) != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "empty propagation reference");
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_row_key(q, 'G', id),
            ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_tx_commit(tx);
        if (q->status == 0) {
            *removed = 1;
        }
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    ngx_http_cache_purge_index_cursor_open(q, &q->files,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'F', id),
        ngx_http_cache_purge_op_empty, NULL);
    ngx_http_cache_purge_index_require(q,
        !ngx_http_cache_purge_index_cursor_next(q, &q->files),
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "empty key still has file postings");
    ngx_http_cache_purge_index_cursor_close(q, &q->files);
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_URL,
        ngx_http_cache_purge_index_variant_key(q, url, tag_namespace, misc),
        ngx_http_cache_purge_op_empty, 0);
    raw = ngx_http_cache_purge_index_tx_need(tx,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_row_key(q, 'u', url));
    uses = ngx_http_cache_purge_index_integer(q, raw);
    if (ngx_http_cache_purge_index_require(q, uses != 0,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "URL relationship count underflow"))
    {
        if (uses == 1) {
            text = ngx_http_cache_purge_index_dict_text(tx, url);
            ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_URL,
                ngx_http_cache_purge_index_text_key(q, 'U', text),
                ngx_http_cache_purge_op_empty, 0);
            ngx_http_cache_purge_index_tx_set(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_row_key(q, 'u', url),
                ngx_http_cache_purge_op_empty, 0);
            ngx_http_cache_purge_index_dict_drop(tx, url);
        } else {
            ngx_http_cache_purge_index_counter(tx, 'u', url, uses - 1);
        }
    }
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_relation(q, 'B', host, id),
        ngx_http_cache_purge_op_empty, 0);
    ngx_http_cache_purge_index_dict_drop(tx, host);
    ngx_http_cache_purge_index_dict_drop(tx, tag_namespace);
    ngx_http_cache_purge_index_dict_drop(tx, misc);
    if (q->status == 0) {
        ngx_http_cache_purge_index_require(q,
            ngx_http_cache_purge_index_key_text(q, NULL, id),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing collected key identity");
        text = ngx_http_cache_purge_index_bytes(&q->lookup);
        ngx_http_cache_purge_index_key_projection(q, text);
        raw = ngx_http_cache_purge_index_tx_need(
            tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->key));
        if (ngx_http_cache_purge_index_require(
                q, ngx_http_cache_purge_index_integer(q, raw) == id,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "key projection identity mismatch"))
        {
            /*
             * text points into lookup, which the integer reader did not alter.
             */
            ngx_http_cache_purge_index_key_projection(q, text);
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_bytes(&q->key),
                ngx_http_cache_purge_op_empty, 0);
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_row_key(q, 'k', id),
                ngx_http_cache_purge_op_empty, 0);
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_row_key(q, 'G', id),
                ngx_http_cache_purge_op_empty, 0);
        }
    }
    ngx_http_cache_purge_index_tx_commit(tx);
    if (q->status == 0) {
        *removed = 1;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_collect_keys(ngx_http_cache_rocksdb_t *store,
    size_t budget, size_t *removed, ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_collect_keys_internal(store, budget,
                                                          removed, 0, error);
}

int
ngx_http_cache_purge_index_collect_keys_deferred(
    ngx_http_cache_rocksdb_t *store, size_t budget, size_t *removed,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_collect_keys_internal(store, budget,
                                                          removed, 1, error);
}

typedef struct {
    ngx_http_cache_purge_retention_t  times;
    uint64_t                           tag_namespace, tags[64];
    size_t                             count;
} ngx_http_cache_purge_retained_t;

struct ngx_http_cache_purge_retained_query_s {
    ngx_http_cache_purge_query_t  *query;
    uint64_t                       key_id;
};

struct ngx_http_cache_purge_representation_query_s {
    ngx_http_cache_purge_query_t        *query;
    ngx_http_cache_rocksdb_cursor_t     *view;
    uint64_t                             native_id, retained_id, selected;
    int                                  started;
    unsigned                             phase;
    unsigned char                        member_prefix[49];
};

static int
ngx_http_cache_purge_index_retention_valid(
    const ngx_http_cache_purge_retention_t *r)
{
    return r != NULL && (r->invalidate_ms || r->delete_ms)
           && (!!r->invalidate_ms == !!r->invalidate_until_ms)
           && (!!r->delete_ms == !!r->delete_until_ms);
}

static int
ngx_http_cache_purge_index_retention_decode(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t value, ngx_http_cache_purge_retained_t *out)
{
    ngx_http_cache_purge_index_reader_t  reader;
    size_t                               i, j;

    memset(out, 0, sizeof(*out));
    if (!ngx_http_cache_purge_index_require(q, value.len >= 41,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid retained propagation"))
    {
        return 0;
    }
    reader.query = q;
    reader.data = value;
    reader.at = 0;
    out->times.invalidate_ms =
        ngx_http_cache_purge_index_read_number(&reader, 8);
    out->times.invalidate_until_ms =
        ngx_http_cache_purge_index_read_number(&reader, 8);
    out->times.delete_ms = ngx_http_cache_purge_index_read_number(&reader, 8);
    out->times.delete_until_ms =
        ngx_http_cache_purge_index_read_number(&reader, 8);
    out->tag_namespace = ngx_http_cache_purge_index_read_number(&reader, 8);
    out->count = (size_t) ngx_http_cache_purge_index_read_number(&reader, 1);
    if (!ngx_http_cache_purge_index_require(q,
            out->tag_namespace && out->count <= 64
            && value.len == 41 + 8 * out->count,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid retained TAG set"))
    {
        return 0;
    }
    for (i = 0; i < out->count; i++) {
        out->tags[i] = ngx_http_cache_purge_index_read_number(&reader, 8);
        for (j = 0; j < i && out->tags[j] != out->tags[i]; j++) {
            /* void */
        }
        if (!ngx_http_cache_purge_index_require(q, out->tags[i] && j == i,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid retained TAG"))
        {
            return 0;
        }
    }
    return ngx_http_cache_purge_index_require(q,
        ngx_http_cache_purge_index_retention_valid(&out->times),
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid retained propagation windows");
}

static void
ngx_http_cache_purge_index_retention_tags(ngx_http_cache_purge_query_t *q,
    const ngx_http_cache_purge_index_record_t *record)
{
    unsigned char  count;
    size_t         i;

    ngx_http_cache_purge_index_number(q, &q->lookup, record->refs[3]);
    count = (unsigned char) record->tag_count;
    ngx_http_cache_purge_index_append(q, &q->lookup, &count, 1);
    for (i = 0; i < record->tag_count; i++) {
        ngx_http_cache_purge_index_number(q, &q->lookup, record->tags[i]);
    }
}

static int
ngx_http_cache_purge_index_retention_check(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t key, ngx_http_cache_rocksdb_bytes_t value)
{
    ngx_http_cache_rocksdb_bytes_t  tags;
    unsigned char                   digest[32];

    if (!ngx_http_cache_purge_index_require(q, key.len == 81 && value.len >= 41,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid retained snapshot"))
    {
        return 0;
    }
    tags.data = value.data + 32;
    tags.len = value.len - 32;
    return ngx_http_cache_purge_index_digest(q, tags, digest)
           && ngx_http_cache_purge_index_require(q,
               memcmp(digest, key.data + 49, 32) == 0,
               NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "retained TAG digest mismatch");
}

static uint64_t
ngx_http_cache_purge_index_retention_until(
    const ngx_http_cache_purge_retention_t *r)
{
    return r->invalidate_until_ms > r->delete_until_ms
           ? r->invalidate_until_ms : r->delete_until_ms;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_retention_key(ngx_http_cache_purge_query_t *q,
    uint64_t key, const unsigned char main[16], uint64_t vary,
    const unsigned char variant[16])
{
    ngx_http_cache_purge_index_row(q, &q->key, 'q', key);
    ngx_http_cache_purge_index_append(q, &q->key, main, 16);
    ngx_http_cache_purge_index_number(q, &q->key, vary);
    ngx_http_cache_purge_index_append(q, &q->key, variant, 16);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_index_retention_expiry(ngx_http_cache_purge_query_t *q,
    ngx_http_cache_rocksdb_bytes_t key, uint64_t until)
{
    ngx_http_cache_purge_index_row(q, &q->key, 'r', until);
    ngx_http_cache_purge_index_append(q, &q->key, key.data + 1, key.len - 1);
    return ngx_http_cache_purge_index_bytes(&q->key);
}

static int
ngx_http_cache_purge_index_keep_propagation_internal(
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const unsigned char main[16], const unsigned char variant[16], uint64_t now,
    const ngx_http_cache_purge_retention_t *retention, int deferred,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t      *tx;
    ngx_http_cache_purge_query_t         *q;
    ngx_http_cache_purge_index_record_t   record;
    ngx_http_cache_purge_index_value_t    current, references;
    ngx_http_cache_purge_retention_t      previous, merged;
    ngx_http_cache_purge_retained_t       saved;
    ngx_http_cache_rocksdb_bytes_t        key, value;
    unsigned char                         raw[81], digest[32];
    uint64_t                              count, until;
    size_t                                i;

    if (store == NULL || file == NULL || main == NULL || variant == NULL
        || now == 0 || !ngx_http_cache_purge_index_retention_valid(retention)
        || (retention->invalidate_ms && retention->invalidate_until_ms <= now)
        || (retention->delete_ms && retention->delete_until_ms <= now))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid propagation retention");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    tx->deferred = deferred;
    q = tx->context;
    if (!ngx_http_cache_purge_index_load(tx, file, &record)
        || !ngx_http_cache_purge_index_require(q,
            record.state == 2 || record.state == 3,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "file is not published"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    q->lookup.len = 0;
    ngx_http_cache_purge_index_retention_tags(q, &record);
    if (!ngx_http_cache_purge_index_digest(q,
            ngx_http_cache_purge_index_bytes(&q->lookup), digest))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    (void) ngx_http_cache_purge_index_retention_key(q, record.refs[0], main,
        record.refs[7], variant);
    ngx_http_cache_purge_index_append(q, &q->key, digest, 32);
    value = ngx_http_cache_purge_index_bytes(&q->key);
    if (q->status != 0) {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    memcpy(raw, value.data, sizeof(raw));
    key.data = raw;
    key.len = sizeof(raw);
    current = ngx_http_cache_purge_index_tx_get(
        tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    memset(&previous, 0, sizeof(previous));
    if (current.exists) {
        if (!ngx_http_cache_purge_index_retention_check(q, key, current.bytes)
            || !ngx_http_cache_purge_index_retention_decode(q, current.bytes,
                &saved)
            || !ngx_http_cache_purge_index_require(q,
                saved.tag_namespace == record.refs[3]
                && saved.count == record.tag_count
                && memcmp(saved.tags, record.tags,
                          record.tag_count * sizeof(uint64_t)) == 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "retained TAG set changed"))
        {
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
        previous = saved.times;
    }
    merged = previous;
    if (merged.invalidate_until_ms <= now) {
        merged.invalidate_ms = 0;
        merged.invalidate_until_ms = 0;
    }
    if (merged.delete_until_ms <= now) {
        merged.delete_ms = 0;
        merged.delete_until_ms = 0;
    }
    if (retention->invalidate_ms > merged.invalidate_ms) {
        merged.invalidate_ms = retention->invalidate_ms;
    }
    if (retention->invalidate_until_ms > merged.invalidate_until_ms) {
        merged.invalidate_until_ms = retention->invalidate_until_ms;
    }
    if (retention->delete_ms > merged.delete_ms) {
        merged.delete_ms = retention->delete_ms;
    }
    if (retention->delete_until_ms > merged.delete_until_ms) {
        merged.delete_until_ms = retention->delete_until_ms;
    }
    if (current.exists && merged.invalidate_ms == previous.invalidate_ms
        && merged.invalidate_until_ms == previous.invalidate_until_ms
        && merged.delete_ms == previous.delete_ms
        && merged.delete_until_ms == previous.delete_until_ms)
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    if (!current.exists) {
        references = ngx_http_cache_purge_index_tx_get(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_row_key(q, 'h', record.refs[0]));
        count = references.exists
                ? ngx_http_cache_purge_index_integer(q, references.bytes) : 0;
        if (!ngx_http_cache_purge_index_require(q,
                count != UINT64_MAX && (!references.exists || count != 0),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid propagation reference count"))
        {
            return ngx_http_cache_purge_index_tx_end(tx, error);
        }
        ngx_http_cache_purge_index_counter(tx, 'h', record.refs[0], count + 1);
        ngx_http_cache_purge_index_dict_retain(tx, record.refs[7]);
        ngx_http_cache_purge_index_dict_retain(tx, record.refs[3]);
        for (i = 0; i < record.tag_count; i++) {
            ngx_http_cache_purge_index_dict_retain(tx, record.tags[i]);
        }
    } else {
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_retention_expiry(q, key,
                ngx_http_cache_purge_index_retention_until(&previous)),
            ngx_http_cache_purge_op_empty, 0);
    }
    q->lookup.len = 0;
    ngx_http_cache_purge_index_number(q, &q->lookup, merged.invalidate_ms);
    ngx_http_cache_purge_index_number(q, &q->lookup,
        merged.invalidate_until_ms);
    ngx_http_cache_purge_index_number(q, &q->lookup, merged.delete_ms);
    ngx_http_cache_purge_index_number(q, &q->lookup, merged.delete_until_ms);
    ngx_http_cache_purge_index_retention_tags(q, &record);
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key,
        ngx_http_cache_purge_index_bytes(&q->lookup), 1);
    until = ngx_http_cache_purge_index_retention_until(&merged);
    ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_retention_expiry(q, key, until),
        ngx_http_cache_purge_op_empty, 1);
    ngx_http_cache_purge_index_tx_commit(tx);
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_keep_propagation(ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const unsigned char main[16], const unsigned char variant[16], uint64_t now,
    const ngx_http_cache_purge_retention_t *retention,
    ngx_http_cache_rocksdb_error_t *error)
{
    int  rc;

    rc = ngx_http_cache_purge_index_keep_propagation_internal(store, file,
        main, variant, now, retention, 0, error);
    if (rc == 0) {
        /* A replay can confirm a snapshot written by the deferred path. */
        rc = ngx_http_cache_rocksdb_sync(store, error);
    }
    return rc;
}

int
ngx_http_cache_purge_index_keep_propagation_deferred(
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *file,
    const unsigned char main[16], const unsigned char variant[16], uint64_t now,
    const ngx_http_cache_purge_retention_t *retention,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_keep_propagation_internal(store, file,
        main, variant, now, retention, 1, error);
}

int
ngx_http_cache_purge_index_get_propagation(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_bytes_t key, const unsigned char main[16],
    ngx_http_cache_rocksdb_bytes_t vary, const unsigned char variant[16],
    uint64_t now, ngx_http_cache_purge_fence_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t     *tx;
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_retained_t      retained;
    ngx_http_cache_purge_fence_t         result;
    uint64_t                             key_id, vary_id;
    size_t                               count;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (store == NULL || out == NULL || main == NULL || variant == NULL
        || now == 0 || !key.len
        || !ngx_http_cache_purge_op_bytes_valid(key, 32768)
        || !ngx_http_cache_purge_op_bytes_valid(vary, 32768))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid retention query");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    key_id = ngx_http_cache_purge_index_key_find(tx->context, key);
    vary_id = ngx_http_cache_purge_index_dict_find(tx, vary);
    if (!ngx_http_cache_purge_index_require(q, key_id && vary_id,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "propagation identity is absent"))
    {
        return ngx_http_cache_purge_index_tx_end(tx, error);
    }
    ngx_http_cache_purge_index_cursor_open(q, &q->outer,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_index_retention_key(q, key_id, main, vary_id,
            variant), ngx_http_cache_purge_op_empty, NULL);
    memset(&result, 0, sizeof(result));
    count = 0;
    while (q->status == 0
           && ngx_http_cache_purge_index_cursor_next(q, &q->outer))
    {
        if (!ngx_http_cache_purge_index_require(q, count++ < 64,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "too many retained TAG sets")
            || !ngx_http_cache_purge_index_retention_check(q,
                ngx_http_cache_purge_index_bytes(&q->outer.key),
                ngx_http_cache_purge_index_bytes(&q->outer.value))
            || !ngx_http_cache_purge_index_retention_decode(q,
                ngx_http_cache_purge_index_bytes(&q->outer.value), &retained))
        {
            break;
        }
        if (now < retained.times.invalidate_until_ms
            && result.invalidate_ms < retained.times.invalidate_ms)
        {
            result.invalidate_ms = retained.times.invalidate_ms;
        }
        if (now < retained.times.delete_until_ms
            && result.delete_ms < retained.times.delete_ms)
        {
            result.delete_ms = retained.times.delete_ms;
        }
    }
    ngx_http_cache_purge_index_require(q, count != 0,
        NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "retained propagation is absent");
    if (q->status == 0) {
        *out = result;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

static int
ngx_http_cache_purge_index_retention_context(
    ngx_http_cache_purge_query_t *q, ngx_http_cache_rocksdb_cursor_t *view,
    uint64_t key,
    const ngx_http_cache_purge_retained_t *retained,
    ngx_http_cache_purge_retained_context_t *out)
{
    ngx_http_cache_rocksdb_bytes_t  value, fields[66];
    size_t                          offsets[66], i;
    uint64_t                        id;

    memset(out, 0, sizeof(*out));
    /* Dictionary reconstruction uses scratch for every component read. Keep
     * the borrowed callback fields in a separate, reusable buffer. */
    q->current_outer.len = 0;
    for (i = 0; i < retained->count + 2 && q->status == 0; i++) {
        id = i == 0 ? key : i == 1 ? retained->tag_namespace
                                   : retained->tags[i - 2];
        if (!ngx_http_cache_purge_index_require(q,
                (i == 0 ? ngx_http_cache_purge_index_key_text(q, view, id) :
                          ngx_http_cache_purge_index_value(q, view, id, 0)),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing retained identity"))
        {
            return 0;
        }
        value = ngx_http_cache_purge_index_bytes(&q->lookup);
        if (!ngx_http_cache_purge_index_require(q,
                value.len <= 131072 - q->current_outer.len,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "retained identity is too large"))
        {
            return 0;
        }
        offsets[i] = q->current_outer.len;
        fields[i].len = value.len;
        ngx_http_cache_purge_index_append(q, &q->current_outer, value.data,
            value.len);
    }
    if (q->status != 0) {
        return 0;
    }
    for (i = 0; i < retained->count + 2; i++) {
        fields[i].data = q->current_outer.data + offsets[i];
    }
    out->key = fields[0];
    out->tag_namespace = fields[1];
    out->tag_count = retained->count;
    memcpy(out->tags, fields + 2, retained->count * sizeof(fields[0]));
    out->times = retained->times;
    return 1;
}

static uint64_t
ngx_http_cache_purge_index_representation_read(
    ngx_http_cache_purge_query_t *q,
    ngx_http_cache_purge_index_cursor_t *cursor,
    ngx_http_cache_rocksdb_cursor_t *view, int skip, int retained)
{
    ngx_http_cache_rocksdb_bytes_t  prefix, after, number;
    unsigned char                    raw[81];
    uint64_t                         id;

    if (skip) {
        /* The row suffix is variant + file ID or TAG-set digest. Seeking
         * beyond the whole group avoids walking every language/Cookie value. */
        memcpy(raw, cursor->key.data, 33);
        memset(raw + 33, 0xff, sizeof(raw) - 33);
        prefix.data = raw;
        prefix.len = 25;
        after.data = raw;
        after.len = retained ? 81 : 65;
        ngx_http_cache_purge_index_cursor_open(q, cursor,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix, after, view);

    } else if (cursor->handle == NULL) {
        memcpy(raw, q->prefix.data, 25);
        raw[0] = retained ? 'q' : 'n';
        prefix.data = raw;
        prefix.len = 25;
        ngx_http_cache_purge_index_cursor_open(q, cursor,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix,
            ngx_http_cache_purge_op_empty, view);
    }
    if (!ngx_http_cache_purge_index_cursor_next(q, cursor)) {
        return 0;
    }
    if (!ngx_http_cache_purge_index_require(q,
            cursor->key.len == (retained ? 81 : 65)
            && (retained || cursor->value.len == 0),
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid representation posting"))
    {
        return 0;
    }
    number.data = cursor->key.data + 25;
    number.len = 8;
    id = ngx_http_cache_purge_index_integer(q, number);
    ngx_http_cache_purge_index_require(q, id != 0,
        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing representation Vary ID");
    return q->status == 0 ? id : 0;
}

int
ngx_http_cache_purge_index_representations_open(
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_bytes_t key,
    const unsigned char main[16],
    ngx_http_cache_purge_representation_query_t **out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_representation_query_t  *r;
    ngx_http_cache_purge_query_t                 *q;
    uint64_t                                      id;
    int                                           rc;

    if (out != NULL) {
        *out = NULL;
    }
    if (store == NULL || out == NULL || main == NULL || !key.len
        || !ngx_http_cache_purge_op_bytes_valid(key, 32768))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid representation query");
    }
    r = calloc(1, sizeof(*r));
    q = calloc(1, sizeof(*q));
    if (r == NULL || q == NULL) {
        free(r);
        free(q);
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "representation allocation failed");
    }
    r->query = q;
    q->store = store;
    q->bytes = sizeof(*q) + sizeof(*r);
    /* Identity, migration readiness and memberships must share one snapshot:
     * a read handle may run while the writer replaces or retires a file. */
    id = 0;
    if (ngx_http_cache_purge_index_key_projection(q, key)) {
        ngx_http_cache_purge_index_cursor_open(q, &q->outer,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->key),
            ngx_http_cache_purge_op_empty, NULL);
        r->view = q->outer.handle;
        q->outer.handle = NULL;
        if (q->status == 0
            && ngx_http_cache_purge_index_get(q,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_bytes(&q->key), r->view))
        {
            id = ngx_http_cache_purge_index_integer(q,
                ngx_http_cache_purge_index_bytes(&q->scratch));
        }
    }
    q->empty = id == 0;
    if (q->status == 0 && id != 0) {
        if (ngx_http_cache_purge_index_get(q, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_op_literal("M:native-cursor"), r->view))
        {
            ngx_http_cache_purge_index_fail(q, NGX_HTTP_CACHE_ROCKSDB_BUSY,
                "native representations incomplete");
        }
        ngx_http_cache_purge_index_cursor_open(q, &q->outer,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_row_key(q, 'z', id),
            ngx_http_cache_purge_op_empty, r->view);
        if (ngx_http_cache_purge_index_cursor_next(q, &q->outer)) {
            if (ngx_http_cache_purge_index_require(q,
                    q->outer.key.len == 25 && q->outer.value.len == 0,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "invalid unresolved identity"))
            {
                ngx_http_cache_purge_index_fail(q, NGX_HTTP_CACHE_ROCKSDB_BUSY,
                    "native representations incomplete");
            }
        }
        ngx_http_cache_purge_index_cursor_close(q, &q->outer);
        ngx_http_cache_purge_index_row(q, &q->prefix, 'n', id);
        ngx_http_cache_purge_index_append(q, &q->prefix, main, 16);
    }
    rc = q->status;
    if (rc != 0) {
        if (error != NULL) {
            *error = q->error;
        }
        (void) ngx_http_cache_purge_index_representations_close(&r, NULL);
        return rc;
    }
    *out = r;
    return ngx_http_cache_purge_index_error(error, 0, "");
}

int
ngx_http_cache_purge_index_representations_next(
    ngx_http_cache_purge_representation_query_t *r,
    ngx_http_cache_rocksdb_bytes_t *vary,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t  *q;
    uint64_t                       id;

    if (vary != NULL) {
        *vary = ngx_http_cache_purge_op_empty;
    }
    if (r == NULL || vary == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid representation output");
    }
    q = r->query;
    ngx_http_cache_purge_index_cursor_close(q, &q->files);
    r->phase = 0;
    if (q->status == 0 && !q->empty) {
        if (!r->started || r->native_id == r->selected) {
            r->native_id = ngx_http_cache_purge_index_representation_read(q,
                &q->outer, r->view, r->started, 0);
        }
        if (!r->started || r->retained_id == r->selected) {
            r->retained_id = ngx_http_cache_purge_index_representation_read(q,
                &q->links, r->view, r->started, 1);
        }
        r->started = 1;
        id = !r->native_id ? r->retained_id : !r->retained_id ? r->native_id
             : r->native_id < r->retained_id ? r->native_id : r->retained_id;
        q->empty = id == 0;
        if (id != 0 && q->status == 0) {
            ngx_http_cache_purge_index_require(q,
                ngx_http_cache_purge_index_value(q, r->view, id, 0),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "missing representation Vary definition");
            if (q->status == 0) {
                *vary = ngx_http_cache_purge_index_bytes(&q->lookup);
                r->selected = id;
            }
        }
    }
    if (q->status != 0) {
        if (error != NULL) {
            *error = q->error;
        }
        return q->status;
    }
    return ngx_http_cache_purge_index_error(error,
        q->empty ? NGX_HTTP_CACHE_ROCKSDB_DONE : 0, "");
}

int
ngx_http_cache_purge_index_representations_yield(
    ngx_http_cache_purge_representation_query_t *r,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t  *q;
    int                            rc;

    if (r == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "null representation iterator");
    }
    q = r->query;
    rc = ngx_http_cache_purge_index_cursor_close(q, &q->files);
    if (rc == 0) {
        rc = ngx_http_cache_purge_index_cursor_close(q, &q->links);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_index_cursor_close(q, &q->outer);
    }
    if (error != NULL) {
        *error = q->error;
    }
    return rc;
}

int
ngx_http_cache_purge_index_representations_close(
    ngx_http_cache_purge_representation_query_t **out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_representation_query_t  *r;
    int                                           rc;

    if (out == NULL || *out == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "null representation iterator");
    }
    r = *out;
    if (r->view != NULL) {
        rc = ngx_http_cache_rocksdb_cursor_close(&r->view, error);
        if (rc != 0) {
            return rc;
        }
    }
    rc = ngx_http_cache_purge_query_close(&r->query, error);
    if (rc == 0) {
        free(r);
        *out = NULL;
    }
    return rc;
}

int
ngx_http_cache_purge_index_representations_select(
    ngx_http_cache_purge_representation_query_t *r,
    const unsigned char variant[16], ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t   *q;
    ngx_http_cache_rocksdb_bytes_t  prefix;
    unsigned char                  *row;

    if (r == NULL || variant == NULL || !r->selected || r->query->empty) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "no selected Vary definition");
    }
    q = r->query;
    if (q->status == 0) {
        row = r->native_id == r->selected ? q->outer.key.data
                                         : q->links.key.data;
        memcpy(r->member_prefix, row, 33);
        r->member_prefix[0] = 'n';
        memcpy(r->member_prefix + 33, variant, 16);
        prefix.data = r->member_prefix;
        prefix.len = sizeof(r->member_prefix);
        ngx_http_cache_purge_index_cursor_open(q, &q->files,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix,
            ngx_http_cache_purge_op_empty, r->view);
        q->files.key.len = 0;
        r->phase = 1;
    }
    if (error != NULL) {
        *error = q->error;
    }
    return q->status;
}

int
ngx_http_cache_purge_index_representations_member(
    ngx_http_cache_purge_representation_query_t *r, size_t budget,
    ngx_http_cache_purge_retained_context_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t        *q;
    ngx_http_cache_purge_index_record_t  file;
    ngx_http_cache_purge_retained_t      retained;
    ngx_http_cache_rocksdb_bytes_t       prefix, number;
    uint64_t                              key_id;
    size_t                                i;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (r == NULL || out == NULL || !r->phase || !budget || budget > 32) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid member query");
    }
    q = r->query;
    number.data = r->member_prefix + 1;
    number.len = 8;
    key_id = ngx_http_cache_purge_index_integer(q, number);
    if (q->status == 0 && r->phase != 3 && q->files.handle == NULL) {
        prefix.data = r->member_prefix;
        prefix.len = sizeof(r->member_prefix);
        ngx_http_cache_purge_index_cursor_open(q, &q->files,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix,
            ngx_http_cache_purge_index_bytes(&q->files.key), r->view);
    }
    for (i = 0; i < budget && q->status == 0 && r->phase != 3; i++) {
        if (!ngx_http_cache_purge_index_cursor_next(q, &q->files)) {
            if (r->phase == 2) {
                r->phase = 3;
                break;
            }
            r->member_prefix[0] = 'q';
            prefix.data = r->member_prefix;
            prefix.len = sizeof(r->member_prefix);
            ngx_http_cache_purge_index_cursor_open(q, &q->files,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix,
                ngx_http_cache_purge_op_empty, r->view);
            q->files.key.len = 0;
            r->phase = 2;
            continue;
        }
        memset(&retained, 0, sizeof(retained));
        if (r->phase == 1) {
            if (!ngx_http_cache_purge_index_require(q,
                    q->files.key.len == 65 && q->files.value.len == 0,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "invalid native representation member"))
            {
                break;
            }
            if (!ngx_http_cache_purge_index_read(q,
                    NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                    ngx_http_cache_purge_index_object_key(q, 'O',
                        q->files.key.data + 49), q->files.handle))
            {
                ngx_http_cache_purge_index_fail(q,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "native member is absent");
                break;
            }
            if (!ngx_http_cache_purge_index_decode(q,
                    ngx_http_cache_purge_index_bytes(&q->scratch), &file)
                || !ngx_http_cache_purge_index_require(q,
                    file.refs[0] == key_id && file.refs[7] == r->selected
                    && memcmp(file.id, q->files.key.data + 49, 16) == 0,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "native member differs"))
            {
                break;
            }
            if (!ngx_http_cache_purge_index_read(q,
                    NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                    ngx_http_cache_purge_index_object_key(q, 'j', file.id),
                    q->files.handle)
                || !ngx_http_cache_purge_index_require(q,
                    q->scratch.len == 32
                    && memcmp(q->scratch.data, r->member_prefix + 9, 16) == 0
                    && memcmp(q->scratch.data + 16,
                        r->member_prefix + 33, 16) == 0,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "native binding differs"))
            {
                ngx_http_cache_purge_index_fail(q,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "native binding absent");
                break;
            }
            if (file.state == NGX_HTTP_CACHE_PURGE_INDEX_PREPARED
                || (file.state == NGX_HTTP_CACHE_PURGE_INDEX_RETIRED
                    && file.revision == 2))
            {
                continue;
            }
            retained.tag_namespace = file.refs[3];
            retained.count = file.tag_count;
            memcpy(retained.tags, file.tags,
                file.tag_count * sizeof(file.tags[0]));
        } else {
            if (!ngx_http_cache_purge_index_retention_check(q,
                    ngx_http_cache_purge_index_bytes(&q->files.key),
                    ngx_http_cache_purge_index_bytes(&q->files.value))
                || !ngx_http_cache_purge_index_retention_decode(q,
                    ngx_http_cache_purge_index_bytes(&q->files.value),
                    &retained))
            {
                break;
            }
        }
        if (ngx_http_cache_purge_index_retention_context(q, q->files.handle,
                key_id, &retained, out))
        {
            return ngx_http_cache_purge_index_error(error, 0, "");
        }
    }
    if (q->status != 0) {
        memset(out, 0, sizeof(*out));
        if (error != NULL) {
            *error = q->error;
        }
        return q->status;
    }
    return ngx_http_cache_purge_index_error(error,
        r->phase == 3 ? NGX_HTTP_CACHE_ROCKSDB_DONE
                      : NGX_HTTP_CACHE_ROCKSDB_BUSY, "");
}

int
ngx_http_cache_purge_index_propagation_open(
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_bytes_t key,
    const unsigned char main[16], ngx_http_cache_rocksdb_bytes_t vary,
    const unsigned char variant[16],
    ngx_http_cache_purge_retained_query_t **out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_retained_query_t  *r;
    ngx_http_cache_purge_query_t           *q;
    uint64_t                                vary_id;
    int                                     rc;

    if (out != NULL) {
        *out = NULL;
    }
    if (store == NULL || out == NULL || main == NULL || variant == NULL
        || !key.len || !ngx_http_cache_purge_op_bytes_valid(key, 32768)
        || !ngx_http_cache_purge_op_bytes_valid(vary, 32768))
    {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid retained iterator");
    }
    r = calloc(1, sizeof(*r));
    q = calloc(1, sizeof(*q));
    if (r == NULL || q == NULL) {
        free(r);
        free(q);
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "retained iterator allocation failed");
    }
    r->query = q;
    q->store = store;
    q->bytes = sizeof(*q) + sizeof(*r);
    r->key_id = ngx_http_cache_purge_index_key_find(q, key);
    vary_id = ngx_http_cache_purge_index_find(q, vary);
    if (ngx_http_cache_purge_index_require(q, r->key_id && vary_id,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "propagation identity is absent"))
    {
        ngx_http_cache_purge_index_cursor_open(q, &q->outer,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_retention_key(q, r->key_id, main,
                vary_id, variant), ngx_http_cache_purge_op_empty, NULL);
    }
    rc = q->status;
    if (rc != 0) {
        if (error != NULL) {
            *error = q->error;
        }
        (void) ngx_http_cache_purge_index_propagation_close(&r, NULL);
        return rc;
    }
    *out = r;
    return ngx_http_cache_purge_index_error(error, 0, "");
}

int
ngx_http_cache_purge_index_propagation_next(
    ngx_http_cache_purge_retained_query_t *r,
    ngx_http_cache_purge_retained_context_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t     *q;
    ngx_http_cache_purge_retained_t   retained;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (r == NULL || out == NULL) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid retained iterator output");
    }
    q = r->query;
    if (!q->empty && q->status == 0) {
        if (!ngx_http_cache_purge_index_cursor_next(q, &q->outer)) {
            q->empty = 1;
        } else if (ngx_http_cache_purge_index_retention_check(q,
                       ngx_http_cache_purge_index_bytes(&q->outer.key),
                       ngx_http_cache_purge_index_bytes(&q->outer.value))
                   && ngx_http_cache_purge_index_retention_decode(q,
                       ngx_http_cache_purge_index_bytes(&q->outer.value),
                       &retained))
        {
            (void) ngx_http_cache_purge_index_retention_context(q,
                q->outer.handle, r->key_id, &retained, out);
        }
    }
    if (q->status != 0) {
        memset(out, 0, sizeof(*out));
        if (error != NULL) {
            *error = q->error;
        }
        return q->status;
    }
    return ngx_http_cache_purge_index_error(error,
        q->empty ? NGX_HTTP_CACHE_ROCKSDB_DONE : 0, "");
}

int
ngx_http_cache_purge_index_propagation_close(
    ngx_http_cache_purge_retained_query_t **query,
    ngx_http_cache_rocksdb_error_t *error)
{
    int  rc;

    if (query == NULL || *query == NULL) {
        return ngx_http_cache_purge_index_error(error, 0, "");
    }
    rc = ngx_http_cache_purge_query_close(&(*query)->query, error);
    if (rc == 0) {
        free(*query);
        *query = NULL;
    }
    return rc;
}

int
ngx_http_cache_purge_index_expire_propagation_checked(
    ngx_http_cache_rocksdb_t *store, uint64_t now, size_t budget,
    ngx_http_cache_purge_retention_guard_pt guard, void *data, size_t *removed,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_tx_t         *tx;
    ngx_http_cache_purge_query_t            *q;
    ngx_http_cache_purge_index_reader_t      reader;
    ngx_http_cache_purge_retained_t          retained;
    ngx_http_cache_purge_retained_context_t  context;
    ngx_http_cache_purge_retention_t         keep;
    ngx_http_cache_rocksdb_error_t           failure;
    ngx_http_cache_rocksdb_bytes_t           value, key;
    unsigned char                            raw[81];
    uint64_t                                 until, key_id, vary_id, count;
    size_t                                   done, i;
    int                                      rc;

    if (removed != NULL) {
        *removed = 0;
    }
    if (store == NULL || removed == NULL || !now || !budget || budget > 32) {
        return ngx_http_cache_purge_index_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid retention expiry budget");
    }
    tx = ngx_http_cache_purge_index_tx_begin(store, error);
    if (tx == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    q = tx->context;
    ngx_http_cache_purge_index_cursor_open(q, &q->outer,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, ngx_http_cache_purge_op_literal("r"),
        ngx_http_cache_purge_op_empty, NULL);
    done = 0;
    while (done < budget && q->status == 0
           && ngx_http_cache_purge_index_cursor_next(q, &q->outer))
    {
        if (!ngx_http_cache_purge_index_require(q,
                q->outer.key.len == 89 && q->outer.value.len == 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid propagation expiry entry"))
        {
            break;
        }
        reader.query = q;
        reader.data = ngx_http_cache_purge_index_bytes(&q->outer.key);
        reader.at = 1;
        until = ngx_http_cache_purge_index_read_number(&reader, 8);
        if (until > now) {
            break;
        }
        key_id = ngx_http_cache_purge_index_read_number(&reader, 8);
        reader.at += 16;
        vary_id = ngx_http_cache_purge_index_read_number(&reader, 8);
        raw[0] = 'q';
        memcpy(raw + 1, q->outer.key.data + 9, sizeof(raw) - 1);
        key.data = raw;
        key.len = sizeof(raw);
        value = ngx_http_cache_purge_index_tx_need(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
        if (!ngx_http_cache_purge_index_retention_check(q, key, value)
            || !ngx_http_cache_purge_index_retention_decode(q, value, &retained)
            || !ngx_http_cache_purge_index_require(q,
                until == ngx_http_cache_purge_index_retention_until(
                    &retained.times),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "propagation expiry does not match its record"))
        {
            break;
        }
        if (guard != NULL) {
            if (!ngx_http_cache_purge_index_retention_context(q, NULL, key_id,
                    &retained, &context))
            {
                break;
            }
            memset(&keep, 0, sizeof(keep));
            memset(&failure, 0, sizeof(failure));
            rc = guard(data, &context, now, &keep, &failure);
            if (rc != 0) {
                ngx_http_cache_purge_index_fail(q, rc,
                    failure.message[0] ? failure.message
                                       : "retention guard failed");
                break;
            }
            if (keep.invalidate_ms || keep.delete_ms
                || keep.invalidate_until_ms || keep.delete_until_ms)
            {
                if (!ngx_http_cache_purge_index_require(q,
                        ngx_http_cache_purge_index_retention_valid(&keep)
                        && (!keep.invalidate_ms
                            || keep.invalidate_until_ms > now)
                        && (!keep.delete_ms || keep.delete_until_ms > now),
                        NGX_HTTP_CACHE_ROCKSDB_INVALID,
                        "retention guard returned an expired window"))
                {
                    break;
                }
                q->lookup.len = 0;
                ngx_http_cache_purge_index_number(q, &q->lookup,
                    keep.invalidate_ms);
                ngx_http_cache_purge_index_number(q, &q->lookup,
                    keep.invalidate_until_ms);
                ngx_http_cache_purge_index_number(q, &q->lookup,
                    keep.delete_ms);
                ngx_http_cache_purge_index_number(q, &q->lookup,
                    keep.delete_until_ms);
                ngx_http_cache_purge_index_append(q, &q->lookup,
                    value.data + 32, value.len - 32);
                ngx_http_cache_purge_index_tx_set(tx,
                    NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key,
                    ngx_http_cache_purge_index_bytes(&q->lookup), 1);
                ngx_http_cache_purge_index_tx_set(tx,
                    NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                    ngx_http_cache_purge_index_bytes(&q->outer.key),
                    ngx_http_cache_purge_op_empty, 0);
                ngx_http_cache_purge_index_tx_set(tx,
                    NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                    ngx_http_cache_purge_index_retention_expiry(q, key,
                        ngx_http_cache_purge_index_retention_until(&keep)),
                    ngx_http_cache_purge_op_empty, 1);
                done++;
                continue;
            }
        }
        value = ngx_http_cache_purge_index_tx_need(tx,
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_row_key(q, 'h', key_id));
        count = ngx_http_cache_purge_index_integer(q, value);
        if (!ngx_http_cache_purge_index_require(q, count != 0,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "propagation reference count underflow"))
        {
            break;
        }
        if (count == 1) {
            ngx_http_cache_purge_index_tx_set(
                tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_row_key(q, 'h', key_id),
                ngx_http_cache_purge_op_empty, 0);
            value = ngx_http_cache_purge_index_tx_need(tx,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_index_row_key(q, 'k', key_id));
            if (ngx_http_cache_purge_index_key_number(q, value, 0) == 0) {
                ngx_http_cache_purge_index_tx_set(tx,
                    NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                    ngx_http_cache_purge_index_row_key(q, 'G', key_id),
                    ngx_http_cache_purge_op_empty, 1);
            }
        } else {
            ngx_http_cache_purge_index_counter(tx, 'h', key_id, count - 1);
        }
        ngx_http_cache_purge_index_dict_drop(tx, vary_id);
        ngx_http_cache_purge_index_dict_drop(tx, retained.tag_namespace);
        for (i = 0; i < retained.count; i++) {
            ngx_http_cache_purge_index_dict_drop(tx, retained.tags[i]);
        }
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            key, ngx_http_cache_purge_op_empty, 0);
        ngx_http_cache_purge_index_tx_set(tx, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_index_bytes(&q->outer.key),
            ngx_http_cache_purge_op_empty, 0);
        done++;
    }
    ngx_http_cache_purge_index_tx_commit(tx);
    if (q->status == 0) {
        *removed = done;
    }
    return ngx_http_cache_purge_index_tx_end(tx, error);
}

int
ngx_http_cache_purge_index_expire_propagation(ngx_http_cache_rocksdb_t *store,
    uint64_t now, size_t budget, size_t *removed,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_index_expire_propagation_checked(store, now,
        budget, NULL, NULL, removed, error);
}
