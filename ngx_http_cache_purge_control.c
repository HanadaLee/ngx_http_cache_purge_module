/*
 * Copyright (C) Hanada
 */

#include "ngx_http_cache_purge_internal.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/types.h>
#include <unistd.h>

#define NGX_HTTP_CACHE_PURGE_CONTROL_MAGIC  UINT64_C(0x4e43504354524c31)
#define NGX_HTTP_CACHE_PURGE_CONTROL_NONE   UINT32_MAX
#define NGX_HTTP_CACHE_PURGE_CONTROL_MAX    65536
#define NGX_HTTP_CACHE_PURGE_CONTROL_BYTES  (64 * 1024 * 1024)

#if (!defined(__GCC_ATOMIC_LLONG_LOCK_FREE) \
     || __GCC_ATOMIC_LLONG_LOCK_FREE != 2)
#error shared control requires lock-free 64-bit atomics
#endif

typedef struct {
    uint64_t  token;
    uint64_t  pin;
    int64_t   pid;
    uint64_t  birth;
} __attribute__((aligned(64))) ngx_http_cache_purge_control_slot_t;

typedef struct {
    uint64_t         magic_value;
    uint64_t         bytes;
    uint64_t         bank_bytes;
    uint64_t         bank_offset;
    uint64_t         publication;
    uint64_t         serving;
    uint64_t         next_token;
    uint64_t         owner_token;
    int64_t          owner_pid;
    uint64_t         owner_birth;
    uint64_t         salt[2];
    uint32_t         abi;
    uint32_t         readers;
    pthread_mutex_t  mutex;
} __attribute__((aligned(64))) ngx_http_cache_purge_control_shared_t;

typedef struct {
    uint32_t  offset;
    uint32_t  size;
} ngx_http_cache_purge_control_slice_t;

typedef struct {
    uint64_t                              sequence;
    uint64_t                              issued;
    uint64_t                              until;
    uint32_t                              flags;
    uint32_t                              mode;
    uint32_t                              kind;
    uint32_t                              next;
    ngx_http_cache_purge_control_slice_t  id;
    ngx_http_cache_purge_control_slice_t  value;
    ngx_http_cache_purge_control_slice_t  scope;
} ngx_http_cache_purge_control_record_t;

typedef struct {
    ngx_http_cache_purge_control_slice_t  edge;
    uint32_t                              child;
    uint32_t                              sibling;
    uint32_t                              record;
} ngx_http_cache_purge_control_node_t;

typedef struct {
    uint64_t  magic_value;
    uint32_t  bytes;
    uint32_t  records;
    uint32_t  nodes;
    uint32_t  buckets;
    uint32_t  records_at;
    uint32_t  nodes_at;
    uint32_t  buckets_at;
    uint32_t  strings_at;
} ngx_http_cache_purge_control_header_t;

struct ngx_http_cache_purge_control_reader_s {
    ngx_http_cache_purge_control_shared_t  *shared;
    uint32_t                                slot;
    uint64_t                                token;
    pid_t                                   pid;
    pthread_t                               thread;
    int                                     pinned;
};

struct ngx_http_cache_purge_control_writer_s {
    ngx_http_cache_purge_control_shared_t  *shared;
    uint64_t                                token;
    uint64_t                                published;
    pid_t                                   pid;
    pthread_t                               thread;
};

struct ngx_http_cache_purge_control_image_s {
    ngx_http_cache_purge_control_shared_t  *shared;
    uint64_t                                owner;
    uint64_t                                previous;
    unsigned char                          *data;
    size_t                                  len;
};

typedef struct {
    uint64_t  v0;
    uint64_t  v1;
    uint64_t  v2;
    uint64_t  v3;
    uint64_t  word;
    uint64_t  count;
    unsigned  used;
} ngx_http_cache_purge_control_hash_t;

typedef struct {
    ngx_http_cache_rocksdb_bytes_t  id;
    uint64_t                        sequence;
} ngx_http_cache_purge_control_identity_t;

typedef struct {
    ngx_http_cache_purge_control_record_t  *records;
    ngx_http_cache_purge_control_node_t    *nodes;
    uint32_t                               *buckets;
    unsigned char                          *strings;
    uint32_t                                nnodes;
    uint32_t                                max_nodes;
    uint32_t                                nstrings;
} ngx_http_cache_purge_control_build_t;

typedef struct {
    ngx_http_cache_purge_control_shared_t         *shared;
    const unsigned char                           *base;
    const ngx_http_cache_purge_control_header_t   *image;
    const ngx_http_cache_purge_control_record_t   *records;
    const ngx_http_cache_purge_control_node_t     *nodes;
    const uint32_t                                *buckets;
    const ngx_http_cache_purge_control_context_t  *context;
    size_t                                         count;
    size_t                                         remaining;
    size_t                                         limit;
    ngx_http_cache_purge_control_visit_pt         visit;
    void                            *data;
    ngx_http_cache_rocksdb_error_t  *error;
} ngx_http_cache_purge_control_matcher_t;

#if (defined(NGX_HTTP_CACHE_PURGE_CONTROL_FAULT_TEST))
static int ngx_http_cache_purge_control_fault_point;
static int ngx_http_cache_purge_control_fault_fd;

void
ngx_http_cache_purge_control_test_fault(int point, int fd)
{
    ngx_http_cache_purge_control_fault_point = point;
    ngx_http_cache_purge_control_fault_fd = fd;
}

static void
ngx_http_cache_purge_control_fault(int point)
{
    const char byte = 1;

    if (point != ngx_http_cache_purge_control_fault_point) {
        return;
    }

    if (write(ngx_http_cache_purge_control_fault_fd, &byte, 1) != 1) {
        _exit(90);
    }

    for ( ;; ) {
        pause();
    }
}
#else
#define ngx_http_cache_purge_control_fault(point)
#endif

static int
ngx_http_cache_purge_control_error(ngx_http_cache_rocksdb_error_t *error,
    int code, const char *message)
{
    if (error != NULL) {
        error->code = code;
        snprintf(error->message, sizeof(error->message), "%s", message);
    }

    return code;
}

static void
ngx_http_cache_purge_control_clear(ngx_http_cache_rocksdb_error_t *error)
{
    if (error != NULL) {
        error->code = 0;
        error->message[0] = '\0';
    }
}

static uint64_t
ngx_http_cache_purge_control_load(const uint64_t *value)
{
    return __atomic_load_n(value, __ATOMIC_SEQ_CST);
}

static void
ngx_http_cache_purge_control_store(uint64_t *value, uint64_t next)
{
    __atomic_store_n(value, next, __ATOMIC_SEQ_CST);
}

static size_t
ngx_http_cache_purge_control_align(size_t value)
{
    return (value + 63) & ~(size_t) 63;
}

static ngx_http_cache_purge_control_slot_t *
ngx_http_cache_purge_control_slots(ngx_http_cache_purge_control_shared_t *s)
{
    return (ngx_http_cache_purge_control_slot_t
                *) ((unsigned char *) s
                    + ngx_http_cache_purge_control_align(sizeof(*s)));
}

static unsigned char *
ngx_http_cache_purge_control_bank(ngx_http_cache_purge_control_shared_t *s,
    unsigned i)
{
    return (unsigned char *) s + s->bank_offset + i * s->bank_bytes;
}

static uint64_t
ngx_http_cache_purge_control_birth(pid_t pid)
{
    FILE    *file;
    char      path[64];
    char      data[4096];
    char     *p;
    char     *end;
    unsigned  field;
    uint64_t  value;

    snprintf(path, sizeof(path), "/proc/%ld/stat", (long) pid);
    file = fopen(path, "r");
    if (file == NULL) {
        return 0;
    }

    p = fgets(data, sizeof(data), file);
    fclose(file);
    if (p == NULL) {
        return 0;
    }

    p = strrchr(data, ')');
    if (p == NULL || p[1] != ' ') {
        return 0;
    }

    p += 2;
    for (field = 3; field < 22; field++) {
        while (*p != '\0' && *p != ' ') {
            p++;
        }
        while (*p == ' ') {
            p++;
        }
        if (*p == '\0') {
            return 0;
        }
    }

    if (*p < '0' || *p > '9') {
        return 0;
    }

    errno = 0;
    value = strtoull(p, &end, 10);
    return errno == 0 && end != p && (*end == ' ' || *end == '\n') ? value : 0;
}

static int
ngx_http_cache_purge_control_gone(int64_t pid, uint64_t expected)
{
    uint64_t current;

    if (pid <= 0 || expected == 0) {
        return 0;
    }

    current = ngx_http_cache_purge_control_birth((pid_t) pid);
    if (current != 0) {
        return current != expected;
    }

    /* A failed /proc read alone is not proof of death. */
    return kill((pid_t) pid, 0) < 0 && errno == ESRCH;
}

static int
ngx_http_cache_purge_control_lock(ngx_http_cache_purge_control_shared_t *s,
    ngx_http_cache_rocksdb_error_t *error)
{
    int rc;

    rc = pthread_mutex_trylock(&s->mutex);
    if (rc == EOWNERDEAD) {
        ngx_http_cache_purge_control_store(&s->serving, 0);
        rc = pthread_mutex_consistent(&s->mutex);
        if (rc != 0) {
            pthread_mutex_unlock(&s->mutex);
            return ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_IO,
                "control mutex recovery failed");
        }
        return 0;
    }

    if (rc == EBUSY) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "control publisher/registration busy");
    }
    if (rc != 0) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO, "control mutex failure");
    }

    ngx_http_cache_purge_control_fault(1);
    return 0;
}

static int
ngx_http_cache_purge_control_attach(void *memory, size_t bytes,
    ngx_http_cache_purge_control_shared_t **out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_shared_t *s;

    if (memory == NULL || (uintptr_t) memory % 64
        || bytes < sizeof(ngx_http_cache_purge_control_shared_t))
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid shared control memory");
    }

    s = memory;
    if (s->magic_value != NGX_HTTP_CACHE_PURGE_CONTROL_MAGIC
        || s->abi != NGX_HTTP_CACHE_PURGE_CONTROL_ABI || s->bytes != bytes
        || s->bank_offset
               != ngx_http_cache_purge_control_align(sizeof(*s))
                      + s->readers * sizeof(ngx_http_cache_purge_control_slot_t)
        || ngx_http_cache_purge_control_size(s->bank_bytes, s->readers)
               != bytes)
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
            "incompatible shared control layout");
    }

    *out = s;
    return 0;
}

static int
ngx_http_cache_purge_control_token(ngx_http_cache_purge_control_shared_t *s,
    uint64_t *out, ngx_http_cache_rocksdb_error_t *error)
{
    if (s->next_token == 0 || s->next_token == UINT64_MAX) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "control registration tokens exhausted");
    }

    *out = s->next_token++;
    return 0;
}

static void
ngx_http_cache_purge_control_reclaim(ngx_http_cache_purge_control_shared_t *s)
{
    uint32_t                              i;
    ngx_http_cache_purge_control_slot_t  *slot;

    for (i = 0; i < s->readers; i++) {
        slot = &ngx_http_cache_purge_control_slots(s)[i];
        if (ngx_http_cache_purge_control_load(&slot->token)
            && ngx_http_cache_purge_control_gone(slot->pid, slot->birth))
        {
            ngx_http_cache_purge_control_store(&slot->pin, 0);
            ngx_http_cache_purge_control_store(&slot->token, 0);
        }
    }
}

static int
ngx_http_cache_purge_control_reader_owner(
    ngx_http_cache_purge_control_reader_t *r,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (r == NULL || r->pid != getpid()
        || !pthread_equal(r->thread, pthread_self()))
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "control reader owner mismatch");
    }

    if (ngx_http_cache_purge_control_load(
            &ngx_http_cache_purge_control_slots(r->shared)[r->slot].token)
        != r->token)
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "control reader registration changed");
    }

    return 0;
}

static int
ngx_http_cache_purge_control_writer_owner(
    ngx_http_cache_purge_control_writer_t *w,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (w == NULL || w->pid != getpid()
        || !pthread_equal(w->thread, pthread_self()))
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "control writer owner mismatch");
    }

    if (ngx_http_cache_purge_control_load(&w->shared->owner_token) != w->token)
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "control publisher lease changed");
    }

    return 0;
}

static int
ngx_http_cache_purge_control_pin(ngx_http_cache_purge_control_reader_t *r,
    uint64_t expected, uint64_t *out, ngx_http_cache_rocksdb_error_t *error)
{
    int                                     rc;
    uint64_t                                version;
    ngx_http_cache_purge_control_shared_t  *s;

    rc = ngx_http_cache_purge_control_reader_owner(r, error);
    if (rc != 0) {
        return rc;
    }
    if (r->pinned) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "control reader already pinned");
    }

    s = r->shared;
    if (!ngx_http_cache_purge_control_load(&s->serving)) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "control admission gate closed");
    }
    version = ngx_http_cache_purge_control_load(&s->publication);
    if (version == 0 || (expected && version != expected)) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control publication changed");
    }

    ngx_http_cache_purge_control_store(
        &ngx_http_cache_purge_control_slots(s)[r->slot].pin, version);
    if (ngx_http_cache_purge_control_load(&s->publication) != version
        || !ngx_http_cache_purge_control_load(&s->serving))
    {
        ngx_http_cache_purge_control_store(
            &ngx_http_cache_purge_control_slots(s)[r->slot].pin, 0);
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "control publication changed during admission");
    }

    r->pinned = 1;
    *out = version;
    return 0;
}

static void
ngx_http_cache_purge_control_unpin(ngx_http_cache_purge_control_reader_t *r)
{
    ngx_http_cache_purge_control_store(
        &ngx_http_cache_purge_control_slots(r->shared)[r->slot].pin, 0);
    r->pinned = 0;
}

static int
ngx_http_cache_purge_control_bank_pinned(
    ngx_http_cache_purge_control_shared_t *s, unsigned target)
{
    uint32_t  i;
    uint64_t  value;

    for (i = 0; i < s->readers; i++) {
        value = ngx_http_cache_purge_control_load(
            &ngx_http_cache_purge_control_slots(s)[i].pin);
        if (value && (value & 1) == target) {
            return 1;
        }
    }

    return 0;
}

static uint64_t
ngx_http_cache_purge_control_rotate(uint64_t n, unsigned bits)
{
    return (n << bits) | (n >> (64 - bits));
}

static void
ngx_http_cache_purge_control_hash_round(ngx_http_cache_purge_control_hash_t *h)
{
    h->v0 += h->v1;
    h->v1 = ngx_http_cache_purge_control_rotate(h->v1, 13);
    h->v1 ^= h->v0;
    h->v0 = ngx_http_cache_purge_control_rotate(h->v0, 32);
    h->v2 += h->v3;
    h->v3 = ngx_http_cache_purge_control_rotate(h->v3, 16);
    h->v3 ^= h->v2;
    h->v0 += h->v3;
    h->v3 = ngx_http_cache_purge_control_rotate(h->v3, 21);
    h->v3 ^= h->v0;
    h->v2 += h->v1;
    h->v1 = ngx_http_cache_purge_control_rotate(h->v1, 17);
    h->v1 ^= h->v2;
    h->v2 = ngx_http_cache_purge_control_rotate(h->v2, 32);
}

static void
ngx_http_cache_purge_control_hash_block(ngx_http_cache_purge_control_hash_t *h,
    uint64_t value)
{
    h->v3 ^= value;
    ngx_http_cache_purge_control_hash_round(h);
    ngx_http_cache_purge_control_hash_round(h);
    h->v0 ^= value;
}

static void
ngx_http_cache_purge_control_hash_byte(ngx_http_cache_purge_control_hash_t *h,
    unsigned char c)
{
    h->word |= (uint64_t) c << (h->used++ * 8);
    h->count++;
    if (h->used == 8) {
        ngx_http_cache_purge_control_hash_block(h, h->word);
        h->word = 0;
        h->used = 0;
    }
}

static void
ngx_http_cache_purge_control_hash_string(ngx_http_cache_purge_control_hash_t *h,
    ngx_http_cache_rocksdb_bytes_t value)
{
    size_t i;

    for (i = 0; i != 4; i++) {
        ngx_http_cache_purge_control_hash_byte(
            h, (unsigned char) (value.len >> (i * 8)));
    }
    for (i = 0; i < value.len; i++) {
        ngx_http_cache_purge_control_hash_byte(h, value.data[i]);
    }
}

/* SipHash-2-4 with a per-SHM key and explicit scope/value lengths. */
static uint64_t
ngx_http_cache_purge_control_key_hash(const uint64_t *salt, uint32_t kind,
    ngx_http_cache_rocksdb_bytes_t value, ngx_http_cache_rocksdb_bytes_t scope)
{
    ngx_http_cache_purge_control_hash_t  h;
    unsigned                             i;

    memset(&h, 0, sizeof(h));
    h.v0 = salt[0] ^ UINT64_C(0x736f6d6570736575);
    h.v1 = salt[1] ^ UINT64_C(0x646f72616e646f6d);
    h.v2 = salt[0] ^ UINT64_C(0x6c7967656e657261);
    h.v3 = salt[1] ^ UINT64_C(0x7465646279746573);

    ngx_http_cache_purge_control_hash_byte(&h, (unsigned char) kind);
    ngx_http_cache_purge_control_hash_string(&h, scope);
    ngx_http_cache_purge_control_hash_string(&h, value);
    ngx_http_cache_purge_control_hash_block(&h,
                                            h.word | ((h.count & 255) << 56));
    h.v2 ^= 255;
    for (i = 0; i != 4; i++) {
        ngx_http_cache_purge_control_hash_round(&h);
    }
    return h.v0 ^ h.v1 ^ h.v2 ^ h.v3;
}

static int
ngx_http_cache_purge_control_bytes_valid(ngx_http_cache_rocksdb_bytes_t b,
    size_t limit)
{
    return b.len <= limit && (b.len == 0 || b.data != NULL);
}

static int
ngx_http_cache_purge_control_bytes_equal(ngx_http_cache_rocksdb_bytes_t a,
    ngx_http_cache_rocksdb_bytes_t b)
{
    return a.len == b.len && (a.len == 0 || memcmp(a.data, b.data, a.len) == 0);
}

static int
ngx_http_cache_purge_control_identity_id(const void *left, const void *right)
{
    const ngx_http_cache_purge_control_identity_t  *a;
    const ngx_http_cache_purge_control_identity_t  *b;
    size_t                                          size;
    int                                             rc;

    a = left;
    b = right;
    size = a->id.len < b->id.len ? a->id.len : b->id.len;
    rc = memcmp(a->id.data, b->id.data, size);
    return rc != 0 ? rc : (a->id.len > b->id.len) - (a->id.len < b->id.len);
}

static int
ngx_http_cache_purge_control_identity_sequence(const void *left,
    const void *right)
{
    const ngx_http_cache_purge_control_identity_t  *a;
    const ngx_http_cache_purge_control_identity_t  *b;

    a = left;
    b = right;
    return (a->sequence > b->sequence) - (a->sequence < b->sequence);
}

static int
ngx_http_cache_purge_control_validate(
    const ngx_http_cache_purge_operation_t *op)
{
    uint32_t                                       kind;
    const ngx_http_cache_purge_operation_input_t  *input;

    input = &op->input;
    kind = input->selector.kind;
    if (!op->sequence || !input->issued_at_ms || !input->id.len
        || !ngx_http_cache_purge_control_bytes_valid(input->id, 128)
        || !ngx_http_cache_purge_control_bytes_valid(input->selector.value,
                                                     32768)
        || !ngx_http_cache_purge_control_bytes_valid(input->selector.scope,
                                                     32768)
        || op->stage < NGX_HTTP_CACHE_PURGE_PREPARING
        || op->stage > NGX_HTTP_CACHE_PURGE_MATERIALIZED
        || (input->mode != NGX_HTTP_CACHE_PURGE_INVALIDATE
            && input->mode != NGX_HTTP_CACHE_PURGE_DELETE)
        || kind > NGX_HTTP_CACHE_PURGE_HOST_TAG)
    {
        return 0;
    }

    if (kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG
        || kind == NGX_HTTP_CACHE_PURGE_HOST_TAG)
    {
        if (!input->selector.scope.len || !input->selector.value.len
            || input->selector.value.len > 1024)
        {
            return 0;
        }
    } else if (input->selector.scope.len) {
        return 0;
    }

    if (!input->selector.value.len && kind != NGX_HTTP_CACHE_PURGE_KEY_PREFIX
        && kind != NGX_HTTP_CACHE_PURGE_URL_PREFIX)
    {
        return 0;
    }

    if (op->propagate
        && (op->stage == NGX_HTTP_CACHE_PURGE_PREPARING
            || !op->propagation_until_ms))
    {
        return 0;
    }

    return op->stage != NGX_HTTP_CACHE_PURGE_MATERIALIZED || op->propagate;
}

static ngx_http_cache_purge_control_slice_t
ngx_http_cache_purge_control_append(ngx_http_cache_purge_control_build_t *b,
    ngx_http_cache_rocksdb_bytes_t value)
{
    ngx_http_cache_purge_control_slice_t slice;

    slice.offset = b->nstrings;
    slice.size = (uint32_t) value.len;
    if (value.len != 0) {
        memcpy(b->strings + b->nstrings, value.data, value.len);
        b->nstrings += (uint32_t) value.len;
    }
    return slice;
}

static void
ngx_http_cache_purge_control_node_init(
    ngx_http_cache_purge_control_node_t *node)
{
    memset(node, 0, sizeof(*node));
    node->child = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;
    node->sibling = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;
    node->record = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;
}

static int
ngx_http_cache_purge_control_prefix_build(
    ngx_http_cache_purge_control_build_t *b, uint32_t root, uint32_t record)
{
    ngx_http_cache_purge_control_slice_t  remaining;
    ngx_http_cache_purge_control_slice_t  edge;
    ngx_http_cache_purge_control_node_t  *node;
    uint32_t                              parent;
    uint32_t                              child;
    uint32_t                              previous;
    uint32_t                              common;
    uint32_t                              id;
    unsigned char                         first;

    remaining = b->records[record].value;
    parent = root;

    while (remaining.size) {
        first = b->strings[remaining.offset];
        child = b->nodes[parent].child;
        previous = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;

        while (child != NGX_HTTP_CACHE_PURGE_CONTROL_NONE
               && b->strings[b->nodes[child].edge.offset] < first)
        {
            previous = child;
            child = b->nodes[child].sibling;
        }

        if (child == NGX_HTTP_CACHE_PURGE_CONTROL_NONE
            || b->strings[b->nodes[child].edge.offset] != first)
        {
            if (b->nnodes == b->max_nodes) {
                return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
            }
            id = b->nnodes++;
            node = &b->nodes[id];
            ngx_http_cache_purge_control_node_init(node);
            node->edge = remaining;
            node->sibling = child;
            node->record = record;
            if (previous == NGX_HTTP_CACHE_PURGE_CONTROL_NONE) {
                b->nodes[parent].child = id;
            } else {
                b->nodes[previous].sibling = id;
            }
            return 0;
        }

        edge = b->nodes[child].edge;
        common = 0;
        while (common < edge.size && common < remaining.size
               && b->strings[edge.offset + common]
                      == b->strings[remaining.offset + common])
        {
            common++;
        }
        if (common < edge.size) {
            if (b->nnodes == b->max_nodes) {
                return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
            }
            id = b->nnodes++;
            node = &b->nodes[id];
            ngx_http_cache_purge_control_node_init(node);
            node->edge.offset = edge.offset;
            node->edge.size = common;
            node->child = child;
            node->sibling = b->nodes[child].sibling;
            b->nodes[child].edge.offset = edge.offset + common;
            b->nodes[child].edge.size = edge.size - common;
            b->nodes[child].sibling = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;
            if (previous == NGX_HTTP_CACHE_PURGE_CONTROL_NONE) {
                b->nodes[parent].child = id;
            } else {
                b->nodes[previous].sibling = id;
            }
            child = id;
        }
        remaining.offset += common;
        remaining.size -= common;
        parent = child;
    }
    b->records[record].next = b->nodes[parent].record;
    b->nodes[parent].record = record;
    return 0;
}

static int
ngx_http_cache_purge_control_build(ngx_http_cache_purge_control_shared_t *s,
    const ngx_http_cache_purge_operation_t *ops, size_t count,
    ngx_http_cache_purge_control_image_t *image,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_identity_t  *ids;
    ngx_http_cache_purge_control_header_t    *header;
    ngx_http_cache_purge_control_build_t      b;
    ngx_http_cache_purge_control_record_t    *record;
    size_t                                    i;
    size_t                                    strings;
    size_t                                    buckets;
    size_t                                    fixed;
    size_t                                    capacity;
    size_t                                    original;
    uint32_t                                  bucket;
    uint32_t                                  nodes;
    int                                       rc;

    if (count > NGX_HTTP_CACHE_PURGE_CONTROL_MAX) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "too many control operations");
    }
    if (count && ops == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid control operations");
    }

    ids = count ? malloc(count * sizeof(*ids)) : NULL;
    if (count && ids == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }
    strings = 0;
    for (i = 0; i < count; i++) {
        if (!ngx_http_cache_purge_control_validate(&ops[i])) {
            free(ids);
            return ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "invalid control operation");
        }
        ids[i].id = ops[i].input.id;
        ids[i].sequence = ops[i].sequence;
        strings += ops[i].input.id.len + ops[i].input.selector.value.len
                   + ops[i].input.selector.scope.len;
        if (strings > s->bank_bytes) {
            free(ids);
            return ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control selector budget");
        }
    }

    if (count > 1) {
        qsort(ids, count, sizeof(*ids),
              ngx_http_cache_purge_control_identity_id);
        for (i = 1; i < count; i++) {
            if (ngx_http_cache_purge_control_bytes_equal(ids[i - 1].id,
                                                         ids[i].id))
            {
                break;
            }
        }
        if (i == count) {
            qsort(ids, count, sizeof(*ids),
                  ngx_http_cache_purge_control_identity_sequence);
            for (i = 1; i < count; i++) {
                if (ids[i - 1].sequence == ids[i].sequence) {
                    break;
                }
            }
        }
        if (i != count) {
            free(ids);
            return ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "duplicate control operation identity");
        }
    }
    free(ids);

    buckets = 8;
    while (buckets < count * 2) {
        buckets *= 2;
    }

    fixed = ngx_http_cache_purge_control_align(sizeof(*header))
            + count * sizeof(ngx_http_cache_purge_control_record_t)
            + buckets * sizeof(uint32_t) + strings;
    if (fixed > s->bank_bytes
        || 2 * sizeof(ngx_http_cache_purge_control_node_t)
               > s->bank_bytes - fixed)
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "shared control image exceeds bank capacity");
    }

    nodes = (uint32_t) ((s->bank_bytes - fixed)
                        / sizeof(ngx_http_cache_purge_control_node_t));
    if (nodes > count * 2 + 2) {
        nodes = (uint32_t) (count * 2 + 2);
    }
    capacity = fixed + nodes * sizeof(ngx_http_cache_purge_control_node_t);
    image->data = malloc(capacity);
    if (image->data == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }

    memset(image->data, 0, ngx_http_cache_purge_control_align(sizeof(*header)));
    header = (ngx_http_cache_purge_control_header_t *) image->data;
    header->magic_value = NGX_HTTP_CACHE_PURGE_CONTROL_MAGIC;
    header->records = (uint32_t) count;
    header->buckets = (uint32_t) buckets;
    header->records_at =
        (uint32_t) ngx_http_cache_purge_control_align(sizeof(*header));
    header->nodes_at = header->records_at
                       + count * sizeof(ngx_http_cache_purge_control_record_t);
    header->buckets_at =
        header->nodes_at + nodes * sizeof(ngx_http_cache_purge_control_node_t);
    header->strings_at = header->buckets_at + buckets * sizeof(uint32_t);

    memset(&b, 0, sizeof(b));
    b.records =
        (ngx_http_cache_purge_control_record_t *) (image->data
                                                   + header->records_at);
    b.nodes = (ngx_http_cache_purge_control_node_t *) (image->data
                                                       + header->nodes_at);
    b.buckets = (uint32_t *) (image->data + header->buckets_at);
    b.strings = image->data + header->strings_at;
    b.max_nodes = nodes;
    b.nnodes = 2;
    ngx_http_cache_purge_control_node_init(&b.nodes[0]);
    ngx_http_cache_purge_control_node_init(&b.nodes[1]);
    for (i = 0; i < buckets; i++) {
        b.buckets[i] = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;
    }

    for (i = 0; i < count; i++) {
        record = &b.records[i];
        memset(record, 0, sizeof(*record));
        record->next = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;
        record->sequence = ops[i].sequence;
        record->issued = ops[i].input.issued_at_ms;
        record->until = ops[i].propagation_until_ms;
        record->mode = ops[i].input.mode;
        record->kind = ops[i].input.selector.kind;
        if (ops[i].stage != NGX_HTTP_CACHE_PURGE_MATERIALIZED) {
            record->flags |= NGX_HTTP_CACHE_PURGE_CONTROL_FENCE;
        }
        if (ops[i].stage == NGX_HTTP_CACHE_PURGE_PREPARING) {
            record->flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PREPARING;
        }
        if (ops[i].propagate) {
            record->flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
        }
        record->id = ngx_http_cache_purge_control_append(&b, ops[i].input.id);
        record->value = ngx_http_cache_purge_control_append(
            &b, ops[i].input.selector.value);
        record->scope = ngx_http_cache_purge_control_append(
            &b, ops[i].input.selector.scope);

        if (record->kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX
            || record->kind == NGX_HTTP_CACHE_PURGE_URL_PREFIX)
        {
            rc = ngx_http_cache_purge_control_prefix_build(
                &b, record->kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX ? 0 : 1,
                (uint32_t) i);
            if (rc != 0) {
                return ngx_http_cache_purge_control_error(
                    error, rc, "shared control image exceeds bank capacity");
            }
        } else {
            bucket = (uint32_t) (ngx_http_cache_purge_control_key_hash(
                                     s->salt, record->kind,
                                     ops[i].input.selector.value,
                                     ops[i].input.selector.scope)
                                 & (buckets - 1));
            record->next = b.buckets[bucket];
            b.buckets[bucket] = (uint32_t) i;
        }
    }

    /* Pack the actual radix nodes without a second image-sized allocation. */
    original = header->buckets_at;
    header->nodes = b.nnodes;
    header->buckets_at =
        header->nodes_at
        + b.nnodes * sizeof(ngx_http_cache_purge_control_node_t);
    header->strings_at = header->buckets_at + buckets * sizeof(uint32_t);
    header->bytes = header->strings_at + strings;
    memmove(image->data + header->buckets_at, image->data + original,
            buckets * sizeof(uint32_t) + strings);

    for (i = 0; i < count; i++) {
        b.records[i].id.offset += header->strings_at;
        b.records[i].value.offset += header->strings_at;
        b.records[i].scope.offset += header->strings_at;
    }
    for (i = 0; i < b.nnodes; i++) {
        b.nodes[i].edge.offset += header->strings_at;
    }
    image->len = header->bytes;
    return 0;
}

size_t
ngx_http_cache_purge_control_size(size_t bank_bytes, uint32_t readers)
{
    if (bank_bytes < 4096 || bank_bytes > NGX_HTTP_CACHE_PURGE_CONTROL_BYTES
        || bank_bytes % 64 || readers == 0 || readers > 1024)
    {
        return 0;
    }
    return ngx_http_cache_purge_control_align(
               sizeof(ngx_http_cache_purge_control_shared_t))
           + readers * sizeof(ngx_http_cache_purge_control_slot_t)
           + bank_bytes * 2;
}

int
ngx_http_cache_purge_control_init(void *memory, size_t bytes, size_t bank_bytes,
    uint32_t readers, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_shared_t  *s;
    size_t                                  obtained;
    ssize_t                                 n;
    pthread_mutexattr_t                     attr;
    int                                     rc;

    ngx_http_cache_purge_control_clear(error);
    if (memory == NULL || (uintptr_t) memory % 64 || bytes == 0
        || ngx_http_cache_purge_control_size(bank_bytes, readers) != bytes)
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid shared control allocation");
    }
    s = memory;

    /* Leave unused banks demand-paged. */
    memset(memory, 0,
           ngx_http_cache_purge_control_align(sizeof(*s))
               + readers * sizeof(ngx_http_cache_purge_control_slot_t));
    s->bytes = bytes;
    s->bank_bytes = bank_bytes;
    s->bank_offset = ngx_http_cache_purge_control_align(sizeof(*s))
                     + readers * sizeof(ngx_http_cache_purge_control_slot_t);
    s->abi = NGX_HTTP_CACHE_PURGE_CONTROL_ABI;
    s->readers = readers;
    s->next_token = 1;

    obtained = 0;
    while (obtained < sizeof(s->salt)) {
        n = getrandom((unsigned char *) s->salt + obtained,
                      sizeof(s->salt) - obtained, 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_IO,
                "cannot initialize control hash seed");
        }
        obtained += (size_t) n;
    }

    rc = pthread_mutexattr_init(&attr);
    if (rc != 0) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO, "control mutex attributes");
    }
    rc = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    if (rc == 0) {
        rc = pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    }
    if (rc == 0) {
        rc = pthread_mutex_init(&s->mutex, &attr);
    }
    pthread_mutexattr_destroy(&attr);
    if (rc != 0) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO,
            "cannot initialize robust shared control mutex");
    }

    s->magic_value = NGX_HTTP_CACHE_PURGE_CONTROL_MAGIC;
    return 0;
}

int
ngx_http_cache_purge_control_reader_open(void *memory, size_t bytes,
    ngx_http_cache_purge_control_reader_t **output,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_shared_t  *s;
    ngx_http_cache_purge_control_slot_t    *slot;
    ngx_http_cache_purge_control_reader_t  *r;
    uint64_t                                stamp;
    uint32_t                                selected;
    uint32_t                                i;
    uint32_t                                pass;
    int                                     rc;

    ngx_http_cache_purge_control_clear(error);
    if (output == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "null control reader output");
    }
    *output = NULL;
    rc = ngx_http_cache_purge_control_attach(memory, bytes, &s, error);
    if (rc != 0) {
        return rc;
    }
    stamp = ngx_http_cache_purge_control_birth(getpid());
    if (stamp == 0) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO,
            "cannot identify control reader process");
    }
    r = calloc(1, sizeof(*r));
    if (r == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }
    rc = ngx_http_cache_purge_control_lock(s, error);
    if (rc != 0) {
        free(r);
        return rc;
    }
    selected = s->readers;
    for (pass = 0; pass != 2 && selected == s->readers; pass++) {
        if (pass) {
            ngx_http_cache_purge_control_reclaim(s);
        }
        for (i = 0; i < s->readers; i++) {
            if (!ngx_http_cache_purge_control_load(
                    &ngx_http_cache_purge_control_slots(s)[i].token))
            {
                selected = i;
                break;
            }
        }
    }
    if (selected == s->readers) {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "control reader slots exhausted");
    } else {
        rc = ngx_http_cache_purge_control_token(s, &r->token, error);
    }
    if (rc == 0) {
        r->shared = s;
        r->slot = selected;
        r->pid = getpid();
        r->thread = pthread_self();
        slot = &ngx_http_cache_purge_control_slots(s)[selected];
        slot->pid = r->pid;
        slot->birth = stamp;
        ngx_http_cache_purge_control_store(&slot->pin, 0);
        ngx_http_cache_purge_control_store(&slot->token, r->token);
        *output = r;
    }
    pthread_mutex_unlock(&s->mutex);
    if (rc != 0) {
        free(r);
    }
    return rc;
}

int
ngx_http_cache_purge_control_reader_close(
    ngx_http_cache_purge_control_reader_t **output,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_reader_t  *r;
    int                                     rc;

    ngx_http_cache_purge_control_clear(error);
    if (output == NULL || *output == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null control reader close");
    }
    r = *output;
    rc = ngx_http_cache_purge_control_reader_owner(r, error);
    if (rc != 0) {
        return rc;
    }
    if (r->pinned) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control reader still pinned");
    }
    rc = ngx_http_cache_purge_control_lock(r->shared, error);
    if (rc != 0) {
        return rc;
    }
    ngx_http_cache_purge_control_store(
        &ngx_http_cache_purge_control_slots(r->shared)[r->slot].token, 0);
    pthread_mutex_unlock(&r->shared->mutex);
    free(r);
    *output = NULL;
    return 0;
}

int
ngx_http_cache_purge_control_writer_open(void *memory, size_t bytes,
    ngx_http_cache_purge_control_writer_t **output,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_shared_t  *s;
    ngx_http_cache_purge_control_writer_t  *w;
    uint64_t                                stamp;
    int                                     rc;

    ngx_http_cache_purge_control_clear(error);
    if (output == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "null control writer output");
    }
    *output = NULL;
    rc = ngx_http_cache_purge_control_attach(memory, bytes, &s, error);
    if (rc != 0) {
        return rc;
    }
    stamp = ngx_http_cache_purge_control_birth(getpid());
    if (stamp == 0) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO,
            "cannot identify control writer process");
    }
    w = calloc(1, sizeof(*w));
    if (w == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }
    rc = ngx_http_cache_purge_control_lock(s, error);
    if (rc != 0) {
        free(w);
        return rc;
    }
    if (ngx_http_cache_purge_control_load(&s->owner_token)
        && !ngx_http_cache_purge_control_gone(s->owner_pid, s->owner_birth))
    {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "another control publisher owns the table");
    } else {
        rc = ngx_http_cache_purge_control_token(s, &w->token, error);
    }
    if (rc == 0) {
        w->shared = s;
        w->pid = getpid();
        w->thread = pthread_self();
        s->owner_pid = w->pid;
        s->owner_birth = stamp;
        ngx_http_cache_purge_control_store(&s->serving, 0);
        ngx_http_cache_purge_control_store(&s->owner_token, w->token);
        *output = w;
    }
    pthread_mutex_unlock(&s->mutex);
    if (rc != 0) {
        free(w);
    }
    return rc;
}

int
ngx_http_cache_purge_control_writer_close(
    ngx_http_cache_purge_control_writer_t **output,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_writer_t  *w;
    int                                     rc;

    ngx_http_cache_purge_control_clear(error);
    if (output == NULL || *output == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null control writer close");
    }
    w = *output;
    rc = ngx_http_cache_purge_control_writer_owner(w, error);
    if (rc != 0) {
        return rc;
    }
    ngx_http_cache_purge_control_store(&w->shared->serving, 0);

    /* Do not wait for unrelated live registrations during shutdown. */
    ngx_http_cache_purge_control_store(&w->shared->owner_token, 0);
    free(w);
    *output = NULL;
    return 0;
}

int
ngx_http_cache_purge_control_gate_close(
    ngx_http_cache_purge_control_writer_t *w,
    ngx_http_cache_rocksdb_error_t *error)
{
    int rc;

    ngx_http_cache_purge_control_clear(error);
    rc = ngx_http_cache_purge_control_writer_owner(w, error);
    if (rc == 0) {
        ngx_http_cache_purge_control_store(&w->shared->serving, 0);
    }
    return rc;
}

int
ngx_http_cache_purge_control_gate_open(ngx_http_cache_purge_control_writer_t *w,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_shared_t  *s;
    uint64_t                                version;
    uint64_t                                value;
    uint32_t                                i;
    int                                     rc;

    ngx_http_cache_purge_control_clear(error);
    rc = ngx_http_cache_purge_control_writer_owner(w, error);
    if (rc != 0) {
        return rc;
    }
    s = w->shared;
    rc = ngx_http_cache_purge_control_lock(s, error);
    if (rc != 0) {
        return rc;
    }
    version = ngx_http_cache_purge_control_load(&s->publication);
    if (version == 0 || version != w->published) {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "publisher has not rebuilt recovered controls");
        goto done;
    }

    for (i = 0; i < s->readers; i++) {
        value = ngx_http_cache_purge_control_load(
            &ngx_http_cache_purge_control_slots(s)[i].pin);
        if (value && value != version) {
            ngx_http_cache_purge_control_reclaim(s);
            break;
        }
    }
    for (i = 0; i < s->readers; i++) {
        value = ngx_http_cache_purge_control_load(
            &ngx_http_cache_purge_control_slots(s)[i].pin);
        if (value && value != version) {
            rc = ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
                "old control admission still pinned");
            goto done;
        }
    }
    ngx_http_cache_purge_control_store(&s->serving, 1);

done:
    pthread_mutex_unlock(&s->mutex);
    return rc;
}

int
ngx_http_cache_purge_control_guard_begin(
    ngx_http_cache_purge_control_reader_t *r, uint64_t version,
    ngx_http_cache_rocksdb_error_t *error)
{
    uint64_t pinned;

    ngx_http_cache_purge_control_clear(error);
    if (version == 0) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "empty control publication");
    }
    return ngx_http_cache_purge_control_pin(r, version, &pinned, error);
}

int
ngx_http_cache_purge_control_guard_end(ngx_http_cache_purge_control_reader_t *r,
    ngx_http_cache_rocksdb_error_t *error)
{
    int rc;

    ngx_http_cache_purge_control_clear(error);
    rc = ngx_http_cache_purge_control_reader_owner(r, error);
    if (rc != 0) {
        return rc;
    }
    if (!r->pinned) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "no control guard");
    }
    ngx_http_cache_purge_control_unpin(r);
    return 0;
}

void
ngx_http_cache_purge_control_image_free(
    ngx_http_cache_purge_control_image_t **p)
{
    if (p != NULL && *p != NULL) {
        free((*p)->data);
        free(*p);
        *p = NULL;
    }
}

int
ngx_http_cache_purge_control_prepare(ngx_http_cache_purge_control_writer_t *w,
    const ngx_http_cache_purge_operation_t *ops, size_t count,
    ngx_http_cache_purge_control_image_t **output,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_image_t  *image;
    int                                    rc;

    ngx_http_cache_purge_control_clear(error);
    if (output == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null control image output");
    }
    *output = NULL;
    rc = ngx_http_cache_purge_control_writer_owner(w, error);
    if (rc != 0) {
        return rc;
    }
    image = calloc(1, sizeof(*image));
    if (image == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }
    image->shared = w->shared;
    image->owner = w->token;
    image->previous =
        ngx_http_cache_purge_control_load(&w->shared->publication);
    rc =
        ngx_http_cache_purge_control_build(w->shared, ops, count, image, error);
    if (rc != 0) {
        ngx_http_cache_purge_control_image_free(&image);
        return rc;
    }
    *output = image;
    return 0;
}

int
ngx_http_cache_purge_control_publish(ngx_http_cache_purge_control_writer_t *w,
    const ngx_http_cache_purge_control_image_t *image, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_shared_t  *s;
    uint64_t                                current;
    uint64_t                                next;
    unsigned                                target;
    int                                     rc;
#if (defined(NGX_HTTP_CACHE_PURGE_CONTROL_FAULT_TEST))
    size_t split;
#endif

    ngx_http_cache_purge_control_clear(error);
    if (publication == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "null control publication output");
    }
    *publication = 0;
    rc = ngx_http_cache_purge_control_writer_owner(w, error);
    if (rc != 0) {
        return rc;
    }
    s = w->shared;
    if (image == NULL || image->shared != s || image->owner != w->token) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "prepared control image belongs to another publisher");
    }
    if (ngx_http_cache_purge_control_load(&s->serving)) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "close control gate before publishing");
    }
    rc = ngx_http_cache_purge_control_lock(s, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_control_writer_owner(w, error);
    if (rc != 0) {
        goto done;
    }
    current = ngx_http_cache_purge_control_load(&s->publication);
    if (current != image->previous) {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "prepared control image is stale");
        goto done;
    }
    if (current == UINT64_MAX) {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "control publications exhausted");
        goto done;
    }
    next = current ? current + 1 : 2;
    target = (unsigned) (next & 1);
    if (ngx_http_cache_purge_control_bank_pinned(s, target)) {
        ngx_http_cache_purge_control_reclaim(s);
    }
    if (ngx_http_cache_purge_control_bank_pinned(s, target)) {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "inactive control image remains pinned");
        goto done;
    }

#if (defined(NGX_HTTP_CACHE_PURGE_CONTROL_FAULT_TEST))
    split = image->len / 2;
    memcpy(ngx_http_cache_purge_control_bank(s, target), image->data, split);
    ngx_http_cache_purge_control_fault(2);
    memcpy(ngx_http_cache_purge_control_bank(s, target) + split,
           image->data + split, image->len - split);
#else
    memcpy(ngx_http_cache_purge_control_bank(s, target), image->data,
           image->len);
#endif

    ngx_http_cache_purge_control_store(&s->publication, next);
    w->published = next;
    *publication = next;

done:
    pthread_mutex_unlock(&s->mutex);
    return rc;
}

static int
ngx_http_cache_purge_control_matcher_init(
    ngx_http_cache_purge_control_matcher_t *m)
{
    const ngx_http_cache_purge_control_header_t *h;

    h = m->image;
    if (h->magic_value != NGX_HTTP_CACHE_PURGE_CONTROL_MAGIC
        || h->bytes > m->shared->bank_bytes || h->bytes < sizeof(*h)
        || h->records > NGX_HTTP_CACHE_PURGE_CONTROL_MAX || h->nodes < 2
        || h->nodes > 2 * h->records + 2 || h->buckets < 8
        || h->buckets > 131072 || (h->buckets & (h->buckets - 1))
        || h->records_at != ngx_http_cache_purge_control_align(sizeof(*h))
        || h->nodes_at
               != h->records_at
                      + h->records
                            * sizeof(ngx_http_cache_purge_control_record_t)
        || h->buckets_at
               != h->nodes_at
                      + h->nodes * sizeof(ngx_http_cache_purge_control_node_t)
        || h->strings_at != h->buckets_at + h->buckets * sizeof(uint32_t)
        || h->strings_at > h->bytes)
    {
        return ngx_http_cache_purge_control_error(
            m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "invalid shared control image");
    }
    m->records =
        (const ngx_http_cache_purge_control_record_t *) (m->base
                                                         + h->records_at);
    m->nodes =
        (const ngx_http_cache_purge_control_node_t *) (m->base + h->nodes_at);
    m->buckets = (const uint32_t *) (m->base + h->buckets_at);
    return 0;
}

static int
ngx_http_cache_purge_control_string(ngx_http_cache_purge_control_matcher_t *m,
    ngx_http_cache_purge_control_slice_t slice,
    ngx_http_cache_rocksdb_bytes_t *out)
{
    if (slice.offset < m->image->strings_at || slice.offset > m->image->bytes
        || slice.size > m->image->bytes - slice.offset)
    {
        return ngx_http_cache_purge_control_error(
            m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "control string outside image");
    }
    out->data = m->base + slice.offset;
    out->len = slice.size;
    return 0;
}

static int
ngx_http_cache_purge_control_emit(ngx_http_cache_purge_control_matcher_t *m,
    uint32_t index)
{
    const ngx_http_cache_purge_control_record_t  *r;
    ngx_http_cache_purge_control_match_t          match;
    size_t                                        size;
    int                                           rc;

    if (index >= m->image->records) {
        return ngx_http_cache_purge_control_error(
            m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "control record outside image");
    }
    r = &m->records[index];
    memset(&match, 0, sizeof(match));
    match.flags = r->flags;
    if (!m->context->proxy || m->context->now_ms >= r->until) {
        match.flags &= ~NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
    }
    if (!match.flags) {
        return 0;
    }
    rc = ngx_http_cache_purge_control_string(m, r->id, &match.operation_id);
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_string(m, r->value, &match.value);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_string(m, r->scope, &match.scope);
    }
    if (rc != 0) {
        return rc;
    }
    size = sizeof(match) + match.operation_id.len + match.value.len
           + match.scope.len;
    if (m->count >= m->limit || size > m->remaining) {
        return ngx_http_cache_purge_control_error(
            m->error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "control match output budget exhausted");
    }
    match.sequence = r->sequence;
    match.issued_at_ms = r->issued;
    match.propagation_until_ms = r->until;
    match.mode = r->mode;
    match.kind = r->kind;
    rc = m->visit(m->data, &match);
    if (rc != 0) {
        return ngx_http_cache_purge_control_error(
            m->error, rc, "control match visitor failed");
    }
    m->count++;
    m->remaining -= size;
    return 0;
}

static int
ngx_http_cache_purge_control_exact(ngx_http_cache_purge_control_matcher_t *m,
    uint32_t kind, ngx_http_cache_rocksdb_bytes_t value,
    ngx_http_cache_rocksdb_bytes_t scope)
{
    const ngx_http_cache_purge_control_record_t  *r;
    ngx_http_cache_rocksdb_bytes_t                rv;
    ngx_http_cache_rocksdb_bytes_t                rs;
    uint32_t                                      i;
    uint32_t                                      steps;
    int                                           rc;

    i = m->buckets[ngx_http_cache_purge_control_key_hash(m->shared->salt, kind,
                                                         value, scope)
                   & (m->image->buckets - 1)];
    steps = 0;
    while (i != NGX_HTTP_CACHE_PURGE_CONTROL_NONE) {
        if (++steps > m->image->records || i >= m->image->records) {
            return ngx_http_cache_purge_control_error(
                m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid control hash chain");
        }
        r = &m->records[i];
        if (r->kind == kind) {
            rc = ngx_http_cache_purge_control_string(m, r->value, &rv);
            if (rc != 0) {
                return rc;
            }
            if (ngx_http_cache_purge_control_bytes_equal(rv, value)) {
                rc = ngx_http_cache_purge_control_string(m, r->scope, &rs);
                if (rc != 0) {
                    return rc;
                }
                if (ngx_http_cache_purge_control_bytes_equal(rs, scope)) {
                    rc = ngx_http_cache_purge_control_emit(m, i);
                    if (rc != 0) {
                        return rc;
                    }
                }
            }
        }
        i = r->next;
    }
    return 0;
}

static int
ngx_http_cache_purge_control_prefix_match(
    ngx_http_cache_purge_control_matcher_t *m, uint32_t root,
    ngx_http_cache_rocksdb_bytes_t value)
{
    ngx_http_cache_rocksdb_bytes_t  edge;
    uint32_t                        current;
    uint32_t                        child;
    uint32_t                        record;
    uint32_t                        steps;
    uint32_t                        linked;
    uint32_t                        siblings;
    size_t                          offset;
    int                             rc;

    current = root;
    offset = 0;
    steps = 0;
    for ( ;; ) {
        if (++steps > m->image->nodes || current >= m->image->nodes) {
            return ngx_http_cache_purge_control_error(
                m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid control prefix node");
        }
        record = m->nodes[current].record;
        linked = 0;
        while (record != NGX_HTTP_CACHE_PURGE_CONTROL_NONE) {
            if (++linked > m->image->records) {
                return ngx_http_cache_purge_control_error(
                    m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "control prefix record cycle");
            }
            rc = ngx_http_cache_purge_control_emit(m, record);
            if (rc != 0) {
                return rc;
            }
            record = m->records[record].next;
        }
        if (offset == value.len) {
            return 0;
        }
        child = m->nodes[current].child;
        siblings = 0;
        while (child != NGX_HTTP_CACHE_PURGE_CONTROL_NONE) {
            if (++siblings > 256 || child >= m->image->nodes) {
                return ngx_http_cache_purge_control_error(
                    m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "invalid control prefix sibling");
            }
            rc = ngx_http_cache_purge_control_string(m, m->nodes[child].edge,
                                                     &edge);
            if (rc != 0) {
                return rc;
            }
            if (edge.len == 0) {
                return ngx_http_cache_purge_control_error(
                    m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    "empty control prefix edge");
            }
            if (edge.data[0] > value.data[offset]) {
                return 0;
            }
            if (edge.data[0] == value.data[offset]) {
                if (edge.len > value.len - offset
                    || memcmp(value.data + offset, edge.data, edge.len) != 0)
                {
                    return 0;
                }
                offset += edge.len;
                current = child;
                break;
            }
            child = m->nodes[child].sibling;
        }
        if (child == NGX_HTTP_CACHE_PURGE_CONTROL_NONE) {
            return 0;
        }
    }
}

static int
ngx_http_cache_purge_control_match_run(
    ngx_http_cache_purge_control_matcher_t *m)
{
    const ngx_http_cache_purge_control_context_t  *c;
    ngx_http_cache_rocksdb_bytes_t                 empty;
    size_t                                         i;
    size_t                                         j;
    int                                            rc;

    c = m->context;
    empty.data = NULL;
    empty.len = 0;
    if (!ngx_http_cache_purge_control_bytes_valid(c->key, 32768)
        || !ngx_http_cache_purge_control_bytes_valid(c->url, 32768)
        || !ngx_http_cache_purge_control_bytes_valid(c->host, 32768)
        || !ngx_http_cache_purge_control_bytes_valid(c->tag_namespace, 32768)
        || c->tag_count > 64 || (c->tag_count && c->tags == NULL))
    {
        return ngx_http_cache_purge_control_error(
            m->error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid control request field");
    }
    for (i = 0; i < c->tag_count; i++) {
        if (!ngx_http_cache_purge_control_bytes_valid(c->tags[i], 1024)) {
            return ngx_http_cache_purge_control_error(
                m->error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "invalid control TAG input");
        }
    }
    if (m->image->records == 0) {
        return 0;
    }
    rc = ngx_http_cache_purge_control_exact(m, NGX_HTTP_CACHE_PURGE_KEY, c->key,
                                            empty);
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_prefix_match(m, 0, c->key);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_exact(m, NGX_HTTP_CACHE_PURGE_URL,
                                                c->url, empty);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_prefix_match(m, 1, c->url);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_exact(m, NGX_HTTP_CACHE_PURGE_HOST,
                                                c->host, empty);
    }
    for (i = 0; rc == 0 && i < c->tag_count; i++) {
        for (j = 0; j < i; j++) {
            if (ngx_http_cache_purge_control_bytes_equal(c->tags[i],
                                                         c->tags[j]))
            {
                break;
            }
        }
        if (j != i) {
            continue;
        }
        rc = ngx_http_cache_purge_control_exact(
            m, NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG, c->tags[i],
            c->tag_namespace);
        if (rc == 0) {
            rc = ngx_http_cache_purge_control_exact(
                m, NGX_HTTP_CACHE_PURGE_HOST_TAG, c->tags[i], c->host);
        }
    }
    return rc;
}

int
ngx_http_cache_purge_control_match(ngx_http_cache_purge_control_reader_t *r,
    const ngx_http_cache_purge_control_context_t *context, size_t max_matches,
    size_t max_bytes, ngx_http_cache_purge_control_visit_pt visit, void *data,
    uint64_t *publication, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_matcher_t  m;
    uint64_t                                version;
    int                                     rc;

    ngx_http_cache_purge_control_clear(error);
    if (publication != NULL) {
        *publication = 0;
    }
    if (context == NULL || visit == NULL || publication == NULL
        || max_matches == 0 || max_matches > NGX_HTTP_CACHE_PURGE_CONTROL_MAX
        || max_bytes > NGX_HTTP_CACHE_PURGE_CONTROL_BYTES)
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid control lookup output");
    }

    rc = ngx_http_cache_purge_control_pin(r, 0, &version, error);
    if (rc != 0) {
        return rc;
    }
    memset(&m, 0, sizeof(m));
    m.shared = r->shared;
    m.base =
        ngx_http_cache_purge_control_bank(r->shared, (unsigned) (version & 1));
    m.image = (const ngx_http_cache_purge_control_header_t *) m.base;
    m.context = context;
    m.limit = max_matches;
    m.remaining = max_bytes;
    m.visit = visit;
    m.data = data;
    m.error = error;

    rc = ngx_http_cache_purge_control_matcher_init(&m);
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_match_run(&m);
    }
    if (rc == 0
        && (!ngx_http_cache_purge_control_load(&r->shared->serving)
            || ngx_http_cache_purge_control_load(&r->shared->publication)
                   != version))
    {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "control image changed during lookup");
    }
    if (rc == 0) {
        *publication = version;
    }
    if (rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT) {
        ngx_http_cache_purge_control_store(&r->shared->serving, 0);
    }
    ngx_http_cache_purge_control_unpin(r);
    return rc;
}

struct ngx_http_cache_purge_coordinator_s {
    ngx_http_cache_purge_operations_t       operations;
    void                                   *shared;
    size_t                                  shared_bytes;
    ngx_http_cache_purge_control_writer_t  *writer;
    ngx_http_cache_purge_control_image_t   *pending;
    ngx_http_cache_purge_operation_usage_t  expected;
    ngx_http_cache_purge_operation_t       *controls;
    size_t                                  count;
    size_t                                  capacity;
    size_t                                  after_len;
    unsigned char                           after[17];
    uint64_t                                loaded_bytes;
    uint64_t                                publication;
    uint32_t                                state;
    pid_t                                   pid;
    pthread_t                               thread;
};

static int
ngx_http_cache_purge_control_coordinator_owner(
    ngx_http_cache_purge_coordinator_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (c == NULL || c->pid != getpid()
        || !pthread_equal(c->thread, pthread_self()))
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "control coordinator owner mismatch");
    }
    return 0;
}

static void
ngx_http_cache_purge_control_controls_free(
    ngx_http_cache_purge_coordinator_t *c)
{
    size_t i;

    for (i = 0; i < c->count; i++) {
        ngx_http_cache_purge_operation_free(&c->controls[i]);
    }
    c->count = 0;
}

static int
ngx_http_cache_purge_control_controls_reserve(
    ngx_http_cache_purge_coordinator_t *c, size_t count,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operation_t  *controls;
    size_t                             capacity;

    if (count <= c->capacity) {
        return 0;
    }
    if (count > NGX_HTTP_CACHE_PURGE_CONTROL_MAX) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "too many control operations");
    }
    capacity = c->capacity ? c->capacity * 2 : 8;
    if (capacity < count) {
        capacity = count;
    }
    if (capacity > NGX_HTTP_CACHE_PURGE_CONTROL_MAX) {
        capacity = NGX_HTTP_CACHE_PURGE_CONTROL_MAX;
    }
    controls = realloc(c->controls, capacity * sizeof(*controls));
    if (controls == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }
    c->controls = controls;
    c->capacity = capacity;
    return 0;
}

static unsigned char *
ngx_http_cache_purge_control_copy(unsigned char *p,
    ngx_http_cache_rocksdb_bytes_t src, ngx_http_cache_rocksdb_bytes_t *dst)
{
    dst->data = p;
    dst->len = src.len;
    if (src.len != 0) {
        memcpy(p, src.data, src.len);
    }
    return p + src.len;
}

static int
ngx_http_cache_purge_control_control_copy(ngx_http_cache_purge_operation_t *dst,
    const ngx_http_cache_purge_operation_t *src,
    ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char  *p;
    size_t          size;

    memset(dst, 0, sizeof(*dst));
    size = src->input.id.len + src->input.selector.value.len
           + src->input.selector.scope.len + src->input.binding.len;
    dst->storage = malloc(size ? size : 1);
    if (dst->storage == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }

    /* Do not retain disk bookmarks, error text or per-file work statistics. */
    p = ngx_http_cache_purge_control_copy(dst->storage, src->input.id,
                                          &dst->input.id);
    p = ngx_http_cache_purge_control_copy(p, src->input.selector.value,
                                          &dst->input.selector.value);
    p = ngx_http_cache_purge_control_copy(p, src->input.selector.scope,
                                          &dst->input.selector.scope);
    ngx_http_cache_purge_control_copy(p, src->input.binding,
                                      &dst->input.binding);
    dst->input.selector.kind = src->input.selector.kind;
    dst->input.mode = src->input.mode;
    dst->input.issued_at_ms = src->input.issued_at_ms;
    dst->sequence = src->sequence;
    dst->revision = src->revision;
    dst->stage = src->stage;
    dst->accepted_at_ms = src->accepted_at_ms;
    dst->effective_at_ms = src->effective_at_ms;
    dst->propagation_until_ms = src->propagation_until_ms;
    dst->propagation_ms = src->propagation_ms;
    dst->propagate = src->propagate;
    return 0;
}

static int
ngx_http_cache_purge_control_load_begin(ngx_http_cache_purge_coordinator_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    int rc;

    ngx_http_cache_purge_control_image_free(&c->pending);
    ngx_http_cache_purge_control_controls_free(c);
    c->after_len = 0;
    c->loaded_bytes = 0;
    rc = ngx_http_cache_purge_operations_usage(&c->operations, &c->expected,
                                               error);
    if (rc != 0) {
        return rc;
    }
    if (c->expected.live_records > NGX_HTTP_CACHE_PURGE_CONTROL_MAX
        || c->expected.live_selector_bytes > NGX_HTTP_CACHE_PURGE_CONTROL_BYTES)
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "durable control usage exceeds supported bounds");
    }
    rc = ngx_http_cache_purge_control_controls_reserve(
        c, (size_t) c->expected.live_records, error);
    if (rc == 0) {
        c->state = NGX_HTTP_CACHE_PURGE_COORD_LOADING;
    }
    return rc;
}

int
ngx_http_cache_purge_coordinator_open(ngx_http_cache_purge_operations_t *ops,
    void *shared, size_t shared_bytes,
    ngx_http_cache_purge_coordinator_t **output,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_coordinator_t *c;

    ngx_http_cache_purge_control_clear(error);
    if (output != NULL) {
        *output = NULL;
    }
    if (output == NULL || ops == NULL || ops->store == NULL || shared == NULL
        || shared_bytes == 0)
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid coordinator inputs");
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }
    c->operations = *ops;
    c->shared = shared;
    c->shared_bytes = shared_bytes;
    c->state = NGX_HTTP_CACHE_PURGE_COORD_CLAIMING;
    c->pid = getpid();
    c->thread = pthread_self();
    *output = c;
    return 0;
}

int
ngx_http_cache_purge_coordinator_close(ngx_http_cache_purge_coordinator_t **p,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_coordinator_t  *c;
    int                                  rc;

    ngx_http_cache_purge_control_clear(error);
    if (p == NULL || *p == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null coordinator close");
    }
    c = *p;
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->writer != NULL) {
        rc = ngx_http_cache_purge_control_writer_close(&c->writer, error);
        if (rc != 0) {
            return rc;
        }
    }
    ngx_http_cache_purge_control_image_free(&c->pending);
    ngx_http_cache_purge_control_controls_free(c);
    free(c->controls);
    free(c);
    *p = NULL;
    return 0;
}

int
ngx_http_cache_purge_coordinator_status(ngx_http_cache_purge_coordinator_t *c,
    uint32_t *state, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error)
{
    int rc;

    ngx_http_cache_purge_control_clear(error);
    if (state != NULL) {
        *state = 0;
    }
    if (publication != NULL) {
        *publication = 0;
    }
    if (state == NULL || publication == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "null coordinator status output");
    }
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc == 0) {
        *state = c->state;
        *publication = c->publication;
    }
    return rc;
}

int
ngx_http_cache_purge_coordinator_refresh(ngx_http_cache_purge_coordinator_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    int rc;

    ngx_http_cache_purge_control_clear(error);
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "control synchronization already in progress");
    }
    return ngx_http_cache_purge_control_load_begin(c, error);
}

static int
ngx_http_cache_purge_control_load_page(ngx_http_cache_purge_coordinator_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operation_page_t   page;
    ngx_http_cache_purge_operation_usage_t  usage;
    ngx_http_cache_purge_operation_t       *op;
    ngx_http_cache_rocksdb_bytes_t          after;
    size_t                                  i;
    uint64_t                                size;
    int                                     rc;

    after.data = c->after;
    after.len = c->after_len;
    rc = ngx_http_cache_purge_operations_page(&c->operations, 0, 0, after, 32,
                                              &page, error);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < page.count; i++) {
        op = &page.items[i];
        size = ngx_http_cache_purge_operation_charge(&op->input);
        if (c->count >= c->expected.live_records
            || c->loaded_bytes > c->expected.live_selector_bytes
            || size > c->expected.live_selector_bytes - c->loaded_bytes
            || op->sequence > c->expected.last_sequence
            || (c->count && c->controls[c->count - 1].sequence >= op->sequence))
        {
            rc = ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "live control enumeration exceeds durable budget");
            goto done;
        }
        rc = ngx_http_cache_purge_control_control_copy(&c->controls[c->count],
                                                       op, error);
        if (rc != 0) {
            goto done;
        }
        c->count++;
        c->loaded_bytes += size;
    }
    c->after_len = page.after_len;
    memcpy(c->after, page.after, page.after_len);
    if (page.done) {
        rc = ngx_http_cache_purge_operations_usage(&c->operations, &usage,
                                                   error);
        if (rc != 0) {
            goto done;
        }
        if (c->count != c->expected.live_records
            || c->loaded_bytes != c->expected.live_selector_bytes
            || usage.live_records != c->expected.live_records
            || usage.live_selector_bytes != c->expected.live_selector_bytes
            || usage.last_sequence != c->expected.last_sequence)
        {
            rc = ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "live control recovery watermark mismatch");
            goto done;
        }
        c->state = NGX_HTTP_CACHE_PURGE_COORD_BUILDING;
    }

done:
    ngx_http_cache_purge_operation_page_free(&page);
    return rc;
}

int
ngx_http_cache_purge_coordinator_step(ngx_http_cache_purge_coordinator_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    int       rc;
    uint64_t  publication;

    ngx_http_cache_purge_control_clear(error);
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    switch (c->state) {
    case NGX_HTTP_CACHE_PURGE_COORD_CLAIMING:
        rc = ngx_http_cache_purge_control_writer_open(
            c->shared, c->shared_bytes, &c->writer, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_control_load_begin(c, error);
        }
        break;
    case NGX_HTTP_CACHE_PURGE_COORD_LOADING:
        rc = ngx_http_cache_purge_control_load_page(c, error);
        break;
    case NGX_HTTP_CACHE_PURGE_COORD_BUILDING:
        ngx_http_cache_purge_control_image_free(&c->pending);
        rc = ngx_http_cache_purge_control_prepare(c->writer, c->controls,
                                                  c->count, &c->pending, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_control_gate_close(c->writer, error);
        }
        if (rc == 0) {
            c->state = NGX_HTTP_CACHE_PURGE_COORD_PUBLISHING;
        }
        break;
    case NGX_HTTP_CACHE_PURGE_COORD_PUBLISHING:
        rc = ngx_http_cache_purge_control_publish(c->writer, c->pending,
                                                  &publication, error);
        if (rc == 0) {
            c->publication = publication;
            ngx_http_cache_purge_control_image_free(&c->pending);
            c->state = NGX_HTTP_CACHE_PURGE_COORD_DRAINING;
        }
        break;
    case NGX_HTTP_CACHE_PURGE_COORD_DRAINING:
        rc = ngx_http_cache_purge_control_gate_open(c->writer, error);
        if (rc == 0) {
            c->state = NGX_HTTP_CACHE_PURGE_COORD_READY;
        }
        break;
    case NGX_HTTP_CACHE_PURGE_COORD_READY:
        return 0;
    default:
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid coordinator state");
        break;
    }
    if (rc != 0 && rc != NGX_HTTP_CACHE_ROCKSDB_BUSY && c->writer != NULL) {
        ngx_http_cache_purge_control_gate_close(c->writer, NULL);
    }
    return rc;
}

int
ngx_http_cache_purge_coordinator_accept(ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operation_t        preview;
    ngx_http_cache_purge_operation_usage_t  durable;
    ngx_http_cache_purge_control_image_t   *image;
    int                                     rc;

    ngx_http_cache_purge_control_clear(error);
    if (out == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "null coordinator operation output");
    }
    memset(out, 0, sizeof(*out));
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_operations_usage(&c->operations, &durable, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_operations_preview(
        &c->operations, input, now, propagation_ms, &preview, error);
    if (rc != 0) {
        return rc;
    }
    if (preview.sequence <= durable.last_sequence) {
        *out = preview;
        return 0;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "control publication is pending");
        goto done;
    }
    if (c->expected.last_sequence == UINT64_MAX
        || preview.sequence != c->expected.last_sequence + 1)
    {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "operation sequence changed outside coordinator");
        goto done;
    }
    rc = ngx_http_cache_purge_control_controls_reserve(c, c->count + 1, error);
    if (rc != 0) {
        goto done;
    }
    rc = ngx_http_cache_purge_control_control_copy(&c->controls[c->count],
                                                   &preview, error);
    if (rc != 0) {
        goto done;
    }
    c->count++;
    image = NULL;
    rc = ngx_http_cache_purge_control_prepare(c->writer, c->controls, c->count,
                                              &image, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_gate_close(c->writer, error);
    }
    if (rc != 0) {
        ngx_http_cache_purge_operation_free(&c->controls[--c->count]);
        ngx_http_cache_purge_control_image_free(&image);
        goto done;
    }
    rc = ngx_http_cache_purge_operations_accept(&c->operations, input, now,
                                                propagation_ms, out, error);
    if (rc == 0
        && (out->sequence != preview.sequence
            || out->revision != preview.revision))
    {
        rc = ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "accepted operation differs from prepared controls");
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_operations_usage(&c->operations, &c->expected,
                                                   error);
    }
    if (rc != 0) {
        /* A write error is ambiguous; recover with the serving gate closed. */
        ngx_http_cache_purge_control_image_free(&image);
        ngx_http_cache_purge_operation_free(out);
        ngx_http_cache_purge_control_load_begin(c, NULL);
        goto done;
    }

    c->loaded_bytes = c->expected.live_selector_bytes;
    c->pending = image;
    c->state = NGX_HTTP_CACHE_PURGE_COORD_PUBLISHING;

done:
    ngx_http_cache_purge_operation_free(&preview);
    return rc;
}
