/*
 * Copyright (C) Hanada
 */

#include "ngx_http_cache_purge_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/random.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    unsigned char  *data;
    size_t          len;
    size_t          capacity;
} ngx_http_cache_purge_component_buffer_t;

typedef struct {
    const unsigned char             *data;
    size_t                           len;
    size_t                           at;
    int                              rc;
    ngx_http_cache_rocksdb_error_t  *error;
} ngx_http_cache_purge_component_decoder_t;

typedef struct {
    ngx_http_cache_purge_component_buffer_t  buffer;
    size_t                                   limit;
    int                                      rc;
    ngx_http_cache_rocksdb_error_t          *error;
} ngx_http_cache_purge_component_encoder_t;

typedef struct {
    uint16_t  opcode;
    uint64_t  id;
    uint32_t  size;
    uint32_t  result;
} ngx_http_cache_purge_component_frame_t;

typedef struct {
    int     fd;
    int     output_lease;
    int     input_rights[2];
    size_t  rights_count;
    uid_t   uid;

    unsigned char header[NGX_HTTP_CACHE_PURGE_FRAME_SIZE];

    size_t                                   header_used;
    size_t                                   input_used;
    size_t                                   output_used;
    ngx_http_cache_purge_component_buffer_t  input;
    ngx_http_cache_purge_component_buffer_t  output;
    ngx_http_cache_purge_component_frame_t   request;
    int                                      parsed;
    uint64_t                                 deadline;
} ngx_http_cache_purge_component_peer_t;

struct ngx_http_cache_purge_component_s {
    char path[sizeof(((struct sockaddr_un *) 0)->sun_path)];
    char socket_path[sizeof(((struct sockaddr_un *) 0)->sun_path)];
    char lease_path[sizeof(((struct sockaddr_un *) 0)->sun_path)];
    ngx_http_cache_purge_component_options_t  options;
    pid_t                                     pid;
    pthread_t                                 creator;
    pthread_t                                 executor;
    int                                       wake_read;
    int                                       wake_write;
    int                                       listener;
    int                                       lease_anchor;
    int                                       cleanup_read;
    int                                       cleanup_write;
    int                                       cleanup_lease;
    int                                       cleanup_result;
    uint32_t                                  cleanup_state;
    ngx_http_cache_purge_cleanup_t            cleanup;
    ngx_http_cache_purge_component_buffer_t   cleanup_buffer;
    ngx_http_cache_rocksdb_bytes_t            cleanup_operation;
    unsigned char                             cleanup_file[16];
    int                                       socket_owned;
    dev_t                                     socket_device;
    ino_t                                     socket_inode;
    uint32_t                                  stopping;
    uint32_t                                  state;
    uint32_t                                  error_code;
    uint64_t                                  connections;
    uint64_t                                  requests;
    uint64_t                                  rejected;
    uint64_t                                  buffered;
    uint64_t                                  peak;
    uint32_t                                  control_state;
    uint64_t                                  control_publication;
    ngx_http_cache_rocksdb_error_t            terminal_error;
    ngx_http_cache_purge_component_peer_t    *peers;
    ngx_http_cache_purge_root_t              *roots;
    void                                     *root_storage;
    ngx_http_cache_purge_publications_t      *publications;
    ngx_http_cache_purge_coordinator_t       *coordinator;
    ngx_http_cache_purge_operations_t         operations;
    size_t                                    next_peer;
    ngx_http_cache_purge_component_buffer_t   recovery_bookmark;
    uint64_t                                  recovery_at;
    unsigned char                             work_after[17];
    size_t                                    work_after_len;
    uint64_t                                  work_at;
    uint64_t                                  activation_at;
    ngx_http_cache_purge_component_buffer_t   collect_bookmark[2];
    uint64_t                                  collect_at;
    unsigned                                  collect_next, collect_idle;
};

typedef struct {
    ngx_http_cache_purge_component_t    *component;
    ngx_http_cache_purge_coordinator_t  *coordinator;
    ngx_http_cache_purge_operations_t   *operations;
    ngx_http_cache_purge_watermark_t     origin;
} ngx_http_cache_purge_validation_guard_t;

static int ngx_http_cache_purge_component_validation_guard(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error);
static int ngx_http_cache_purge_component_validation_finish_guard(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error);
static int ngx_http_cache_purge_component_publication_guard(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error);
static int ngx_http_cache_purge_component_read_stamp(
    ngx_http_cache_purge_coordinator_t *coordinator,
    const ngx_http_cache_purge_index_file_t *file, uint64_t *publication,
    uint64_t *invalidate,
    ngx_http_cache_rocksdb_error_t *error);
static int ngx_http_cache_purge_coordinator_checkpoint_pin(
    ngx_http_cache_purge_coordinator_t *coordinator,
    const ngx_http_cache_purge_operation_t *operation,
    ngx_http_cache_rocksdb_error_t *error);
static int ngx_http_cache_purge_coordinator_collectible(
    ngx_http_cache_purge_coordinator_t *coordinator,
    const unsigned char id[16], ngx_http_cache_rocksdb_error_t *error);

static int
ngx_http_cache_purge_component_error(ngx_http_cache_rocksdb_error_t *error,
    int code, const char *message)
{
    if (error != NULL) {
        error->code = code;
        snprintf(error->message, sizeof(error->message), "%s", message);
    }
    return code;
}

static int
ngx_http_cache_purge_component_io(ngx_http_cache_rocksdb_error_t *error,
    const char *operation)
{
    int saved;

    saved = errno;
    if (error != NULL) {
        error->code = NGX_HTTP_CACHE_ROCKSDB_IO;
        snprintf(error->message, sizeof(error->message), "%s: errno %d",
                 operation, saved);
    }
    return NGX_HTTP_CACHE_ROCKSDB_IO;
}

static uint32_t
ngx_http_cache_purge_component_load32(const uint32_t *p)
{
    return __atomic_load_n(p, __ATOMIC_SEQ_CST);
}

static uint64_t
ngx_http_cache_purge_component_load64(const uint64_t *p)
{
    return __atomic_load_n(p, __ATOMIC_SEQ_CST);
}

static void
ngx_http_cache_purge_component_store32(uint32_t *p, uint32_t value)
{
    __atomic_store_n(p, value, __ATOMIC_SEQ_CST);
}

static void
ngx_http_cache_purge_component_store64(uint64_t *p, uint64_t value)
{
    __atomic_store_n(p, value, __ATOMIC_SEQ_CST);
}

static void
ngx_http_cache_purge_component_add64(uint64_t *p, uint64_t value)
{
    __atomic_fetch_add(p, value, __ATOMIC_SEQ_CST);
}

static uint64_t
ngx_http_cache_purge_component_now(clockid_t clock)
{
    struct timespec ts;

    if (clock_gettime(clock, &ts) != 0 || ts.tv_sec < 0) {
        return 0;
    }
    return (uint64_t) ts.tv_sec * 1000 + (uint64_t) ts.tv_nsec / 1000000;
}

static int
ngx_http_cache_purge_component_nonblocking(int fd,
    ngx_http_cache_rocksdb_error_t *error)
{
    int flags;

    flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0
        || fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
    {
        return ngx_http_cache_purge_component_io(error,
                                                 "component descriptor flags");
    }
    return 0;
}

static void
ngx_http_cache_purge_component_close(int *fd)
{
    if (*fd >= 0) {
        close(*fd);
        *fd = -1;
    }
}

static void
ngx_http_cache_purge_component_wake(ngx_http_cache_purge_component_t *c)
{
    const char byte = 1;
    ssize_t    rc;

    do {
        rc = write(c->wake_write, &byte, 1);
    } while (rc < 0 && errno == EINTR);
    /* EAGAIN means a wakeup is pending; stopping is independently atomic. */
}

static void
ngx_http_cache_purge_component_clear_wake(ngx_http_cache_purge_component_t *c)
{
    unsigned char data[64];

    while (read(c->wake_read, data, sizeof(data)) > 0) {
        /* void */
    }
}

static void
ngx_http_cache_purge_component_fail(ngx_http_cache_purge_component_t *c,
    const ngx_http_cache_rocksdb_error_t *error)
{
    if (!ngx_http_cache_purge_component_load32(&c->error_code)) {
        c->terminal_error = *error;
        ngx_http_cache_purge_component_store32(&c->error_code,
                                               (uint32_t) error->code);
    }
}

static void
ngx_http_cache_purge_component_buffer_free(
    ngx_http_cache_purge_component_buffer_t *buffer)
{
    free(buffer->data);
    memset(buffer, 0, sizeof(*buffer));
}

static int
ngx_http_cache_purge_component_buffer_size(
    ngx_http_cache_purge_component_buffer_t *buffer, size_t size, size_t limit,
    int grow, ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char  *data;
    size_t          capacity;

    if (size > limit) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "component buffer limit");
    }
    if (size <= buffer->capacity) {
        buffer->len = size;
        return 0;
    }
    capacity = size;
    if (grow) {
        capacity = buffer->capacity ? buffer->capacity : 256;
        while (capacity < size) {
            capacity *= 2;
        }
        if (capacity > limit) {
            capacity = limit;
        }
    }
    data = realloc(buffer->data, capacity);
    if (data == NULL) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "component allocation failed");
    }
    buffer->data = data;
    buffer->capacity = capacity;
    buffer->len = size;
    return 0;
}

static void
ngx_http_cache_purge_component_release(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_purge_component_buffer_t *buffer)
{
    ngx_http_cache_purge_component_store64(
        &c->buffered,
        ngx_http_cache_purge_component_load64(&c->buffered) - buffer->capacity);
    ngx_http_cache_purge_component_buffer_free(buffer);
}

static int
ngx_http_cache_purge_component_input_size(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_purge_component_peer_t *p, size_t size,
    ngx_http_cache_rocksdb_error_t *error)
{
    uint64_t  used;
    int       rc;

    used =
        ngx_http_cache_purge_component_load64(&c->buffered) - p->input.capacity;
    rc = ngx_http_cache_purge_component_buffer_size(
        &p->input, size, c->options.buffer_bytes - used, 0, error);
    if (rc != 0) {
        return rc;
    }
    used += p->input.capacity;
    ngx_http_cache_purge_component_store64(&c->buffered, used);
    if (used > ngx_http_cache_purge_component_load64(&c->peak)) {
        ngx_http_cache_purge_component_store64(&c->peak, used);
    }
    return 0;
}

static void
ngx_http_cache_purge_component_drop(ngx_http_cache_purge_component_t *c,
    size_t i)
{
    ngx_http_cache_purge_component_peer_t  *p;
    size_t                                  n;

    p = &c->peers[i];
    if (p->fd < 0) {
        return;
    }
    ngx_http_cache_purge_component_release(c, &p->input);
    ngx_http_cache_purge_component_release(c, &p->output);
    ngx_http_cache_purge_component_close(&p->fd);
    ngx_http_cache_purge_component_close(&p->output_lease);
    for (n = 0; n < 2; n++) {
        ngx_http_cache_purge_component_close(&p->input_rights[n]);
    }
    memset(p, 0, sizeof(*p));
    p->fd = -1;
    p->output_lease = -1;
    p->input_rights[0] = -1;
    p->input_rights[1] = -1;
    ngx_http_cache_purge_component_store64(
        &c->connections,
        ngx_http_cache_purge_component_load64(&c->connections) - 1);
}

static const unsigned char *
ngx_http_cache_purge_component_take(ngx_http_cache_purge_component_decoder_t *d,
    size_t size)
{
    const unsigned char *value;

    if (d->rc != 0) {
        return NULL;
    }
    if (size > d->len - d->at) {
        d->rc = ngx_http_cache_purge_component_error(
            d->error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "truncated IPC payload");
        return NULL;
    }
    value = d->data + d->at;
    d->at += size;
    return value;
}

static uint64_t
ngx_http_cache_purge_component_integer(
    ngx_http_cache_purge_component_decoder_t *d, unsigned width)
{
    const unsigned char  *p;
    uint64_t              value;

    p = ngx_http_cache_purge_component_take(d, width);
    if (p == NULL) {
        return 0;
    }
    value = 0;
    while (width--) {
        value = (value << 8) | *p++;
    }
    return value;
}

static int
ngx_http_cache_purge_component_flag(ngx_http_cache_purge_component_decoder_t *d)
{
    uint64_t value;

    value = ngx_http_cache_purge_component_integer(d, 1);
    if (d->rc == 0 && value > 1) {
        d->rc = ngx_http_cache_purge_component_error(
            d->error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid IPC boolean");
    }
    return value == 1;
}

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_component_string(
    ngx_http_cache_purge_component_decoder_t *d, size_t limit)
{
    ngx_http_cache_rocksdb_bytes_t  value;
    size_t                          size;

    memset(&value, 0, sizeof(value));
    size = (size_t) ngx_http_cache_purge_component_integer(d, 4);
    if (d->rc != 0) {
        return value;
    }
    if (size > limit) {
        d->rc = ngx_http_cache_purge_component_error(
            d->error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC string exceeds limit");
        return value;
    }
    value.data = ngx_http_cache_purge_component_take(d, size);
    if (value.data != NULL) {
        value.len = size;
    }
    return value;
}

static void
ngx_http_cache_purge_component_id(ngx_http_cache_purge_component_decoder_t *d,
    unsigned char id[16])
{
    const unsigned char *p;

    p = ngx_http_cache_purge_component_take(d, 16);
    if (p != NULL) {
        memcpy(id, p, 16);
    }
}

static int
ngx_http_cache_purge_component_end(ngx_http_cache_purge_component_decoder_t *d)
{
    if (d->rc == 0 && d->at != d->len) {
        d->rc = ngx_http_cache_purge_component_error(
            d->error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "unexpected IPC payload suffix");
    }
    return d->rc;
}

static size_t
ngx_http_cache_purge_component_tags(ngx_http_cache_purge_component_decoder_t *d,
    ngx_http_cache_rocksdb_bytes_t tags[64])
{
    size_t  n;
    size_t  i;

    n = (size_t) ngx_http_cache_purge_component_integer(d, 1);
    if (d->rc != 0) {
        return 0;
    }
    if (n > 64) {
        d->rc = ngx_http_cache_purge_component_error(
            d->error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC TAG count");
        return 0;
    }
    for (i = 0; i < n; i++) {
        tags[i] = ngx_http_cache_purge_component_string(d, 1024);
    }
    return n;
}

static void
ngx_http_cache_purge_component_file_fields(
    ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_bytes_t *fields[10])
{
    fields[0] = &file->key;
    fields[1] = &file->url;
    fields[2] = &file->host;
    fields[3] = &file->tag_namespace;
    fields[4] = &file->root;
    fields[5] = &file->zone;
    fields[6] = &file->path;
    fields[7] = &file->range;
    fields[8] = &file->vary;
    fields[9] = &file->metadata;
}

static void
ngx_http_cache_purge_component_file(ngx_http_cache_purge_component_decoder_t *d,
    ngx_http_cache_purge_index_file_t *file)
{
    ngx_http_cache_rocksdb_bytes_t  *fields[10];
    size_t                           i;

    memset(file, 0, sizeof(*file));
    file->state = NGX_HTTP_CACHE_PURGE_INDEX_PREPARED;
    ngx_http_cache_purge_component_id(d, file->id);
    ngx_http_cache_purge_component_file_fields(file, fields);
    for (i = 0; i < 10; i++) {
        *fields[i] =
            ngx_http_cache_purge_component_string(d, i == 9 ? 16384 : 32768);
    }
    file->tag_count = ngx_http_cache_purge_component_tags(d, file->tags);
    file->structured_key = ngx_http_cache_purge_component_flag(d);
    file->structured_url = ngx_http_cache_purge_component_flag(d);
    for (i = 0; i < 6; i++) {
        file->components[i] = ngx_http_cache_purge_component_string(d, 32768);
    }
}

static void
ngx_http_cache_purge_component_selector(
    ngx_http_cache_purge_component_decoder_t *d,
    ngx_http_cache_purge_selector_t *selector)
{
    selector->kind = (uint32_t) ngx_http_cache_purge_component_integer(d, 1);
    selector->value = ngx_http_cache_purge_component_string(d, 32768);
    selector->scope = ngx_http_cache_purge_component_string(d, 32768);
}

static void
ngx_http_cache_purge_component_operation_input(
    ngx_http_cache_purge_component_decoder_t *d,
    ngx_http_cache_purge_operation_input_t *in)
{
    memset(in, 0, sizeof(*in));
    in->id = ngx_http_cache_purge_component_string(d, 128);
    in->mode = (uint32_t) ngx_http_cache_purge_component_integer(d, 1);
    in->issued_at_ms = ngx_http_cache_purge_component_integer(d, 8);
    ngx_http_cache_purge_component_selector(d, &in->selector);
    in->binding = ngx_http_cache_purge_component_string(d, 256);
}

static unsigned char *
ngx_http_cache_purge_component_put(ngx_http_cache_purge_component_encoder_t *e,
    size_t size)
{
    size_t at;

    if (e->rc != 0) {
        return NULL;
    }
    at = e->buffer.len;
    if (size > e->limit - at) {
        e->rc = ngx_http_cache_purge_component_error(
            e->error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC response size limit");
        return NULL;
    }
    e->rc = ngx_http_cache_purge_component_buffer_size(&e->buffer, at + size,
                                                       e->limit, 1, e->error);
    return e->rc == 0 ? e->buffer.data + at : NULL;
}

static void
ngx_http_cache_purge_component_put_raw(
    ngx_http_cache_purge_component_encoder_t *e, const void *data, size_t size)
{
    unsigned char *p;

    p = ngx_http_cache_purge_component_put(e, size);
    if (p != NULL && size) {
        memcpy(p, data, size);
    }
}

static void
ngx_http_cache_purge_component_put_integer(
    ngx_http_cache_purge_component_encoder_t *e, uint64_t value, unsigned width)
{
    unsigned char *p;

    p = ngx_http_cache_purge_component_put(e, width);
    if (p == NULL) {
        return;
    }
    while (width) {
        *p++ = (unsigned char) (value >> (--width * 8));
    }
}

static void
ngx_http_cache_purge_component_put_string(
    ngx_http_cache_purge_component_encoder_t *e,
    ngx_http_cache_rocksdb_bytes_t value)
{
    ngx_http_cache_purge_component_put_integer(e, value.len, 4);
    ngx_http_cache_purge_component_put_raw(e, value.data, value.len);
}

static void
ngx_http_cache_purge_component_put_match(
    ngx_http_cache_purge_component_encoder_t *e,
    const ngx_http_cache_purge_index_match_t *m)
{
    ngx_http_cache_purge_component_put_raw(e, m->id, 16);
    ngx_http_cache_purge_component_put_integer(e, m->revision, 8);
    ngx_http_cache_purge_component_put_integer(e, m->state, 1);
}

static void
ngx_http_cache_purge_component_put_file(
    ngx_http_cache_purge_component_encoder_t *e,
    ngx_http_cache_purge_index_file_t *file)
{
    ngx_http_cache_rocksdb_bytes_t  *fields[10];
    size_t                           i;

    ngx_http_cache_purge_component_put_raw(e, file->id, 16);
    ngx_http_cache_purge_component_file_fields(file, fields);
    for (i = 0; i < 10; i++) {
        ngx_http_cache_purge_component_put_string(e, *fields[i]);
    }
    ngx_http_cache_purge_component_put_integer(e, file->tag_count, 1);
    for (i = 0; i < file->tag_count; i++) {
        ngx_http_cache_purge_component_put_string(e, file->tags[i]);
    }
    ngx_http_cache_purge_component_put_integer(e, file->structured_key, 1);
    ngx_http_cache_purge_component_put_integer(e, file->structured_url, 1);
    for (i = 0; i < 6; i++) {
        ngx_http_cache_purge_component_put_string(e, file->components[i]);
    }
}

static void
ngx_http_cache_purge_component_put_operation(
    ngx_http_cache_purge_component_encoder_t *e,
    const ngx_http_cache_purge_operation_t *op)
{
    ngx_http_cache_purge_component_put_string(e, op->input.id);
    ngx_http_cache_purge_component_put_integer(e, op->input.mode, 1);
    ngx_http_cache_purge_component_put_integer(e, op->input.issued_at_ms, 8);
    ngx_http_cache_purge_component_put_integer(e, op->input.selector.kind, 1);
    ngx_http_cache_purge_component_put_string(e, op->input.selector.value);
    ngx_http_cache_purge_component_put_string(e, op->input.selector.scope);
    ngx_http_cache_purge_component_put_string(e, op->input.binding);
    ngx_http_cache_purge_component_put_integer(e, op->sequence, 8);
    ngx_http_cache_purge_component_put_integer(e, op->revision, 8);
    ngx_http_cache_purge_component_put_integer(e, op->accepted_at_ms, 8);
    ngx_http_cache_purge_component_put_integer(e, op->effective_at_ms, 8);
    ngx_http_cache_purge_component_put_integer(e, op->propagation_until_ms, 8);
    ngx_http_cache_purge_component_put_integer(e, op->stage, 1);
    ngx_http_cache_purge_component_put_integer(e, op->propagation_ms, 4);
    ngx_http_cache_purge_component_put_integer(e, op->propagate, 1);
    ngx_http_cache_purge_component_put_string(e, op->bookmark);
    ngx_http_cache_purge_component_put_integer(e, op->scan_done, 1);
    ngx_http_cache_purge_component_put_integer(e, op->next_run_ms, 8);
    ngx_http_cache_purge_component_put_integer(e, op->visits, 8);
    ngx_http_cache_purge_component_put_integer(e, op->pending, 8);
    ngx_http_cache_purge_component_put_integer(e, op->failed, 8);
    ngx_http_cache_purge_component_put_integer(e, op->checkpoint_id, 8);
    ngx_http_cache_purge_component_put_string(e, op->last_error);
}

static int
ngx_http_cache_purge_component_logical(ngx_http_cache_rocksdb_t *store,
    const unsigned char id[16], int missing,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_file_t  file;
    int                                rc;

    rc = ngx_http_cache_purge_index_get_file(store, id, &file, error);
    if (rc != 0) {
        return missing && rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND ?
                   ngx_http_cache_purge_component_error(error, 0, "") :
                   rc;
    }
    if (file.metadata.len >= 4
        && (memcmp(file.metadata.data, "NFP1", 4) == 0
            || memcmp(file.metadata.data, "NFV", 3) == 0
            || memcmp(file.metadata.data, "NFR1", 4) == 0))
    {
        rc = ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "physical file publication requires filesystem operations");
    }
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}

static int
ngx_http_cache_purge_component_reserved(ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (metadata.len >= 4
        && (memcmp(metadata.data, "NFP1", 4) == 0
            || memcmp(metadata.data, "NFV", 3) == 0
            || memcmp(metadata.data, "NFR1", 4) == 0))
    {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "reserved physical publication metadata");
    }
    return 0;
}

static int
ngx_http_cache_purge_component_anchor(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (c->lease_anchor < 0) {
        return ngx_http_cache_purge_file_locks_open(c->lease_path, geteuid(), 1,
                                                    &c->lease_anchor, error);
    }
    return 0;
}

static int
ngx_http_cache_purge_component_put_status(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_purge_component_encoder_t *e,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operation_usage_t  usage;
    uint64_t                                publication;
    uint32_t                                state;
    int                                     rc;

    state = 0;
    publication = 0;
    rc = c->coordinator ? ngx_http_cache_purge_coordinator_status(
                      c->coordinator, &state, &publication, error) : 0;
    if (rc == 0) {
        rc = ngx_http_cache_purge_operations_usage(&c->operations, &usage,
                                                     error);
    }
    if (rc == 0) {
        ngx_http_cache_purge_component_put_integer(e, state, 4);
        ngx_http_cache_purge_component_put_integer(e, publication, 8);
        ngx_http_cache_purge_component_put_raw(e, c->operations.ledger, 16);
        ngx_http_cache_purge_component_put_integer(e, usage.last_sequence, 8);
        ngx_http_cache_purge_component_put_integer(e,
                                                  usage.retired_sequence, 8);
        ngx_http_cache_purge_component_put_integer(e, usage.live_records, 8);
        ngx_http_cache_purge_component_put_integer(e,
                                                 usage.live_selector_bytes, 8);
        ngx_http_cache_purge_component_put_integer(e,
                                                 c->options.propagation_ms, 4);
    }
    return rc;
}

static int
ngx_http_cache_purge_component_preserve(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_t      *c;
    ngx_http_cache_purge_index_expected_t  expected;
    size_t                                 i;
    int                                    rc;

    c = data;
    for (i = 0; i < c->options.root_count; i++) {
        if (c->roots[i].kind == NGX_HTTP_CACHE_PURGE_ROOT_PROXY
            && c->roots[i].zone.len == file->zone.len
            && memcmp(c->roots[i].zone.data, file->zone.data,
                      file->zone.len) == 0)
        {
            break;
        }
    }
    if (i == c->options.root_count || c->coordinator == NULL) {
        return 0;
    }
    memcpy(expected.id, file->id, 16);
    expected.revision = file->revision;
    rc = ngx_http_cache_purge_coordinator_keep_file(c->coordinator, &expected,
        ngx_http_cache_purge_component_now(CLOCK_REALTIME), error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
        || rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND)
    {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "retention identity is not ready");
    }
    return rc;
}

static int
ngx_http_cache_purge_component_retire_selected(
    ngx_http_cache_purge_component_t *c, ngx_http_cache_rocksdb_bytes_t root,
    ngx_http_cache_rocksdb_bytes_t zone, ngx_http_cache_rocksdb_bytes_t path,
    ngx_http_cache_rocksdb_bytes_t before, int fd, uint32_t uid,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_target_t  target;
    ngx_http_cache_purge_index_file_t          file;
    ngx_http_cache_purge_index_expected_t      expected;
    unsigned char                              header[4096], digest[16];
    uint64_t                                   offset;
    size_t                                     at;
    ssize_t                                    n;
    int                                        lease, rc;

    memset(&target, 0, sizeof(target));
    memset(&file, 0, sizeof(file));
    lease = -1;
    if (before.len == 0 || before.len > sizeof(header)) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid selected cache header");
    }
    rc = ngx_http_cache_purge_publications_target(c->publications,
        c->operations.store, root, zone, path, &target, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_address(target.address, digest,
                                                 &offset, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_anchor(c, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_lease_acquire(c->lease_path,
            c->lease_anchor, offset, &lease, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publications_inspect(c->publications,
            c->operations.store, root, zone, path, fd, uid, &file, error);
    }
    at = 0;
    while (rc == 0 && at < before.len) {
        n = pread(fd, header + at, before.len - at, (off_t) at);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            rc = ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_PURGE_FILE_ERROR, "read selected cache header");
        } else if (n == 0) {
            rc = ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "selected cache was truncated");
        } else {
            at += (size_t) n;
        }
    }
    if (rc == 0 && memcmp(header, before.data, before.len) != 0) {
        rc = ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "selected cache header changed");
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_preserve(c, &file, error);
    }
    if (rc == 0) {
        memcpy(expected.id, file.id, 16);
        expected.revision = file.revision;
        rc = ngx_http_cache_purge_index_apply(c->operations.store,
            NGX_HTTP_CACHE_PURGE_INDEX_RETIRE, &expected, 1, out, error);
    }
    if (rc == 0) {
        c->collect_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
    }
    ngx_http_cache_purge_component_close(&lease);
    ngx_http_cache_purge_index_file_free(&file);
    ngx_http_cache_purge_publication_target_free(&target);
    return rc;
}

static int
ngx_http_cache_purge_component_dispatch(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_purge_component_peer_t *p,
    ngx_http_cache_purge_component_encoder_t *e,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_decoder_t  d;
    ngx_http_cache_purge_index_file_t         file;
    ngx_http_cache_purge_index_fill_t        *fills;
    ngx_http_cache_purge_index_match_t        matches[32];
    ngx_http_cache_purge_index_expected_t     expected[32];
    ngx_http_cache_purge_index_action_e       action;
    ngx_http_cache_purge_publication_target_t  target;
    ngx_http_cache_purge_validation_t          validation;
    ngx_http_cache_purge_validation_guard_t    validation_guard;
    ngx_http_cache_purge_watermark_t           watermark;
    ngx_http_cache_purge_fence_t               propagation;
    ngx_http_cache_purge_control_context_t     propagation_request;
    ngx_http_cache_purge_operation_input_t     input;
    ngx_http_cache_purge_operation_t           operation;
    ngx_http_cache_purge_selector_t            selector;
    ngx_http_cache_purge_query_t              *query;
    ngx_http_cache_rocksdb_stats_t             stats;
    ngx_http_cache_rocksdb_bytes_t             root;
    ngx_http_cache_rocksdb_bytes_t             zone;
    ngx_http_cache_rocksdb_bytes_t             name;
    ngx_http_cache_rocksdb_bytes_t             temporary;
    ngx_http_cache_rocksdb_bytes_t             address;
    ngx_http_cache_rocksdb_bytes_t             id;
    ngx_http_cache_rocksdb_bytes_t             bookmark;
    ngx_http_cache_rocksdb_bytes_t             key;
    ngx_http_cache_rocksdb_bytes_t             url;
    ngx_http_cache_rocksdb_bytes_t             host;
    ngx_http_cache_rocksdb_bytes_t             metadata;
    ngx_http_cache_rocksdb_bytes_t             tags[64];
    unsigned char                              digest[16];
    uint64_t                                   offset;
    uint64_t                                   now;
    uint64_t                                   publication;
    uint64_t                                   invalidate;
    size_t                                     count;
    size_t                                     i;
    size_t                                     limit;
    size_t                                     removed;
    uint32_t                                   mode;
    uint32_t                                   state;
    int                                        rc;
    int                                        done;

    memset(&d, 0, sizeof(d));
    d.data = p->input.data ? p->input.data : (const unsigned char *) "";
    d.len = p->input.len;
    d.error = error;
    memset(&file, 0, sizeof(file));
    memset(&operation, 0, sizeof(operation));
    memset(&target, 0, sizeof(target));
    memset(expected, 0, sizeof(expected));
    fills = NULL;
    query = NULL;
    rc = 0;

    switch (p->request.opcode) {
    case NGX_HTTP_CACHE_PURGE_OP_FILE_PROPAGATION:
        ngx_http_cache_purge_component_id(&d, expected[0].id);
        expected[0].revision = ngx_http_cache_purge_component_integer(&d, 8);
        memset(&propagation_request, 0, sizeof(propagation_request));
        propagation_request.key =
            ngx_http_cache_purge_component_string(&d, 32768);
        propagation_request.url =
            ngx_http_cache_purge_component_string(&d, 32768);
        propagation_request.host =
            ngx_http_cache_purge_component_string(&d, 32768);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        if (propagation_request.key.len == 0
            && (propagation_request.url.len || propagation_request.host.len))
        {
            rc = ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "propagation request has no key");
            break;
        }
        if (c->coordinator == NULL) {
            rc = ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_BUSY, "propagation has no coordinator");
            break;
        }
        now = ngx_http_cache_purge_component_now(CLOCK_REALTIME);
        rc = ngx_http_cache_purge_coordinator_file_propagation(c->coordinator,
            &expected[0], propagation_request.key.len ? &propagation_request
                                                     : NULL,
            now, &propagation, &publication, error);
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e,
                propagation.invalidate_ms, 8);
            ngx_http_cache_purge_component_put_integer(e,
                propagation.delete_ms, 8);
            ngx_http_cache_purge_component_put_integer(e, publication, 8);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_RETIRE_SELECTED:
        root = ngx_http_cache_purge_component_string(&d, 32768);
        zone = ngx_http_cache_purge_component_string(&d, 32768);
        name = ngx_http_cache_purge_component_string(&d, 32768);
        metadata = ngx_http_cache_purge_component_string(&d, 4096);
        if ((rc = ngx_http_cache_purge_component_end(&d)) == 0) {
            rc = ngx_http_cache_purge_component_retire_selected(c,
                root, zone, name, metadata, p->input_rights[0], p->uid,
                &matches[0], error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_match(e, &matches[0]);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_REMOVE_FILE:
        ngx_http_cache_purge_component_id(&d, expected[0].id);
        expected[0].revision = ngx_http_cache_purge_component_integer(&d, 8);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_component_anchor(c, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_publications_remove_checked(
                c->publications,
                store, &expected[0], c->lease_anchor, p->input_rights[0],
                p->uid, ngx_http_cache_purge_component_preserve, c,
                &state, &matches[0], error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, state, 4);
            ngx_http_cache_purge_component_put_match(e, &matches[0]);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_GET_WRITERS:
        now = ngx_http_cache_purge_component_integer(&d, 8);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_coordinator_writers(c->coordinator, now,
                                                        &state, error);
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, state, 4);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_VALIDATE_FILE:
        memset(&validation, 0, sizeof(validation));
        ngx_http_cache_purge_component_id(&d, validation.file.id);
        validation.file.revision =
            ngx_http_cache_purge_component_integer(&d, 8);
        ngx_http_cache_purge_component_id(&d, validation.transaction);
        validation.before = ngx_http_cache_purge_component_string(&d, 4096);
        validation.after = ngx_http_cache_purge_component_string(&d, 4096);
        validation.replace_tags =
            (int) ngx_http_cache_purge_component_integer(&d, 1);
        validation.tag_count = ngx_http_cache_purge_component_integer(&d, 1);
        validation.tags = tags;
        if (validation.tag_count > 64 || validation.replace_tags > 1
            || (!validation.replace_tags && validation.tag_count))
        {
            rc = ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid validation TAG mode");
            break;
        }
        for (i = 0; i < validation.tag_count; i++) {
            tags[i] = ngx_http_cache_purge_component_string(&d, 1024);
        }
        ngx_http_cache_purge_component_id(&d, validation_guard.origin.ledger);
        validation_guard.origin.sequence =
            ngx_http_cache_purge_component_integer(&d, 8);
        validation.origin = &validation_guard.origin;
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        validation_guard.coordinator = c->coordinator;
        validation_guard.component = c;
        validation_guard.operations = &c->operations;
        rc = ngx_http_cache_purge_component_anchor(c, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_publications_validate_prepare(
                c->publications, store, &validation, c->lease_anchor,
                p->input_rights[0], p->input_rights[1], p->uid,
                ngx_http_cache_purge_component_validation_guard,
                &validation_guard, &matches[0], error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_publications_validate_finish_checked(
                c->publications, store, validation.file.id,
                validation.transaction, validation.file.revision,
                c->lease_anchor, p->input_rights[0], p->uid,
                ngx_http_cache_purge_component_validation_finish_guard, c,
                &matches[0], error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_index_get_file(store, matches[0].id,
                                                      &file, error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_component_read_stamp(c->coordinator,
                &file, &publication, &invalidate, error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
                || rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
                || rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT)
            {
                /* The durable receipt survives a later read rejection.
                 * A zero stamp cannot authorize native response admission. */
                rc = ngx_http_cache_purge_component_error(error, 0, "");
            }
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, file.state, 1);
            ngx_http_cache_purge_component_put_integer(e, file.revision, 8);
            ngx_http_cache_purge_component_put_file(e, &file);
            ngx_http_cache_purge_component_put_integer(e, publication, 8);
            ngx_http_cache_purge_component_put_integer(e, invalidate, 8);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_ADMIT_FILE:
    case NGX_HTTP_CACHE_PURGE_OP_INSPECT_FILE:
        root = ngx_http_cache_purge_component_string(&d, 32768);
        zone = ngx_http_cache_purge_component_string(&d, 32768);
        name = ngx_http_cache_purge_component_string(&d, 32768);
        if (p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_ADMIT_FILE) {
            key = ngx_http_cache_purge_component_string(&d, 32768);
            url = ngx_http_cache_purge_component_string(&d, 32768);
            host = ngx_http_cache_purge_component_string(&d, 32768);
        }
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_publications_inspect(
            c->publications, store, root, zone, name, p->input_rights[0],
            p->uid, &file, error);
        if (rc == 0
            && p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_ADMIT_FILE)
        {
            if (key.len == 0 || url.len == 0 || host.len == 0
                || key.len != file.key.len
                || memcmp(key.data, file.key.data, key.len))
            {
                rc = ngx_http_cache_purge_component_error(error,
                    NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                    "read key or URL does not identify the selected file");
            } else if (c->coordinator == NULL) {
                rc = ngx_http_cache_purge_component_error(error,
                    NGX_HTTP_CACHE_ROCKSDB_BUSY,
                    "cache admission requires a control coordinator");
            } else {
                rc = ngx_http_cache_purge_coordinator_status(c->coordinator,
                    &state, &publication, error);
                if (rc == 0 && state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
                    rc = ngx_http_cache_purge_component_error(error,
                        NGX_HTTP_CACHE_ROCKSDB_BUSY,
                        "cache admission awaits control publication");
                }
            }
            if (rc == 0) {
                rc = ngx_http_cache_purge_index_observe(store, key, url,
                                                         host, error);
            }
            if (rc == 0) {
                rc = ngx_http_cache_purge_component_read_stamp(c->coordinator,
                    &file, &publication, &invalidate, error);
            }
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, file.state, 1);
            ngx_http_cache_purge_component_put_integer(e, file.revision, 8);
            ngx_http_cache_purge_component_put_file(e, &file);
            if (p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_ADMIT_FILE) {
                ngx_http_cache_purge_component_put_integer(e, publication, 8);
                ngx_http_cache_purge_component_put_integer(e, invalidate, 8);
            }
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION:
        root = ngx_http_cache_purge_component_string(&d, 32768);
        zone = ngx_http_cache_purge_component_string(&d, 32768);
        name = ngx_http_cache_purge_component_string(&d, 32768);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_publications_target(
            c->publications, store, root, zone, name, &target, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_file_address(target.address, digest,
                                                   &offset, error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_component_anchor(c, error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_file_lease_acquire(
                c->lease_path, c->lease_anchor, offset, &p->output_lease,
                error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_string(e, target.root);
            ngx_http_cache_purge_component_put_raw(e, target.previous, 16);
            ngx_http_cache_purge_component_put_raw(e, digest, 16);
            ngx_http_cache_purge_component_put_integer(e, offset, 8);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_FINISH_FILE:
        ngx_http_cache_purge_component_id(&d, expected[0].id);
        expected[0].revision = ngx_http_cache_purge_component_integer(&d, 8);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_component_anchor(c, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_publications_finish_checked(
                c->publications, store, expected[0].id, expected[0].revision,
                c->lease_anchor, p->input_rights[0], p->uid,
                ngx_http_cache_purge_component_publication_guard,
                c, &matches[0], error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_match(e, &matches[0]);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_PREPARE_FILE:
        fills = calloc(1, sizeof(*fills));
        if (fills == NULL) {
            rc = ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "component allocation failed");
            break;
        }
        ngx_http_cache_purge_component_file(&d, &fills[0].file);
        ngx_http_cache_purge_component_id(&d, fills[0].previous);
        temporary = ngx_http_cache_purge_component_string(&d, 32768);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_watermark_decode(fills[0].file.metadata,
                                                    &watermark, error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            rc = ngx_http_cache_purge_operations_origin(&c->operations,
                                                          NULL, error);
        } else if (rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT) {
            rc = ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "malformed origin watermark");
        } else if (rc == 0) {
            rc = ngx_http_cache_purge_operations_origin(&c->operations,
                                                          &watermark, error);
        }
        if (rc != 0) {
            break;
        }
        rc = ngx_http_cache_purge_component_anchor(c, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_publications_prepare(
                c->publications, store, &fills[0], temporary, c->lease_anchor,
                p->input_rights[0], p->input_rights[1], p->uid, &matches[0],
                error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_match(e, &matches[0]);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_FILE_LEASE:
        address = ngx_http_cache_purge_component_string(&d, 65536);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_file_address(address, digest, &offset, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_component_anchor(c, error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_file_lease_acquire(
                c->lease_path, c->lease_anchor, offset, &p->output_lease,
                error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_raw(e, digest, 16);
            ngx_http_cache_purge_component_put_integer(e, offset, 8);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_PING:
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_rocksdb_stats(store, &stats, error);
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, (uint64_t) c->pid, 8);
            ngx_http_cache_purge_component_put_integer(e, stats.writes, 8);
            ngx_http_cache_purge_component_put_integer(e, stats.cache_usage, 8);
            ngx_http_cache_purge_component_put_integer(
                e, stats.write_buffer_usage, 8);
            ngx_http_cache_purge_component_put_integer(
                e, ngx_http_cache_purge_component_load64(&c->connections), 8);
            ngx_http_cache_purge_component_put_integer(
                e, ngx_http_cache_purge_component_load64(&c->requests), 8);
            ngx_http_cache_purge_component_put_integer(
                e, ngx_http_cache_purge_component_load64(&c->rejected), 8);
            ngx_http_cache_purge_component_put_integer(
                e, ngx_http_cache_purge_component_load64(&c->buffered), 8);
            ngx_http_cache_purge_component_put_integer(
                e, ngx_http_cache_purge_component_load64(&c->peak), 8);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_GET:
        ngx_http_cache_purge_component_id(&d, expected[0].id);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_index_get_file(store, expected[0].id, &file,
                                                 error);
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, file.state, 1);
            ngx_http_cache_purge_component_put_integer(e, file.revision, 8);
            ngx_http_cache_purge_component_put_file(e, &file);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_PREPARE:
        count = (size_t) ngx_http_cache_purge_component_integer(&d, 1);
        if (d.rc != 0) {
            rc = d.rc;
            break;
        }
        if (count == 0 || count > 32) {
            rc = ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC prepare batch count");
            break;
        }
        fills = calloc(count, sizeof(*fills));
        if (fills == NULL) {
            rc = ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "component allocation failed");
            break;
        }
        for (i = 0; i < count && d.rc == 0; i++) {
            ngx_http_cache_purge_component_file(&d, &fills[i].file);
            ngx_http_cache_purge_component_id(&d, fills[i].previous);
        }
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        for (i = 0; i < count; i++) {
            rc = ngx_http_cache_purge_component_reserved(fills[i].file.metadata,
                                                         error);
            if (rc != 0) {
                break;
            }
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_index_prepare_batch(store, fills, count,
                                                          matches, error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, count, 1);
            for (i = 0; i < count; i++) {
                ngx_http_cache_purge_component_put_match(e, &matches[i]);
            }
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_COMMIT:
    case NGX_HTTP_CACHE_PURGE_OP_RETIRE:
    case NGX_HTTP_CACHE_PURGE_OP_COLLECT:
        count = (size_t) ngx_http_cache_purge_component_integer(&d, 1);
        if (d.rc != 0) {
            rc = d.rc;
            break;
        }
        if (count == 0 || count > 32) {
            rc = ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC batch count");
            break;
        }
        for (i = 0; i < count; i++) {
            ngx_http_cache_purge_component_id(&d, expected[i].id);
            expected[i].revision =
                ngx_http_cache_purge_component_integer(&d, 8);
        }
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        for (i = 0; i < count; i++) {
            rc = ngx_http_cache_purge_component_logical(
                store, expected[i].id,
                p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_COLLECT, error);
            if (rc != 0) {
                break;
            }
        }
        if (rc != 0) {
            break;
        }
        if (p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_COLLECT) {
            for (i = 0; i < count; i++) {
                if (ngx_http_cache_purge_component_load32(&c->cleanup_state)
                    && memcmp(expected[i].id, c->cleanup_file, 16) == 0)
                {
                    rc = ngx_http_cache_purge_component_error(error,
                        NGX_HTTP_CACHE_ROCKSDB_BUSY,
                        "file awaits native cleanup acknowledgement");
                } else if (c->coordinator != NULL) {
                    rc = ngx_http_cache_purge_coordinator_collectible(
                        c->coordinator, expected[i].id, error);
                }
                if (rc != 0) {
                    break;
                }
            }
            if (rc != 0) {
                break;
            }
            rc = ngx_http_cache_purge_index_collect(store, expected, count,
                                                    error);
            break;
        }
        action = p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_COMMIT ?
                     NGX_HTTP_CACHE_PURGE_INDEX_COMMIT :
                     NGX_HTTP_CACHE_PURGE_INDEX_RETIRE;
        rc = ngx_http_cache_purge_index_apply(store, action, expected, count,
                                              matches, error);
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, count, 1);
            for (i = 0; i < count; i++) {
                ngx_http_cache_purge_component_put_match(e, &matches[i]);
            }
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_ABORT:
        ngx_http_cache_purge_component_id(&d, expected[0].id);
        expected[0].revision = ngx_http_cache_purge_component_integer(&d, 8);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_component_logical(store, expected[0].id, 0,
                                                    error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_index_apply(
                store, NGX_HTTP_CACHE_PURGE_INDEX_ABORT, expected, 1, matches,
                error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_match(e, &matches[0]);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_QUERY:
    case NGX_HTTP_CACHE_PURGE_OP_PENDING:
    case NGX_HTTP_CACHE_PURGE_OP_RETIRED:
        memset(&selector, 0, sizeof(selector));
        if (p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_QUERY) {
            ngx_http_cache_purge_component_selector(&d, &selector);
        }
        bookmark = ngx_http_cache_purge_component_string(&d, 33024);
        limit = (size_t) ngx_http_cache_purge_component_integer(&d, 1);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        if (limit == 0 || limit > 32) {
            rc = ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC query page limit");
            break;
        }
        mode = p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_QUERY ?
                   NGX_HTTP_CACHE_PURGE_QUERY_SELECT :
               p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_PENDING ?
                   NGX_HTTP_CACHE_PURGE_QUERY_PENDING :
                   NGX_HTTP_CACHE_PURGE_QUERY_RETIRED;
        rc = ngx_http_cache_purge_query_open(
            store, mode == NGX_HTTP_CACHE_PURGE_QUERY_SELECT ? &selector : NULL,
            mode, bookmark, &query, error);
        if (rc != 0) {
            break;
        }
        count = 0;
        done = 0;
        while (count < limit) {
            rc = ngx_http_cache_purge_query_next(query, &matches[count], error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
                done = 1;
                rc = 0;
                break;
            }
            if (rc != 0) {
                if (rc == NGX_HTTP_CACHE_ROCKSDB_EXPIRED && count != 0) {
                    rc = 0;
                }
                break;
            }
            count++;
            bookmark = ngx_http_cache_purge_query_bookmark(query);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, count, 1);
            ngx_http_cache_purge_component_put_integer(e, done, 1);
            ngx_http_cache_purge_component_put_string(e, bookmark);
            for (i = 0; i < count; i++) {
                ngx_http_cache_purge_component_put_match(e, &matches[i]);
            }
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_COLLECT_KEYS:
        limit = (size_t) ngx_http_cache_purge_component_integer(&d, 1);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_index_collect_keys(store, limit, &removed,
                                                     error);
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, removed, 4);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_ADD_URL:
        key = ngx_http_cache_purge_component_string(&d, 32768);
        url = ngx_http_cache_purge_component_string(&d, 32768);
        if ((rc = ngx_http_cache_purge_component_end(&d)) == 0) {
            rc = ngx_http_cache_purge_index_add_url(store, key, url, error);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_UPDATE:
        ngx_http_cache_purge_component_id(&d, expected[0].id);
        expected[0].revision = ngx_http_cache_purge_component_integer(&d, 8);
        metadata = ngx_http_cache_purge_component_string(&d, 16384);
        count = ngx_http_cache_purge_component_tags(&d, tags);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        rc = ngx_http_cache_purge_component_logical(store, expected[0].id, 0,
                                                    error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_component_reserved(metadata, error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_index_update(store, expected, metadata,
                                                   tags, count, matches, error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_match(e, &matches[0]);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_ACCEPT_OPERATION:
    case NGX_HTTP_CACHE_PURGE_OP_SUBMIT_OPERATION:
        ngx_http_cache_purge_component_operation_input(&d, &input);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        now = ngx_http_cache_purge_component_now(CLOCK_REALTIME);
        if (now == 0) {
            rc = ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_IO,
                "invalid component wall clock");
            break;
        }
        if (p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_SUBMIT_OPERATION) {
            rc = ngx_http_cache_purge_coordinator_submit(c->coordinator,
                &input, now, c->options.propagation_ms, &operation, error);
            if (rc == 0 && operation.stage == NGX_HTTP_CACHE_PURGE_PREPARING) {
                c->activation_at = 0;
            }
        } else {
            rc = c->coordinator ?
                 ngx_http_cache_purge_coordinator_accept(
                     c->coordinator, &input, now, c->options.propagation_ms,
                     &operation, error) :
                 ngx_http_cache_purge_operations_accept(
                     &c->operations, &input, now, c->options.propagation_ms,
                     &operation, error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_operation(e, &operation);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_GET_OPERATION:
    case NGX_HTTP_CACHE_PURGE_OP_GET_RECEIPT:
        id = ngx_http_cache_purge_component_string(&d, 128);
        if ((rc = ngx_http_cache_purge_component_end(&d)) == 0) {
            rc = ngx_http_cache_purge_operations_get(&c->operations, id,
                                                     &operation, error);
        }
        if (rc == 0
            && p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_GET_RECEIPT)
        {
            rc = ngx_http_cache_purge_component_put_status(c, e, error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_operation(e, &operation);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_ACTIVATE_OPERATION:
        id = ngx_http_cache_purge_component_string(&d, 128);
        rc = ngx_http_cache_purge_component_end(&d);
        if (rc == 0) {
            now = ngx_http_cache_purge_component_now(CLOCK_REALTIME);
            rc = ngx_http_cache_purge_coordinator_activate(c->coordinator,
                                              id, now, &operation, error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_operation(e, &operation);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_GET_CONTROL_STATUS:
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            break;
        }
        state = 0;
        publication = 0;
        if (c->coordinator) {
            rc = ngx_http_cache_purge_coordinator_status(c->coordinator, &state,
                                                         &publication, error);
        }
        if (rc == 0) {
            ngx_http_cache_purge_component_put_integer(e, state, 4);
            ngx_http_cache_purge_component_put_integer(e, publication, 8);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OP_GET_RUNTIME_STATUS:
        rc = ngx_http_cache_purge_component_end(&d);
        if (rc == 0) {
            rc = ngx_http_cache_purge_component_put_status(c, e, error);
        }
        break;

    default:
        rc = ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "unknown component operation");
        break;
    }

    free(fills);
    ngx_http_cache_purge_index_file_free(&file);
    ngx_http_cache_purge_operation_free(&operation);
    ngx_http_cache_purge_publication_target_free(&target);
    if (query != NULL) {
        ngx_http_cache_purge_query_close(&query, NULL);
    }
    return rc == 0 ? e->rc : rc;
}

static int
ngx_http_cache_purge_component_execute(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_purge_component_peer_t *p,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_encoder_t  e;
    ngx_http_cache_rocksdb_bytes_t            message;
    unsigned char                            *data;
    char                                     text[sizeof(error->message)];
    uint64_t  used;
    size_t    i;
    size_t    length;
    uint32_t  code;
    int       rc;

    memset(&e, 0, sizeof(e));
    e.limit = c->options.max_payload_bytes + NGX_HTTP_CACHE_PURGE_FRAME_SIZE;
    e.error = error;
    ngx_http_cache_purge_component_put(&e, NGX_HTTP_CACHE_PURGE_FRAME_SIZE);
    if (e.rc != 0) {
        ngx_http_cache_purge_component_buffer_free(&e.buffer);
        return e.rc;
    }
    ngx_http_cache_purge_component_add64(&c->requests, 1);
    rc = ngx_http_cache_purge_component_dispatch(c, store, p, &e, error);
    code = (uint32_t) rc;
    if (rc != 0) {
        ngx_http_cache_purge_component_close(&p->output_lease);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT
            || rc == NGX_HTTP_CACHE_ROCKSDB_SCHEMA
            || rc == NGX_HTTP_CACHE_ROCKSDB_IO)
        {
            ngx_http_cache_purge_component_buffer_free(&e.buffer);
            return rc;
        }
        snprintf(text, sizeof(text), "%s", error->message);
        e.rc = 0;
        e.buffer.len = NGX_HTTP_CACHE_PURGE_FRAME_SIZE;
        message.data = (const unsigned char *) text;
        message.len = strlen(text);
        ngx_http_cache_purge_component_put_string(&e, message);
    }
    ngx_http_cache_purge_component_release(c, &p->input);
    for (i = 0; i < 2; i++) {
        ngx_http_cache_purge_component_close(&p->input_rights[i]);
    }
    p->rights_count = 0;
    if (e.rc != 0) {
        rc = e.rc;
        ngx_http_cache_purge_component_buffer_free(&e.buffer);
        return rc;
    }

    /* Frame in place: no additional payload-sized packet copy. */
    length = e.buffer.len;
    e.buffer.len = 0;
    ngx_http_cache_purge_component_put_raw(&e, "NCP1", 4);
    ngx_http_cache_purge_component_put_integer(
        &e, NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION, 2);
    ngx_http_cache_purge_component_put_integer(&e, p->request.opcode, 2);
    ngx_http_cache_purge_component_put_integer(&e, p->request.id, 8);
    ngx_http_cache_purge_component_put_integer(
        &e, length - NGX_HTTP_CACHE_PURGE_FRAME_SIZE, 4);
    ngx_http_cache_purge_component_put_integer(&e, code, 4);
    e.buffer.len = length;

    used = ngx_http_cache_purge_component_load64(&c->buffered);
    if (length > c->options.buffer_bytes - used) {
        ngx_http_cache_purge_component_buffer_free(&e.buffer);
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "component input/output budget");
    }
    if (e.buffer.capacity > c->options.buffer_bytes - used) {
        data = realloc(e.buffer.data, length);
        if (data == NULL) {
            ngx_http_cache_purge_component_buffer_free(&e.buffer);
            return ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "component allocation failed");
        }
        e.buffer.data = data;
        e.buffer.capacity = length;
    }
    p->output = e.buffer;
    used += p->output.capacity;
    ngx_http_cache_purge_component_store64(&c->buffered, used);
    if (used > ngx_http_cache_purge_component_load64(&c->peak)) {
        ngx_http_cache_purge_component_store64(&c->peak, used);
    }
    p->deadline = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC)
                  + c->options.io_timeout_ms;
    return 0;
}

static int
ngx_http_cache_purge_component_bind(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    struct stat         st;
    struct sockaddr_un  address;
    int                 rc;

    if (lstat(c->socket_path, &st) == 0) {
        if (!S_ISSOCK(st.st_mode)
            || (st.st_uid != geteuid() && st.st_uid != c->options.client_uid))
        {
            return ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "component socket path is not an owned socket");
        }
        if (unlink(c->socket_path) < 0) {
            return ngx_http_cache_purge_component_io(
                error, "remove stale component socket");
        }
    } else if (errno != ENOENT) {
        return ngx_http_cache_purge_component_io(error,
                                                 "inspect component socket");
    }
    c->listener = socket(AF_UNIX, SOCK_STREAM, 0);
    if (c->listener < 0) {
        return ngx_http_cache_purge_component_io(error, "component socket");
    }
    rc = ngx_http_cache_purge_component_nonblocking(c->listener, error);
    if (rc != 0) {
        return rc;
    }
    memset(&address, 0, sizeof(address));
    address.sun_family = AF_UNIX;
    memcpy(address.sun_path, c->socket_path, strlen(c->socket_path) + 1);
    if (bind(c->listener, (struct sockaddr *) &address, sizeof(address)) < 0) {
        return ngx_http_cache_purge_component_io(error,
                                                 "bind component socket");
    }
    if (lstat(c->socket_path, &st) < 0) {
        return ngx_http_cache_purge_component_io(
            error, "inspect bound component socket");
    }
    c->socket_owned = 1;
    c->socket_device = st.st_dev;
    c->socket_inode = st.st_ino;
    if (c->options.client_uid != geteuid()
        && lchown(c->socket_path, (uid_t) c->options.client_uid, (gid_t) -1)
               < 0)
    {
        return ngx_http_cache_purge_component_io(
            error, "set component socket client owner");
    }
    if (chmod(c->socket_path, 0600) < 0
        || listen(c->listener, (int) c->options.max_connections) < 0)
    {
        return ngx_http_cache_purge_component_io(error,
                                                 "listen component socket");
    }
    return 0;
}

static void
ngx_http_cache_purge_component_remove_socket(
    ngx_http_cache_purge_component_t *c)
{
    struct stat st;

    ngx_http_cache_purge_component_close(&c->listener);
    if (!c->socket_owned) {
        return;
    }
    if (lstat(c->socket_path, &st) == 0 && S_ISSOCK(st.st_mode)
        && st.st_dev == c->socket_device && st.st_ino == c->socket_inode)
    {
        unlink(c->socket_path);
    }
    c->socket_owned = 0;
}

static int
ngx_http_cache_purge_component_accept_peers(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    unsigned                                accepted;
    size_t                                  slot;
    int                                     fd;
    int                                     rc;
    struct ucred                            credentials;
    socklen_t                               length;
    ngx_http_cache_purge_component_peer_t  *p;

    for (accepted = 0; accepted != 4; accepted++) {
        fd = accept(c->listener, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;
            }
            if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS
                || errno == ENOMEM)
            {
                ngx_http_cache_purge_component_add64(&c->rejected, 1);
                return 0;
            }
            return ngx_http_cache_purge_component_io(error,
                                                     "accept component peer");
        }
        for (slot = 0; slot < c->options.max_connections; slot++) {
            if (c->peers[slot].fd < 0) {
                break;
            }
        }
        length = sizeof(credentials);
        if (slot == c->options.max_connections
            || getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credentials, &length)
                   != 0
            || length != sizeof(credentials)
            || (credentials.uid != geteuid()
                && credentials.uid != c->options.client_uid))
        {
            ngx_http_cache_purge_component_add64(&c->rejected, 1);
            close(fd);
            continue;
        }
        rc = ngx_http_cache_purge_component_nonblocking(fd, error);
        if (rc != 0) {
            close(fd);
            return rc;
        }
        p = &c->peers[slot];
        p->fd = fd;
        p->uid = credentials.uid;
        p->deadline = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC)
                      + c->options.io_timeout_ms;
        ngx_http_cache_purge_component_add64(&c->connections, 1);
    }
    return 0;
}

static int
ngx_http_cache_purge_component_read(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_purge_component_peer_t *p, int *alive,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_decoder_t  d;
    const unsigned char                      *magic;
    unsigned char                            *destination;
    size_t                                    budget;
    size_t                                    remaining;
    size_t                                    count;
    size_t                                    i;
    size_t                                    expected;

    int rights[NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS];

    int      first;
    int      rc;
    ssize_t  n;

    budget = 65536;
    *alive = 1;
    while (budget != 0) {
        remaining = p->parsed ? p->input.len - p->input_used :
                                sizeof(p->header) - p->header_used;
        if (remaining == 0) {
            return 0;
        }
        destination = p->parsed ? p->input.data + p->input_used :
                                  p->header + p->header_used;
        first = !p->parsed && p->header_used == 0;
        n = ngx_http_cache_purge_transport_recv(
            p->fd, destination,
            first ? 1 : (budget < remaining ? budget : remaining), first,
            rights, &count);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            *alive = errno == EAGAIN || errno == EWOULDBLOCK;
            return 0;
        }
        if (n == 0) {
            *alive = 0;
            return 0;
        }
        if (count != 0) {
            for (i = 0; i < count; i++) {
                p->input_rights[i] = rights[i];
            }
            p->rights_count = count;
        }
        budget -= (size_t) n;
        if (p->parsed) {
            p->input_used += (size_t) n;
            continue;
        }
        p->header_used += (size_t) n;
        if (p->header_used != sizeof(p->header)) {
            continue;
        }
        memset(&d, 0, sizeof(d));
        d.data = p->header;
        d.len = sizeof(p->header);
        d.error = error;
        magic = ngx_http_cache_purge_component_take(&d, 4);
        if (magic == NULL || memcmp(magic, "NCP1", 4)
            || ngx_http_cache_purge_component_integer(&d, 2)
                   != NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION)
        {
            return ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "unknown IPC protocol");
        }
        p->request.opcode =
            (uint16_t) ngx_http_cache_purge_component_integer(&d, 2);
        p->request.id = ngx_http_cache_purge_component_integer(&d, 8);
        p->request.size =
            (uint32_t) ngx_http_cache_purge_component_integer(&d, 4);
        p->request.result =
            (uint32_t) ngx_http_cache_purge_component_integer(&d, 4);
        if ((rc = ngx_http_cache_purge_component_end(&d)) != 0) {
            return rc;
        }
        if (p->request.result || p->request.size > c->options.max_payload_bytes)
        {
            return ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "invalid component request size/result");
        }
        expected =
            p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_VALIDATE_FILE ? 2 :
            p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_PREPARE_FILE ? 2 :
            p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_FINISH_FILE  ? 1 :
            p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_REMOVE_FILE  ? 1 :
            p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_INSPECT_FILE ? 1 :
            p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_ADMIT_FILE   ? 1 :
            p->request.opcode == NGX_HTTP_CACHE_PURGE_OP_RETIRE_SELECTED ? 1 :
                                                                        0;
        if (p->rights_count != expected) {
            return ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "unexpected component request descriptors");
        }
        rc = ngx_http_cache_purge_component_input_size(c, p, p->request.size,
                                                       error);
        if (rc != 0) {
            return rc;
        }
        p->parsed = 1;
    }
    return 0;
}

static int
ngx_http_cache_purge_component_write(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_purge_component_peer_t *p)
{
    size_t   remaining;
    int      attach;
    ssize_t  n;

    remaining = p->output.len - p->output_used;
    attach = p->output_lease >= 0;
    n = ngx_http_cache_purge_transport_send(
        p->fd, p->output.data + p->output_used,
        attach ? 1 : (remaining < 65536 ? remaining : 65536), &p->output_lease,
        attach ? 1 : 0);
    if (n < 0) {
        return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
    }
    if (n == 0) {
        return 0;
    }
    if (attach) {
        /* The socket queue owns the transferred reference after sendmsg. */
        ngx_http_cache_purge_component_close(&p->output_lease);
    }
    p->output_used += (size_t) n;
    if (p->output_used == p->output.len) {
        ngx_http_cache_purge_component_release(c, &p->output);
        p->output_used = 0;
        p->header_used = 0;
        p->input_used = 0;
        p->parsed = 0;
        p->deadline = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC)
                      + c->options.io_timeout_ms;
    }
    return 1;
}

static int
ngx_http_cache_purge_component_recover(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t              *query;
    ngx_http_cache_purge_index_match_t         match;
    ngx_http_cache_purge_index_file_t          file;
    ngx_http_cache_purge_publication_target_t  target;
    ngx_http_cache_rocksdb_bytes_t             bookmark;
    unsigned char                              digest[16];
    uint64_t                                   offset;
    int                                       lease, rc;

    query = NULL;
    lease = -1;
    memset(&file, 0, sizeof(file));
    memset(&target, 0, sizeof(target));
    bookmark.data = c->recovery_bookmark.data;
    bookmark.len = c->recovery_bookmark.len;
    c->recovery_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC) + 1000;

    rc = ngx_http_cache_purge_query_open(store, NULL,
                                         NGX_HTTP_CACHE_PURGE_QUERY_PENDING,
                                         bookmark, &query, error);
    if (rc != 0) {
        goto done;
    }
    rc = ngx_http_cache_purge_query_next(query, &match, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
        c->recovery_bookmark.len = 0;
        rc = 0;
        goto done;
    }
    if (rc != 0) {
        goto done;
    }
    bookmark = ngx_http_cache_purge_query_bookmark(query);
    rc = ngx_http_cache_purge_component_buffer_size(
        &c->recovery_bookmark, bookmark.len, 33024, 0, error);
    if (rc != 0) {
        goto done;
    }
    if (bookmark.len) {
        memcpy(c->recovery_bookmark.data, bookmark.data, bookmark.len);
    }
    ngx_http_cache_purge_query_close(&query, NULL);
    c->recovery_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC) + 10;
    rc = ngx_http_cache_purge_index_get_file(store, match.id, &file, error);
    if (rc != 0) {
        goto done;
    }

    /* Logical preparations are not native filesystem publication intents. */
    if (file.metadata.len < 4
        || (memcmp(file.metadata.data, "NFP1", 4)
            && memcmp(file.metadata.data, "NFV", 3)))
    {
        rc = 0;
        goto done;
    }
    rc = ngx_http_cache_purge_component_preserve(c, &file, error);
    if (rc != 0) {
        goto done;
    }
    rc = ngx_http_cache_purge_publications_target(c->publications, store,
                                                  file.root, file.zone,
                                                  file.path, &target, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_address(target.address, digest, &offset,
                                               error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_anchor(c, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_lease_acquire(
            c->lease_path, c->lease_anchor, offset, &lease, error);
    }
    if (rc == 0) {
        /*
         * A worker, an in-flight SCM_RIGHTS reference, or another publisher
         * keeps its OFD lease until writing/renaming is finished. Acquisition
         * on an independent description proves none of them still owns it.
         */
        if (memcmp(file.metadata.data, "NFV", 3) == 0) {
            rc = ngx_http_cache_purge_publications_validate_finish_checked(
                c->publications, store, file.id, NULL, 0, c->lease_anchor,
                lease, c->options.client_uid,
                c->coordinator ?
                    ngx_http_cache_purge_component_validation_finish_guard :
                    NULL,
                c, &match, error);
        } else {
            rc = ngx_http_cache_purge_publications_finish_checked(
                c->publications, store, file.id, file.revision, c->lease_anchor,
                lease, c->options.client_uid,
                ngx_http_cache_purge_component_publication_guard,
                c, &match, error);
        }
    }

done:
    if (query != NULL) {
        ngx_http_cache_purge_query_close(&query, NULL);
    }
    ngx_http_cache_purge_component_close(&lease);
    ngx_http_cache_purge_index_file_free(&file);
    ngx_http_cache_purge_publication_target_free(&target);

    /*
     * Active leases, unavailable roots and changed identities remain durable
     * and are revisited on the next bounded scan. Never stamp them READY.
     * Store corruption and I/O errors retain the executor's fail-closed path.
     */
    if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
        || rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
        || rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
        || rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
        || rc == NGX_HTTP_CACHE_ROCKSDB_EXPIRED
        || rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT)
    {
        return ngx_http_cache_purge_component_error(error, 0, "");
    }
    return rc;
}

static int
ngx_http_cache_purge_component_checkpoint(ngx_http_cache_purge_component_t *c,
    const ngx_http_cache_purge_operation_t *op,
    const ngx_http_cache_purge_index_expected_t *file,
    ngx_http_cache_rocksdb_bytes_t cursor, int done, uint64_t visits,
    int failed, uint64_t next, const char *message,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_encoder_t  e;
    ngx_http_cache_purge_checkpoint_t         point;
    ngx_http_cache_purge_operation_t          updated;
    unsigned char                             zero[16];
    int                                       rc;

    if (op->checkpoint_id == UINT64_MAX) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "purge checkpoint counter overflow");
    }
    memset(&e, 0, sizeof(e));
    memset(&point, 0, sizeof(point));
    memset(zero, 0, sizeof(zero));
    e.limit = 33024;
    e.error = error;
    ngx_http_cache_purge_component_put_raw(&e, "NDW1", 4);
    ngx_http_cache_purge_component_put_integer(&e, file != NULL, 1);
    ngx_http_cache_purge_component_put_raw(&e, file ? file->id : zero, 16);
    ngx_http_cache_purge_component_put_integer(&e,
                                                 file ? file->revision : 0, 8);
    ngx_http_cache_purge_component_put_string(&e, cursor);
    rc = e.rc;
    if (rc == 0) {
        point.id = op->checkpoint_id + 1;
        point.bookmark.data = e.buffer.data;
        point.bookmark.len = e.buffer.len;
        point.scan_done = done;
        point.visits_delta = visits;
        point.pending = file != NULL;
        point.failed = failed;
        point.next_run_ms = next;
        point.last_error.data = (const unsigned char *) message;
        point.last_error.len = strlen(message);
        rc = ngx_http_cache_purge_operations_checkpoint(&c->operations,
            op->input.id, op->revision, &point, &updated, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_coordinator_checkpoint_pin(c->coordinator,
                                                                 &updated,
                                                                 error);
        }
        ngx_http_cache_purge_operation_free(&updated);
    }
    ngx_http_cache_purge_component_buffer_free(&e.buffer);
    return rc;
}

static int
ngx_http_cache_purge_component_queue_cleanup(
    ngx_http_cache_purge_component_t *c,
    const ngx_http_cache_purge_operation_t *op,
    const ngx_http_cache_purge_index_file_t *file,
    uint32_t outcome, int *lease, int *waiting,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_rocksdb_bytes_t  fields[4], *target[4];
    unsigned char                   *p;
    size_t                            i, size;
    ssize_t                           written;
    int                               native, rc;

    *waiting = 0;
    native = 0;
    for (i = 0; i < c->options.root_count; i++) {
        native |= (c->roots[i].kind == NGX_HTTP_CACHE_PURGE_ROOT_NATIVE
                   || c->roots[i].kind == NGX_HTTP_CACHE_PURGE_ROOT_PROXY)
                  && c->roots[i].zone.len == file->zone.len
                  && memcmp(c->roots[i].zone.data, file->zone.data,
                            file->zone.len) == 0;
    }
    if (!native) {
        return 0;
    }
    if (ngx_http_cache_purge_component_load32(&c->cleanup_state) != 0
        || c->cleanup.token == UINT64_MAX)
    {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "native cleanup slot unavailable");
    }
    rc = ngx_http_cache_purge_publication_inode(file->metadata,
                                                   &c->cleanup.inode, error);
    if (rc != 0) {
        return rc;
    }
    fields[0] = file->root;
    fields[1] = file->zone;
    fields[2] = file->path;
    fields[3].data = op ? op->input.id.data : NULL;
    fields[3].len = op ? op->input.id.len : 0;
    target[0] = &c->cleanup.root;
    target[1] = &c->cleanup.zone;
    target[2] = &c->cleanup.path;
    target[3] = &c->cleanup_operation;
    size = 0;
    for (i = 0; i < 4; i++) {
        if (fields[i].len > 131072 - size) {
            return ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "native cleanup exceeds budget");
        }
        size += fields[i].len;
    }
    rc = ngx_http_cache_purge_component_buffer_size(&c->cleanup_buffer,
                                                   size, 131072, 0, error);
    if (rc != 0) {
        return rc;
    }
    p = c->cleanup_buffer.data;
    for (i = 0; i < 4; i++) {
        target[i]->data = p;
        target[i]->len = fields[i].len;
        if (fields[i].len != 0) {
            memcpy(p, fields[i].data, fields[i].len);
        }
        p += fields[i].len;
    }
    memcpy(c->cleanup_file, file->id, 16);
    c->cleanup.outcome = outcome;
    c->cleanup.token++;
    c->cleanup_lease = *lease;
    *lease = -1;
    ngx_http_cache_purge_component_store32(&c->cleanup_state, 1);
    do {
        written = write(c->cleanup_write, "n", 1);
    } while (written < 0 && errno == EINTR);
    if (written != 1 && (written >= 0 || errno != EAGAIN)) {
        return ngx_http_cache_purge_component_io(error,
                                                "notify native cache cleanup");
    }
    *waiting = 1;
    return 0;
}

static int
ngx_http_cache_purge_component_process_file(ngx_http_cache_purge_component_t *c,
    const ngx_http_cache_purge_operation_t *op, uint64_t now,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_decoder_t  d;
    ngx_http_cache_purge_query_t             *query;
    ngx_http_cache_purge_index_expected_t     expected;
    ngx_http_cache_purge_index_match_t        match;
    ngx_http_cache_purge_index_file_t         file;
    ngx_http_cache_purge_watermark_t          origin, validated, required;
    ngx_http_cache_rocksdb_bytes_t            cursor, address;
    ngx_http_cache_rocksdb_error_t            failure;
    unsigned char                             digest[16], *path;
    uint64_t                                  offset;
    uint32_t                                  outcome;
    int                                       pending, lease, rc, waiting;

    memset(&d, 0, sizeof(d));
    memset(&expected, 0, sizeof(expected));
    memset(&cursor, 0, sizeof(cursor));
    memset(&file, 0, sizeof(file));
    query = NULL;
    path = NULL;
    lease = -1;
    pending = 0;
    waiting = 0;
    if (op->bookmark.len != 0) {
        if (op->bookmark.len < 33
            || memcmp(op->bookmark.data, "NDW1", 4) != 0)
        {
            return ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "unknown purge checkpoint");
        }
        d.data = op->bookmark.data;
        d.len = op->bookmark.len;
        d.at = 4;
        d.error = error;
        pending = ngx_http_cache_purge_component_flag(&d);
        ngx_http_cache_purge_component_id(&d, expected.id);
        expected.revision = ngx_http_cache_purge_component_integer(&d, 8);
        cursor = ngx_http_cache_purge_component_string(&d, 32991);
        if (ngx_http_cache_purge_component_end(&d) != 0
            || pending != (int) op->pending || op->pending > 1
            || op->failed > 1 || (pending && expected.revision == 0)
            || (!pending && expected.revision != 0))
        {
            return ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid purge checkpoint");
        }
    } else if (op->pending || op->failed || op->checkpoint_id) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing purge checkpoint");
    }

    if (ngx_http_cache_purge_component_load32(&c->cleanup_state) == 2) {
        if (!pending || memcmp(expected.id, c->cleanup_file, 16) != 0) {
            return ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "native cleanup does not match pending instance");
        }
        rc = ngx_http_cache_purge_component_error(error, c->cleanup_result,
            c->cleanup_result == 0 ? "" : "native cache node is not drained");
        ngx_http_cache_purge_component_close(&c->cleanup_lease);
        ngx_http_cache_purge_component_store32(&c->cleanup_state, 0);
        goto done;
    }

    if (!pending) {
        rc = ngx_http_cache_purge_query_open(c->operations.store,
            &op->input.selector, NGX_HTTP_CACHE_PURGE_QUERY_SELECT,
            cursor, &query, error);
        if (rc != 0) {
            return rc;
        }
        rc = ngx_http_cache_purge_query_step(query, 32, &match, error);
        if (rc == 0) {
            memcpy(expected.id, match.id, 16);
            expected.revision = match.revision;
            cursor = ngx_http_cache_purge_query_bookmark(query);
            /* Reserve the exact instance before retirement can remove it
             * from selector enumeration. The cursor advances only on replay. */
            rc = ngx_http_cache_purge_component_checkpoint(c, op, &expected,
                cursor, 0, 0, 0, now, "", error);
        } else if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            rc = ngx_http_cache_purge_component_checkpoint(c, op, NULL,
                cursor, 1, 0, 0, now + 1000,
                op->input.mode == NGX_HTTP_CACHE_PURGE_INVALIDATE
                    ? "Invalidate awaits origin and file transaction drain"
                    : "native cache cleanup and retirement remain pending",
                error);
        } else if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
                   || rc == NGX_HTTP_CACHE_ROCKSDB_EXPIRED)
        {
            cursor = ngx_http_cache_purge_query_bookmark(query);
            rc = ngx_http_cache_purge_component_checkpoint(c, op, NULL,
                cursor, 0, 0, 0, now, "", error);
        }
        ngx_http_cache_purge_query_close(&query, NULL);
        return rc;
    }

    rc = ngx_http_cache_purge_index_get_file(c->operations.store,
                                               expected.id, &file, error);
    if (rc != 0) {
        goto done;
    }
    if (file.revision < expected.revision) {
        rc = ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "purge file revision regressed");
        goto done;
    }
    expected.revision = file.revision;
    if (file.state == NGX_HTTP_CACHE_PURGE_INDEX_PREPARED) {
        rc = ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "purge awaits file publication");
        goto done;
    }
    if (op->input.mode == NGX_HTTP_CACHE_PURGE_INVALIDATE
        && file.state == NGX_HTTP_CACHE_PURGE_INDEX_RETIRED)
    {
        goto done;
    }
    rc = ngx_http_cache_purge_publication_watermarks(file.metadata,
        &origin, &validated, error);
    if (op->input.mode == NGX_HTTP_CACHE_PURGE_INVALIDATE) {
        origin = validated;
    }
    if (rc == 0 && memcmp(origin.ledger, c->operations.ledger, 16) == 0
        && origin.sequence >= op->sequence)
    {
        /* Conditional coverage satisfies Invalidate, never Delete. */
        goto done;
    }
    if (rc != 0 && rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        goto done;
    }
    if (op->input.mode == NGX_HTTP_CACHE_PURGE_INVALIDATE) {
        memcpy(required.ledger, c->operations.ledger, 16);
        required.sequence = op->sequence;
        rc = ngx_http_cache_purge_index_require_validation(
            c->operations.store, &expected, &required, error);
        goto done;
    }
    address.len = file.root.len + 1 + file.path.len;
    path = malloc(address.len);
    if (path == NULL) {
        rc = ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "Delete path allocation failed");
        goto done;
    }
    memcpy(path, file.root.data, file.root.len);
    path[file.root.len] = '/';
    memcpy(path + file.root.len + 1, file.path.data, file.path.len);
    address.data = path;
    rc = ngx_http_cache_purge_file_address(address, digest, &offset, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_anchor(c, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_lease_acquire(c->lease_path,
            c->lease_anchor, offset, &lease, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publications_remove_checked(c->publications,
            c->operations.store, &expected, c->lease_anchor, lease,
            c->options.client_uid, ngx_http_cache_purge_component_preserve, c,
            &outcome, &match, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_queue_cleanup(c, op, &file,
            outcome, &lease, &waiting, error);
    }

done:
    free(path);
    ngx_http_cache_purge_component_close(&lease);
    ngx_http_cache_purge_index_file_free(&file);
    if (waiting) {
        return 0;
    }
    if (rc == 0) {
        return ngx_http_cache_purge_component_checkpoint(c, op, NULL,
            cursor, 0, 1, 0, now, "", error);
    }
    if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
        || rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
        || rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
        || rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
        || rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT
        || rc == NGX_HTTP_CACHE_PURGE_FILE_ERROR)
    {
        failure = *error;
        return ngx_http_cache_purge_component_checkpoint(c, op, &expected,
            cursor, 0, 1, rc != NGX_HTTP_CACHE_ROCKSDB_BUSY, now + 1000,
            failure.message, error);
    }
    return rc;
}

static int
ngx_http_cache_purge_component_retire_operation(
    ngx_http_cache_purge_component_t *c,
    const ngx_http_cache_purge_operation_t *op,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t           *query;
    ngx_http_cache_purge_index_match_t      match;
    ngx_http_cache_purge_operation_t        updated;
    ngx_http_cache_rocksdb_bytes_t          empty;
    uint32_t                                writers;
    int                                     rc;

    if (op->stage != NGX_HTTP_CACHE_PURGE_DRAINING || !op->scan_done
        || op->pending || op->failed)
    {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "purge has unfinished files");
    }
    rc = ngx_http_cache_purge_coordinator_writers(c->coordinator,
        op->sequence, &writers, error);
    if (rc != 0) {
        return rc;
    }
    if (writers != 0) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "purge awaits old origins");
    }

    /* Require an empty intent catalog in this executor step. Even a newer
     * intent postpones retirement; no unbounded provenance scan is needed.
     * Queued old writes cannot interleave with the durable floor below. */
    empty.data = NULL;
    empty.len = 0;
    query = NULL;
    rc = ngx_http_cache_purge_query_open(c->operations.store, NULL,
        NGX_HTTP_CACHE_PURGE_QUERY_PENDING, empty, &query, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_query_step(query, 32, &match, error);
    }
    ngx_http_cache_purge_query_close(&query, NULL);
    if (rc == 0) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "purge awaits file transactions");
    }
    if (rc != NGX_HTTP_CACHE_ROCKSDB_DONE) {
        return rc;
    }

    /* File and selector requirements outlive this FENCE. Advance the global
     * write floor with its durable removal, then publish the replacement
     * image through the existing reader gate. PROPAGATE keeps its deadline. */
    rc = ngx_http_cache_purge_operations_materialize(&c->operations,
        op->input.id, op->revision, op->sequence, &updated, error);
    ngx_http_cache_purge_operation_free(&updated);
    if (rc == 0) {
        rc = ngx_http_cache_purge_coordinator_refresh(c->coordinator, error);
    }
    return rc;
}

static int
ngx_http_cache_purge_component_collect_result(int rc,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
        || rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
        || rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
        || rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
        || rc == NGX_HTTP_CACHE_ROCKSDB_EXPIRED
        || rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT
        || rc == NGX_HTTP_CACHE_PURGE_FILE_ERROR)
    {
        /* Keep the durable queue entry, but let other instances progress. */
        return ngx_http_cache_purge_component_error(error, 0, "");
    }
    return rc;
}

static int
ngx_http_cache_purge_component_collect_ack(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_file_t      file;
    ngx_http_cache_purge_index_expected_t  expected;
    int                                    rc;

    memset(&file, 0, sizeof(file));
    rc = c->cleanup_result;
    if (rc == 0) {
        rc = ngx_http_cache_purge_coordinator_collectible(c->coordinator,
                                                         c->cleanup_file,
                                                         error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_index_get_file(c->operations.store,
                                                  c->cleanup_file, &file,
                                                  error);
    }
    if (rc == 0) {
        memcpy(expected.id, file.id, 16);
        expected.revision = file.revision;
        rc = ngx_http_cache_purge_index_collect(c->operations.store,
                                                   &expected, 1, error);
    }
    ngx_http_cache_purge_index_file_free(&file);
    ngx_http_cache_purge_component_close(&c->cleanup_lease);
    ngx_http_cache_purge_component_store32(&c->cleanup_state, 0);
    c->collect_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
    return ngx_http_cache_purge_component_collect_result(rc, error);
}

static int
ngx_http_cache_purge_component_collect_file(ngx_http_cache_purge_component_t *c,
    const ngx_http_cache_purge_index_match_t *selected,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_file_t      file;
    ngx_http_cache_purge_index_match_t     removed;
    ngx_http_cache_purge_index_expected_t  expected;
    ngx_http_cache_purge_operation_usage_t usage;
    ngx_http_cache_purge_watermark_t       required, origin, validated;
    ngx_http_cache_rocksdb_bytes_t         address;
    unsigned char                          *path, digest[16];
    uint64_t                                offset;
    uint32_t                                outcome;
    int                                     rc, lease, waiting;

    memset(&file, 0, sizeof(file));
    path = NULL;
    lease = -1;
    waiting = 0;
    rc = ngx_http_cache_purge_coordinator_collectible(c->coordinator,
                                                       selected->id, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_index_get_file(c->operations.store,
                                                selected->id, &file, error);
    if (rc != 0) {
        return rc;
    }
    memcpy(expected.id, file.id, 16);
    expected.revision = file.revision;
    /* Logical diagnostic records have no physical publication proof. */
    rc = ngx_http_cache_purge_publication_inode(file.metadata, &offset, error);
    if (rc != 0) {
        goto done;
    }
    if (file.state == NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED) {
        rc = ngx_http_cache_purge_publication_watermarks(file.metadata,
                                                       &origin, &validated,
                                                       error);
        if (rc != 0 && rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            goto done;
        }
        rc = ngx_http_cache_purge_index_delete_requirement(c->operations.store,
                                                        &expected, &required,
                                                        error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_operations_usage(&c->operations,
                                                         &usage, error);
        }
        if (rc != 0) {
            goto done;
        }
        if (required.sequence == 0
            || memcmp(required.ledger, c->operations.ledger, 16) != 0
            || required.sequence > usage.last_sequence)
        {
            rc = ngx_http_cache_purge_component_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "deferred Delete is outside the operation ledger");
            goto done;
        }
        if (memcmp(origin.ledger, required.ledger, 16) == 0
            && origin.sequence >= required.sequence)
        {
            rc = ngx_http_cache_purge_index_clear_delete(c->operations.store,
                                                        &expected, &required,
                                                        error);
            goto done;
        }
    }
    address.len = file.root.len + 1 + file.path.len;
    path = malloc(address.len);
    if (path == NULL) {
        rc = ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "collection path allocation failed");
        goto done;
    }
    memcpy(path, file.root.data, file.root.len);
    path[file.root.len] = '/';
    memcpy(path + file.root.len + 1, file.path.data, file.path.len);
    address.data = path;
    rc = ngx_http_cache_purge_file_address(address, digest, &offset, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_anchor(c, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_lease_acquire(c->lease_path,
            c->lease_anchor, offset, &lease, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publications_remove_checked(c->publications,
            c->operations.store, &expected, c->lease_anchor, lease,
            c->options.client_uid, ngx_http_cache_purge_component_preserve, c,
            &outcome, &removed, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_queue_cleanup(c, NULL, &file,
            outcome, &lease, &waiting, error);
    }
    if (rc == 0 && !waiting) {
        expected.revision = removed.revision;
        rc = ngx_http_cache_purge_index_collect(c->operations.store,
                                                   &expected, 1, error);
    }

done:
    free(path);
    ngx_http_cache_purge_component_close(&lease);
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}

static int
ngx_http_cache_purge_component_collect(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_query_t             *query;
    ngx_http_cache_purge_index_match_t        match;
    ngx_http_cache_purge_component_buffer_t  *saved;
    ngx_http_cache_rocksdb_bytes_t            cursor;
    uint64_t                                  now;
    size_t                                    removed, history;
    unsigned                                  queue, mode;
    int                                       rc, found;

    now = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
    c->collect_at = now + 1000;
    if (c->coordinator == NULL
        || ngx_http_cache_purge_component_load32(&c->control_state)
           != NGX_HTTP_CACHE_PURGE_COORD_READY
        || ngx_http_cache_purge_component_load32(&c->cleanup_state) != 0)
    {
        return 0;
    }
    queue = c->collect_next;
    if (c->collect_idle & (1u << queue)) {
        queue ^= 1;
    }
    c->collect_next = queue ^ 1;
    mode = queue ? NGX_HTTP_CACHE_PURGE_QUERY_RETIRED
                 : NGX_HTTP_CACHE_PURGE_QUERY_DELETE;
    saved = &c->collect_bookmark[queue];
    cursor.data = saved->data;
    cursor.len = saved->len;
    query = NULL;
    rc = ngx_http_cache_purge_query_open(c->operations.store, NULL,
                                           mode, cursor, &query, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_query_step(query, 32, &match, error);
    found = rc == 0;
    if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
        saved->len = 0;
        c->collect_idle |= 1u << queue;
        rc = 0;
    } else if (found || rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
                    || rc == NGX_HTTP_CACHE_ROCKSDB_EXPIRED)
    {
        cursor = ngx_http_cache_purge_query_bookmark(query);
        rc = ngx_http_cache_purge_component_buffer_size(saved,
                                           cursor.len, 33024, 0, error);
        if (rc == 0 && cursor.len != 0) {
            memcpy(saved->data, cursor.data, cursor.len);
        }
    }
    ngx_http_cache_purge_query_close(&query, NULL);
    if (rc == 0 && found) {
        rc = ngx_http_cache_purge_component_collect_file(c, &match, error);
        rc = ngx_http_cache_purge_component_collect_result(rc, error);
    }
    history = 0;
    if (rc == 0) {
        rc = ngx_http_cache_purge_coordinator_expire_propagation(c->coordinator,
            ngx_http_cache_purge_component_now(CLOCK_REALTIME), 8, &history,
            error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT) {
            rc = ngx_http_cache_purge_coordinator_expire_propagation(
                c->coordinator,
                ngx_http_cache_purge_component_now(CLOCK_REALTIME), 1,
                &history, error);
        }
        rc = ngx_http_cache_purge_component_collect_result(rc, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_index_collect_keys(c->operations.store,
                                                       8, &removed, error);
    }
    /* Complete both passes before revisiting busy rows. Empty/blocked
     * catalogs sleep; one blocked instance never stalls the remaining scan. */
    if (c->collect_idle == 3) {
        c->collect_idle = 0;
        if (rc == 0 && (removed != 0 || history != 0)) {
            c->collect_at = now;
        }
    } else {
        c->collect_at = now;
    }
    return rc;
}

static int
ngx_http_cache_purge_component_work(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operation_page_t     page;
    ngx_http_cache_purge_operation_t         *op;
    ngx_http_cache_purge_operation_t          cleanup;
    ngx_http_cache_rocksdb_bytes_t            after;
    uint64_t                                  now;
    int                                       rc;

    c->work_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC) + 1000;
    if (ngx_http_cache_purge_component_load32(&c->cleanup_state) == 1) {
        return 0;
    }
    if (c->coordinator == NULL
        || ngx_http_cache_purge_component_load32(&c->control_state)
           != NGX_HTTP_CACHE_PURGE_COORD_READY)
    {
        return 0;
    }
    now = ngx_http_cache_purge_component_now(CLOCK_REALTIME);
    if (now == 0 || now > UINT64_MAX - 1000) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_IO, "invalid background task clock");
    }
    if (ngx_http_cache_purge_component_load32(&c->cleanup_state) == 2) {
        if (c->cleanup_operation.len == 0) {
            rc = ngx_http_cache_purge_component_collect_ack(c, error);
        } else {
            rc = ngx_http_cache_purge_operations_get(&c->operations,
                c->cleanup_operation, &cleanup, error);
            if (rc == 0) {
                rc = ngx_http_cache_purge_component_process_file(c, &cleanup,
                                                                   now, error);
            }
            ngx_http_cache_purge_operation_free(&cleanup);
        }
        if (rc == 0) {
            /* Collection acknowledgements must not keep postponing tasks. */
            c->work_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
        }
        return rc;
    }
    after.data = c->work_after;
    after.len = c->work_after_len;
    rc = ngx_http_cache_purge_operations_page(&c->operations, 1, now,
                                                after, 1, &page, error);
    if (rc != 0) {
        return rc;
    }
    if (page.count != 0) {
        c->work_after_len = page.after_len;
        memcpy(c->work_after, page.after, page.after_len);
        c->work_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
        op = &page.items[0];
        if (!op->scan_done) {
            rc = ngx_http_cache_purge_component_process_file(c, op, now, error);
        } else {
            rc = ngx_http_cache_purge_component_retire_operation(c, op,
                                                                   error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
                || rc == NGX_HTTP_CACHE_ROCKSDB_EXPIRED)
            {
                rc = 0;
                if (page.done) {
                    c->work_at += 1000;
                }
            }
        }
    }
    if (page.done) {
        c->work_after_len = 0;
    }
    ngx_http_cache_purge_operation_page_free(&page);
    return rc;
}

static int
ngx_http_cache_purge_component_controls(ngx_http_cache_purge_component_t *c,
    uint64_t *expire_at, int *progress, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operation_t  pending, activated;
    uint32_t  state;
    uint64_t  publication;
    uint64_t  now;
    size_t    expired;
    int       rc;

    *progress = 0;
    if (c->coordinator == NULL) {
        return 0;
    }
    rc = ngx_http_cache_purge_coordinator_status(c->coordinator, &state,
                                                 &publication, error);
    if (rc != 0) {
        return rc;
    }
    if (state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        rc = ngx_http_cache_purge_coordinator_step(c->coordinator, error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            rc = 0;
        } else if (rc == 0) {
            *progress = 1;
        }
    } else if (ngx_http_cache_purge_component_now(CLOCK_MONOTONIC)
               >= c->activation_at)
    {
        rc = ngx_http_cache_purge_operations_activation(&c->operations,
                                                           &pending, error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            rc = ngx_http_cache_purge_component_error(error, 0, "");
            c->activation_at =
                ngx_http_cache_purge_component_now(CLOCK_MONOTONIC) + 1000;
        } else if (rc == 0) {
            now = ngx_http_cache_purge_component_now(CLOCK_REALTIME);
            rc = ngx_http_cache_purge_coordinator_activate(c->coordinator,
                pending.input.id, now, &activated, error);
            ngx_http_cache_purge_operation_free(&activated);
            if (rc == 0) {
                *progress = 1;
            }
        }
        ngx_http_cache_purge_operation_free(&pending);
    } else if (ngx_http_cache_purge_component_now(CLOCK_MONOTONIC)
               >= *expire_at)
    {
        now = ngx_http_cache_purge_component_now(CLOCK_REALTIME);
        if (now == 0) {
            return ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_IO,
                "invalid component expiration clock");
        }
        rc = ngx_http_cache_purge_operations_expire(&c->operations, now, 32,
                                                    &expired, error);
        if (rc == 0 && expired != 0) {
            rc =
                ngx_http_cache_purge_coordinator_refresh(c->coordinator, error);
        }
        *expire_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC) + 1000;
    }
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_coordinator_status(c->coordinator, &state,
                                                 &publication, error);
    if (rc == 0) {
        if (state == NGX_HTTP_CACHE_PURGE_COORD_READY
            && ngx_http_cache_purge_component_load32(&c->control_state)
               != NGX_HTTP_CACHE_PURGE_COORD_READY)
        {
            /* Resume work held for publication without the idle backoff. */
            c->work_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
        }
        ngx_http_cache_purge_component_store32(&c->control_state, state);
        ngx_http_cache_purge_component_store64(&c->control_publication,
                                               publication);
    }
    return rc;
}

static int
ngx_http_cache_purge_component_serve(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operation_limits_t  limits;
    struct pollfd                            events[66];
    ngx_http_cache_purge_component_peer_t   *p;
    size_t                                   i;
    size_t                                   step;
    size_t                                   start;
    uint64_t                                 expire_at;
    uint64_t                                 now;
    int                                      rc;
    int                                      progress;
    int                                      ready;
    int                                      complete;
    int                                      pending;
    int                                      executed;
    int                                      alive;
    int                                      timeout;
    short                                    event;

    rc = ngx_http_cache_purge_publications_open(
        c->roots, c->options.root_count, c->path, &c->publications, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_index_init(store, error);
    }
    if (rc == 0) {
        limits.live_records = c->options.max_live_operations;
        limits.live_selector_bytes = c->options.live_selector_bytes;
        rc = ngx_http_cache_purge_operations_init(&c->operations, store,
                                                  &limits, error);
    }
    if (rc == 0 && c->options.control_memory != NULL) {
        rc = ngx_http_cache_purge_coordinator_open(
            &c->operations, c->options.control_memory, c->options.control_bytes,
            &c->coordinator, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_bind(c, error);
    }
    if (rc != 0) {
        return rc;
    }
    expire_at = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC) + 1000;
    ngx_http_cache_purge_component_store32(&c->state,
                                           NGX_HTTP_CACHE_PURGE_COMPONENT_OPEN);
    while (!ngx_http_cache_purge_component_load32(&c->stopping)) {
        rc = ngx_http_cache_purge_component_controls(c, &expire_at, &progress,
                                                     error);
        if (rc != 0) {
            return rc;
        }
        now = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
        if (now >= c->recovery_at) {
            rc = ngx_http_cache_purge_component_recover(c, store, error);
            if (rc != 0) {
                return rc;
            }
        }
        if (now >= c->work_at
            || ngx_http_cache_purge_component_load32(&c->cleanup_state) == 2)
        {
            rc = ngx_http_cache_purge_component_work(c, error);
            if (rc != 0) {
                return rc;
            }
        }
        if (now >= c->collect_at) {
            rc = ngx_http_cache_purge_component_collect(c, error);
            if (rc != 0) {
                return rc;
            }
        }
        memset(events, 0, sizeof(events));
        events[0].fd = c->wake_read;
        events[0].events = POLLIN;
        events[1].fd = c->listener;
        events[1].events = POLLIN;
        ready = 0;
        now = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
        for (i = 0; i < c->options.max_connections; i++) {
            events[i + 2].fd = -1;
            p = &c->peers[i];
            if (p->fd < 0) {
                continue;
            }
            if (now >= p->deadline) {
                ngx_http_cache_purge_component_add64(&c->rejected, 1);
                ngx_http_cache_purge_component_drop(c, i);
                continue;
            }
            complete = p->parsed && p->input_used == p->input.len;
            events[i + 2].fd = p->fd;
            events[i + 2].events = p->output.len ? POLLOUT :
                                   complete      ? 0 :
                                                   POLLIN;
            ready |= complete && p->output.len == 0;
        }
        pending = c->coordinator != NULL
                  && ngx_http_cache_purge_component_load32(&c->control_state)
                         != NGX_HTTP_CACHE_PURGE_COORD_READY;
        timeout = 50;
        now = ngx_http_cache_purge_component_now(CLOCK_MONOTONIC);
        if (c->recovery_at <= now) {
            timeout = 0;
        } else if (c->recovery_at - now < 50) {
            timeout = (int) (c->recovery_at - now);
        }
        if (c->work_at <= now) {
            timeout = 0;
        } else if (c->work_at - now < (uint64_t) timeout) {
            timeout = (int) (c->work_at - now);
        }
        if (c->collect_at <= now) {
            timeout = 0;
        } else if (c->collect_at - now < (uint64_t) timeout) {
            timeout = (int) (c->collect_at - now);
        }
        rc = poll(events, c->options.max_connections + 2,
                  ready || (pending && progress) ? 0 : timeout);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return ngx_http_cache_purge_component_io(error, "component poll");
        }
        if (events[0].revents) {
            ngx_http_cache_purge_component_clear_wake(c);
        }
        if (ngx_http_cache_purge_component_load32(&c->stopping)) {
            break;
        }
        if (events[1].revents & POLLIN) {
            rc = ngx_http_cache_purge_component_accept_peers(c, error);
            if (rc != 0) {
                return rc;
            }
        }
        executed = 0;
        start = c->next_peer;
        for (step = 0; step < c->options.max_connections; step++) {
            i = (start + step) % c->options.max_connections;
            p = &c->peers[i];
            if (p->fd < 0) {
                continue;
            }
            event = events[i + 2].revents;
            if (event & (POLLERR | POLLHUP | POLLNVAL)) {
                ngx_http_cache_purge_component_drop(c, i);
                continue;
            }
            if (event & POLLOUT) {
                if (!ngx_http_cache_purge_component_write(c, p)) {
                    ngx_http_cache_purge_component_drop(c, i);
                }
                continue;
            }
            rc = 0;
            if (event & POLLIN) {
                rc = ngx_http_cache_purge_component_read(c, p, &alive, error);
                if (rc == 0 && !alive) {
                    ngx_http_cache_purge_component_drop(c, i);
                    continue;
                }
            }
            if (rc == 0 && !executed && !p->output.len && p->parsed
                && p->input_used == p->input.len)
            {
                rc = ngx_http_cache_purge_component_execute(c, store, p, error);
                if (rc == 0) {
                    executed = 1;
                    c->next_peer = (i + 1) % c->options.max_connections;
                }
            }
            if (rc != 0) {
                if (rc != NGX_HTTP_CACHE_ROCKSDB_INVALID
                    && rc != NGX_HTTP_CACHE_ROCKSDB_LIMIT)
                {
                    return rc;
                }
                ngx_http_cache_purge_component_add64(&c->rejected, 1);
                ngx_http_cache_purge_component_drop(c, i);
            }
        }
        /* Queries close per dispatch; idle peers retain no DB snapshots. */
    }
    return 0;
}

static void *
ngx_http_cache_purge_component_run(void *data)
{
    ngx_http_cache_purge_component_t  *c;
    ngx_http_cache_rocksdb_t          *store;
    ngx_http_cache_rocksdb_error_t     error;
    struct pollfd                      p;
    size_t                             i;
    int                                rc;

    c = data;
    store = NULL;
    memset(&error, 0, sizeof(error));
    while (!ngx_http_cache_purge_component_load32(&c->stopping)) {
        rc = ngx_http_cache_rocksdb_open(c->path, &c->options.store, &store,
                                         &error);
        if (rc == 0) {
            break;
        }
        if (rc != NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            ngx_http_cache_purge_component_fail(c, &error);
            break;
        }
        ngx_http_cache_purge_component_store32(
            &c->state, NGX_HTTP_CACHE_PURGE_COMPONENT_STANDBY);
        memset(&p, 0, sizeof(p));
        p.fd = c->wake_read;
        p.events = POLLIN;
        if (poll(&p, 1, (int) c->options.owner_retry_ms) < 0 && errno != EINTR)
        {
            ngx_http_cache_purge_component_io(&error, "component standby poll");
            ngx_http_cache_purge_component_fail(c, &error);
            break;
        }
        if (p.revents) {
            ngx_http_cache_purge_component_clear_wake(c);
        }
    }
    if (store != NULL && !ngx_http_cache_purge_component_load32(&c->stopping)) {
        rc = ngx_http_cache_purge_component_serve(c, store, &error);
        if (rc != 0) {
            ngx_http_cache_purge_component_fail(c, &error);
        }
    }
    ngx_http_cache_purge_component_store32(
        &c->state, NGX_HTTP_CACHE_PURGE_COMPONENT_STOPPING);
    if (c->coordinator != NULL) {
        rc = ngx_http_cache_purge_coordinator_close(&c->coordinator, &error);
        if (rc != 0) {
            ngx_http_cache_purge_component_fail(c, &error);
        }
    }
    for (i = 0; i < c->options.max_connections; i++) {
        ngx_http_cache_purge_component_drop(c, i);
    }
    /* Remove the rendezvous socket before releasing the database owner lock. */
    ngx_http_cache_purge_component_remove_socket(c);
    if (ngx_http_cache_purge_component_load32(&c->stopping)) {
        /* The creator has detached its event consumer before requesting stop.
         * Release the pending path before handing the DB to a new worker. */
        ngx_http_cache_purge_component_close(&c->cleanup_lease);
    }
    ngx_http_cache_purge_component_close(&c->lease_anchor);
    if (c->publications != NULL) {
        rc = ngx_http_cache_purge_publications_close(&c->publications, &error);
        if (rc != 0) {
            ngx_http_cache_purge_component_fail(c, &error);
        }
    }
    if (store != NULL) {
        rc = ngx_http_cache_rocksdb_close(&store, &error);
        if (rc != 0) {
            ngx_http_cache_purge_component_fail(c, &error);
        }
    }
    ngx_http_cache_purge_component_store32(
        &c->state, ngx_http_cache_purge_component_load32(&c->error_code) ?
                       NGX_HTTP_CACHE_PURGE_COMPONENT_FAILED :
                       NGX_HTTP_CACHE_PURGE_COMPONENT_STOPPED);
    return NULL;
}

static int
ngx_http_cache_purge_component_owner(ngx_http_cache_purge_component_t *c)
{
    return c != NULL && c->pid == getpid()
           && pthread_equal(c->creator, pthread_self());
}

static void
ngx_http_cache_purge_component_destroy(ngx_http_cache_purge_component_t *c)
{
    ngx_http_cache_purge_component_close(&c->cleanup_lease);
    ngx_http_cache_purge_component_close(&c->cleanup_read);
    ngx_http_cache_purge_component_close(&c->cleanup_write);
    ngx_http_cache_purge_component_buffer_free(&c->cleanup_buffer);
    ngx_http_cache_purge_component_close(&c->wake_read);
    ngx_http_cache_purge_component_close(&c->wake_write);
    free(c->peers);
    free(c->roots);
    free(c->root_storage);
    ngx_http_cache_purge_component_buffer_free(&c->recovery_bookmark);
    ngx_http_cache_purge_component_buffer_free(&c->collect_bookmark[0]);
    ngx_http_cache_purge_component_buffer_free(&c->collect_bookmark[1]);
    free(c);
}

void
ngx_http_cache_purge_component_options(
    ngx_http_cache_purge_component_options_t *o)
{
    if (o == NULL) {
        return;
    }
    memset(o, 0, sizeof(*o));
    o->abi = NGX_HTTP_CACHE_PURGE_COMPONENT_ABI;
    ngx_http_cache_rocksdb_options(&o->store);
    o->max_connections = 32;
    o->max_payload_bytes = 512 * 1024;
    o->buffer_bytes = 4 * 1024 * 1024;
    o->io_timeout_ms = 30000;
    o->owner_retry_ms = 50;
    o->client_uid = UINT32_MAX;
    o->propagation_ms = 120000;
    o->max_live_operations = 4096;
    o->live_selector_bytes = 4 * 1024 * 1024;
}

uint32_t
ngx_http_cache_purge_component_abi_8(void)
{
    return NGX_HTTP_CACHE_PURGE_COMPONENT_ABI;
}

int
ngx_http_cache_purge_component_start(const char *path,
    const ngx_http_cache_purge_component_options_t *o,
    ngx_http_cache_purge_component_t **output,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_t   *c;
    const ngx_http_cache_purge_root_t  *root;
    unsigned char                      *bytes;
    size_t                              i;
    size_t                              total;
    size_t                              length;
    int                                 rc;
    int                                 started;
    int                                 restored;
    int                                 descriptors[2];
    sigset_t                            blocked;
    sigset_t                            previous;

    ngx_http_cache_purge_component_error(error, 0, "");
    if (output != NULL) {
        *output = NULL;
    }
    if (path == NULL || o == NULL || output == NULL
        || o->abi != NGX_HTTP_CACHE_PURGE_COMPONENT_ABI
        || o->max_connections == 0 || o->max_connections > 64
        || o->max_payload_bytes < 1024 || o->max_payload_bytes > 1024 * 1024
        || o->buffer_bytes
               < 2 * o->max_payload_bytes + NGX_HTTP_CACHE_PURGE_FRAME_SIZE
        || o->buffer_bytes > 64 * 1024 * 1024 || o->io_timeout_ms == 0
        || o->io_timeout_ms > 300000 || o->owner_retry_ms == 0
        || o->owner_retry_ms > 1000 || o->max_live_operations == 0
        || o->max_live_operations > 65536 || o->live_selector_bytes < 1024
        || o->live_selector_bytes > 64 * 1024 * 1024
        || (o->control_memory == NULL) != (o->control_bytes == 0)
        || o->root_count > 256 || (o->root_count && o->roots == NULL))
    {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid component options");
    }
    length = strlen(path);
    if (path[0] != '/'
        || length + sizeof("/component.sock")
               > sizeof(((struct sockaddr_un *) 0)->sun_path))
    {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "component requires a short absolute Unix socket path");
    }
    total = 0;
    for (i = 0; i < o->root_count; i++) {
        root = &o->roots[i];
        if (root->path.data == NULL || root->path.len < 2
            || root->path.len > 4096 || root->zone.len > 128
            || (root->zone.len && root->zone.data == NULL)
            || root->path.data[0] != '/'
            || memchr(root->path.data, 0, root->path.len)
            || (root->zone.len && memchr(root->zone.data, 0, root->zone.len))
            || (root->kind < NGX_HTTP_CACHE_PURGE_ROOT_CACHE
                || root->kind > NGX_HTTP_CACHE_PURGE_ROOT_PROXY)
            || (root->kind != NGX_HTTP_CACHE_PURGE_ROOT_TEMP
                && !root->zone.len))
        {
            return ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "invalid publication root");
        }
        total += root->path.len + root->zone.len;
        if (total > 65536) {
            return ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "publication roots exceed budget");
        }
    }
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "component allocation failed");
    }
    c->wake_read = -1;
    c->wake_write = -1;
    c->listener = -1;
    c->lease_anchor = -1;
    c->cleanup_read = -1;
    c->cleanup_write = -1;
    c->cleanup_lease = -1;
    c->pid = getpid();
    c->creator = pthread_self();
    c->state = NGX_HTTP_CACHE_PURGE_COMPONENT_STARTING;
    c->options = *o;
    c->options.roots = NULL;
    memcpy(c->path, path, length + 1);
    memcpy(c->socket_path, path, length);
    memcpy(c->socket_path + length, "/component.sock",
           sizeof("/component.sock"));
    memcpy(c->lease_path, path, length);
    memcpy(c->lease_path + length, "/file.leases", sizeof("/file.leases"));

    c->peers = calloc(o->max_connections, sizeof(*c->peers));
    if (c->peers == NULL) {
        rc = ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "component allocation failed");
        goto failed;
    }
    for (i = 0; i < o->max_connections; i++) {
        c->peers[i].fd = -1;
        c->peers[i].output_lease = -1;
        c->peers[i].input_rights[0] = -1;
        c->peers[i].input_rights[1] = -1;
    }
    if (o->root_count != 0) {
        c->roots = calloc(o->root_count, sizeof(*c->roots));
        c->root_storage = malloc(total);
        if (c->roots == NULL || c->root_storage == NULL) {
            rc = ngx_http_cache_purge_component_error(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "component allocation failed");
            goto failed;
        }
        bytes = c->root_storage;
        for (i = 0; i < o->root_count; i++) {
            root = &o->roots[i];
            c->roots[i] = *root;
            c->roots[i].path.data = bytes;
            memcpy(bytes, root->path.data, root->path.len);
            bytes += root->path.len;
            c->roots[i].zone.data = bytes;
            if (root->zone.len != 0) {
                memcpy(bytes, root->zone.data, root->zone.len);
                bytes += root->zone.len;
            }
        }
    }
    if (c->options.client_uid == UINT32_MAX) {
        c->options.client_uid = (uint32_t) geteuid();
    }
    if (c->options.client_uid != geteuid() && geteuid() != 0) {
        rc = ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "component client UID requires root creator");
        goto failed;
    }
    if (pipe(descriptors) != 0) {
        rc = ngx_http_cache_purge_component_io(error, "component wake pipe");
        goto failed;
    }
    c->wake_read = descriptors[0];
    c->wake_write = descriptors[1];
    rc = ngx_http_cache_purge_component_nonblocking(c->wake_read, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_nonblocking(c->wake_write, error);
    }
    if (rc != 0) {
        goto failed;
    }
    if (pipe(descriptors) != 0) {
        rc = ngx_http_cache_purge_component_io(error,
                                               "component notification pipe");
        goto failed;
    }
    c->cleanup_read = descriptors[0];
    c->cleanup_write = descriptors[1];
    rc = ngx_http_cache_purge_component_nonblocking(c->cleanup_read, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_nonblocking(c->cleanup_write,
                                                         error);
    }
    if (rc != 0) {
        goto failed;
    }
    sigfillset(&blocked);
    sigdelset(&blocked, SIGSEGV);
    sigdelset(&blocked, SIGBUS);
    sigdelset(&blocked, SIGILL);
    sigdelset(&blocked, SIGFPE);
    sigdelset(&blocked, SIGABRT);
    if (pthread_sigmask(SIG_BLOCK, &blocked, &previous) != 0) {
        rc = ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO,
            "cannot block component control signals");
        goto failed;
    }
    started = pthread_create(&c->executor, NULL,
                             ngx_http_cache_purge_component_run, c);
    restored = pthread_sigmask(SIG_SETMASK, &previous, NULL);
    if (started != 0) {
        rc = ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO,
            "cannot start component executor");
        goto failed;
    }
    if (restored != 0) {
        ngx_http_cache_purge_component_store32(&c->stopping, 1);
        ngx_http_cache_purge_component_wake(c);
        pthread_join(c->executor, NULL);
        rc = ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO,
            "cannot restore manager signal mask");
        goto failed;
    }
    *output = c;
    return 0;

failed:
    ngx_http_cache_purge_component_destroy(c);
    return rc;
}

int
ngx_http_cache_purge_component_status(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_purge_component_status_t *status,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_error(error, 0, "");
    if (c == NULL || status == NULL) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null component status");
    }
    if (!ngx_http_cache_purge_component_owner(c)) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER, "component creator mismatch");
    }
    memset(status, 0, sizeof(*status));
    status->state = ngx_http_cache_purge_component_load32(&c->state);
    status->error_code = ngx_http_cache_purge_component_load32(&c->error_code);
    status->connections =
        ngx_http_cache_purge_component_load64(&c->connections);
    status->requests = ngx_http_cache_purge_component_load64(&c->requests);
    status->rejected = ngx_http_cache_purge_component_load64(&c->rejected);
    status->buffered_bytes =
        ngx_http_cache_purge_component_load64(&c->buffered);
    status->peak_buffered_bytes =
        ngx_http_cache_purge_component_load64(&c->peak);
    status->control_state =
        ngx_http_cache_purge_component_load32(&c->control_state);
    status->control_publication =
        ngx_http_cache_purge_component_load64(&c->control_publication);
    return 0;
}

int
ngx_http_cache_purge_component_cleanup_fd(ngx_http_cache_purge_component_t *c)
{
    return ngx_http_cache_purge_component_owner(c) ? c->cleanup_read : -1;
}

int
ngx_http_cache_purge_component_cleanup_next(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_purge_cleanup_t *out, ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char  buffer[64];
    ssize_t        n;

    if (!ngx_http_cache_purge_component_owner(c) || out == NULL) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_OWNER, "invalid cleanup consumer");
    }
    memset(out, 0, sizeof(*out));
    do {
        n = read(c->cleanup_read, buffer, sizeof(buffer));
    } while (n > 0 || (n < 0 && errno == EINTR));
    if (n == 0 || errno != EAGAIN) {
        return ngx_http_cache_purge_component_io(error,
                                                "read cleanup notification");
    }
    if (ngx_http_cache_purge_component_load32(&c->cleanup_state) != 1) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_DONE, "no native cleanup request");
    }
    if (ngx_http_cache_purge_component_load32(&c->stopping)) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_DONE, "cleanup consumer is stopping");
    }
    *out = c->cleanup;
    return ngx_http_cache_purge_component_error(error, 0, "");
}

int
ngx_http_cache_purge_component_cleanup_complete(
    ngx_http_cache_purge_component_t *c, uint64_t token, int result,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (!ngx_http_cache_purge_component_owner(c)) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_OWNER, "invalid cleanup consumer");
    }
    if (ngx_http_cache_purge_component_load32(&c->cleanup_state) != 1
        || token != c->cleanup.token)
    {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "stale native cleanup reply");
    }
    if (result != 0 && result != NGX_HTTP_CACHE_ROCKSDB_BUSY
        && result != NGX_HTTP_CACHE_ROCKSDB_INVALID)
    {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native cleanup result");
    }
    c->cleanup_result = result;
    ngx_http_cache_purge_component_store32(&c->cleanup_state, 2);
    ngx_http_cache_purge_component_wake(c);
    return ngx_http_cache_purge_component_error(error, 0, "");
}

int
ngx_http_cache_purge_component_stop(ngx_http_cache_purge_component_t **output,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_t  *c;
    int                                rc;

    ngx_http_cache_purge_component_error(error, 0, "");
    if (output == NULL || *output == NULL) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null component stop");
    }
    c = *output;
    if (!ngx_http_cache_purge_component_owner(c)) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER, "component creator mismatch");
    }
    ngx_http_cache_purge_component_store32(&c->stopping, 1);
    ngx_http_cache_purge_component_wake(c);
    if (pthread_join(c->executor, NULL) != 0) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot join component executor");
    }
    rc = (int) ngx_http_cache_purge_component_load32(&c->error_code);
    if (rc != 0 && error != NULL) {
        *error = c->terminal_error;
    }
    ngx_http_cache_purge_component_destroy(c);
    *output = NULL;
    return rc;
}

int
ngx_http_cache_purge_component_request_stop(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_error(error, 0, "");
    if (c == NULL) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "null component stop request");
    }
    if (!ngx_http_cache_purge_component_owner(c)) {
        return ngx_http_cache_purge_component_error(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER, "component creator mismatch");
    }
    ngx_http_cache_purge_component_store32(&c->stopping, 1);
    ngx_http_cache_purge_component_wake(c);
    return 0;
}


/* Shared controls and durable operation publication. */

#define NGX_HTTP_CACHE_PURGE_CONTROL_MAGIC  UINT64_C(0x4e43504354524c31)
#define NGX_HTTP_CACHE_PURGE_CONTROL_NONE   UINT32_MAX
#define NGX_HTTP_CACHE_PURGE_CONTROL_MAX    65536
#define NGX_HTTP_CACHE_PURGE_SEEN  1024
#define NGX_HTTP_CACHE_PURGE_CONTROL_BYTES  (64 * 1024 * 1024)

#if (!defined(__GCC_ATOMIC_LLONG_LOCK_FREE)                                    \
     || __GCC_ATOMIC_LLONG_LOCK_FREE != 2)
#error shared control requires lock-free 64-bit atomics
#endif

typedef struct {
    uint64_t  token;
    uint64_t  pin;
    int64_t   pid;
    uint64_t  birth;
    uint64_t  origins;
    uint64_t  ledger[2];
    uint64_t  revision;
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
    uint32_t                              record;
    uint32_t                              next;
    ngx_http_cache_purge_control_slice_t  value;
} ngx_http_cache_purge_control_link_t;

typedef struct {
    ngx_http_cache_purge_control_slice_t  edge;
    uint32_t                              child;
    uint32_t                              sibling;
    uint32_t                              record;
} ngx_http_cache_purge_control_node_t;

typedef struct {
    uint64_t       magic_value;
    uint32_t       bytes;
    uint32_t       records;
    uint32_t       links;
    uint32_t       nodes;
    uint32_t       buckets;
    uint32_t       scopes;
    uint32_t       records_at;
    uint32_t       nodes_at;
    uint32_t       links_at;
    uint32_t       buckets_at;
    uint32_t       scopes_at;
    uint32_t       strings_at;
    uint64_t       sequence;
    unsigned char  ledger[16];
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
    ngx_http_cache_purge_control_link_t    *links;
    ngx_http_cache_purge_control_node_t    *nodes;
    uint32_t                               *buckets;
    uint32_t                               *scopes;
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
    const ngx_http_cache_purge_control_link_t     *links;
    const ngx_http_cache_purge_control_node_t     *nodes;
    const uint32_t                                *buckets;
    const uint32_t                                *scopes;
    const ngx_http_cache_purge_control_context_t  *context;
    size_t                                         count;
    size_t                                         remaining;
    size_t                                         limit;
    uint64_t                                      *seen;
    int                                            seen_initialized;
    int                                            propagation;
    int                                            tags_known;
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
            ngx_http_cache_purge_control_store(&slot->origins, 0);
            ngx_http_cache_purge_control_store(&slot->ledger[0], 0);
            ngx_http_cache_purge_control_store(&slot->ledger[1], 0);
            ngx_http_cache_purge_control_store(&slot->revision, 0);
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
    ngx_http_cache_rocksdb_bytes_t                 tags[64];
    size_t                                         count;

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
        || kind > NGX_HTTP_CACHE_PURGE_HOST_TAGS)
    {
        return 0;
    }

    if (kind >= NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG)
    {
        if (!input->selector.scope.len || !input->selector.value.len
            || (kind <= NGX_HTTP_CACHE_PURGE_HOST_TAG
                && input->selector.value.len > 1024)
            || (kind > NGX_HTTP_CACHE_PURGE_HOST_TAG
                && ngx_http_cache_purge_tags_decode(input->selector.value,
                                                      tags, &count) != 0))
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
    ngx_http_cache_purge_control_link_t      *link;
    ngx_http_cache_rocksdb_bytes_t            tags[64];
    ngx_http_cache_rocksdb_bytes_t            empty = { NULL, 0 };
    size_t                                    i, j, members, links, next;
    size_t                                    strings;
    size_t                                    buckets;
    size_t                                    scopes;
    size_t                                    scoped;
    size_t                                    fixed;
    size_t                                    capacity;
    size_t                                    original;
    uint32_t                                  bucket;
    uint32_t                                  nodes;
    uint32_t                                  kind;
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
    links = 0;
    scoped = 0;
    for (i = 0; i < count; i++) {
        if (!ngx_http_cache_purge_control_validate(&ops[i])) {
            free(ids);
            return ngx_http_cache_purge_control_error(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "invalid control operation");
        }
        ids[i].id = ops[i].input.id;
        ids[i].sequence = ops[i].sequence;
        kind = ops[i].input.selector.kind;
        if (kind >= NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG) {
            scoped++;
        }
        members = 1;
        if (kind > NGX_HTTP_CACHE_PURGE_HOST_TAG) {
            (void) ngx_http_cache_purge_tags_decode(ops[i].input.selector.value,
                                                       tags, &members);
        } else if (kind == NGX_HTTP_CACHE_PURGE_KEY_PREFIX
                   || kind == NGX_HTTP_CACHE_PURGE_URL_PREFIX)
        {
            members = 0;
        }
        links += members;
        if (links > NGX_HTTP_CACHE_PURGE_CONTROL_MAX) {
            free(ids);
            return ngx_http_cache_purge_control_error(error,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "too many control memberships");
        }
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
    while (buckets < links * 2) {
        buckets *= 2;
    }
    scopes = scoped ? 8 : 0;
    while (scopes < scoped * 2) {
        scopes *= 2;
    }

    fixed = ngx_http_cache_purge_control_align(sizeof(*header))
            + count * sizeof(ngx_http_cache_purge_control_record_t)
            + links * sizeof(ngx_http_cache_purge_control_link_t)
            + (buckets + scopes) * sizeof(uint32_t) + strings;
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
    header->links = (uint32_t) links;
    header->buckets = (uint32_t) buckets;
    header->scopes = (uint32_t) scopes;
    header->records_at =
        (uint32_t) ngx_http_cache_purge_control_align(sizeof(*header));
    header->nodes_at = header->records_at
                       + count * sizeof(ngx_http_cache_purge_control_record_t);
    header->links_at =
        header->nodes_at + nodes * sizeof(ngx_http_cache_purge_control_node_t);
    header->buckets_at = header->links_at
                        + links * sizeof(ngx_http_cache_purge_control_link_t);
    header->scopes_at = header->buckets_at + buckets * sizeof(uint32_t);
    header->strings_at = header->scopes_at + scopes * sizeof(uint32_t);

    memset(&b, 0, sizeof(b));
    b.records =
        (ngx_http_cache_purge_control_record_t *) (image->data
                                                   + header->records_at);
    b.nodes = (ngx_http_cache_purge_control_node_t *) (image->data
                                                       + header->nodes_at);
    b.links = (ngx_http_cache_purge_control_link_t *) (image->data
                                                       + header->links_at);
    b.buckets = (uint32_t *) (image->data + header->buckets_at);
    b.scopes = (uint32_t *) (image->data + header->scopes_at);
    b.strings = image->data + header->strings_at;
    b.max_nodes = nodes;
    b.nnodes = 2;
    ngx_http_cache_purge_control_node_init(&b.nodes[0]);
    ngx_http_cache_purge_control_node_init(&b.nodes[1]);
    for (i = 0; i < buckets; i++) {
        b.buckets[i] = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;
    }
    for (i = 0; i < scopes; i++) {
        b.scopes[i] = NGX_HTTP_CACHE_PURGE_CONTROL_NONE;
    }

    next = 0;
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
            kind = record->kind;
            members = 1;
            tags[0] = ops[i].input.selector.value;
            if (kind > NGX_HTTP_CACHE_PURGE_HOST_TAG) {
                (void) ngx_http_cache_purge_tags_decode(
                    ops[i].input.selector.value, tags, &members);
                kind -= 2;
            }
            for (j = 0; j < members; j++) {
                link = &b.links[next];
                link->record = (uint32_t) i;
                link->value = record->value;
                link->value.offset += (uint32_t) (tags[j].data
                                        - ops[i].input.selector.value.data);
                link->value.size = (uint32_t) tags[j].len;
                bucket = (uint32_t) (ngx_http_cache_purge_control_key_hash(
                    s->salt, kind, tags[j], ops[i].input.selector.scope)
                    & (buckets - 1));
                link->next = b.buckets[bucket];
                b.buckets[bucket] = (uint32_t) next++;
            }
            if (kind >= NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG) {
                bucket = (uint32_t) (ngx_http_cache_purge_control_key_hash(
                    s->salt, kind, empty, ops[i].input.selector.scope)
                    & (scopes - 1));
                record->next = b.scopes[bucket];
                b.scopes[bucket] = (uint32_t) i;
            }
        }
    }

    /* Pack the actual radix nodes without a second image-sized allocation. */
    original = header->links_at;
    header->nodes = b.nnodes;
    header->links_at =
        header->nodes_at
        + b.nnodes * sizeof(ngx_http_cache_purge_control_node_t);
    header->buckets_at = header->links_at
                        + links * sizeof(ngx_http_cache_purge_control_link_t);
    header->scopes_at = header->buckets_at + buckets * sizeof(uint32_t);
    header->strings_at = header->scopes_at + scopes * sizeof(uint32_t);
    header->bytes = header->strings_at + strings;
    memmove(image->data + header->links_at, image->data + original,
            links * sizeof(ngx_http_cache_purge_control_link_t)
            + (buckets + scopes) * sizeof(uint32_t) + strings);
    b.links = (ngx_http_cache_purge_control_link_t *) (image->data
                                                       + header->links_at);

    for (i = 0; i < count; i++) {
        b.records[i].id.offset += header->strings_at;
        b.records[i].value.offset += header->strings_at;
        b.records[i].scope.offset += header->strings_at;
    }
    for (i = 0; i < b.nnodes; i++) {
        b.nodes[i].edge.offset += header->strings_at;
    }
    for (i = 0; i < links; i++) {
        b.links[i].value.offset += header->strings_at;
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
        ngx_http_cache_purge_control_store(&slot->origins, 0);
        ngx_http_cache_purge_control_store(&slot->ledger[0], 0);
        ngx_http_cache_purge_control_store(&slot->ledger[1], 0);
        ngx_http_cache_purge_control_store(&slot->revision, 0);
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
    if (r->pinned
        || ngx_http_cache_purge_control_load(
            &ngx_http_cache_purge_control_slots(r->shared)[r->slot].origins))
    {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control reader still active");
    }
    /* Only this thread owns the slot. Clearing its token is the final shared
     * access; a new reader may reuse it immediately. No registration mutex
     * is needed, so unrelated shutdowns cannot leave an idle handle BUSY. */
    ngx_http_cache_purge_control_store(
        &ngx_http_cache_purge_control_slots(r->shared)[r->slot].token, 0);
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
        || h->records > NGX_HTTP_CACHE_PURGE_CONTROL_MAX
        || h->links > NGX_HTTP_CACHE_PURGE_CONTROL_MAX || h->nodes < 2
        || h->nodes > 2 * h->records + 2 || h->buckets < 8
        || h->buckets > 131072 || (h->buckets & (h->buckets - 1))
        || (h->scopes && (h->scopes < 8 || h->scopes > 131072
                         || (h->scopes & (h->scopes - 1))))
        || h->records_at != ngx_http_cache_purge_control_align(sizeof(*h))
        || h->nodes_at
               != h->records_at
                      + h->records
                            * sizeof(ngx_http_cache_purge_control_record_t)
        || h->links_at
               != h->nodes_at
                      + h->nodes * sizeof(ngx_http_cache_purge_control_node_t)
        || h->buckets_at
               != h->links_at
                      + h->links * sizeof(ngx_http_cache_purge_control_link_t)
        || h->scopes_at != h->buckets_at + h->buckets * sizeof(uint32_t)
        || h->strings_at != h->scopes_at + h->scopes * sizeof(uint32_t)
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
    m->links =
        (const ngx_http_cache_purge_control_link_t *) (m->base + h->links_at);
    m->buckets = (const uint32_t *) (m->base + h->buckets_at);
    m->scopes = (const uint32_t *) (m->base + h->scopes_at);
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
    if (r->kind > NGX_HTTP_CACHE_PURGE_HOST_TAG) {
        if (!m->seen_initialized) {
            memset(m->seen, 0,
                   ((m->image->records + 63) / 64) * sizeof(uint64_t));
            m->seen_initialized = 1;
        }
        if (m->seen[index / 64] & (UINT64_C(1) << (index % 64))) {
            return 0;
        }
    }
    memset(&match, 0, sizeof(match));
    match.flags = r->flags;
    if (!m->context->proxy || m->context->now_ms >= r->until) {
        match.flags &= ~NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
    }
    if (m->propagation) {
        if (m->context->proxy
            && (r->flags & NGX_HTTP_CACHE_PURGE_CONTROL_PREPARING))
        {
            /* Published FENCEs already constrain new origins. Propagate before
             * first activation starts the local window. */
            match.flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
        }
        match.flags &= NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
    }
    if (!match.flags) {
        return 0;
    }
    if (m->propagation && !m->tags_known
        && r->kind >= NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG)
    {
        match.flags |= NGX_HTTP_CACHE_PURGE_CONTROL_TAG_CANDIDATE;
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
    if (r->kind > NGX_HTTP_CACHE_PURGE_HOST_TAG) {
        m->seen[index / 64] |= UINT64_C(1) << (index % 64);
    }
    return 0;
}

static int
ngx_http_cache_purge_control_exact(ngx_http_cache_purge_control_matcher_t *m,
    uint32_t kind, ngx_http_cache_rocksdb_bytes_t value,
    ngx_http_cache_rocksdb_bytes_t scope)
{
    const ngx_http_cache_purge_control_record_t  *r;
    const ngx_http_cache_purge_control_link_t    *link;
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
        if (++steps > m->image->links || i >= m->image->links) {
            return ngx_http_cache_purge_control_error(
                m->error, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid control hash chain");
        }
        link = &m->links[i];
        if (link->record >= m->image->records) {
            return ngx_http_cache_purge_control_error(m->error,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "control membership references an invalid operation");
        }
        r = &m->records[link->record];
        if ((r->kind > NGX_HTTP_CACHE_PURGE_HOST_TAG ? r->kind - 2 : r->kind)
            == kind)
        {
            rc = ngx_http_cache_purge_control_string(m, link->value, &rv);
            if (rc != 0) {
                return rc;
            }
            if (ngx_http_cache_purge_control_bytes_equal(rv, value)) {
                rc = ngx_http_cache_purge_control_string(m, r->scope, &rs);
                if (rc != 0) {
                    return rc;
                }
                if (ngx_http_cache_purge_control_bytes_equal(rs, scope)) {
                    rc = ngx_http_cache_purge_control_emit(m, link->record);
                    if (rc != 0) {
                        return rc;
                    }
                }
            }
        }
        i = link->next;
    }
    return 0;
}

static int
ngx_http_cache_purge_control_scope(ngx_http_cache_purge_control_matcher_t *m,
    uint32_t kind, ngx_http_cache_rocksdb_bytes_t scope)
{
    const ngx_http_cache_purge_control_record_t  *r;
    ngx_http_cache_rocksdb_bytes_t                empty = { NULL, 0 };
    ngx_http_cache_rocksdb_bytes_t                rs;
    uint32_t                                      i, steps, actual;
    int                                           rc;

    if (m->image->scopes == 0 || scope.len == 0) {
        return 0;
    }
    i = m->scopes[ngx_http_cache_purge_control_key_hash(m->shared->salt, kind,
                                                      empty, scope)
                  & (m->image->scopes - 1)];
    steps = 0;
    while (i != NGX_HTTP_CACHE_PURGE_CONTROL_NONE) {
        if (++steps > m->image->records || i >= m->image->records) {
            return ngx_http_cache_purge_control_error(m->error,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid control scope chain");
        }
        r = &m->records[i];
        if (r->kind < NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG
            || r->kind > NGX_HTTP_CACHE_PURGE_HOST_TAGS)
        {
            return ngx_http_cache_purge_control_error(m->error,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "control scope references a non-TAG operation");
        }
        actual = r->kind > NGX_HTTP_CACHE_PURGE_HOST_TAG ? r->kind - 2 :
                                                         r->kind;
        if (actual == kind) {
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
    if (m->image->records == 0 || (m->propagation && !c->proxy)) {
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
    if (m->propagation && !m->tags_known) {
        if (rc == 0) {
            rc = ngx_http_cache_purge_control_scope(m,
                NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG, c->tag_namespace);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_control_scope(m,
                NGX_HTTP_CACHE_PURGE_HOST_TAG, c->host);
        }
        return rc;
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

static int
ngx_http_cache_purge_control_lookup(ngx_http_cache_purge_control_reader_t *r,
    const ngx_http_cache_purge_control_context_t *context, int propagation,
    int tags_known, size_t max_matches,
    size_t max_bytes, ngx_http_cache_purge_control_visit_pt visit, void *data,
    ngx_http_cache_purge_watermark_t *watermark, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_matcher_t  m;
    uint64_t                                version;
    uint64_t                                seen[NGX_HTTP_CACHE_PURGE_SEEN];
    int                                     rc;
    static const unsigned char zero[16] = { 0 };

    ngx_http_cache_purge_control_clear(error);
    if (publication != NULL) {
        *publication = 0;
    }
    if (watermark != NULL) {
        memset(watermark, 0, sizeof(*watermark));
    }
    if (context == NULL || visit == NULL || publication == NULL
        || max_matches == 0 || max_matches > NGX_HTTP_CACHE_PURGE_CONTROL_MAX
        || max_bytes > NGX_HTTP_CACHE_PURGE_CONTROL_BYTES
        || (propagation && (watermark == NULL
                            || (tags_known != 0 && tags_known != 1)
                            || (!tags_known && context->tag_count))))
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
    m.seen = seen;
    m.propagation = propagation;
    m.tags_known = tags_known;

    rc = ngx_http_cache_purge_control_matcher_init(&m);
    if (rc == 0 && propagation
        && memcmp(m.image->ledger, zero, sizeof(zero)) == 0)
    {
        rc = ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "control has no durable ledger");
    }
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
        if (watermark != NULL) {
            memcpy(watermark->ledger, m.image->ledger, 16);
            watermark->sequence = m.image->sequence;
        }
        *publication = version;
    }
    if (rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT) {
        ngx_http_cache_purge_control_store(&r->shared->serving, 0);
    }
    ngx_http_cache_purge_control_unpin(r);
    return rc;
}

int
ngx_http_cache_purge_control_match(ngx_http_cache_purge_control_reader_t *r,
    const ngx_http_cache_purge_control_context_t *context, size_t max_matches,
    size_t max_bytes, ngx_http_cache_purge_control_visit_pt visit, void *data,
    uint64_t *publication, ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_control_lookup(r, context, 0, 1, max_matches,
        max_bytes, visit, data, NULL, publication, error);
}

int
ngx_http_cache_purge_control_propagation(
    ngx_http_cache_purge_control_reader_t *r,
    const ngx_http_cache_purge_control_context_t *context, int tags_known,
    size_t max_matches, size_t max_bytes,
    ngx_http_cache_purge_control_visit_pt visit, void *data,
    ngx_http_cache_purge_watermark_t *watermark, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_control_lookup(r, context, 1, tags_known,
        max_matches, max_bytes, visit, data, watermark, publication, error);
}

int
ngx_http_cache_purge_fence_encode(const ngx_http_cache_purge_fence_t *fence,
    unsigned char *output, size_t capacity, size_t *length,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_encoder_t  e;

    ngx_http_cache_purge_control_clear(error);
    if (length == NULL) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing propagation length");
    }
    *length = 0;
    if (fence == NULL || (!fence->invalidate_ms && !fence->delete_ms)
        || (output == NULL && capacity != 0))
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid propagation cutoffs");
    }
    if (output == NULL) {
        *length = NGX_HTTP_CACHE_PURGE_FENCE_SIZE;
        return 0;
    }
    if (capacity < NGX_HTTP_CACHE_PURGE_FENCE_SIZE) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "propagation output is too small");
    }
    memset(&e, 0, sizeof(e));
    e.buffer.data = output;
    e.buffer.capacity = NGX_HTTP_CACHE_PURGE_FENCE_SIZE;
    e.limit = NGX_HTTP_CACHE_PURGE_FENCE_SIZE;
    e.error = error;
    ngx_http_cache_purge_component_put_raw(&e, "NPF2", 4);
    ngx_http_cache_purge_component_put_integer(&e, fence->invalidate_ms, 8);
    ngx_http_cache_purge_component_put_integer(&e, fence->delete_ms, 8);
    if (e.rc == 0) {
        *length = e.buffer.len;
    }
    return e.rc;
}

int
ngx_http_cache_purge_fence_decode(ngx_http_cache_rocksdb_bytes_t input,
    ngx_http_cache_purge_fence_t *out, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_decoder_t  d;
    ngx_http_cache_purge_fence_t              fence;

    ngx_http_cache_purge_control_clear(error);
    if (out == NULL) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing propagation output");
    }
    memset(out, 0, sizeof(*out));
    if (input.data == NULL || input.len != NGX_HTTP_CACHE_PURGE_FENCE_SIZE
        || memcmp(input.data, "NPF2", 4) != 0)
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid propagation envelope");
    }
    memset(&d, 0, sizeof(d));
    d.data = input.data;
    d.len = input.len;
    d.at = 4;
    d.error = error;
    fence.invalidate_ms = ngx_http_cache_purge_component_integer(&d, 8);
    fence.delete_ms = ngx_http_cache_purge_component_integer(&d, 8);
    if (!fence.invalidate_ms && !fence.delete_ms) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "empty propagation cutoffs");
    }
    *out = fence;
    return 0;
}

int
ngx_http_cache_purge_control_watermark(
    ngx_http_cache_purge_control_reader_t *r,
    ngx_http_cache_purge_watermark_t *watermark, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_matcher_t  m;
    uint64_t                                version;
    int                                     rc;
    static const unsigned char zero[16] = { 0 };

    ngx_http_cache_purge_control_clear(error);
    if (watermark == NULL || publication == NULL) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing watermark output");
    }
    memset(watermark, 0, sizeof(*watermark));
    *publication = 0;
    rc = ngx_http_cache_purge_control_pin(r, 0, &version, error);
    if (rc != 0) {
        return rc;
    }
    memset(&m, 0, sizeof(m));
    m.shared = r->shared;
    m.base = ngx_http_cache_purge_control_bank(r->shared,
                                               (unsigned) (version & 1));
    m.image = (const ngx_http_cache_purge_control_header_t *) m.base;
    m.error = error;
    rc = ngx_http_cache_purge_control_matcher_init(&m);
    if (rc == 0 && memcmp(m.image->ledger, zero, sizeof(zero)) == 0) {
        rc = ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "control has no durable ledger");
    }
    if (rc == 0
        && (!ngx_http_cache_purge_control_load(&r->shared->serving)
            || ngx_http_cache_purge_control_load(&r->shared->publication)
               != version))
    {
        rc = ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "watermark publication changed");
    }
    if (rc == 0) {
        memcpy(watermark->ledger, m.image->ledger, 16);
        watermark->sequence = m.image->sequence;
        *publication = version;
    }
    if (rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT) {
        ngx_http_cache_purge_control_store(&r->shared->serving, 0);
    }
    ngx_http_cache_purge_control_unpin(r);
    return rc;
}

int
ngx_http_cache_purge_control_origins(ngx_http_cache_purge_control_reader_t *r,
    const ngx_http_cache_purge_watermark_t *oldest, int active,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_slot_t  *slot;
    uint64_t                             revision, floor, ledger[2];
    int                                  rc;
    static const unsigned char zero[16] = { 0 };

    ngx_http_cache_purge_control_clear(error);
    if ((active != 0 && active != 1) || (!active && oldest != NULL)
        || (oldest != NULL
            && (oldest->sequence == UINT64_MAX
                || memcmp(oldest->ledger, zero, sizeof(zero)) == 0)))
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid origin registration");
    }
    rc = ngx_http_cache_purge_control_reader_owner(r, error);
    if (rc != 0) {
        return rc;
    }
    slot = &ngx_http_cache_purge_control_slots(r->shared)[r->slot];
    revision = ngx_http_cache_purge_control_load(&slot->revision);
    if (revision & 1 || revision > UINT64_MAX - 2) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "origin revision exhausted");
    }
    memset(ledger, 0, sizeof(ledger));
    floor = active ? 1 : 0;
    if (oldest != NULL) {
        memcpy(ledger, oldest->ledger, sizeof(ledger));
        floor = oldest->sequence + 1;
    }
    ngx_http_cache_purge_control_store(&slot->revision, revision + 1);
    ngx_http_cache_purge_control_store(&slot->origins, floor);
    ngx_http_cache_purge_control_store(&slot->ledger[0], ledger[0]);
    ngx_http_cache_purge_control_store(&slot->ledger[1], ledger[1]);
    ngx_http_cache_purge_control_store(&slot->revision, revision + 2);
    return 0;
}

static void
ngx_http_cache_purge_control_stamp(ngx_http_cache_purge_control_image_t *image,
    const unsigned char ledger[16], uint64_t sequence)
{
    ngx_http_cache_purge_control_header_t  *header;

    header = (ngx_http_cache_purge_control_header_t *) image->data;
    memcpy(header->ledger, ledger, 16);
    header->sequence = sequence;
}

struct ngx_http_cache_purge_coordinator_s {
    ngx_http_cache_purge_operations_t       operations;
    void                                   *shared;
    size_t                                  shared_bytes;
    ngx_http_cache_purge_control_writer_t  *writer;
    ngx_http_cache_purge_control_reader_t  *validation_reader;
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

typedef struct {
    ngx_http_cache_purge_coordinator_t     *coordinator;
    ngx_http_cache_purge_retained_query_t  *history;
    ngx_http_cache_purge_control_context_t  context;
    ngx_http_cache_purge_fence_t            fence;
    ngx_http_cache_purge_fence_t            until;
    ngx_http_cache_rocksdb_bytes_t          observed_url, observed_host;
    size_t                                  remaining;
    int                                     preparing;
} ngx_http_cache_purge_propagation_context_t;

static int
ngx_http_cache_purge_component_propagation_match(void *data,
    const ngx_http_cache_purge_control_match_t *match)
{
    ngx_http_cache_purge_propagation_context_t  *p = data;
    uint64_t                                    *cutoff, *until;

    if (match->flags & NGX_HTTP_CACHE_PURGE_CONTROL_TAG_CANDIDATE) {
        return NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
    }
    if (p->remaining == 0) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }
    p->remaining--;
    cutoff = match->mode == NGX_HTTP_CACHE_PURGE_INVALIDATE
             ? &p->fence.invalidate_ms : &p->fence.delete_ms;
    until = match->mode == NGX_HTTP_CACHE_PURGE_INVALIDATE
            ? &p->until.invalidate_ms : &p->until.delete_ms;
    if (match->issued_at_ms > *cutoff) {
        *cutoff = match->issued_at_ms;
    }
    if (match->propagation_until_ms == 0) {
        p->preparing = 1;
    } else if (match->propagation_until_ms > *until) {
        *until = match->propagation_until_ms;
    }
    return 0;
}

static int
ngx_http_cache_purge_component_propagation_lookup(
    ngx_http_cache_purge_propagation_context_t *p,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_watermark_t  watermark;
    uint64_t                          publication;
    int                               rc;

    /* One callback completes before its caller advances the database cursor. */
    rc = ngx_http_cache_purge_control_propagation(
        p->coordinator->validation_reader, &p->context, 1, 1024, 262144,
        ngx_http_cache_purge_component_propagation_match, p,
        &watermark, &publication, error);
    if (rc == 0 && publication != p->coordinator->publication) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "propagation control image changed");
    }
    return rc;
}

static int
ngx_http_cache_purge_component_propagation_host(void *data,
    ngx_http_cache_rocksdb_bytes_t host, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_propagation_context_t  *p = data;

    if (ngx_http_cache_purge_control_bytes_equal(host, p->observed_host)) {
        return 0;
    }
    p->context.host = host;
    return ngx_http_cache_purge_component_propagation_lookup(p, error);
}

static int
ngx_http_cache_purge_component_propagation_url(void *data,
    ngx_http_cache_rocksdb_bytes_t url, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_propagation_context_t  *p = data;

    if (ngx_http_cache_purge_control_bytes_equal(url, p->observed_url)) {
        return 0;
    }
    p->context.url = url;
    return ngx_http_cache_purge_component_propagation_lookup(p, error);
}

static int
ngx_http_cache_purge_component_propagation_resolve(
    ngx_http_cache_purge_propagation_context_t *p,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_coordinator_t  *c;
    ngx_http_cache_rocksdb_bytes_t       key;
    int                                  rc;
    static const ngx_http_cache_rocksdb_bytes_t empty = { NULL, 0 };

    c = p->coordinator;
    key = p->context.key;
    if (c->validation_reader == NULL) {
        rc = ngx_http_cache_purge_control_reader_open(c->shared,
            c->shared_bytes, &c->validation_reader, error);
        if (rc != 0) {
            return rc;
        }
    }
    p->remaining = 1024;
    p->observed_url = p->context.url;
    p->observed_host = p->context.host;
    rc = ngx_http_cache_purge_component_propagation_lookup(p, error);
    if (rc != 0) {
        return rc;
    }
    p->context.key = empty;
    p->context.url = empty;
    p->context.host = empty;
    p->context.tag_namespace = empty;
    if (p->history != NULL) {
        rc = ngx_http_cache_purge_index_propagation_aliases(p->history,
            NGX_HTTP_CACHE_PURGE_HOST, 64, 65536,
            ngx_http_cache_purge_component_propagation_host, p, error);
    } else {
        rc = ngx_http_cache_purge_index_visit_key_hosts(c->operations.store,
            key, 64, 65536, ngx_http_cache_purge_component_propagation_host,
            p, error);
    }
    if (rc != 0) {
        return rc;
    }
    p->context.host = empty;
    p->context.tags = NULL;
    p->context.tag_count = 0;
    if (p->history != NULL) {
        return ngx_http_cache_purge_index_propagation_aliases(p->history,
            NGX_HTTP_CACHE_PURGE_URL, 64, 65536,
            ngx_http_cache_purge_component_propagation_url, p, error);
    }
    return ngx_http_cache_purge_index_visit_key_urls(c->operations.store,
        key, 64, 65536, ngx_http_cache_purge_component_propagation_url,
        p, error);
}

int
ngx_http_cache_purge_coordinator_file_propagation(
    ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_index_expected_t *expected,
    const ngx_http_cache_purge_control_context_t *request, uint64_t now,
    ngx_http_cache_purge_fence_t *out, uint64_t *publication,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_file_t           file;
    ngx_http_cache_purge_propagation_context_t  p;
    int                                         rc;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (publication != NULL) {
        *publication = 0;
    }
    if (expected == NULL || out == NULL || publication == NULL || now == 0) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid file propagation query");
    }
    if (request != NULL
        && (!request->key.len || !request->url.len || !request->host.len
            || !ngx_http_cache_purge_control_bytes_valid(request->key, 32768)
            || !ngx_http_cache_purge_control_bytes_valid(request->url, 32768)
            || !ngx_http_cache_purge_control_bytes_valid(request->host, 32768)))
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid current propagation identity");
    }
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "propagation awaits control publication");
    }
    memset(&file, 0, sizeof(file));
    rc = ngx_http_cache_purge_index_get_file(c->operations.store, expected->id,
                                              &file, error);
    if (rc != 0) {
        return rc;
    }
    if (file.state != NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED
        || file.revision != expected->revision
        || (request && !ngx_http_cache_purge_control_bytes_equal(file.key,
                                                               request->key)))
    {
        rc = ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "selected propagation file changed");
        goto done;
    }
    memset(&p, 0, sizeof(p));
    p.coordinator = c;
    p.context.key = file.key;
    p.context.tag_namespace = file.tag_namespace;
    p.context.tags = file.tags;
    p.context.tag_count = file.tag_count;
    p.context.proxy = 1;
    p.context.now_ms = now;
    if (request != NULL) {
        p.context.url = request->url;
        p.context.host = request->host;
    }
    rc = ngx_http_cache_purge_component_propagation_resolve(&p, error);
    if (rc == 0) {
        *out = p.fence;
        *publication = c->publication;
    }

done:
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}

static int
ngx_http_cache_purge_component_retention_result(
    const ngx_http_cache_purge_propagation_context_t *p,
    ngx_http_cache_purge_retention_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (p->preparing) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "retention awaits rule activation");
    }
    out->invalidate_ms = p->fence.invalidate_ms;
    out->invalidate_until_ms = p->until.invalidate_ms;
    out->delete_ms = p->fence.delete_ms;
    out->delete_until_ms = p->until.delete_ms;
    return 0;
}

int
ngx_http_cache_purge_coordinator_keep_file(
    ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_index_expected_t *expected, uint64_t now,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_file_t           file;
    ngx_http_cache_purge_propagation_context_t  p;
    ngx_http_cache_purge_retention_t            keep;
    unsigned char                               main[16], variant[16];
    int                                         rc;

    if (expected == NULL || now == 0) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid file retention input");
    }
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "retention awaits publication");
    }
    if (c->count == 0) {
        return 0;
    }
    memset(&file, 0, sizeof(file));
    rc = ngx_http_cache_purge_index_get_file(c->operations.store, expected->id,
        &file, error);
    if (rc != 0) {
        return rc;
    }
    if (file.revision != expected->revision) {
        rc = ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "retention file changed");
        goto done;
    }
    if (file.state == NGX_HTTP_CACHE_PURGE_INDEX_PREPARED
        || (file.state == NGX_HTTP_CACHE_PURGE_INDEX_RETIRED
            && file.revision == 2))
    {
        goto done;
    }
    memset(&p, 0, sizeof(p));
    p.coordinator = c;
    p.context.key = file.key;
    p.context.tag_namespace = file.tag_namespace;
    p.context.tags = file.tags;
    p.context.tag_count = file.tag_count;
    p.context.now_ms = now;
    p.context.proxy = 1;
    rc = ngx_http_cache_purge_component_propagation_resolve(&p, error);
    if (rc != 0 || (!p.fence.invalidate_ms && !p.fence.delete_ms)) {
        goto done;
    }
    rc = ngx_http_cache_purge_component_retention_result(&p, &keep, error);
    if (rc != 0) {
        goto done;
    }
    rc = ngx_http_cache_purge_publication_native(&file, main, variant, error);
    if (rc != 0) {
        goto done;
    }
    rc = ngx_http_cache_purge_index_keep_propagation(c->operations.store,
        expected, main, variant, now, &keep, error);
done:
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}

static int
ngx_http_cache_purge_component_retention_guard(void *data,
    const ngx_http_cache_purge_retained_context_t *snapshot, uint64_t now,
    ngx_http_cache_purge_retention_t *keep,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_propagation_context_t  p;
    int                                         rc;

    memset(&p, 0, sizeof(p));
    p.coordinator = data;
    p.context.key = snapshot->key;
    p.context.tag_namespace = snapshot->tag_namespace;
    p.context.tags = snapshot->tags;
    p.context.tag_count = snapshot->tag_count;
    p.context.now_ms = now;
    p.context.proxy = 1;
    rc = ngx_http_cache_purge_component_propagation_resolve(&p, error);
    return rc ? rc : ngx_http_cache_purge_component_retention_result(&p, keep,
        error);
}

int
ngx_http_cache_purge_coordinator_expire_propagation(
    ngx_http_cache_purge_coordinator_t *c, uint64_t now, size_t budget,
    size_t *processed, ngx_http_cache_rocksdb_error_t *error)
{
    int  rc;

    if (processed != NULL) {
        *processed = 0;
    }
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "expiry awaits control publication");
    }
    return ngx_http_cache_purge_index_expire_propagation_checked(
        c->operations.store, now, budget,
        c->count ? ngx_http_cache_purge_component_retention_guard : NULL,
        c, processed, error);
}

struct ngx_http_cache_purge_history_query_s {
    ngx_http_cache_purge_coordinator_t    *coordinator;
    ngx_http_cache_purge_retained_query_t *history;
    ngx_http_cache_purge_fence_t           result;
    uint64_t                               now, publication;
    int                                    found, done, status;
};

int
ngx_http_cache_purge_coordinator_history_open(
    ngx_http_cache_purge_coordinator_t *c,
    ngx_http_cache_rocksdb_bytes_t key, const unsigned char main[16],
    ngx_http_cache_rocksdb_bytes_t vary, const unsigned char variant[16],
    uint64_t now, ngx_http_cache_purge_history_query_t **out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_history_query_t  *query;
    int                                    rc;

    if (out != NULL) {
        *out = NULL;
    }
    if (out == NULL || now == 0) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid historical query");
    }
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "history awaits control publication");
    }
    query = calloc(1, sizeof(*query));
    if (query == NULL) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "history query allocation failed");
    }
    rc = ngx_http_cache_purge_index_propagation_open(c->operations.store,
        key, main, vary, variant, &query->history, error);
    if (rc != 0) {
        free(query);
        return rc;
    }
    query->coordinator = c;
    query->publication = c->publication;
    query->now = now;
    *out = query;
    return 0;
}

int
ngx_http_cache_purge_coordinator_history_step(
    ngx_http_cache_purge_history_query_t *query, size_t budget,
    ngx_http_cache_purge_fence_t *out, uint64_t *publication, int *done,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_retained_context_t     snapshot;
    ngx_http_cache_purge_propagation_context_t  p;
    ngx_http_cache_purge_coordinator_t         *c;
    size_t                                      i;
    int                                         rc;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (publication != NULL) {
        *publication = 0;
    }
    if (done != NULL) {
        *done = 0;
    }
    if (query == NULL || out == NULL || publication == NULL || done == NULL
        || budget == 0 || budget > 32)
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid historical query step");
    }
    c = query->coordinator;
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (query->status != 0) {
        return ngx_http_cache_purge_control_error(error, query->status,
            "historical query must be restarted");
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY
        || c->publication != query->publication)
    {
        query->status = NGX_HTTP_CACHE_ROCKSDB_BUSY;
        return ngx_http_cache_purge_control_error(error, query->status,
            "historical query control publication changed");
    }
    for (i = 0; i < budget && !query->done; i++) {
        rc = ngx_http_cache_purge_index_propagation_next(query->history,
            &snapshot, error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            query->done = 1;
            break;
        }
        if (rc != 0) {
            query->status = rc;
            return rc;
        }
        query->found = 1;
        memset(&p, 0, sizeof(p));
        p.coordinator = c;
        p.history = query->history;
        p.context.key = snapshot.key;
        p.context.tag_namespace = snapshot.tag_namespace;
        p.context.tags = snapshot.tags;
        p.context.tag_count = snapshot.tag_count;
        p.context.now_ms = query->now;
        p.context.proxy = 1;
        if (query->now < snapshot.times.invalidate_until_ms) {
            p.fence.invalidate_ms = snapshot.times.invalidate_ms;
        }
        if (query->now < snapshot.times.delete_until_ms) {
            p.fence.delete_ms = snapshot.times.delete_ms;
        }
        rc = ngx_http_cache_purge_component_propagation_resolve(&p, error);
        if (rc != 0) {
            query->status = rc;
            return rc;
        }
        if (p.fence.invalidate_ms > query->result.invalidate_ms) {
            query->result.invalidate_ms = p.fence.invalidate_ms;
        }
        if (p.fence.delete_ms > query->result.delete_ms) {
            query->result.delete_ms = p.fence.delete_ms;
        }
    }
    if (query->done) {
        if (!query->found) {
            query->status = NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
            return ngx_http_cache_purge_control_error(error, query->status,
                "historical membership is absent");
        }
        *out = query->result;
        *publication = query->publication;
        *done = 1;
    }
    return ngx_http_cache_purge_control_error(error, 0, "");
}

int
ngx_http_cache_purge_coordinator_history_close(
    ngx_http_cache_purge_history_query_t **query,
    ngx_http_cache_rocksdb_error_t *error)
{
    int  rc;

    if (query == NULL || *query == NULL) {
        return ngx_http_cache_purge_control_error(error, 0, "");
    }
    rc = ngx_http_cache_purge_index_propagation_close(&(*query)->history,
        error);
    if (rc == 0) {
        free(*query);
        *query = NULL;
    }
    return rc;
}

typedef struct {
    ngx_http_cache_purge_coordinator_t     *coordinator;
    ngx_http_cache_purge_control_context_t  context;
    uint64_t                                covered, validated, required;
    uint64_t                               *remove;
    size_t                                  budget;
    int                                     allow_invalidate;
} ngx_http_cache_purge_validation_context_t;

static int
ngx_http_cache_purge_component_validation_match(void *data,
    const ngx_http_cache_purge_control_match_t *match)
{
    ngx_http_cache_purge_validation_context_t  *v;

    v = data;
    if (!(match->flags & NGX_HTTP_CACHE_PURGE_CONTROL_FENCE)
        || match->sequence <= (match->mode == NGX_HTTP_CACHE_PURGE_INVALIDATE
                              ? v->validated : v->covered))
    {
        return 0;
    }
    if (match->mode == NGX_HTTP_CACHE_PURGE_INVALIDATE && v->allow_invalidate) {
        if (match->sequence > v->required) {
            v->required = match->sequence;
        }
        return 0;
    }
    if (match->mode == NGX_HTTP_CACHE_PURGE_DELETE && v->remove != NULL) {
        *v->remove = match->sequence;
    }
    return NGX_HTTP_CACHE_ROCKSDB_CONFLICT;
}

static int
ngx_http_cache_purge_component_validation_rules(
    ngx_http_cache_purge_validation_context_t *v, uint32_t kind,
    ngx_http_cache_rocksdb_bytes_t value, ngx_http_cache_rocksdb_bytes_t scope,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_selector_t       selector;
    ngx_http_cache_purge_control_match_t  match;
    uint64_t                               invalidate, remove;
    int                                    rc;

    selector.kind = kind;
    selector.value = value;
    selector.scope = scope;
    rc = ngx_http_cache_purge_operations_requirements(
        &v->coordinator->operations, &selector, &v->budget,
        &invalidate, &remove, error);
    if (rc != 0) {
        return rc;
    }
    if (invalidate > v->coordinator->expected.last_sequence
        || remove > v->coordinator->expected.last_sequence)
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "selector requirement is outside the operation ledger");
    }
    memset(&match, 0, sizeof(match));
    match.flags = NGX_HTTP_CACHE_PURGE_CONTROL_FENCE;
    match.mode = NGX_HTTP_CACHE_PURGE_DELETE;
    match.sequence = remove;
    rc = ngx_http_cache_purge_component_validation_match(v, &match);
    if (rc == 0) {
        match.mode = NGX_HTTP_CACHE_PURGE_INVALIDATE;
        match.sequence = invalidate;
        rc = ngx_http_cache_purge_component_validation_match(v, &match);
    }
    if (rc != 0) {
        return ngx_http_cache_purge_control_error(error, rc,
            "file has an unmet persistent selector requirement");
    }
    return 0;
}

static int
ngx_http_cache_purge_component_validation_host(void *data,
    ngx_http_cache_rocksdb_bytes_t host, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_validation_context_t  *v;
    uint64_t                                    publication;
    size_t                                      i;
    int                                         rc;
    static const ngx_http_cache_rocksdb_bytes_t empty = { NULL, 0 };

    v = data;
    v->context.host = host;
    v->context.url = empty;
    rc = ngx_http_cache_purge_component_validation_rules(v,
        NGX_HTTP_CACHE_PURGE_HOST, host, empty, error);
    for (i = 0; rc == 0 && i < v->context.tag_count; i++) {
        rc = ngx_http_cache_purge_component_validation_rules(v,
            NGX_HTTP_CACHE_PURGE_HOST_TAG, v->context.tags[i], host, error);
    }
    if (rc != 0 || v->coordinator->count == 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_control_match(
        v->coordinator->validation_reader, &v->context, 64, 65536,
        ngx_http_cache_purge_component_validation_match, v,
        &publication, error);
    if (rc == 0 && publication != v->coordinator->publication) {
        rc = ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "host validation image changed");
    }
    return rc;
}

static int
ngx_http_cache_purge_component_validation_url(void *data,
    ngx_http_cache_rocksdb_bytes_t url, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_validation_context_t  *v;
    uint64_t                                    publication;
    int                                         rc;
    static const ngx_http_cache_rocksdb_bytes_t empty = { NULL, 0 };

    v = data;
    v->context.url = url;
    rc = ngx_http_cache_purge_component_validation_rules(v,
        NGX_HTTP_CACHE_PURGE_URL, url, empty, error);
    if (rc != 0 || v->coordinator->count == 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_control_match(
        v->coordinator->validation_reader, &v->context, 64, 65536,
        ngx_http_cache_purge_component_validation_match, v,
        &publication, error);
    if (rc == 0 && publication != v->coordinator->publication) {
        rc = ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "validation control image changed");
    }
    return rc;
}

static int
ngx_http_cache_purge_component_record_guard(
    ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_bytes_t metadata,
    const ngx_http_cache_purge_watermark_t *validation, uint64_t *invalidate,
    uint64_t *remove, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_validation_context_t  v;
    ngx_http_cache_purge_index_expected_t      expected;
    ngx_http_cache_purge_watermark_t           watermark, validated, required;
    ngx_http_cache_purge_watermark_t           deletion;
    ngx_http_cache_rocksdb_bytes_t             empty;
    size_t                                     i;
    int                                        rc;

    if (invalidate != NULL) {
        *invalidate = 0;
    }
    if (remove != NULL) {
        *remove = 0;
    }
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "validation awaits control publication");
    }
    memcpy(expected.id, file->id, 16);
    expected.revision = file->revision;
    rc = ngx_http_cache_purge_index_validation_requirement(c->operations.store,
        &expected, &required, error);
    if (rc != 0) {
        return rc;
    }
    if (required.sequence != 0
        && (memcmp(required.ledger, c->operations.ledger, 16) != 0
            || required.sequence > c->expected.last_sequence))
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "file validation requirement is outside the operation ledger");
    }
    rc = ngx_http_cache_purge_index_delete_requirement(c->operations.store,
                                                        &expected, &deletion,
                                                        error);
    if (rc != 0) {
        return rc;
    }
    if (deletion.sequence != 0
        && (memcmp(deletion.ledger, c->operations.ledger, 16) != 0
            || deletion.sequence > c->expected.last_sequence))
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "file Delete requirement is outside the operation ledger");
    }
    if (c->count != 0 && c->validation_reader == NULL) {
        rc = ngx_http_cache_purge_control_reader_open(c->shared,
            c->shared_bytes, &c->validation_reader, error);
        if (rc != 0) {
            return rc;
        }
    }
    memset(&v, 0, sizeof(v));
    v.coordinator = c;
    v.remove = remove;
    v.allow_invalidate = invalidate != NULL;
    v.budget = 512;
    rc = ngx_http_cache_purge_publication_watermarks(metadata, &watermark,
                                                       &validated, error);
    if (rc != 0 && rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        return rc;
    }
    if (rc == 0 && memcmp(watermark.ledger, c->operations.ledger, 16) == 0) {
        if (watermark.sequence > c->expected.last_sequence) {
            return ngx_http_cache_purge_control_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "file watermark is in future");
        }
        v.covered = watermark.sequence;
    }
    if (deletion.sequence > v.covered) {
        if (remove != NULL) {
            *remove = deletion.sequence;
        }
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "file is queued for Delete");
    }
    if (rc == 0 && memcmp(validated.ledger, c->operations.ledger, 16) == 0) {
        if (validated.sequence > c->expected.last_sequence) {
            return ngx_http_cache_purge_control_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "validation watermark is in future");
        }
        v.validated = validated.sequence;
    }
    if (validation != NULL
        && memcmp(validation->ledger, c->operations.ledger, 16) == 0)
    {
        v.validated = validation->sequence;
    }
    if (required.sequence > v.validated) {
        if (invalidate == NULL) {
            return ngx_http_cache_purge_control_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "file still requires origin validation");
        }
        v.required = required.sequence;
    }
    if (v.covered >= c->expected.last_sequence && v.required == 0) {
        return 0;
    }
    empty.data = NULL;
    empty.len = 0;
    rc = ngx_http_cache_purge_component_validation_rules(&v,
        NGX_HTTP_CACHE_PURGE_KEY, file->key, empty, error);
    for (i = 0; rc == 0 && i < file->tag_count; i++) {
        if (file->tag_namespace.len != 0) {
            rc = ngx_http_cache_purge_component_validation_rules(&v,
                NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG, file->tags[i],
                file->tag_namespace, error);
        }
    }
    if (rc != 0) {
        return rc;
    }
    v.context.key = file->key;
    v.context.tag_namespace = file->tag_namespace;
    v.context.tags = file->tags;
    v.context.tag_count = file->tag_count;
    rc = ngx_http_cache_purge_index_visit_hosts(c->operations.store,
        &expected, 64, 65536, ngx_http_cache_purge_component_validation_host,
        &v, error);
    if (rc != 0) {
        return rc;
    }
    v.context.host = empty;

    /* Each match unpins its image before the next database read. Acceptance
     * and metadata updates share this executor, so none can interleave here. */
    rc = ngx_http_cache_purge_index_visit_urls(c->operations.store,
        &expected, 64, 65536, ngx_http_cache_purge_component_validation_url,
        &v, error);
    if (rc == 0 && invalidate != NULL) {
        *invalidate = v.required;
    }
    return rc;
}

static int
ngx_http_cache_purge_component_read_stamp(
    ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_index_file_t *file, uint64_t *publication,
    uint64_t *invalidate,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_expected_t  expected;
    ngx_http_cache_purge_watermark_t       required;
    uint64_t                               remove;
    int                                    rc;

    *publication = 0;
    *invalidate = 0;
    if (c == NULL) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "read admission has no coordinator");
    }
    if (file->state != NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED) {
        return ngx_http_cache_purge_component_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "read instance is not committed");
    }
    rc = ngx_http_cache_purge_component_record_guard(c, file,
        file->metadata, NULL, invalidate, &remove, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT && remove != 0) {
        memcpy(expected.id, file->id, 16);
        expected.revision = file->revision;
        memcpy(required.ledger, c->operations.ledger, 16);
        required.sequence = remove;
        rc = ngx_http_cache_purge_index_require_delete(c->operations.store,
                                                       &expected, &required,
                                                       error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_control_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "file is queued for Delete");
        }
    }
    if (rc == 0) {
        *publication = c->publication;
    }
    return rc;
}

static int
ngx_http_cache_purge_component_validation_guard(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_validation_guard_t  *guard;
    int                                       rc;

    guard = data;
    rc = ngx_http_cache_purge_component_preserve(guard->component, file, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_operations_origin(guard->operations,
                                                  &guard->origin, error);
    if (rc != 0) {
        return rc;
    }
    return ngx_http_cache_purge_component_record_guard(guard->coordinator,
        file, file->metadata, &guard->origin, NULL, NULL, error);
}

static int
ngx_http_cache_purge_component_validation_finish_guard(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_t  *c;
    ngx_http_cache_purge_watermark_t   origin, validated;
    int                                 rc;

    c = data;
    rc = ngx_http_cache_purge_component_preserve(c, file, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_publication_watermarks(file->metadata,
                                                      &origin, &validated,
                                                      error);
    if (rc != 0 && rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        return rc;
    }
    rc = ngx_http_cache_purge_operations_origin(&c->operations,
                                                  rc == 0 ? &validated : NULL,
                                                  error);
    if (rc != 0) {
        return rc;
    }
    return ngx_http_cache_purge_component_record_guard(c->coordinator,
        file, file->metadata, NULL, NULL, NULL, error);
}

static int
ngx_http_cache_purge_component_publication_guard(void *data,
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_t    *component;
    ngx_http_cache_purge_coordinator_t  *c;
    ngx_http_cache_purge_index_file_t    previous;
    ngx_http_cache_purge_watermark_t     older, newer;
    int                                  rc;
    static const unsigned char zero[16] = { 0 };

    component = data;
    c = component->coordinator;
    rc = ngx_http_cache_purge_publication_watermark(file->metadata, &newer,
                                                     error);
    if (rc == 0 || rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        rc = ngx_http_cache_purge_operations_origin(&component->operations,
            rc == 0 ? &newer : NULL, error);
    }
    if (rc != 0 || c == NULL) {
        return rc;
    }
    rc = ngx_http_cache_purge_component_record_guard(c, file,
        file->metadata, NULL, NULL, NULL, error);
    if (rc != 0 || memcmp(file->previous, zero, sizeof(zero)) == 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_index_get_file(c->operations.store,
                                              file->previous, &previous, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "fill predecessor is missing");
    }
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_publication_watermark(file->metadata, &newer,
                                                     error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        memset(&newer, 0, sizeof(newer));
        rc = 0;
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_watermark(previous.metadata,
                                                         &older, error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            rc = 0;
        } else if (rc == 0
                   && memcmp(older.ledger, c->operations.ledger, 16) == 0
                   && (memcmp(newer.ledger, older.ledger, 16)
                       || newer.sequence < older.sequence))
        {
            rc = ngx_http_cache_purge_control_error(error,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "fill watermark regressed");
        }
    }
    if (rc != 0) {
        ngx_http_cache_purge_index_file_free(&previous);
        return rc;
    }
    rc = ngx_http_cache_purge_component_record_guard(c, &previous,
        file->metadata, NULL, NULL, NULL, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_component_preserve(component, &previous,
            error);
    }
    ngx_http_cache_purge_index_file_free(&previous);
    return rc;
}

int
ngx_http_cache_purge_coordinator_writers(ngx_http_cache_purge_coordinator_t *c,
    uint64_t sequence, uint32_t *writers,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_control_shared_t  *shared;
    ngx_http_cache_purge_control_slot_t    *slot;
    uint64_t                               first, floor, ledger[2];
    uint32_t                               i, count;
    int                                    rc;

    ngx_http_cache_purge_control_clear(error);
    if (writers == NULL || sequence == 0) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid writer status request");
    }
    *writers = 0;
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "writer status awaits publication");
    }
    if (sequence > c->expected.last_sequence) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "writer cutoff is in future");
    }
    shared = c->writer->shared;
    rc = ngx_http_cache_purge_control_lock(shared, error);
    if (rc != 0) {
        return rc;
    }
    ngx_http_cache_purge_control_reclaim(shared);
    count = 0;
    for (i = 0; i < shared->readers; i++) {
        slot = &ngx_http_cache_purge_control_slots(shared)[i];
        if (!ngx_http_cache_purge_control_load(&slot->token)) {
            continue;
        }
        first = ngx_http_cache_purge_control_load(&slot->revision);
        floor = ngx_http_cache_purge_control_load(&slot->origins);
        ledger[0] = ngx_http_cache_purge_control_load(&slot->ledger[0]);
        ledger[1] = ngx_http_cache_purge_control_load(&slot->ledger[1]);
        if (first & 1
            || first != ngx_http_cache_purge_control_load(&slot->revision))
        {
            rc = ngx_http_cache_purge_control_error(error,
                NGX_HTTP_CACHE_ROCKSDB_BUSY, "origin registration changed");
            break;
        }
        if (floor && (floor <= sequence
                      || floor - 1 > c->expected.last_sequence
                      || memcmp(ledger, c->operations.ledger, 16)))
        {
            count++;
        }
    }
    pthread_mutex_unlock(&shared->mutex);
    if (rc == 0) {
        *writers = count;
    }
    return rc;
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
ngx_http_cache_purge_control_checkpoint_pin(
    ngx_http_cache_purge_operation_t *dst,
    const ngx_http_cache_purge_operation_t *src,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_component_decoder_t  decoder;
    unsigned char                             id[16];
    uint64_t                                  revision;
    int                                       pending;
    static const unsigned char zero[16] = { 0 };

    dst->pending = src->pending;
    dst->bookmark.len = 0;
    if (!src->pending || src->bookmark.len < 4
        || memcmp(src->bookmark.data, "NDW1", 4) != 0)
    {
        /* Opaque pending work conservatively prevents collection. */
        return 0;
    }
    memset(&decoder, 0, sizeof(decoder));
    decoder.data = src->bookmark.data;
    decoder.len = src->bookmark.len;
    decoder.at = 4;
    decoder.error = error;
    pending = ngx_http_cache_purge_component_flag(&decoder);
    ngx_http_cache_purge_component_id(&decoder, id);
    revision = ngx_http_cache_purge_component_integer(&decoder, 8);
    (void) ngx_http_cache_purge_component_string(&decoder, 32991);
    if (ngx_http_cache_purge_component_end(&decoder) != 0
        || src->pending != 1 || !pending || revision == 0
        || memcmp(id, zero, sizeof(id)) == 0)
    {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid task file pin");
    }
    memcpy((unsigned char *) dst->bookmark.data, id, sizeof(id));
    dst->bookmark.len = sizeof(id);
    return 0;
}

static int
ngx_http_cache_purge_coordinator_checkpoint_pin(
    ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_operation_t *op,
    ngx_http_cache_rocksdb_error_t *error)
{
    size_t  low, high, middle;
    int     rc;

    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    low = 0;
    high = c->count;
    while (low < high) {
        middle = low + (high - low) / 2;
        if (c->controls[middle].sequence < op->sequence) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if (low == c->count || c->controls[low].sequence != op->sequence) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "checkpoint has no resident task");
    }
    return ngx_http_cache_purge_control_checkpoint_pin(&c->controls[low], op,
                                                        error);
}

static int
ngx_http_cache_purge_coordinator_collectible(
    ngx_http_cache_purge_coordinator_t *c, const unsigned char id[16],
    ngx_http_cache_rocksdb_error_t *error)
{
    size_t  i;
    int     rc;

    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "collection awaits task recovery");
    }
    for (i = 0; i < c->count; i++) {
        if (c->controls[i].pending
            && (c->controls[i].bookmark.len != 16
                || memcmp(c->controls[i].bookmark.data, id, 16) == 0))
        {
            return ngx_http_cache_purge_control_error(error,
                NGX_HTTP_CACHE_ROCKSDB_BUSY, "file is pinned by pending work");
        }
    }
    return 0;
}

static int
ngx_http_cache_purge_control_control_copy(ngx_http_cache_purge_operation_t *dst,
    const ngx_http_cache_purge_operation_t *src,
    ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char  *p;
    size_t          size;
    int             rc;

    memset(dst, 0, sizeof(*dst));
    size = src->input.id.len + src->input.selector.value.len
           + src->input.selector.scope.len + src->input.binding.len + 16;
    dst->storage = malloc(size ? size : 1);
    if (dst->storage == NULL) {
        return ngx_http_cache_purge_control_error(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control allocation failed");
    }

    /* Retain only a compact file pin, not disk cursors or work statistics. */
    p = ngx_http_cache_purge_control_copy(dst->storage, src->input.id,
                                          &dst->input.id);
    p = ngx_http_cache_purge_control_copy(p, src->input.selector.value,
                                          &dst->input.selector.value);
    p = ngx_http_cache_purge_control_copy(p, src->input.selector.scope,
                                          &dst->input.selector.scope);
    p = ngx_http_cache_purge_control_copy(p, src->input.binding,
                                            &dst->input.binding);
    dst->bookmark.data = p;
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
    rc = ngx_http_cache_purge_control_checkpoint_pin(dst, src, error);
    if (rc != 0) {
        ngx_http_cache_purge_operation_free(dst);
    }
    return rc;
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
    if (c->validation_reader != NULL) {
        rc = ngx_http_cache_purge_control_reader_close(&c->validation_reader,
                                                       error);
        if (rc != 0) {
            return rc;
        }
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
            || usage.last_sequence != c->expected.last_sequence
            || usage.retired_sequence != c->expected.retired_sequence)
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
            ngx_http_cache_purge_control_stamp(c->pending,
                c->operations.ledger, c->expected.last_sequence);
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

static int
ngx_http_cache_purge_coordinator_accept_internal(
    ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, int submit, ngx_http_cache_purge_operation_t *out,
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
        if (submit) {
            ngx_http_cache_purge_operation_free(&preview);
            rc = ngx_http_cache_purge_operations_submit(&c->operations, input,
                                            now, propagation_ms, out, error);
            if (rc != 0) {
                ngx_http_cache_purge_control_gate_close(c->writer, NULL);
                ngx_http_cache_purge_control_load_begin(c, NULL);
            }
            return rc;
        }
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
        ngx_http_cache_purge_control_stamp(image, c->operations.ledger,
                                             preview.sequence);
        rc = ngx_http_cache_purge_control_gate_close(c->writer, error);
    }
    if (rc != 0) {
        ngx_http_cache_purge_operation_free(&c->controls[--c->count]);
        ngx_http_cache_purge_control_image_free(&image);
        goto done;
    }
    rc = submit ? ngx_http_cache_purge_operations_submit(&c->operations, input,
                                        now, propagation_ms, out, error) :
                  ngx_http_cache_purge_operations_accept(&c->operations, input,
                                        now, propagation_ms, out, error);
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

int
ngx_http_cache_purge_coordinator_accept(ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_coordinator_accept_internal(c, input, now,
                                                propagation_ms, 0, out, error);
}

int
ngx_http_cache_purge_coordinator_submit(ngx_http_cache_purge_coordinator_t *c,
    const ngx_http_cache_purge_operation_input_t *input, uint64_t now,
    uint32_t propagation_ms, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_coordinator_accept_internal(c, input, now,
                                                propagation_ms, 1, out, error);
}

int
ngx_http_cache_purge_coordinator_activate(
    ngx_http_cache_purge_coordinator_t *c, ngx_http_cache_rocksdb_bytes_t id,
    uint64_t now, ngx_http_cache_purge_operation_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_operation_t  before;
    int                               rc;

    if (out == NULL) {
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "null activation output");
    }
    memset(out, 0, sizeof(*out));
    rc = ngx_http_cache_purge_control_coordinator_owner(c, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_operations_get(&c->operations, id, &before,
                                               error);
    if (rc != 0) {
        return rc;
    }
    if (c->state != NGX_HTTP_CACHE_PURGE_COORD_READY
        || before.sequence > c->expected.last_sequence)
    {
        ngx_http_cache_purge_operation_free(&before);
        return ngx_http_cache_purge_control_error(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "activation awaits FENCE publication");
    }
    if (before.stage != NGX_HTTP_CACHE_PURGE_PREPARING) {
        *out = before;
        return 0;
    }
    rc = ngx_http_cache_purge_operations_activate(&c->operations, id,
        before.revision, now, out, error);
    ngx_http_cache_purge_operation_free(&before);
    if (rc == 0) {
        rc = ngx_http_cache_purge_coordinator_refresh(c, error);
    } else {
        /* Activation write errors are ambiguous; recover from the ledger. */
        (void) ngx_http_cache_purge_control_load_begin(c, NULL);
    }
    if (rc != 0) {
        ngx_http_cache_purge_operation_free(out);
    }
    return rc;
}
