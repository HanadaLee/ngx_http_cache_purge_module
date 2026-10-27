/*
 * Copyright (c) 2009-2014, FRiCKLE <info@frickle.com>
 * Copyright (c) 2009-2014, Piotr Sikora <piotr.sikora@frickle.com>
 * Copyright (C) 2016-2026, Denys Denysov
 * Copyright (C) Hanada
 * Copyright (C) Igor Sysoev
 * Copyright (C) Nginx, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <ngx_config.h>
#include "ngx_http_cache_purge.h"

#if (NGX_HTTP_CACHE)

struct ngx_http_cache_purge_fill_s {
    ngx_http_request_t   *request;
    ngx_http_cache_t     *cache;
    ngx_http_upstream_t  *upstream;
    ngx_temp_file_t      *temp_file;
    ngx_http_cache_purge_fill_step_pt step;
    void                             *data;
    void (*finalize)(ngx_http_request_t *r, ngx_int_t rc);
    ngx_event_t  event;
    ngx_uint_t   phase;
    ngx_int_t    result;
    unsigned     armed : 1;
    unsigned     active : 1;
    unsigned     waiting : 1;
    unsigned     native_update : 1;
};

static void ngx_http_cache_purge_fill_finalize(ngx_http_request_t *r,
    ngx_int_t rc);
static void ngx_http_cache_purge_fill_begin(ngx_event_t *event);
static void ngx_http_cache_purge_fill_step(ngx_http_cache_purge_fill_t *f,
    ngx_int_t rc);

static void
ngx_http_cache_purge_fill_cleanup(void *data)
{
    ngx_http_cache_purge_fill_t *f = data;

    if (f->active) {
        ngx_log_error(NGX_LOG_ALERT, f->request->connection->log, 0,
                      "cache fill released with an outstanding callback");
    }
    if (f->armed) {
        f->cache->updated = 0;
        ngx_http_file_cache_free(f->cache, NULL);
    }
}

ngx_int_t
ngx_http_cache_purge_fill_arm(ngx_http_request_t *r,
    ngx_http_cache_purge_fill_t **slot, ngx_http_cache_purge_fill_step_pt step,
    void *data)
{
    ngx_http_cache_purge_fill_t  *f;
    ngx_http_upstream_t          *u = r->upstream;
    ngx_pool_cleanup_t           *cleanup;

    if (slot == NULL || step == NULL) {
        return NGX_ERROR;
    }
    if (*slot != NULL) {
        return (*slot)->cache == r->cache && (*slot)->upstream == u ? NGX_OK :
                                                                      NGX_ERROR;
    }
    if (r->cache == NULL || u == NULL || r->cached || r->cache->updated
        || !u->buffering || !u->cacheable || u->upgrade)
    {
        return NGX_DECLINED;
    }
    if (u->finalize_request == NULL
        || u->finalize_request == ngx_http_cache_purge_fill_finalize)
    {
        return NGX_ERROR;
    }
    cleanup = ngx_pool_cleanup_add(r->pool, sizeof(*f));
    if (cleanup == NULL) {
        return NGX_ERROR;
    }
    f = cleanup->data;
    ngx_memzero(f, sizeof(*f));
    cleanup->handler = ngx_http_cache_purge_fill_cleanup;
    f->request = r;
    f->cache = r->cache;
    f->upstream = u;
    f->step = step;
    f->data = data;
    f->finalize = u->finalize_request;
    f->armed = 1;
    *slot = f;

    /* Native header/Vary construction still runs. Defer only update/free;
     * the finalizer either restores native cleanup or takes this reference. */
    r->cache->updated = 1;
    u->finalize_request = ngx_http_cache_purge_fill_finalize;
    return NGX_OK;
}

static void
ngx_http_cache_purge_fill_finalize(ngx_http_request_t *r, ngx_int_t rc)
{
    ngx_pool_cleanup_t           *cleanup;
    ngx_http_cache_purge_fill_t  *f = NULL;
    ngx_event_pipe_t             *p = r->upstream->pipe;

    /* Pool-owned identity survives module-context resets on redirects. */
    for (cleanup = r->pool->cleanup; cleanup; cleanup = cleanup->next) {
        if (cleanup->handler == ngx_http_cache_purge_fill_cleanup) {
            f = cleanup->data;
            if (f->armed && f->request == r && f->upstream == r->upstream) {
                break;
            }
            f = NULL;
        }
    }
    if (f == NULL) {
        ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0,
                      "cache fill lost upstream finalizer");
        return;
    }
    f->upstream->finalize_request = f->finalize;
    f->armed = 0;

    if (rc == NGX_OK && f->upstream->cacheable && r->cache == f->cache && p
        && p->temp_file && p->temp_file->file.fd != NGX_INVALID_FILE
        && f->cache->node && !p->writing && !p->upstream_error
        && r->main->blocked < NGX_HTTP_MAX_BLOCKED && r->main->count < 65535
        && !r->main->terminated
        && (p->upstream_done
            || (p->upstream_eof && p->length == -1
                && (f->upstream->headers_in.content_length_n == -1
                    || f->upstream->headers_in.content_length_n
                           == p->temp_file->offset
                                  - (off_t) f->cache->body_start))))
    {
        f->temp_file = p->temp_file;
        f->active = 1;
        f->phase = NGX_HTTP_CACHE_PURGE_FILL_PREPARE;
        f->event.data = f;
        f->event.handler = ngx_http_cache_purge_fill_begin;
        f->event.log = r->connection->log;
        r->main->blocked++;
        r->main->count++;
        /* Run only after the native upstream finalizer has returned. */
        ngx_post_event(&f->event, &ngx_posted_events);
    } else {
        f->cache->updated = 0;
    }
    f->finalize(r, rc);
}

static void
ngx_http_cache_purge_fill_begin(ngx_event_t *event)
{
    ngx_http_cache_purge_fill_t  *f = event->data;
    ngx_http_request_t           *r = f->request;

    if (r->aio) {
        /* Output AIO may outlive upstream completion; never steal its hold. */
        ngx_add_timer(event, 1);
        return;
    }
    ngx_http_cache_purge_fill_step(
        f, f->step(r, f->cache, f->temp_file, f->phase, NGX_OK, f->data));
}

static void
ngx_http_cache_purge_fill_step(ngx_http_cache_purge_fill_t *f, ngx_int_t rc)
{
    ngx_http_request_t     *r = f->request;
    ngx_http_cache_t       *c = f->cache;
    ngx_http_file_cache_t  *cache = c->file_cache;
    ngx_connection_t       *connection = r->connection;

    if (rc == NGX_AGAIN) {
        f->waiting = 1;
        return;
    }
    if (f->phase == NGX_HTTP_CACHE_PURGE_FILL_PREPARE) {
        if (r->main->terminated || r->cache != c) {
            rc = NGX_ABORT;
        }
        if (rc == NGX_OK) {
            ngx_shmtx_lock(&cache->shpool->mutex);
            c->node->count++;
            ngx_shmtx_unlock(&cache->shpool->mutex);
            f->native_update = 1;
            c->updated = 0;
            /* Native update returns void on unmodified nginx. FINISH must
             * inspect the final instance, not rely on a rename status. */
            (void) ngx_http_file_cache_update(r, f->temp_file);
        }
        f->result = rc;
        f->phase = NGX_HTTP_CACHE_PURGE_FILL_FINISH;
        rc = f->step(r, c, f->temp_file, f->phase, f->result, f->data);
        if (rc == NGX_AGAIN) {
            f->waiting = 1;
            return;
        }
    }
    f->active = 0;
    if (f->native_update) {
        ngx_shmtx_lock(&cache->shpool->mutex);
        c->node->count--;
        ngx_shmtx_unlock(&cache->shpool->mutex);
    } else {
        c->updated = 0;
        ngx_http_file_cache_free(c, f->temp_file);
    }
    r->main->blocked--;
    if (r->main->terminated) {
        r->main->count--;
        connection->write->handler(connection->write);
        return;
    }
    if (r->main->blocked) {
        ngx_http_close_request(r, 0);
    } else {
        ngx_http_finalize_request(r->main, NGX_DONE);
    }
}

void
ngx_http_cache_purge_fill_complete(ngx_http_cache_purge_fill_t *f, ngx_int_t rc)
{
    if (!f->active || !f->waiting) {
        ngx_log_error(NGX_LOG_ALERT, f->request->connection->log, 0,
                      "unexpected cache fill completion");
        return;
    }
    f->waiting = 0;
    ngx_http_cache_purge_fill_step(f, rc == NGX_AGAIN ? NGX_ERROR : rc);
}

#endif


#if (NGX_HTTP_CACHE_PURGE_ROCKSDB)

#include <sys/random.h>
#include <fcntl.h>

#if (NGX_HTTP_CACHE_KEY)
#include <ngx_http_cache_key_module.h>
#endif

#if (NGX_RESTY_EXT && NGX_EXPR)
#include <ngx_http_expr_module.h>
#endif

#define NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES   (2 * 1024 * 1024)
#define NGX_HTTP_CACHE_PURGE_CAPTURE_COUNT   64
#define NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD  (128 * 1024)
#define NGX_HTTP_CACHE_PURGE_CAPTURE_TIMEOUT 3000

enum {
    NGX_HTTP_CACHE_PURGE_FIELD_KEY,
    NGX_HTTP_CACHE_PURGE_FIELD_URL,
    NGX_HTTP_CACHE_PURGE_FIELD_TAG,
    NGX_HTTP_CACHE_PURGE_FIELD_NAMESPACE,
    NGX_HTTP_CACHE_PURGE_FIELD_COUNT
};
enum {
    NGX_HTTP_CACHE_PURGE_RPC_ACQUIRE,
    NGX_HTTP_CACHE_PURGE_RPC_PREPARE,
    NGX_HTTP_CACHE_PURGE_RPC_FINISH,
    NGX_HTTP_CACHE_PURGE_RPC_VALIDATE_LEASE,
    NGX_HTTP_CACHE_PURGE_RPC_VALIDATE
};

enum {
    NGX_HTTP_CACHE_PURGE_READ_OFF,
    NGX_HTTP_CACHE_PURGE_READ_START,
    NGX_HTTP_CACHE_PURGE_READ_READY,
    NGX_HTTP_CACHE_PURGE_READ_WAITING,
    NGX_HTTP_CACHE_PURGE_READ_DONE
};

enum {
    NGX_HTTP_CACHE_PURGE_ADMIT_DENY,
    NGX_HTTP_CACHE_PURGE_ADMIT_ALLOW,
    NGX_HTTP_CACHE_PURGE_ADMIT_REVALIDATE
};
typedef struct {
    ngx_http_complex_value_t *field[NGX_HTTP_CACHE_PURGE_FIELD_COUNT];
} ngx_http_cache_purge_capture_conf_t;
typedef struct {
    ngx_array_t  locations;
    ngx_int_t    host_index;
    ngx_flag_t   enabled;
#if (NGX_HTTP_CACHE_KEY)
    ngx_int_t (*snapshot)(ngx_http_request_t *,
                          ngx_http_cache_key_snapshot_t **);
    ngx_int_t (*value)(const ngx_http_cache_key_snapshot_t *,
                       ngx_http_cache_key_field_t, ngx_str_t *);
#endif
} ngx_http_cache_purge_capture_main_t;

typedef struct {
    ngx_table_elt_t        *target;
    ngx_list_t              headers;
    ngx_http_headers_out_t  output;
    unsigned                ignored : 1;
    unsigned                allow_ranges : 1;
    unsigned                single_range : 1;
    unsigned                disable_not_modified : 1;
} ngx_http_cache_purge_redirect_t;

typedef struct {
    ngx_http_file_cache_header_t  before, after;
    size_t                        input_bytes;
    u_char                        transaction[16];
    unsigned                      replace_tags : 1;
} ngx_http_cache_purge_revalidation_t;

typedef struct {
    ngx_http_request_t             *request;
    ngx_http_cache_t               *cache;
    ngx_http_cache_purge_fill_t    *fill;
    ngx_http_cache_purge_client_t  *client;
    ngx_http_cache_purge_call_t    *call;
    ngx_http_upstream_t            *read_upstream;
    ngx_http_event_handler_pt       read_resume;
    ngx_int_t                    (*read_process_header)(ngx_http_request_t *);
    ngx_int_t                    (*read_reinit)(ngx_http_request_t *);
    void (*error_finalize)(ngx_http_request_t *, ngx_int_t);
    ngx_http_cache_purge_redirect_t      *redirect;
    ngx_http_cache_purge_revalidation_t  *validation;
    ngx_event_t                           read_event;
    ngx_pool_t                           *publication_pool;
    ngx_temp_file_t                      *temporary;

    ngx_str_t                       key, url, host, namespace, root, zone, path;
    ngx_str_t                       components[6], tags[64];
    ngx_uint_t                      tag_count, rpc;
    ngx_uint_t                      read_state;
    size_t                          reserved, input_bytes;
    ngx_msec_t                              deadline;
    ngx_http_cache_purge_watermark_t        origin;
    uint64_t                                origin_publication;
    ngx_queue_t                             origin_queue;
    ngx_http_cache_purge_control_reader_t  *origin_reader;
    uint64_t                                writer_sequence;
    int                                     lease;
    u_char                          id[16], previous[16];
    uint64_t  revision;
    unsigned  admitted : 1;
    unsigned  failed : 1;
    unsigned  structured_key : 1;
    unsigned  structured_url : 1;
    unsigned  need_finish : 1;
    unsigned  read_held : 1;
    unsigned  read_ref : 1;
    unsigned  read_header_only : 1;
    unsigned  origin_valid : 1;
    unsigned  origin_registered : 1;
    unsigned  invalidation_retry : 1;
    unsigned  error_held : 1;
} ngx_http_cache_purge_capture_t;

typedef struct {
    uint64_t   covered, validated, required;
    ngx_int_t  fenced;
} ngx_http_cache_purge_read_match_t;
typedef struct {
    ngx_http_script_code_pt code;
    ngx_array_t  *predicates;
    uintptr_t     end;
} ngx_http_cache_purge_capture_code_t;

extern ngx_module_t ngx_http_cache_purge_index_module;
static ngx_http_output_header_filter_pt
    ngx_http_cache_purge_capture_next_header;
static size_t     ngx_http_cache_purge_retained_bytes;
static ngx_uint_t ngx_http_cache_purge_retained_count;
static ngx_uint_t ngx_http_cache_purge_read_count;
static ngx_queue_t ngx_http_cache_purge_origins;
static ngx_uint_t ngx_http_cache_purge_origins_initialized;
static u_char ngx_http_cache_purge_origin_ledger[16];
static void ngx_http_cache_purge_capture_origin_release(
    ngx_http_cache_purge_capture_t *ctx);
static void ngx_http_cache_purge_read_event(ngx_event_t *event);
static ngx_int_t ngx_http_cache_purge_read_header(
    ngx_http_cache_purge_capture_t *ctx);
static ngx_int_t ngx_http_cache_purge_read_request(
    ngx_http_cache_purge_capture_t *ctx);
static ngx_int_t ngx_http_cache_purge_read_process_header(
    ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_read_reinit(ngx_http_request_t *r);
static void ngx_http_cache_purge_error_finalize(ngx_http_request_t *r,
    ngx_int_t rc);
static ngx_int_t ngx_http_cache_purge_capture_allocate(
    ngx_http_cache_purge_capture_t *ctx, ngx_str_t *payload, size_t bytes);
static void ngx_http_cache_purge_read_validation_reply(
    ngx_http_cache_purge_call_t *call, void *data,
    const ngx_http_cache_purge_reply_t *reply);
static u_char *ngx_http_cache_purge_capture_string(u_char *p, ngx_str_t value);
static uint64_t ngx_http_cache_purge_capture_read_number(u_char *p,
    unsigned width);
static ngx_int_t  ngx_http_cache_purge_capture_step(ngx_http_request_t *,
    ngx_http_cache_t *, ngx_temp_file_t *, ngx_uint_t, ngx_int_t, void *);
static void
ngx_http_cache_purge_capture_reply(ngx_http_cache_purge_call_t *, void *,
    const ngx_http_cache_purge_reply_t *);

static void *ngx_http_cache_purge_capture_create_main(ngx_conf_t *cf);
static void *ngx_http_cache_purge_capture_create_conf(ngx_conf_t *cf);
static char *ngx_http_cache_purge_capture_merge(ngx_conf_t *cf, void *parent,
    void *child);
static ngx_int_t ngx_http_cache_purge_capture_post(ngx_conf_t *cf);
static ngx_int_t ngx_http_cache_purge_capture_header(ngx_http_request_t *r);


#define ngx_http_cache_purge_capture_directive(name, field) \
    {ngx_string(name),                                                        \
        NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,\
     ngx_http_set_complex_value_slot, NGX_HTTP_LOC_CONF_OFFSET,               \
     offsetof(ngx_http_cache_purge_capture_conf_t, field) , NULL}
/* Explicit offsets keep these four independent, inheritable inputs. */
static ngx_command_t  ngx_http_cache_purge_capture_commands[] = {

    ngx_http_cache_purge_capture_directive(
        "cache_purge_key", field[NGX_HTTP_CACHE_PURGE_FIELD_KEY]),
    ngx_http_cache_purge_capture_directive(
        "cache_purge_url", field[NGX_HTTP_CACHE_PURGE_FIELD_URL]),
    ngx_http_cache_purge_capture_directive(
        "cache_purge_tag", field[NGX_HTTP_CACHE_PURGE_FIELD_TAG]),
    ngx_http_cache_purge_capture_directive(
        "cache_purge_tag_namespace",
        field[NGX_HTTP_CACHE_PURGE_FIELD_NAMESPACE]),
      ngx_null_command
};
static ngx_http_module_t  ngx_http_cache_purge_capture_context = {
    NULL,
    ngx_http_cache_purge_capture_post,
    ngx_http_cache_purge_capture_create_main,
    NULL,
    NULL,
    NULL,
    ngx_http_cache_purge_capture_create_conf,
    ngx_http_cache_purge_capture_merge
};
ngx_module_t  ngx_http_cache_purge_index_module = {
    NGX_MODULE_V1,
    &ngx_http_cache_purge_capture_context,
    ngx_http_cache_purge_capture_commands,
    NGX_HTTP_MODULE,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NGX_MODULE_V1_PADDING
};

static ngx_int_t
ngx_http_cache_purge_capture_equal(ngx_str_t value, const char *literal)
{
    size_t length = ngx_strlen(literal);
    return value.len == length && ngx_memcmp(value.data, literal, length) == 0;
}

ngx_int_t
ngx_http_cache_purge_capture_ready(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_capture_main_t  *main;

    main = ngx_http_cycle_get_module_main_conf(
        cycle, ngx_http_cache_purge_index_module);
    if (main && main->enabled
        && ngx_http_top_header_filter
           != ngx_http_cache_purge_capture_header)
    {
        ngx_log_error(NGX_LOG_EMERG, cycle->log, 0,
                      "cache purge must run before other header filters; "
                      "register or load its filter module last");
        return NGX_ERROR;
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_capture_reserve(ngx_http_cache_purge_capture_t *ctx,
    size_t bytes)
{
    if (bytes > NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES
                    - ngx_http_cache_purge_retained_bytes)
    {
        return NGX_ERROR;
    }
    ngx_http_cache_purge_retained_bytes += bytes;
    ctx->reserved += bytes;
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_capture_copy(ngx_http_cache_purge_capture_t *ctx,
    ngx_str_t *out, ngx_str_t value)
{
    if (value.len > 32768
        || ngx_http_cache_purge_capture_reserve(ctx, value.len) != NGX_OK)
    {
        return NGX_ERROR;
    }
    out->len = value.len;
    out->data =
        value.len ? ngx_pnalloc(ctx->publication_pool, value.len) : NULL;
    if (value.len && out->data == NULL) {
        return NGX_ERROR;
    }
    if (value.len) {
        ngx_memcpy(out->data, value.data, value.len);
    }
    return NGX_OK;
}

static void
ngx_http_cache_purge_capture_close_lease(ngx_http_cache_purge_capture_t *ctx)
{
    if (ctx->lease >= 0) {
        close(ctx->lease);
        ctx->lease = -1;
    }
}

static ngx_int_t
ngx_http_cache_purge_capture_borrow(ngx_http_cache_purge_capture_t *ctx,
    ngx_str_t *out, ngx_str_t value)
{
    if (value.len > 32768
        || value.len > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - ctx->input_bytes)
    {
        return NGX_ERROR;
    }
    ctx->input_bytes += value.len;
    *out = value;
    return NGX_OK;
}

static void
ngx_http_cache_purge_capture_release_publication(
    ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_capture_close_lease(ctx);
    if (ctx->publication_pool) {
        ngx_destroy_pool(ctx->publication_pool);
        ctx->publication_pool = NULL;
    }
    ngx_http_cache_purge_retained_bytes -= ctx->reserved;
    ctx->reserved = 0;
    if (ctx->admitted) {
        ngx_http_cache_purge_retained_count--;
        ctx->admitted = 0;
    }
}

static void
ngx_http_cache_purge_capture_cleanup(void *data)
{
    ngx_http_cache_purge_capture_t *ctx = data;
    if (ctx->error_held) {
        ctx->cache->updated = 0;
        ctx->cache->error = 0;
        ctx->error_held = 0;
        ngx_http_file_cache_free(ctx->cache, NULL);
    }
    if (ctx->call) {
        ngx_http_cache_purge_client_cancel(ctx->call);
    }
    if (ctx->read_event.timer_set) {
        ngx_del_timer(&ctx->read_event);
    }
    if (ctx->read_event.posted) {
        ngx_delete_posted_event(&ctx->read_event);
    }
    ngx_http_cache_purge_capture_release_publication(ctx);
    ngx_http_cache_purge_capture_origin_release(ctx);
}

static ngx_int_t
ngx_http_cache_purge_capture_native_key(ngx_http_cache_purge_capture_t *ctx,
    ngx_str_t *value)
{
    ngx_str_t  *keys = ctx->cache->keys.elts;
    ngx_uint_t  i;
    u_char     *p;
    if (ctx->cache->keys.nelts == 1) {
        return ngx_http_cache_purge_capture_borrow(ctx, value, keys[0]);
    }
    value->len = 0;
    for (i = 0; i < ctx->cache->keys.nelts; i++) {
        if (keys[i].len > 32768 - value->len) {
            return NGX_ERROR;
        }
        value->len += keys[i].len;
    }
    if (value->len > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - ctx->input_bytes) {
        return NGX_ERROR;
    }
    ctx->input_bytes += value->len;
    value->data = ngx_pnalloc(ctx->request->pool, value->len);
    if (value->data == NULL) {
        return NGX_ERROR;
    }
    p = value->data;
    for (i = 0; i < ctx->cache->keys.nelts; i++) {
        if (keys[i].len) {
            p = ngx_cpymem(p, keys[i].data, keys[i].len);
        }
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_capture(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_conf_t *conf =
        ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_index_module);
    ngx_http_cache_purge_capture_main_t *main =
        ngx_http_get_module_main_conf(r, ngx_http_cache_purge_index_module);
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_pool_cleanup_t              *cln;
    ngx_http_variable_value_t       *host;
    ngx_str_t                        value;
    ngx_uint_t                       i;
#if (NGX_HTTP_CACHE_KEY)
    ngx_http_cache_key_snapshot_t  *snapshot = NULL;
    ngx_int_t                       snapshot_result = NGX_DECLINED;
#endif

    if (conf->field[NGX_HTTP_CACHE_PURGE_FIELD_KEY] == NULL) {
        return NGX_DECLINED;
    }
    if (r->cache == NULL) {
        return NGX_OK;
    }
    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ctx && ctx->cache == r->cache) {
        return ctx->failed ? NGX_ERROR : NGX_OK;
    }
    cln = ngx_pool_cleanup_add(r->pool, sizeof(*ctx));
    if (cln == NULL) {
        return NGX_ERROR;
    }
    ctx = cln->data;
    ngx_memzero(ctx, sizeof(*ctx));
    ctx->request = r;
    ctx->cache = r->cache;
    ctx->lease = -1;
    ctx->failed = 1;
    cln->handler = ngx_http_cache_purge_capture_cleanup;
    ngx_http_set_ctx(r, ctx, ngx_http_cache_purge_index_module);
    ctx->read_upstream = r->upstream;
    ctx->read_process_header = r->upstream->process_header;
    ctx->read_reinit = r->upstream->reinit_request;
    ctx->error_finalize = r->upstream->finalize_request;
    r->upstream->process_header = ngx_http_cache_purge_read_process_header;
    r->upstream->reinit_request = ngx_http_cache_purge_read_reinit;
    r->upstream->finalize_request = ngx_http_cache_purge_error_finalize;

#if (NGX_HTTP_CACHE_KEY)
    if (main->snapshot) {
        snapshot_result = main->snapshot(r, &snapshot);
    }
    if (snapshot_result == NGX_ERROR) {
        return NGX_ERROR;
    }
    if (snapshot_result == NGX_OK) {
        for (i = 0; i < 6; i++) {
            if (main->value(snapshot, i, &value) != NGX_OK
                || ngx_http_cache_purge_capture_borrow(ctx, &ctx->components[i],
                                                       value)
                       != NGX_OK)
            {
                return NGX_ERROR;
            }
        }
    }
#endif

    for (i = NGX_HTTP_CACHE_PURGE_FIELD_KEY;
         i < NGX_HTTP_CACHE_PURGE_FIELD_COUNT; i++)
    {
        ngx_str_t *out;
        if (i == NGX_HTTP_CACHE_PURGE_FIELD_TAG) {
            continue;
        }
        out = i == NGX_HTTP_CACHE_PURGE_FIELD_KEY ? &ctx->key :
              i == NGX_HTTP_CACHE_PURGE_FIELD_URL ? &ctx->url :
                                                    &ctx->namespace;
        if (conf->field[i] == NULL) {
            if (ngx_http_cache_purge_capture_native_key(ctx, out) != NGX_OK) {
                return NGX_ERROR;
            }
            continue;
        }
#if (NGX_HTTP_CACHE_KEY)
        if ((i == NGX_HTTP_CACHE_PURGE_FIELD_KEY
             && ngx_http_cache_purge_capture_equal(conf->field[i]->value,
                                                   "$cache_key_base"))
            || (i == NGX_HTTP_CACHE_PURGE_FIELD_URL
                && ngx_http_cache_purge_capture_equal(conf->field[i]->value,
                                                      "$cache_key_url"))
            || (i == NGX_HTTP_CACHE_PURGE_FIELD_NAMESPACE
                && ngx_http_cache_purge_capture_equal(conf->field[i]->value,
                                                      "$cache_key_namespace")))
        {
            if (snapshot_result != NGX_OK
                || main->value(snapshot,
                               i == NGX_HTTP_CACHE_PURGE_FIELD_KEY ?
                                   NGX_HTTP_CACHE_KEY_BASE :
                               i == NGX_HTTP_CACHE_PURGE_FIELD_URL ?
                                   NGX_HTTP_CACHE_KEY_URL :
                                   NGX_HTTP_CACHE_KEY_NAMESPACE,
                               &value)
                       != NGX_OK)
            {
                return NGX_ERROR;
            }
            if (i == NGX_HTTP_CACHE_PURGE_FIELD_KEY) {
                ctx->structured_key = 1;
            }
            if (i == NGX_HTTP_CACHE_PURGE_FIELD_URL) {
                ctx->structured_url = 1;
            }
        } else
#endif
            if (ngx_http_complex_value(r, conf->field[i], &value) != NGX_OK)
            return NGX_ERROR;
        if (ngx_http_cache_purge_capture_borrow(ctx, out, value) != NGX_OK) {
            return NGX_ERROR;
        }
    }
    host = ngx_http_get_flushed_variable(r, main->host_index);
    if (host == NULL || host->not_found) {
        return NGX_ERROR;
    }
    value.data = host->data;
    value.len = host->len;
    if (ctx->structured_key || ctx->structured_url) {
        value = ctx->components[2];
    }
    if (ngx_http_cache_purge_capture_borrow(ctx, &ctx->host, value) != NGX_OK
        || ctx->host.len == 0 || ctx->key.len == 0 || ctx->url.len == 0
        || ctx->namespace.len == 0 || ctx->namespace.len > 255)
    {
        return NGX_ERROR;
    }
    for (i = 0; i < ctx->namespace.len; i++) {
        if (ctx->namespace.data[i] <= 0x20 || ctx->namespace.data[i] >= 0x7f
            || ctx->namespace.data[i] == ',')
        {
            return NGX_ERROR;
        }
    }
    ctx->failed = 0;
    return NGX_OK;
}

static size_t
ngx_http_cache_purge_capture_guard_length(ngx_http_script_engine_t *engine)
{
    engine->ip += sizeof(ngx_http_script_len_code_pt);
    return 1;
}

static ngx_int_t
ngx_http_cache_purge_capture_origin_publish(
    ngx_http_cache_purge_control_reader_t *reader,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_capture_t   *first;
    ngx_http_cache_purge_watermark_t  oldest;

    if (ngx_queue_empty(&ngx_http_cache_purge_origins)) {
        return ngx_http_cache_purge_control_origins(reader, NULL, 0, error);
    }
    first = ngx_queue_data(ngx_queue_head(&ngx_http_cache_purge_origins),
                          ngx_http_cache_purge_capture_t, origin_queue);
    ngx_memcpy(oldest.ledger, ngx_http_cache_purge_origin_ledger, 16);
    oldest.sequence = first->writer_sequence;
    return ngx_http_cache_purge_control_origins(reader, &oldest, 1, error);
}

static void
ngx_http_cache_purge_capture_origin_release(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_rocksdb_error_t  error;

    if (!ctx->origin_registered) {
        return;
    }
    ngx_queue_remove(&ctx->origin_queue);
    ctx->origin_registered = 0;
    if (ngx_http_cache_purge_capture_origin_publish(ctx->origin_reader, &error)
        != 0)
    {
        ngx_log_error(NGX_LOG_WARN, ctx->request->connection->log, 0,
                      "cache origin registration release: %s", error.message);
    }
}

static ngx_int_t
ngx_http_cache_purge_capture_origin(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_rocksdb_error_t          error;
    ngx_http_cache_purge_capture_t         *last;
    ngx_int_t                               announced;

    ctx->origin_valid = 0;
    announced = 0;
    if (ngx_http_cache_purge_runtime_reader(ctx->request, &reader, &error) != 0)
    {
        return NGX_ERROR;
    }
    if (!ngx_http_cache_purge_origins_initialized) {
        ngx_queue_init(&ngx_http_cache_purge_origins);
        ngx_http_cache_purge_origins_initialized = 1;
    }
    if (!ngx_queue_empty(&ngx_http_cache_purge_origins)) {
        last = ngx_queue_data(ngx_queue_last(&ngx_http_cache_purge_origins),
                             ngx_http_cache_purge_capture_t, origin_queue);
        if (last->origin_reader != reader) {
            return NGX_ERROR;
        }
    }
    if (!ctx->origin_registered) {
        if (ngx_http_cache_purge_control_origins(reader, NULL, 1, &error)
            != 0)
        {
            return NGX_ERROR;
        }
        announced = 1;
    }
    if (ngx_http_cache_purge_control_watermark(reader, &ctx->origin,
            &ctx->origin_publication, &error) != 0)
    {
        goto failed;
    }
    if (!ngx_queue_empty(&ngx_http_cache_purge_origins)) {
        last = ngx_queue_data(ngx_queue_last(&ngx_http_cache_purge_origins),
                             ngx_http_cache_purge_capture_t, origin_queue);
        if (ngx_memcmp(ctx->origin.ledger, ngx_http_cache_purge_origin_ledger,
                       16) != 0
            || ctx->origin.sequence < last->writer_sequence)
        {
            goto failed;
        }
    } else {
        ngx_memcpy(ngx_http_cache_purge_origin_ledger, ctx->origin.ledger, 16);
    }
    if (!ctx->origin_registered) {
        ctx->origin_reader = reader;
        ctx->writer_sequence = ctx->origin.sequence;
        ngx_queue_insert_tail(&ngx_http_cache_purge_origins,
                              &ctx->origin_queue);
        ctx->origin_registered = 1;
        if (ngx_http_cache_purge_capture_origin_publish(reader, &error) != 0) {
            ngx_http_cache_purge_capture_origin_release(ctx);
            return NGX_ERROR;
        }
    }
    ctx->origin_valid = 1;
    return NGX_OK;

failed:
    if (announced) {
        (void) ngx_http_cache_purge_capture_origin_publish(reader, &error);
    }
    return NGX_ERROR;
}

static void
ngx_http_cache_purge_capture_guard_value(ngx_http_script_engine_t *engine)
{
    ngx_http_cache_purge_capture_code_t *code =
        (ngx_http_cache_purge_capture_code_t *) engine->ip;
    ngx_int_t rc = ngx_http_test_predicates(engine->request, code->predicates);
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_http_request_t              *r;
    ngx_int_t                        captured;

    engine->ip += sizeof(*code) - sizeof(uintptr_t);
    if (rc == NGX_ERROR) {
        engine->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        return;
    }
    captured = ngx_http_cache_purge_capture(engine->request);
    if (captured != NGX_OK) {
        engine->request->upstream->cacheable = 0;
        rc = NGX_DECLINED;
        ngx_log_error(
            NGX_LOG_WARN, engine->request->connection->log, 0,
            captured == NGX_DECLINED ?
                "cache index key missing at active upstream; skipping cache" :
                "cache index snapshot unavailable or over budget; "
                "skipping cache");
    }
    r = engine->request;
    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ctx && !ctx->origin_valid
        && ngx_http_cache_purge_capture_origin(ctx) != NGX_OK)
    {
        r->upstream->cacheable = 0;
        rc = NGX_DECLINED;
    }
    if (rc == NGX_OK) {
        if (ctx && ctx->read_state == NGX_HTTP_CACHE_PURGE_READ_OFF) {
            if (ngx_http_cache_purge_read_request(ctx) != NGX_OK
                || r->main->blocked >= NGX_HTTP_MAX_BLOCKED
                || r->main->count == 65535)
            {
                rc = NGX_DECLINED;
            } else {
                ctx->read_state = NGX_HTTP_CACHE_PURGE_READ_START;
                ctx->read_event.data = ctx;
                ctx->read_event.handler = ngx_http_cache_purge_read_event;
                ctx->read_event.log = r->connection->log;
                ctx->read_held = 1;
                ctx->read_ref = 1;
                r->main->blocked++;
                r->main->count++;
                r->cache->waiting = 1;
                ngx_post_event(&ctx->read_event, &ngx_posted_events);
            }
        }
    }
    *engine->pos++ = rc == NGX_DECLINED ? '1' : '0';
}

static ngx_int_t
ngx_http_cache_purge_capture_tags(ngx_http_cache_purge_capture_t *ctx,
    ngx_str_t value)
{
    size_t      at = 0, start, end, i;
    ngx_uint_t  n;
    ngx_str_t   tag;
    while (at < value.len) {
        start = at;
        while (at < value.len && value.data[at] != ',') {
            at++;
        }
        end = at;
        if (at < value.len) {
            at++;
        }
        while (start < end
               && (value.data[start] == ' ' || value.data[start] == '\t'))
        {
            start++;
        }
        while (end > start
               && (value.data[end - 1] == ' ' || value.data[end - 1] == '\t'))
        {
            end--;
        }
        if (start == end) {
            continue;
        }
        if (end - start > 1024) {
            return NGX_ERROR;
        }
        for (i = start; i < end; i++) {
            if (value.data[i] < 0x20 || value.data[i] == 0x7f) {
                return NGX_ERROR;
            }
        }
        tag.data = value.data + start;
        tag.len = end - start;
        for (n = 0; n < ctx->tag_count; n++) {
            if (ctx->tags[n].len == tag.len
                && ngx_memcmp(ctx->tags[n].data, tag.data, tag.len) == 0)
            {
                break;
            }
        }
        if (n < ctx->tag_count) {
            continue;
        }
        if (ctx->tag_count == 64
            || ngx_http_cache_purge_capture_borrow(
                   ctx, &ctx->tags[ctx->tag_count], tag)
                   != NGX_OK)
        {
            return NGX_ERROR;
        }
        ctx->tag_count++;
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_capture_header(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_t *ctx =
        ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    ngx_http_cache_purge_capture_conf_t  *conf;
    ngx_str_t                             value;
    ngx_int_t                             rc;
    if (ctx && ctx->error_held && ctx->cache == r->cache
        && ctx->read_upstream == r->upstream && r->upstream->cleanup != NULL)
    {
        /* Native interception did not consume this response. Its ordinary
         * disk-cache path must retain the original cacheability decision. */
        ctx->cache->updated = 0;
        ctx->error_held = 0;
    }
    if (ctx && !ctx->failed && ctx->cache == r->cache && r->cached
        && ctx->read_state == NGX_HTTP_CACHE_PURGE_READ_READY)
    {
        return ngx_http_cache_purge_read_header(ctx);
    }
    if (ctx && !ctx->failed && ctx->cache == r->cache && !r->cached) {
        conf =
            ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_index_module);
        rc = NGX_ERROR;
        if (conf->field[NGX_HTTP_CACHE_PURGE_FIELD_TAG] == NULL
            || (ngx_http_complex_value(r,
                    conf->field[NGX_HTTP_CACHE_PURGE_FIELD_TAG], &value)
                == NGX_OK
                && ngx_http_cache_purge_capture_tags(ctx, value) == NGX_OK))
        {
            rc = ngx_http_cache_purge_fill_arm(r, &ctx->fill,
                ngx_http_cache_purge_capture_step, ctx);
        }
        if (rc == NGX_ERROR)
        {
            ctx->failed = 1;
            r->upstream->cacheable = 0;
            ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                          "cache index response metadata unavailable; skipping "
                          "cache fill");
        }
        if (rc != NGX_OK) {
            ngx_http_cache_purge_capture_origin_release(ctx);
        }
    }
    return ngx_http_cache_purge_capture_next_header(r);
}

static u_char *
ngx_http_cache_purge_capture_number(u_char *p, uint64_t value, unsigned width)
{
    while (width) {
        *p++ = (u_char) (value >> (--width * 8));
    }
    return p;
}
static uint64_t
ngx_http_cache_purge_capture_read_number(u_char *p, unsigned width)
{
    uint64_t value = 0;
    while (width--) {
        value = (value << 8) | *p++;
    }
    return value;
}
static u_char *
ngx_http_cache_purge_capture_string(u_char *p, ngx_str_t value)
{
    p = ngx_http_cache_purge_capture_number(p, value.len, 4);
    return value.len ? ngx_cpymem(p, value.data, value.len) : p;
}
static void
ngx_http_cache_purge_read_validation_capture(
    ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_capture_conf_t  *conf;
    ngx_http_cache_purge_revalidation_t  *v;
    ngx_http_variable_value_t            *variable;
    ngx_http_complex_value_t             *expression;
    ngx_http_request_t                   *r;
    ngx_http_cache_t                     *c;
    ngx_str_t                             value, name;
    size_t                                input;
    ngx_uint_t                            i, direct;
    static ngx_str_t prefix = ngx_string("$upstream_http_");

    r = ctx->request;
    c = ctx->cache;
    input = ctx->input_bytes;
    if (ctx->validation || ctx->publication_pool || c->buf == NULL
        || c->buf->start == NULL
        || c->buf->last - c->buf->start
           < (ptrdiff_t) sizeof(ngx_http_file_cache_header_t)
        || ngx_http_cache_purge_retained_count
           >= NGX_HTTP_CACHE_PURGE_CAPTURE_COUNT
        || ngx_http_cache_purge_capture_reserve(ctx,
            NGX_DEFAULT_POOL_SIZE + sizeof(*v) + input) != NGX_OK)
    {
        return;
    }
    ctx->admitted = 1;
    ngx_http_cache_purge_retained_count++;
    ctx->publication_pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE,
                                           r->connection->log);
    if (ctx->publication_pool == NULL) {
        goto failed;
    }
    v = ngx_pcalloc(ctx->publication_pool, sizeof(*v));
    if (v == NULL) {
        goto failed;
    }
    v->input_bytes = input;
    ngx_memcpy(&v->before, c->buf->start, sizeof(v->before));
    if (v->before.version != NGX_HTTP_CACHE_VERSION
        || v->before.crc32 != c->crc32
        || v->before.header_start != c->header_start
        || v->before.body_start != c->body_start
        || getrandom(v->transaction, sizeof(v->transaction), GRND_NONBLOCK)
           != (ssize_t) sizeof(v->transaction))
    {
        goto failed;
    }
    v->after = v->before;
    v->after.valid_sec = c->valid_sec;
    v->after.updating_sec = c->updating_sec;
    v->after.error_sec = c->error_sec;
    v->after.date = ngx_time();
#if (NGX_RESTY_EXT)
    if (c->must_revalidate) {
        v->after.valid_msec |= NGX_HTTP_CACHE_MUST_REVALIDATE;
    }
#endif
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_index_module);
    expression = conf->field[NGX_HTTP_CACHE_PURGE_FIELD_TAG];
    if (expression != NULL) {
        name = expression->value;
        direct = name.len > prefix.len
                 && ngx_strncmp(name.data, prefix.data, prefix.len) == 0;
        for (i = prefix.len; direct && i < name.len; i++) {
            if (!((name.data[i] >= 'a' && name.data[i] <= 'z')
                  || (name.data[i] >= '0' && name.data[i] <= '9')
                  || name.data[i] == '_'))
            {
                direct = 0;
            }
        }
        v->replace_tags = 1;
        if (direct) {
            name.data++;
            name.len--;
            variable = ngx_http_get_variable(r, &name,
                                             ngx_hash_key(name.data, name.len));
            if (variable == NULL) {
                goto failed;
            }
            v->replace_tags = !variable->not_found;
        }
        if (v->replace_tags) {
            if (ngx_http_complex_value(r, expression, &value) != NGX_OK
                || ngx_http_cache_purge_capture_tags(ctx, value) != NGX_OK)
            {
                goto failed;
            }
            for (i = 0; i < ctx->tag_count; i++) {
                if (ngx_http_cache_purge_capture_copy(ctx, &ctx->tags[i],
                                                      ctx->tags[i]) != NGX_OK)
                {
                    goto failed;
                }
            }
        }
    }
    ctx->validation = v;
    return;

failed:
    ctx->input_bytes = input;
    ctx->tag_count = 0;
    ngx_http_cache_purge_capture_release_publication(ctx);
}

static void
ngx_http_cache_purge_error_finalize(ngx_http_request_t *r, ngx_int_t rc)
{
    ngx_pool_cleanup_t              *cleanup;
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_http_upstream_t             *u;

    u = r->upstream;
    for (cleanup = r->pool->cleanup; cleanup; cleanup = cleanup->next) {
        if (cleanup->handler != ngx_http_cache_purge_capture_cleanup) {
            continue;
        }
        ctx = cleanup->data;
        if (ctx->request != r || ctx->read_upstream != u) {
            continue;
        }
        if (ctx->error_held) {
            ctx->cache->updated = 0;
            ctx->cache->error = 0;
            ctx->error_held = 0;
            u->cacheable = 0;
        }
        if (rc == NGX_HTTP_BAD_GATEWAY || rc == NGX_HTTP_GATEWAY_TIME_OUT) {
            u->cacheable = 0;
        }
        ctx->error_finalize(r, rc);
        return;
    }
    ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0,
                  "cache index lost error finalizer");
}

static ngx_int_t
ngx_http_cache_purge_read_reinit(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_int_t                        rc;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ctx == NULL || ctx->read_reinit == NULL) {
        return NGX_ERROR;
    }
    if (ctx->error_held) {
        ctx->cache->updated = 0;
        ctx->cache->error = 0;
        ctx->error_held = 0;
    }
    if (ctx->cache == r->cache
        && ctx->read_state == NGX_HTTP_CACHE_PURGE_READ_READY
        && r->upstream->headers_in.status_n == NGX_HTTP_NOT_MODIFIED
        && r->upstream->cache_status == NGX_HTTP_CACHE_EXPIRED
        && r->upstream->conf->cache_revalidate)
    {
        ngx_http_cache_purge_read_validation_capture(ctx);
    }
    rc = ctx->read_reinit(r);
    if (r->upstream->process_header != ngx_http_cache_purge_read_process_header)
    {
        ctx->read_process_header = r->upstream->process_header;
        r->upstream->process_header = ngx_http_cache_purge_read_process_header;
    }
    return rc;
}

static ngx_int_t
ngx_http_cache_purge_read_process_header(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_t   *ctx;
    ngx_http_cache_purge_redirect_t  *redirect;
    ngx_http_upstream_t              *u;
    ngx_uint_t                        ignored;
    ngx_int_t                         rc;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ctx == NULL || ctx->read_process_header == NULL) {
        return NGX_ERROR;
    }
    u = r->upstream;
    rc = ctx->read_process_header(r);
    if (u->process_header != ngx_http_cache_purge_read_process_header) {
        ctx->read_process_header = u->process_header;
        u->process_header = ngx_http_cache_purge_read_process_header;
    }
    if (rc == NGX_OK && !r->cached && r->cache == ctx->cache
        && !ctx->cache->updated && u->cacheable
        && u->headers_in.status_n >= NGX_HTTP_SPECIAL_RESPONSE
        && u->headers_in.status_n != NGX_HTTP_NOT_MODIFIED
        && (u->conf->intercept_errors || u->conf->intercept_404))
    {
        /* Interception can free an error node before finalize_request. Defer
         * that free without evaluating error_page/when a second time. */
        ctx->cache->updated = 1;
        ctx->error_held = 1;
    }
    if (rc != NGX_OK || !r->cached || u->headers_in.x_accel_redirect == NULL) {
        return rc;
    }
    redirect = ngx_pcalloc(r->pool, sizeof(*redirect));
    if (redirect == NULL) {
        return NGX_ERROR;
    }
    ignored = u->conf->ignore_headers;
#if (NGX_RESTY_EXT && NGX_EXPR)
    if (u->conf->ignore_headers_conf != NULL
        && u->conf->ignore_headers_conf != NGX_CONF_UNSET_PTR)
    {
        ignored = ngx_http_get_expr_bitmask_value(r,
                                                  u->conf->ignore_headers_conf);
    }
#endif
    redirect->ignored = (ignored & NGX_HTTP_UPSTREAM_IGN_XA_REDIRECT) != 0;
    redirect->target = u->headers_in.x_accel_redirect;
    redirect->headers = u->headers_in.headers;
    redirect->output = r->headers_out;
    redirect->allow_ranges = r->allow_ranges;
    redirect->single_range = r->single_range;
    redirect->disable_not_modified = r->disable_not_modified;
    ctx->redirect = redirect;
    u->headers_in.x_accel_redirect = NULL;
    if (!redirect->ignored
        && ngx_list_init(&u->headers_in.headers, r->pool, 1,
                         sizeof(ngx_table_elt_t)) != NGX_OK)
    {
        return NGX_ERROR;
    }
    return NGX_OK;
}

static void
ngx_http_cache_purge_read_redirect(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_request_t               *r;
    ngx_http_upstream_main_conf_t    *main;
    ngx_http_upstream_header_t       *header;
    ngx_http_cache_purge_redirect_t  *redirect;
    ngx_list_part_t                  *part;
    ngx_table_elt_t                  *h;
    ngx_connection_t                 *connection;
    ngx_str_t                          uri, args;
    ngx_uint_t                         i, flags;

    r = ctx->request;
    redirect = ctx->redirect;
    connection = r->connection;
    r->headers_out = redirect->output;
    r->allow_ranges = redirect->allow_ranges;
    r->single_range = redirect->single_range;
    r->disable_not_modified = redirect->disable_not_modified;
    main = ngx_http_get_module_main_conf(r, ngx_http_upstream_module);
    part = &redirect->headers.part;
    h = part->elts;
    for (i = 0; /* void */; i++) {
        if (i >= part->nelts) {
            if (part->next == NULL) {
                break;
            }
            part = part->next;
            h = part->elts;
            i = 0;
        }
        if (h[i].hash == 0) {
            continue;
        }
        header = ngx_hash_find(&main->headers_in_hash, h[i].hash,
                               h[i].lowcase_key, h[i].key.len);
        if (header && header->redirect
            && header->copy_handler(r, &h[i], header->conf) != NGX_OK)
        {
            ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return;
        }
    }
    uri = redirect->target->value;
    if (uri.len && uri.data[0] == '@') {
        (void) ngx_http_named_location(r, &uri);
    } else {
        ngx_str_null(&args);
        flags = NGX_HTTP_LOG_UNSAFE;
        if (ngx_http_parse_unsafe_uri(r, &uri, &args, &flags) != NGX_OK) {
            ngx_http_finalize_request(r, NGX_HTTP_NOT_FOUND);
            return;
        }
        if (r->method != NGX_HTTP_HEAD) {
            r->method = NGX_HTTP_GET;
            r->method_name = ngx_http_core_get_method;
        }
        (void) ngx_http_internal_redirect(r, &uri, &args);
    }
    ngx_http_finalize_request(r, NGX_DONE);
    ngx_http_run_posted_requests(connection);
}

static ngx_int_t
ngx_http_cache_purge_read_send(ngx_http_request_t *r)
{
    ngx_buf_t         *b;
    ngx_chain_t        out;
    ngx_http_cache_t  *c;
    ngx_int_t          rc;

    c = r->cache;
    b = ngx_calloc_buf(r->pool);
    if (b == NULL) {
        return NGX_ERROR;
    }
    b->file = ngx_pcalloc(r->pool, sizeof(ngx_file_t));
    if (b->file == NULL) {
        return NGX_ERROR;
    }
    rc = ngx_http_cache_purge_capture_next_header(r);
    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }
    b->file_pos = c->body_start;
    b->file_last = c->length;
    b->in_file = c->length > (off_t) c->body_start;
    b->last_buf = r == r->main;
    b->last_in_chain = 1;
    b->sync = !(b->last_buf || b->in_file);
    b->file->fd = c->file.fd;
    b->file->name = c->file.name;
    b->file->log = r->connection->log;
    out.buf = b;
    out.next = NULL;
    return ngx_http_output_filter(r, &out);
}

static void
ngx_http_cache_purge_read_complete(ngx_http_cache_purge_capture_t *ctx,
    ngx_int_t allowed)
{
    ngx_http_request_t        *r;
    ngx_http_upstream_t       *u;
    ngx_http_upstream_conf_t  *conf;
    ngx_connection_t          *connection;
    ngx_int_t                  rc;
    ngx_uint_t                 revalidate;

    r = ctx->request;
    u = ctx->read_upstream;
    connection = r->connection;
    revalidate = allowed == NGX_HTTP_CACHE_PURGE_ADMIT_REVALIDATE
                 && !ctx->invalidation_retry;
    allowed = allowed == NGX_HTTP_CACHE_PURGE_ADMIT_ALLOW;
    if (ctx->validation != NULL) {
        if (allowed) {
            ctx->cache->valid_sec = ctx->validation->after.valid_sec;
            ctx->cache->updating_sec = ctx->validation->after.updating_sec;
            ctx->cache->error_sec = ctx->validation->after.error_sec;
            ctx->cache->date = ctx->validation->after.date;
#if (NGX_RESTY_EXT)
            ctx->cache->purged = 0;
#endif
        }
        ctx->input_bytes = ctx->validation->input_bytes;
        ctx->validation = NULL;
        ctx->tag_count = 0;
        ngx_http_cache_purge_capture_release_publication(ctx);
    }
    if (allowed || r->main->terminated) {
        ngx_http_cache_purge_capture_origin_release(ctx);
    }
    if (ctx->read_state == NGX_HTTP_CACHE_PURGE_READ_WAITING) {
        ngx_http_cache_purge_read_count--;
    }
    ctx->read_state = NGX_HTTP_CACHE_PURGE_READ_DONE;
    if (ctx->read_held) {
        ctx->read_held = 0;
        r->main->blocked--;
    }
    if (ctx->read_ref) {
        ctx->read_ref = 0;
        r->main->count--;
    }
    if (r->main->terminated) {
        ngx_http_file_cache_free(ctx->cache, NULL);
        connection->write->handler(connection->write);
        return;
    }
    r->header_only = ctx->read_header_only;
    if (r->cache != ctx->cache || r->upstream != u) {
        ngx_http_file_cache_free(ctx->cache, NULL);
        ngx_http_finalize_request(r, NGX_ERROR);
        return;
    }
    if (allowed) {
        if (ctx->redirect && !ctx->redirect->ignored) {
            ngx_http_file_cache_free(ctx->cache, NULL);
            ngx_http_cache_purge_read_redirect(ctx);
            return;
        }
        rc = ngx_http_cache_purge_read_send(r);
        ngx_http_file_cache_free(ctx->cache, NULL);
        ngx_http_finalize_request(r, rc);
        ngx_http_run_posted_requests(connection);
        return;
    }

    /* Retain the native key, node, validators and entity for Invalidate.
     * Delete/unknown instances instead require an unconditional replacement. */
    conf = ngx_palloc(r->pool, sizeof(*conf));
    if (conf == NULL) {
        ngx_http_file_cache_free(ctx->cache, NULL);
        ngx_http_finalize_request(r, NGX_ERROR);
        return;
    }
    *conf = *u->conf;
    conf->cache = 0;
    u->conf = conf;
    r->cached = 0;
    u->cache_status = revalidate ? NGX_HTTP_CACHE_EXPIRED :
                                  NGX_HTTP_CACHE_BYPASS;
    u->cacheable = ngx_http_cache_purge_capture_origin(ctx) == NGX_OK;
    if (revalidate) {
        ctx->invalidation_retry = 1;
        ctx->read_state = NGX_HTTP_CACHE_PURGE_READ_READY;
#if (NGX_RESTY_EXT)
        ctx->cache->purged = 1;
#endif
    }
    u->buffer.start = NULL;
    u->request_sent = 1;
    ctx->cache->valid_sec = 0;
    ctx->cache->updating_sec = 0;
    ctx->cache->error_sec = 0;
    ngx_memzero(&r->headers_out, sizeof(r->headers_out));
    r->headers_out.content_length_n = -1;
    r->headers_out.last_modified_time = -1;
    if (ngx_list_init(&r->headers_out.headers, r->pool, 20,
                      sizeof(ngx_table_elt_t)) != NGX_OK
        || ngx_list_init(&r->headers_out.trailers, r->pool, 4,
                         sizeof(ngx_table_elt_t)) != NGX_OK)
    {
        ngx_http_file_cache_free(ctx->cache, NULL);
        ngx_http_finalize_request(r, NGX_ERROR);
        return;
    }
    ctx->read_resume(r);
    ngx_http_run_posted_requests(connection);
}

static ngx_int_t
ngx_http_cache_purge_read_field(u_char **cursor, u_char *end,
    ngx_http_cache_rocksdb_bytes_t *field)
{
    size_t  length;

    if ((size_t) (end - *cursor) < 4) {
        return NGX_ERROR;
    }
    length = ngx_http_cache_purge_capture_read_number(*cursor, 4);
    *cursor += 4;
    if (length > (size_t) (end - *cursor)) {
        return NGX_ERROR;
    }
    field->data = *cursor;
    field->len = length;
    *cursor += length;
    return NGX_OK;
}

static int
ngx_http_cache_purge_read_match(void *data,
    const ngx_http_cache_purge_control_match_t *match)
{
    ngx_http_cache_purge_read_match_t  *result;

    result = data;
    if ((match->flags & NGX_HTTP_CACHE_PURGE_CONTROL_FENCE)
        && match->sequence > (match->mode == NGX_HTTP_CACHE_PURGE_INVALIDATE
                             ? result->validated : result->covered))
    {
        if (match->mode == NGX_HTTP_CACHE_PURGE_INVALIDATE) {
            if (match->sequence > result->required) {
                result->required = match->sequence;
            }
        } else {
            result->fenced = 1;
        }
    }
    return 0;
}

static ngx_int_t
ngx_http_cache_purge_read_request(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_rocksdb_error_t          error;
    int                                     rc;

    if (!ctx->origin_valid
        || ngx_http_cache_purge_runtime_reader(ctx->request, &reader, &error)
        != 0)
    {
        return NGX_DECLINED;
    }
    rc = ngx_http_cache_purge_control_guard_begin(reader,
                                                   ctx->origin_publication,
                                                   &error);
    if (rc != 0) {
        return NGX_DECLINED;
    }
    /* All cache writers in this mode suppress memory-only errors. The file
     * gate can therefore decide every selector against actual entity proof,
     * preserving conditional revalidation and already-covered HITs. */
    rc = ngx_http_cache_purge_control_guard_end(reader, &error);
    return rc == 0 ? NGX_OK : NGX_DECLINED;
}

static ngx_int_t
ngx_http_cache_purge_read_admit(ngx_http_cache_purge_capture_t *ctx,
    ngx_str_t payload)
{
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_purge_control_context_t  context;
    ngx_http_cache_rocksdb_error_t          error;
    ngx_http_cache_rocksdb_bytes_t           fields[10], tags[64], value;
    ngx_http_cache_purge_watermark_t        current, stored, validated;
    uint64_t                                publication, first, stamp;
    ngx_uint_t                              i, count;
    ngx_http_cache_purge_read_match_t       match;
    u_char                                 *p, *end;
    int                                     rc;

    if (payload.len < 45 || payload.data[0] != 2) {
        return 0;
    }
    stamp = ngx_http_cache_purge_capture_read_number(
        payload.data + payload.len - 16, 8);
    if (stamp == 0) {
        return 0;
    }
    p = payload.data + 25;
    end = payload.data + payload.len - 16;
    for (i = 0; i < 10; i++) {
        if (ngx_http_cache_purge_read_field(&p, end, &fields[i]) != NGX_OK) {
            return 0;
        }
    }
    if (fields[0].len != ctx->key.len
        || ngx_memcmp(fields[0].data, ctx->key.data, ctx->key.len) != 0
        || p == end)
    {
        return 0;
    }
    count = *p++;
    if (count > 64) {
        return 0;
    }
    for (i = 0; i < count; i++) {
        if (ngx_http_cache_purge_read_field(&p, end, &tags[i]) != NGX_OK) {
            return 0;
        }
    }
    if (end - p < 2) {
        return 0;
    }
    p += 2;
    for (i = 0; i < 6; i++) {
        if (ngx_http_cache_purge_read_field(&p, end, &value) != NGX_OK) {
            return 0;
        }
    }
    if (p != end
        || ngx_http_cache_purge_runtime_reader(ctx->request, &reader, &error)
           != 0)
    {
        return 0;
    }
    ngx_memzero(&match, sizeof(match));
    match.required = ngx_http_cache_purge_capture_read_number(end + 8, 8);
    rc = ngx_http_cache_purge_control_watermark(reader, &current,
                                                 &first, &error);
    if (rc != 0 || first != stamp || match.required > current.sequence) {
        return 0;
    }
    rc = ngx_http_cache_purge_publication_watermarks(fields[9], &stored,
                                                       &validated, &error);
    if (rc != 0 && rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        return 0;
    }
    if (rc == 0 && ngx_memcmp(current.ledger, stored.ledger, 16) == 0) {
        if (stored.sequence > current.sequence) {
            return 0;
        }
        match.covered = stored.sequence;
    }
    if (rc == 0 && ngx_memcmp(current.ledger, validated.ledger, 16) == 0) {
        if (validated.sequence > current.sequence) {
            return 0;
        }
        match.validated = validated.sequence;
    }
    ngx_memzero(&context, sizeof(context));
    context.key = fields[0];
    context.url.data = ctx->url.data;
    context.url.len = ctx->url.len;
    context.host.data = ctx->host.data;
    context.host.len = ctx->host.len;
    context.tag_namespace = fields[3];
    context.tags = tags;
    context.tag_count = count;
    context.now_ms = (uint64_t) ngx_time() * 1000;
    rc = ngx_http_cache_purge_control_match(reader, &context, 64, 65536,
        ngx_http_cache_purge_read_match, &match, &publication, &error);
    if (rc != 0 || match.fenced || publication != first) {
        return 0;
    }
    first = publication;
    if (fields[1].len != ctx->url.len
        || ngx_memcmp(fields[1].data, ctx->url.data, ctx->url.len) != 0
        || fields[2].len != ctx->host.len
        || ngx_memcmp(fields[2].data, ctx->host.data, ctx->host.len) != 0)
    {
        context.url = fields[1];
        context.host = fields[2];
        rc = ngx_http_cache_purge_control_match(reader, &context, 64, 65536,
            ngx_http_cache_purge_read_match, &match, &publication, &error);
        if (rc != 0 || match.fenced || publication != first) {
            return 0;
        }
    }
    rc = ngx_http_cache_purge_control_guard_begin(reader, publication, &error);
    if (rc != 0) {
        return 0;
    }
    /* Commit this synchronous admission before releasing the publication.
     * No guard spans IPC, output filters or asynchronous body streaming. */
    rc = ngx_http_cache_purge_control_guard_end(reader, &error);
    if (rc != 0) {
        return 0;
    }
#if !(NGX_RESTY_EXT)
    if (ctx->read_upstream->cache_status == NGX_HTTP_CACHE_STALE
        || ctx->read_upstream->cache_status == NGX_HTTP_CACHE_UPDATING)
    {
        return 1;
    }
#endif
    return match.required ? NGX_HTTP_CACHE_PURGE_ADMIT_REVALIDATE :
                            NGX_HTTP_CACHE_PURGE_ADMIT_ALLOW;
}

static void
ngx_http_cache_purge_read_validation_reply(ngx_http_cache_purge_call_t *call,
    void *data, const ngx_http_cache_purge_reply_t *reply)
{
    ngx_http_cache_purge_capture_t       *ctx;
    ngx_http_cache_purge_revalidation_t  *v;
    ngx_str_t                            payload, prefix;
    ngx_msec_t                           timeout;
    size_t                               size, length;
    ngx_uint_t                           i;
    ngx_int_t                            allowed;
    u_char                              *p;
    int                                  fds[2];

    ctx = data;
    ctx->call = NULL;
    v = ctx->validation;
    timeout = ctx->deadline - ngx_current_msec;
    allowed = 0;
    if (v == NULL || ctx->request->main->terminated
        || (ngx_msec_int_t) timeout <= 0
        || reply->transport != NGX_HTTP_CACHE_PURGE_CLIENT_OK
        || reply->result != 0)
    {
        goto done;
    }
    if (ctx->rpc == NGX_HTTP_CACHE_PURGE_RPC_VALIDATE) {
        if (reply->payload.len >= 25
            && ngx_memcmp(reply->payload.data + 9, ctx->id, 16) == 0
            && ngx_http_cache_purge_capture_read_number(
                   reply->payload.data + 1, 8) == ctx->revision + 2)
        {
            allowed = ngx_http_cache_purge_read_admit(ctx, reply->payload);
        }
        goto done;
    }
    if (reply->lease_fd < 0 || reply->payload.len < 44) {
        goto done;
    }
    length = ngx_http_cache_purge_capture_read_number(reply->payload.data, 4);
    if (length == 0 || length > 32768 || reply->payload.len != 44 + length
        || ngx_memcmp(reply->payload.data + 4 + length, ctx->id, 16) != 0)
    {
        goto done;
    }
    ctx->lease = fcntl(reply->lease_fd, F_DUPFD_CLOEXEC, 0);
    if (ctx->lease < 0) {
        goto done;
    }
    if (!ctx->origin_valid) {
        goto done;
    }
    size = 74 + 2 * sizeof(v->before);
    for (i = 0; i < ctx->tag_count; i++) {
        size += 4 + ctx->tags[i].len;
    }
    if (ngx_http_cache_purge_capture_allocate(ctx, &payload, size) != NGX_OK) {
        goto done;
    }
    p = ngx_cpymem(payload.data, ctx->id, 16);
    p = ngx_http_cache_purge_capture_number(p, ctx->revision, 8);
    p = ngx_cpymem(p, v->transaction, 16);
    prefix.len = sizeof(v->before);
    prefix.data = (u_char *) &v->before;
    p = ngx_http_cache_purge_capture_string(p, prefix);
    prefix.data = (u_char *) &v->after;
    p = ngx_http_cache_purge_capture_string(p, prefix);
    *p++ = (u_char) v->replace_tags;
    *p++ = (u_char) ctx->tag_count;
    for (i = 0; i < ctx->tag_count; i++) {
        p = ngx_http_cache_purge_capture_string(p, ctx->tags[i]);
    }
    p = ngx_cpymem(p, ctx->origin.ledger, 16);
    p = ngx_http_cache_purge_capture_number(p, ctx->origin.sequence, 8);
    fds[0] = ctx->lease;
    fds[1] = ctx->cache->file.fd;
    ctx->rpc = NGX_HTTP_CACHE_PURGE_RPC_VALIDATE;
    if ((size_t) (p - payload.data) == size
        && ngx_http_cache_purge_client_validate_file(ctx->client, &payload,
            fds, timeout, ngx_http_cache_purge_read_validation_reply, ctx,
            &ctx->call) == NGX_OK)
    {
        return;
    }

done:
    ngx_http_cache_purge_read_complete(ctx, allowed);
}

static ngx_int_t
ngx_http_cache_purge_read_validation_start(ngx_http_cache_purge_capture_t *ctx,
    ngx_str_t record)
{
    ngx_str_t   payload;
    ngx_msec_t  timeout;
    u_char     *p;

    timeout = ctx->deadline - ngx_current_msec;
    if ((ngx_msec_int_t) timeout <= 0 || record.len < 25
        || ngx_http_cache_purge_capture_allocate(ctx, &payload,
            12 + ctx->root.len + ctx->zone.len + ctx->path.len) != NGX_OK)
    {
        return NGX_ERROR;
    }
    ctx->revision = ngx_http_cache_purge_capture_read_number(
        record.data + 1, 8);
    ngx_memcpy(ctx->id, record.data + 9, 16);
    p = ngx_http_cache_purge_capture_string(payload.data, ctx->root);
    p = ngx_http_cache_purge_capture_string(p, ctx->zone);
    ngx_http_cache_purge_capture_string(p, ctx->path);
    ctx->rpc = NGX_HTTP_CACHE_PURGE_RPC_VALIDATE_LEASE;
    return ngx_http_cache_purge_client_submit(ctx->client,
        NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION, &payload, timeout,
        ngx_http_cache_purge_read_validation_reply, ctx, &ctx->call);
}

static void
ngx_http_cache_purge_read_reply(ngx_http_cache_purge_call_t *call, void *data,
    const ngx_http_cache_purge_reply_t *reply)
{
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_int_t                        allowed;

    ctx = data;
    ctx->call = NULL;
    if (reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK
        && reply->result == NGX_HTTP_CACHE_ROCKSDB_BUSY
        && !ctx->request->main->terminated
        && (ngx_msec_int_t) (ctx->deadline - ngx_current_msec) > 10)
    {
        ngx_add_timer(&ctx->read_event, 10);
        return;
    }
    allowed = 0;
    if (reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK
        && reply->result == 0)
    {
        allowed = ngx_http_cache_purge_read_admit(ctx, reply->payload);
    }
    if (allowed && ctx->validation != NULL) {
        if (ngx_http_cache_purge_read_validation_start(ctx, reply->payload)
            == NGX_OK)
        {
            return;
        }
        allowed = 0;
    }
    ngx_http_cache_purge_read_complete(ctx, allowed);
}

static void
ngx_http_cache_purge_read_event(ngx_event_t *event)
{
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_http_request_t              *r;
    ngx_http_cache_t                *c;
    ngx_connection_t                *connection;
    ngx_str_t                       payload, root, zone, path;
    ngx_msec_t  timeout;
    ngx_int_t   rc;
    u_char     *p;

    ctx = event->data;
    r = ctx->request;
    c = ctx->cache;
    connection = r->connection;
    if (ctx->read_state == NGX_HTTP_CACHE_PURGE_READ_START) {
        ctx->read_resume = r->write_event_handler;
        c->waiting = 0;
        ctx->read_state = NGX_HTTP_CACHE_PURGE_READ_READY;
        ctx->read_held = 0;
        ctx->read_ref = 0;
        r->main->blocked--;
        r->main->count--;
        if (r->main->terminated) {
            connection->write->handler(connection->write);
            return;
        }
        ctx->read_resume(r);
        ngx_http_run_posted_requests(connection);
        return;
    }
    timeout = ctx->deadline - ngx_current_msec;
    if (r->main->terminated || (ngx_msec_int_t) timeout <= 0
        || c->file.fd == NGX_INVALID_FILE
        || (ctx->read_upstream->cache_status == NGX_HTTP_CACHE_REVALIDATED
            && ctx->validation == NULL)
        || ngx_http_cache_purge_runtime_client(r, &ctx->client) != NGX_OK)
    {
        ngx_http_cache_purge_read_complete(ctx, 0);
        return;
    }
    if (ctx->validation != NULL && ctx->validation->after.valid_sec == 0) {
        ctx->validation->after.valid_sec = c->valid_sec;
        ctx->validation->after.updating_sec = c->updating_sec;
        ctx->validation->after.error_sec = c->error_sec;
        if (ctx->validation->after.valid_sec == 0) {
#if (NGX_RESTY_EXT)
            ctx->validation->after.valid_sec = ngx_http_file_cache_valid(r,
                ctx->read_upstream->conf->cache_valid,
                ctx->read_upstream->headers_in.status_n);
#else
            ctx->validation->after.valid_sec = ngx_http_file_cache_valid(
                ctx->read_upstream->conf->cache_valid,
                ctx->read_upstream->headers_in.status_n);
#endif
            if (ctx->validation->after.valid_sec) {
                ctx->validation->after.valid_sec += ctx->validation->after.date;
            }
        }
    }
    root = c->file_cache->path->name;
    zone = c->file_cache->shm_zone->shm.name;
    path = c->file.name;
    if (path.len <= root.len + 1
        || ngx_memcmp(path.data, root.data, root.len) != 0
        || path.data[root.len] != '/')
    {
        ngx_http_cache_purge_read_complete(ctx, 0);
        return;
    }
    path.data += root.len + 1;
    path.len -= root.len + 1;
    ctx->root = root;
    ctx->zone = zone;
    ctx->path = path;
    payload.len = 20 + root.len + zone.len + path.len
                  + ctx->key.len + ctx->url.len;
    if (payload.len > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD) {
        ngx_http_cache_purge_read_complete(ctx, 0);
        return;
    }
    payload.data = ngx_alloc(payload.len, connection->log);
    if (payload.data == NULL) {
        ngx_http_cache_purge_read_complete(ctx, 0);
        return;
    }
    p = ngx_http_cache_purge_capture_string(payload.data, root);
    p = ngx_http_cache_purge_capture_string(p, zone);
    p = ngx_http_cache_purge_capture_string(p, path);
    p = ngx_http_cache_purge_capture_string(p, ctx->key);
    ngx_http_cache_purge_capture_string(p, ctx->url);
    rc = ngx_http_cache_purge_client_admit_file(
        ctx->client, &payload, c->file.fd, timeout,
        ngx_http_cache_purge_read_reply, ctx, &ctx->call);
    ngx_free(payload.data);
    if (rc != NGX_OK) {
        ngx_http_cache_purge_read_complete(ctx, 0);
    }
}

static ngx_int_t
ngx_http_cache_purge_read_header(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_request_t  *r;
    ngx_uint_t           updated;

    r = ctx->request;
    if (ctx->redirect) {
        ctx->read_upstream->headers_in.x_accel_redirect = ctx->redirect->target;
        if (!ctx->redirect->ignored) {
            ctx->read_upstream->headers_in.headers = ctx->redirect->headers;
        }
    }
    if (r->main->blocked >= NGX_HTTP_MAX_BLOCKED || r->main->count == 65535
        || ngx_http_cache_purge_read_count
           >= NGX_HTTP_CACHE_PURGE_CAPTURE_COUNT)
    {
        return NGX_ERROR;
    }
    ctx->read_state = NGX_HTTP_CACHE_PURGE_READ_WAITING;
    ctx->read_header_only = r->header_only;
    ctx->read_held = 1;
    ctx->read_ref = 1;
    ctx->deadline = ngx_current_msec + NGX_HTTP_CACHE_PURGE_CAPTURE_TIMEOUT;
    ngx_http_cache_purge_read_count++;
    r->header_only = 1;
    r->main->blocked++;
    r->main->count++;
    if (ctx->read_upstream->cleanup != NULL) {
        /* Retain the cache node, but stop native upstream EOF/timer handling
         * before the asynchronous decision. Its finalizer consumes one ref. */
        updated = ctx->cache->updated;
        ctx->cache->updated = 1;
        (*ctx->read_upstream->cleanup)(r);
        ctx->cache->updated = updated;
        ctx->read_ref = 0;
    }
    ngx_post_event(&ctx->read_event, &ngx_posted_events);
    return NGX_DONE;
}

static ngx_int_t
ngx_http_cache_purge_capture_allocate(ngx_http_cache_purge_capture_t *ctx,
    ngx_str_t *payload, size_t bytes)
{
    if (bytes > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD
        || ngx_http_cache_purge_capture_reserve(ctx, bytes) != NGX_OK)
    {
        return NGX_ERROR;
    }
    payload->len = bytes;
    payload->data = ngx_pnalloc(ctx->publication_pool, bytes);
    return payload->data ? NGX_OK : NGX_ERROR;
}
static ngx_msec_t
ngx_http_cache_purge_capture_remaining(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_msec_int_t left = (ngx_msec_int_t) (ctx->deadline - ngx_current_msec);
    return left > 0 ? (ngx_msec_t) left : 0;
}

static ngx_int_t
ngx_http_cache_purge_capture_submit_prepare(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_str_t payload, metadata;
    ngx_str_t fields[] = {
        ctx->key,  ctx->url,  ctx->host,          ctx->namespace,  ctx->root,
        ctx->zone, ctx->path, ctx->components[5], ctx->cache->vary
    };
    size_t      size = 16 + 9 * 4 + 4 + 1 + 2 + 6 * 4 + 16 + 4;
    ngx_uint_t  i;
    ngx_msec_t timeout = ngx_http_cache_purge_capture_remaining(ctx);
    ngx_int_t  rc;
    u_char    *p;
    int        fds[2] = { ctx->lease, ctx->temporary->file.fd };
    u_char     watermark[NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE];

    if (!timeout || !ctx->origin_valid) {
        return NGX_ERROR;
    }
    ngx_http_cache_purge_watermark_encode(&ctx->origin, watermark);
    metadata.data = watermark;
    metadata.len = sizeof(watermark);
    size += metadata.len;
    for (i = 0; i < 9; i++) {
        if (fields[i].len > 32768) {
            return NGX_ERROR;
        }
        size += fields[i].len;
    }
    for (i = 0; i < 6; i++) {
        size += ctx->components[i].len;
    }
    for (i = 0; i < ctx->tag_count; i++) {
        size += 4 + ctx->tags[i].len;
    }
    size += ctx->temporary->file.name.len;
    if (ngx_http_cache_purge_capture_allocate(ctx, &payload, size) != NGX_OK) {
        return NGX_ERROR;
    }
    p = ngx_cpymem(payload.data, ctx->id, 16);
    for (i = 0; i < 9; i++) {
        p = ngx_http_cache_purge_capture_string(p, fields[i]);
    }
    p = ngx_http_cache_purge_capture_string(p, metadata);
    *p++ = (u_char) ctx->tag_count;
    for (i = 0; i < ctx->tag_count; i++) {
        p = ngx_http_cache_purge_capture_string(p, ctx->tags[i]);
    }
    *p++ = ctx->structured_key;
    *p++ = ctx->structured_url;
    for (i = 0; i < 6; i++) {
        p = ngx_http_cache_purge_capture_string(p, ctx->components[i]);
    }
    p = ngx_cpymem(p, ctx->previous, 16);
    p = ngx_http_cache_purge_capture_string(p, ctx->temporary->file.name);
    if ((size_t) (p - payload.data) != size) {
        return NGX_ERROR;
    }
    ctx->rpc = NGX_HTTP_CACHE_PURGE_RPC_PREPARE;
    rc = ngx_http_cache_purge_client_submit_files(
        ctx->client, &payload, fds, timeout, ngx_http_cache_purge_capture_reply,
        ctx, &ctx->call);
    if (rc == NGX_OK) {
        ctx->need_finish = 1;
    }
    return rc;
}

static ngx_int_t
ngx_http_cache_purge_capture_step(ngx_http_request_t *r,
    ngx_http_cache_t *cache, ngx_temp_file_t *tf, ngx_uint_t phase,
    ngx_int_t result, void *data)
{
    ngx_http_cache_purge_capture_t  *ctx = data;
    ngx_str_t                        payload, root, name;
    u_char                          *p;
    ngx_int_t                        rc;
    ngx_msec_t                       timeout;
    if (phase == NGX_HTTP_CACHE_PURGE_FILL_FINISH) {
        if (!ctx->need_finish || ctx->lease < 0) {
            ngx_http_cache_purge_capture_release_publication(ctx);
            ngx_http_cache_purge_capture_origin_release(ctx);
            return NGX_OK;
        }
        timeout = ngx_http_cache_purge_capture_remaining(ctx);
        if (!timeout
            || ngx_http_cache_purge_capture_allocate(ctx, &payload, 24)
                   != NGX_OK)
        {
            ngx_http_cache_purge_capture_release_publication(ctx);
            ngx_http_cache_purge_capture_origin_release(ctx);
            return NGX_ERROR;
        }
        p = ngx_cpymem(payload.data, ctx->id, 16);
        ngx_http_cache_purge_capture_number(p, ctx->revision, 8);
        ctx->rpc = NGX_HTTP_CACHE_PURGE_RPC_FINISH;
        rc = ngx_http_cache_purge_client_finish_file(
            ctx->client, &payload, ctx->lease, timeout,
            ngx_http_cache_purge_capture_reply, ctx, &ctx->call);
        if (rc == NGX_OK) {
            return NGX_AGAIN;
        }
        ngx_http_cache_purge_capture_release_publication(ctx);
        ngx_http_cache_purge_capture_origin_release(ctx);
        return NGX_ERROR;
    }
    /* Publication limits apply after a complete body exists, never to HITs
     * or requests still receiving an origin response. Input values belong
     * to immutable key snapshots or complex-value request-pool buffers. */
    if (ngx_http_cache_purge_retained_count
            >= NGX_HTTP_CACHE_PURGE_CAPTURE_COUNT
        || ngx_http_cache_purge_capture_reserve(ctx,
                                                sizeof(*ctx) + ctx->input_bytes)
               != NGX_OK)
    {
        return NGX_DECLINED;
    }
    ngx_http_cache_purge_retained_count++;
    ctx->admitted = 1;
    ctx->publication_pool =
        ngx_create_pool(NGX_DEFAULT_POOL_SIZE, r->connection->log);
    if (ctx->publication_pool == NULL) {
        return NGX_DECLINED;
    }
    ctx->temporary = tf;
    ctx->deadline = ngx_current_msec + NGX_HTTP_CACHE_PURGE_CAPTURE_TIMEOUT;
    /* New FileIds start at revision 1. It also identifies a PREPARE whose
     * reply was lost; FINISH then observes NOT_FOUND or retires its intent. */
    ctx->revision = 1;
    if (ngx_http_cache_purge_runtime_client(r, &ctx->client) != 0
        || getrandom(ctx->id, sizeof(ctx->id), GRND_NONBLOCK)
               != (ssize_t) sizeof(ctx->id))
    {
        return NGX_DECLINED;
    }
    root = cache->file_cache->path->name;
    name = cache->file.name;
    if (name.len <= root.len + 1 || ngx_memcmp(name.data, root.data, root.len)
        || name.data[root.len] != '/')
    {
        return NGX_DECLINED;
    }
    name.data += root.len + 1;
    name.len -= root.len + 1;
    ctx->path = name;
    ctx->zone = cache->file_cache->shm_zone->shm.name;
    if (ctx->zone.len > 32768 || root.len > 32768 || name.len > 32768
        || ngx_http_cache_purge_capture_allocate(
               ctx, &payload, 12 + root.len + ctx->zone.len + name.len)
               != NGX_OK)
    {
        return NGX_DECLINED;
    }
    p = ngx_http_cache_purge_capture_string(payload.data, root);
    p = ngx_http_cache_purge_capture_string(p, ctx->zone);
    ngx_http_cache_purge_capture_string(p, name);
    ctx->rpc = NGX_HTTP_CACHE_PURGE_RPC_ACQUIRE;
    rc = ngx_http_cache_purge_client_submit(
        ctx->client, NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION, &payload,
        ngx_http_cache_purge_capture_remaining(ctx),
        ngx_http_cache_purge_capture_reply, ctx, &ctx->call);
    return rc == NGX_OK ? NGX_AGAIN : NGX_DECLINED;
}

static void
ngx_http_cache_purge_capture_reply(ngx_http_cache_purge_call_t *call,
    void *data, const ngx_http_cache_purge_reply_t *reply)
{
    ngx_http_cache_purge_capture_t  *ctx = data;
    ngx_uint_t                       stage = ctx->rpc;
    ngx_int_t                        rc = NGX_DECLINED;
    ngx_str_t                        root;
    size_t                           length;
    ctx->call = NULL;
    if (stage == NGX_HTTP_CACHE_PURGE_RPC_PREPARE && !reply->may_have_executed)
    {
        ctx->need_finish = 0;
    }
    if (reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK
        && reply->result == 0)
    {
        if (stage == NGX_HTTP_CACHE_PURGE_RPC_ACQUIRE && reply->lease_fd >= 0
            && reply->payload.len >= 44)
        {
            length = ngx_http_cache_purge_capture_read_number(
                reply->payload.data, 4);
            if (length && length <= 32768 && reply->payload.len == 44 + length)
            {
                root.data = reply->payload.data + 4;
                root.len = length;
                ctx->lease = fcntl(reply->lease_fd, F_DUPFD_CLOEXEC, 0);
                if (ctx->lease >= 0
                    && ngx_http_cache_purge_capture_copy(ctx, &ctx->root, root)
                           == NGX_OK)
                {
                    ngx_memcpy(ctx->previous, root.data + root.len, 16);
                    if (ngx_http_cache_purge_capture_submit_prepare(ctx)
                        == NGX_OK)
                    {
                        return;
                    }
                }
            }
        } else if (stage != NGX_HTTP_CACHE_PURGE_RPC_ACQUIRE
                   && reply->payload.len == 25
                   && ngx_memcmp(reply->payload.data, ctx->id, 16) == 0)
        {
            if (stage == NGX_HTTP_CACHE_PURGE_RPC_PREPARE
                && ngx_http_cache_purge_capture_read_number(
                       reply->payload.data + 16, 8)
                       == 1
                && reply->payload.data[24] == 1)
            {
                rc = NGX_OK;
            }
            if (stage == NGX_HTTP_CACHE_PURGE_RPC_FINISH
                && (reply->payload.data[24] == 2
                    || reply->payload.data[24] == 3))
            {
                rc = NGX_OK;
            }
        }
    } else if (stage == NGX_HTTP_CACHE_PURGE_RPC_FINISH
               && reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK
               && reply->result == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND)
    {
        rc = NGX_OK;
    }
    if (rc != NGX_OK) {
        ngx_log_error(NGX_LOG_WARN, ctx->request->connection->log, 0,
                      "cache index publication stage %ui failed: transport=%ui "
                      "result=%uD",
                      stage, (ngx_uint_t) reply->transport, reply->result);
    }
    if (stage == NGX_HTTP_CACHE_PURGE_RPC_FINISH) {
        ngx_http_cache_purge_capture_release_publication(ctx);
        ngx_http_cache_purge_capture_origin_release(ctx);
    }
    ngx_http_cache_purge_fill_complete(ctx->fill, rc);
}

static void *
ngx_http_cache_purge_capture_create_main(ngx_conf_t *cf)
{
    ngx_http_cache_purge_capture_main_t *main =
        ngx_pcalloc(cf->pool, sizeof(*main));
    if (main == NULL
        || ngx_array_init(&main->locations, cf->pool, 4, sizeof(void **))
               != NGX_OK)
    {
        return NULL;
    }
    return main;
}
static void *
ngx_http_cache_purge_capture_create_conf(ngx_conf_t *cf)
{
    return ngx_pcalloc(cf->pool, sizeof(ngx_http_cache_purge_capture_conf_t));
}
static char *
ngx_http_cache_purge_capture_merge(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_cache_purge_capture_conf_t  *p = parent, *c = child;
    ngx_http_cache_purge_capture_main_t  *main;
    ngx_http_compile_complex_value_t      compile;
    ngx_str_t                            host = ngx_string("$host");
    void     ***location;
    ngx_uint_t  i;
    for (i = 0; i < NGX_HTTP_CACHE_PURGE_FIELD_COUNT; i++) {
        if (c->field[i] == NULL) {
            c->field[i] = p->field[i];
        }
    }
    if (c->field[NGX_HTTP_CACHE_PURGE_FIELD_NAMESPACE] == NULL) {
        c->field[NGX_HTTP_CACHE_PURGE_FIELD_NAMESPACE] =
            ngx_pcalloc(cf->pool, sizeof(ngx_http_complex_value_t));
        if (c->field[NGX_HTTP_CACHE_PURGE_FIELD_NAMESPACE] == NULL) {
            return NGX_CONF_ERROR;
        }
        ngx_memzero(&compile, sizeof(compile));
        compile.cf = cf;
        compile.value = &host;
        compile.complex_value = c->field[NGX_HTTP_CACHE_PURGE_FIELD_NAMESPACE];
        if (ngx_http_compile_complex_value(&compile) != NGX_OK) {
            return NGX_CONF_ERROR;
        }
    }
    if (c->field[NGX_HTTP_CACHE_PURGE_FIELD_KEY]
        && ngx_http_cache_purge_capture_equal(
            c->field[NGX_HTTP_CACHE_PURGE_FIELD_KEY]->value, "$cache_key_full"))
    {
        ngx_conf_log_error(
            NGX_LOG_EMERG, cf, 0,
            "cache_purge_key must exclude range; use $cache_key_base");
        return NGX_CONF_ERROR;
    }
    main = ngx_http_conf_get_module_main_conf(
        cf, ngx_http_cache_purge_index_module);
    location = ngx_array_push(&main->locations);
    if (location == NULL) {
        return NGX_CONF_ERROR;
    }
    *location = ((ngx_http_conf_ctx_t *) cf->ctx)->loc_conf;
    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_cache_purge_capture_post(ngx_conf_t *cf)
{
    static struct {
        ngx_http_script_len_code_pt code;
        uintptr_t                   end;
    } lengths = { ngx_http_cache_purge_capture_guard_length, 0 };
    static const char *names[] = { "proxy_cache_bypass", "fastcgi_cache_bypass",
                                   "scgi_cache_bypass", "uwsgi_cache_bypass" };
    ngx_http_cache_purge_capture_main_t *main =
        ngx_http_conf_get_module_main_conf(cf,
                                           ngx_http_cache_purge_index_module);
    ngx_http_cache_purge_capture_conf_t  *conf;
    ngx_http_cache_purge_capture_main_t  *old;
    ngx_http_core_loc_conf_t             *core;
    ngx_module_t                         *module;
    ngx_command_t                        *cmd;
    ngx_array_t                         **slot, *array;
    ngx_http_complex_value_t             *value;
    ngx_http_upstream_conf_t             *upstream;
    ngx_http_cache_purge_capture_code_t  *code;
    void                               ***locations = main->locations.elts;
    ngx_uint_t                            i, j, k, n;
    ngx_str_t                            host = ngx_string("host");
    main->host_index = ngx_http_get_variable_index(cf, &host);
    if (main->host_index == NGX_ERROR) {
        return NGX_ERROR;
    }
#if (NGX_HTTP_CACHE_KEY && NGX_HAVE_DLOPEN)
    {
        void *handle = ngx_dlopen(NULL);
        if (handle) {
            main->snapshot =
                (void *) ngx_dlsym(handle, "ngx_http_cache_key_cache_snapshot");
            main->value =
                (void *) ngx_dlsym(handle, "ngx_http_cache_key_snapshot_value");
            ngx_dlclose(handle);
        }
        if (main->value == NULL) {
            main->snapshot = NULL;
        }
    }
#endif
    for (k = 0; k < main->locations.nelts; k++) {
        conf = locations[k][ngx_http_cache_purge_index_module.ctx_index];
        if (conf->field[NGX_HTTP_CACHE_PURGE_FIELD_KEY] == NULL) {
            continue;
        }
        main->enabled = 1;
        if (!ngx_http_cache_purge_runtime_configured(cf)) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "cache_purge_key requires cache_purge_store and "
                               "cache_purge_state_zone");
            return NGX_ERROR;
        }
        for (n = 0; n < NGX_HTTP_CACHE_PURGE_FIELD_COUNT; n++) {
            if (n == NGX_HTTP_CACHE_PURGE_FIELD_TAG || conf->field[n] == NULL) {
                continue;
            }
            if (ngx_http_cache_purge_capture_equal(conf->field[n]->value,
                                                   "$cache_key_base")
                || ngx_http_cache_purge_capture_equal(conf->field[n]->value,
                                                      "$cache_key_url")
                || ngx_http_cache_purge_capture_equal(conf->field[n]->value,
                                                      "$cache_key_namespace"))
            {
#if (NGX_HTTP_CACHE_KEY)
                if (main->snapshot && main->value) {
                    continue;
                }
#endif
                ngx_conf_log_error(
                    NGX_LOG_EMERG, cf, 0,
                    "cache index requires the native cache key snapshot API");
                return NGX_ERROR;
            }
        }
    }
    if (cf->cycle->old_cycle && cf->cycle->old_cycle->conf_ctx
        && cf->cycle->old_cycle->conf_ctx[ngx_http_module.index])
    {
        old = NULL;
        for (i = 0; cf->cycle->old_cycle->modules[i]; i++) {
            if (cf->cycle->old_cycle->modules[i]
                == &ngx_http_cache_purge_index_module)
            {
                old = ngx_http_cycle_get_module_main_conf(
                    cf->cycle->old_cycle, ngx_http_cache_purge_index_module);
                break;
            }
        }
        if ((old ? old->enabled : 0) != main->enabled) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "enabling/disabling cache_purge_key indexing requires a full "
                "restart to replace memory-only cache error state");
            return NGX_ERROR;
        }
    }
    for (i = 0; cf->cycle->modules[i]; i++) {
        module = cf->cycle->modules[i];
        if (module->type != NGX_HTTP_MODULE || module->commands == NULL) {
            continue;
        }
        for (cmd = module->commands; cmd->name.len; cmd++) {
            for (j = 0;
                 j < 4
                 && !ngx_http_cache_purge_capture_equal(cmd->name, names[j]);
                 j++)
            {
            }
            if (j == 4) {
                continue;
            }
            if (cmd->set != ngx_http_set_predicate_slot
                || cmd->conf != NGX_HTTP_LOC_CONF_OFFSET
                || cmd->offset
                       < offsetof(ngx_http_upstream_conf_t, cache_bypass))
            {
                return NGX_ERROR;
            }
            for (k = 0; k < main->locations.nelts; k++) {
                conf =
                    locations[k][ngx_http_cache_purge_index_module.ctx_index];
                if (locations[k][module->ctx_index] == NULL)
                {
                    continue;
                }
                slot =
                    (ngx_array_t **) ((u_char *) locations[k][module->ctx_index]
                                      + cmd->offset);
                upstream =
                    (ngx_http_upstream_conf_t *) ((u_char *) slot
                                                  - offsetof(
                                                      ngx_http_upstream_conf_t,
                                                      cache_bypass));
                if (upstream->cache <= 0) {
                    continue;
                }
                if (conf->field[NGX_HTTP_CACHE_PURGE_FIELD_KEY] == NULL) {
                    core = locations[k][ngx_http_core_module.ctx_index];
                    if (main->enabled && core->handler != NULL) {
                        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                            "location \"%V\" has upstream caching without "
                            "cache_purge_key; inherit or configure an index "
                            "key, or disable its upstream cache", &core->name);
                        return NGX_ERROR;
                    }
                    if (!main->enabled) {
                        continue;
                    }
                }
                if (*slot && *slot != NGX_CONF_UNSET_PTR && (*slot)->nelts) {
                    value = (*slot)->elts;
                    if (value[0].lengths == (void *) &lengths) {
                        continue;
                    }
                }
                array = ngx_array_create(cf->pool, 1, sizeof(*value));
                code = ngx_pcalloc(cf->pool, sizeof(*code));
                if (array == NULL || code == NULL) {
                    return NGX_ERROR;
                }
                value = ngx_array_push(array);
                if (value == NULL) {
                    return NGX_ERROR;
                }
                ngx_memzero(value, sizeof(*value));
                code->code = ngx_http_cache_purge_capture_guard_value;
                code->predicates = *slot == NGX_CONF_UNSET_PTR ? NULL : *slot;
                value->lengths = (void *) &lengths;
                value->values = (void *) code;
                *slot = array;
            }
        }
    }
    ngx_http_cache_purge_capture_next_header = ngx_http_top_header_filter;
    ngx_http_top_header_filter = ngx_http_cache_purge_capture_header;
    return NGX_OK;
}

#endif /* NGX_HTTP_CACHE_PURGE_ROCKSDB */


#if (NGX_HTTP_CACHE)

#if (NGX_HTTP_FASTCGI || NGX_HTTP_PROXY || NGX_HTTP_SCGI || NGX_HTTP_UWSGI)
static ngx_int_t ngx_http_cache_purge_defer(ngx_http_request_t *r);
#endif
#if (NGX_HTTP_FASTCGI || NGX_HTTP_PROXY || NGX_HTTP_SCGI || NGX_HTTP_UWSGI)
static void ngx_http_cache_purge_request_handler(ngx_event_t *ev);
#endif
#if (NGX_HTTP_FASTCGI || NGX_HTTP_PROXY || NGX_HTTP_SCGI || NGX_HTTP_UWSGI)
static void ngx_http_cache_purge_request_cleanup(void *data);
#endif
#if (NGX_HTTP_FASTCGI || NGX_HTTP_PROXY || NGX_HTTP_SCGI || NGX_HTTP_UWSGI)
static ngx_int_t ngx_http_cache_purge_dispatch(ngx_http_request_t *r);
#endif
ngx_int_t ngx_http_cache_purge_access(ngx_array_t *access, ngx_array_t *access6,
    struct sockaddr *s);
#if (nginx_version >= 1007009)
ngx_int_t ngx_http_cache_purge_cache_get(ngx_http_request_t *r,
    ngx_http_upstream_t *u, ngx_http_file_cache_t **cache);
#endif
ngx_int_t        ngx_http_cache_purge_init(ngx_http_request_t *r,
    ngx_http_file_cache_t *cache, ngx_http_complex_value_t *cache_key);
void             ngx_http_cache_purge_handler(ngx_http_request_t *r);
ngx_int_t        ngx_http_file_cache_purge(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_walk_reply(ngx_http_request_t *r,
    ngx_http_cache_purge_walk_ctx_t *ctx, ngx_int_t rc);
ngx_int_t ngx_http_cache_purge_all(ngx_http_request_t *r,
    ngx_http_file_cache_t *cache);
ngx_int_t ngx_http_cache_purge_partial(ngx_http_request_t *r,
    ngx_http_file_cache_t *cache);
ngx_int_t ngx_http_cache_purge_is_partial(ngx_http_request_t *r);

#if (NGX_HTTP_FASTCGI || NGX_HTTP_PROXY || NGX_HTTP_SCGI || NGX_HTTP_UWSGI)
/* Wait for admission while retaining core disconnect and stream handling. */
static ngx_int_t
ngx_http_cache_purge_defer(ngx_http_request_t *r)
{
    ngx_http_cache_purge_request_t    *pr;
    ngx_http_cleanup_t                *cln;
    ngx_http_cache_purge_main_conf_t  *cmcf;
    ngx_http_cache_purge_path_t       *path;
    ngx_str_t                         *name;

    cln = ngx_http_cleanup_add(r, sizeof(*pr));
    if (cln == NULL) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }
    pr = cln->data;
    ngx_memzero(pr, sizeof(*pr));
    cln->handler = ngx_http_cache_purge_request_cleanup;
    pr->cleanup = cln;
    pr->request = r;
    pr->event.handler = ngx_http_cache_purge_request_handler;
    pr->event.data = pr;
    pr->event.log = r->connection->log;
    /* Keep core disconnect handling, including HTTP/2 and HTTP/3 streams. */
    if (!r->discard_body) {
        r->read_event_handler = ngx_http_test_reading;
        if (r->connection->read->ready) {
            ngx_post_event(r->connection->read, &ngx_posted_events);

        } else if (ngx_handle_read_event(r->connection->read, 0) != NGX_OK) {
            cln->handler = NULL;
            return ngx_http_cache_purge_reply(
                r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
        }
    }
    cmcf = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    name = &r->cache->file_cache->path->name;
    for (path = cmcf->queue->paths; path != NULL; path = path->next) {
        if (path->name.len == ngx_http_cache_purge_path_length(name)
            && ngx_http_cache_purge_path_equal(path->name.data, name->data,
                                               path->name.len))
        {
            pr->path = path;
            ngx_http_cache_purge_stat_wait(path, 1);
            ngx_http_cache_purge_stat_add(path, NGX_HTTP_CACHE_PURGE_OVERFLOW,
                                          1);
            break;
        }
    }
    r->main->count++;
    ngx_add_timer(&pr->event, 10);
    return NGX_DONE;
}


static void
ngx_http_cache_purge_request_handler(ngx_event_t *ev)
{
    ngx_http_cache_purge_request_t   *pr = ev->data;
    ngx_http_request_t               *r = pr->request;
    ngx_http_cache_purge_loc_conf_t  *cplcf;
    ngx_str_t                        *key;
    ngx_int_t                         rc;

    cplcf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    key = r->cache->keys.elts;
    rc = ngx_http_cache_purge_enqueue(r, r->cache->file_cache, &key[0],
                                      cplcf->conf->purge_all, NULL);
    if (rc == NGX_AGAIN) {
        ngx_add_timer(ev, 10);
        return;
    }
    pr->cleanup->handler = NULL;
    ngx_http_cache_purge_stat_wait(pr->path, -1);
    if (rc == NGX_OK) {
        rc = ngx_http_cache_purge_reply(
            r, cplcf->namespace != NULL ?
                   NGX_HTTP_CACHE_PURGE_RESULT_INVALIDATED :
                   NGX_HTTP_CACHE_PURGE_RESULT_ACCEPTED);
    } else {
        rc = rc == NGX_ABORT ?
                 NGX_HTTP_CLOSE :
                 ngx_http_cache_purge_reply(
                     r, rc == NGX_DECLINED ?
                            NGX_HTTP_CACHE_PURGE_RESULT_INVALID_SELECTOR :
                            NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }
    ngx_http_finalize_request(r, rc);
}


static void
ngx_http_cache_purge_request_cleanup(void *data)
{
    ngx_http_cache_purge_request_t *pr = data;

    if (pr->event.timer_set) {
        ngx_del_timer(&pr->event);
    }
    ngx_http_cache_purge_stat_wait(pr->path, -1);
}


static ngx_int_t
ngx_http_cache_purge_dispatch(ngx_http_request_t *r)
{
    ngx_http_cache_purge_main_conf_t  *cmcf;
    ngx_http_cache_purge_loc_conf_t   *conf;
    ngx_http_file_cache_t             *cache;
    ngx_str_t                         *key;
    ngx_int_t                          rc;
    size_t                             len;

    cache = r->cache->file_cache;
    cmcf = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    key = r->cache->keys.elts;
    len = key[0].len;
    if ((conf->namespace != NULL && len > NGX_HTTP_CACHE_PURGE_LOGICAL_KEY_MAX)
        || (conf->namespace == NULL && !conf->conf->purge_all
            && ((ngx_http_cache_purge_is_partial(r)
                 && len > NGX_HTTP_CACHE_PURGE_KEY_MAX_LEN)
                || (!ngx_http_cache_purge_is_partial(r) && cmcf->vary_aware
                    && len >= NGX_HTTP_CACHE_PURGE_KEY_MAX_LEN))))
    {
        ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                      "ngx_http_cache_purge: selector of %uz bytes exceeds the "
                      "supported limit, not purging",
                      len);
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INVALID_SELECTOR);
    }
#if (NGX_HTTP_PROXY)
    if (conf->refresh) {
        return ngx_http_cache_refresh_start(r);
    }
#endif
    if (conf->namespace != NULL
        || (cmcf->background_purge
            && (conf->conf->purge_all || ngx_http_cache_purge_is_partial(r))))
    {
        rc = ngx_http_cache_purge_enqueue(r, cache, key, conf->conf->purge_all,
                                          NULL);
        if (rc == NGX_OK) {
            return ngx_http_cache_purge_reply(
                r, conf->namespace != NULL ?
                       NGX_HTTP_CACHE_PURGE_RESULT_INVALIDATED :
                       NGX_HTTP_CACHE_PURGE_RESULT_ACCEPTED);
        }
        if (rc == NGX_AGAIN) {
            return ngx_http_cache_purge_defer(r);
        }
        return rc == NGX_ABORT ?
                   NGX_HTTP_CLOSE :
                   ngx_http_cache_purge_reply(
                       r, rc == NGX_DECLINED ?
                              NGX_HTTP_CACHE_PURGE_RESULT_INVALID_SELECTOR :
                              NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }
    if (conf->conf->purge_all) {
        return ngx_http_cache_purge_all(r, cache);
    }
    if (ngx_http_cache_purge_is_partial(r)) {
        return ngx_http_cache_purge_partial(r, cache);
    }
    r->main->count++;
    ngx_http_cache_purge_handler(r);
    return NGX_DONE;
}
#endif

ngx_int_t
ngx_http_cache_purge_access_handler(ngx_http_request_t *r)
{
    ngx_http_cache_purge_loc_conf_t *cplcf;

    cplcf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);

    /* A content handler needs a resolved per-protocol configuration. */
    if (cplcf->conf == NULL) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }

    if (r->method_name.len != cplcf->conf->method.len
        || ngx_strncmp(r->method_name.data, cplcf->conf->method.data,
                       r->method_name.len)
               != 0)
    {
        /*
         * Not a purge request.  Forward to the original content handler
         * if one exists (e.g. proxy_pass), otherwise return 405 with Allow.
         * original_handler is NULL when proxy_cache is used without
         * proxy_pass (cache-only / purge-only location).
         */
        if (cplcf->original_handler != NULL) {
            return cplcf->original_handler(r);
        }
        return ngx_http_cache_purge_method_response(r, &cplcf->conf->method);
    }

    if ((cplcf->conf->access || cplcf->conf->access6)
        && ngx_http_cache_purge_access(cplcf->conf->access,
                                       cplcf->conf->access6,
                                       r->connection->sockaddr)
               != NGX_OK)
    {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "ngx_http_cache_purge: access denied for %V",
                      &r->connection->addr_text);
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "ngx_http_cache_purge: client address is not in "
                       "the \"from\" list");
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_ACCESS_DENIED);
    }

    if (cplcf->handler == NULL) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }

    return cplcf->handler(r);
}


ngx_int_t
ngx_http_cache_purge_access(ngx_array_t *access, ngx_array_t *access6,
    struct sockaddr *s)
{
    in_addr_t       inaddr;
    ngx_in_cidr_t  *a;
    ngx_uint_t      i;
#if (NGX_HAVE_INET6)
    struct in6_addr  *inaddr6;
    ngx_in6_cidr_t   *a6;
    u_char           *p;
    ngx_uint_t        n;
#else
    (void) access6;
#endif

    switch (s->sa_family) {
    case AF_INET:
        if (access == NULL) {
            return NGX_DECLINED;
        }

        inaddr = ((struct sockaddr_in *) s)->sin_addr.s_addr;

#if (NGX_HAVE_INET6)
    ipv4:
#endif
        a = access->elts;
        for (i = 0; i < access->nelts; i++) {
            if ((inaddr & a[i].mask) == a[i].addr) {
                return NGX_OK;
            }
        }
        return NGX_DECLINED;

#if (NGX_HAVE_INET6)
    case AF_INET6:
        inaddr6 = &((struct sockaddr_in6 *) s)->sin6_addr;
        p = inaddr6->s6_addr;

        if (access && IN6_IS_ADDR_V4MAPPED(inaddr6)) {
            inaddr = (in_addr_t) p[12] << 24;
            inaddr += (in_addr_t) p[13] << 16;
            inaddr += (in_addr_t) p[14] << 8;
            inaddr += p[15];
            inaddr = htonl(inaddr);
            goto ipv4;
        }

        if (access6 == NULL) {
            return NGX_DECLINED;
        }

        a6 = access6->elts;
        for (i = 0; i < access6->nelts; i++) {
            for (n = 0; n < 16; n++) {
                if ((p[n] & a6[i].mask.s6_addr[n]) != a6[i].addr.s6_addr[n]) {
                    goto next;
                }
            }
            return NGX_OK;
        next:
            continue;
        }
        return NGX_DECLINED;
#endif
    }

    return NGX_DECLINED;
}

#if (nginx_version >= 1007009)
ngx_int_t
ngx_http_cache_purge_cache_get(ngx_http_request_t *r, ngx_http_upstream_t *u,
    ngx_http_file_cache_t **cache)
{
    ngx_str_t               *name;
    ngx_str_t                val;
    ngx_uint_t               i;
    ngx_http_file_cache_t  **caches;

    if (u->conf->cache_zone) {
        *cache = u->conf->cache_zone->data;
        return NGX_OK;
    }

    if (u->conf->cache_value == NULL) {
        ngx_log_error(
            NGX_LOG_ERR, r->connection->log, 0,
            "ngx_http_cache_purge: no cache configured for this location");
        return NGX_HTTP_NOT_FOUND;
    }

    if (ngx_http_complex_value(r, u->conf->cache_value, &val) != NGX_OK) {
        return NGX_ERROR;
    }

    if (val.len == 0 || (val.len == 3 && ngx_strncmp(val.data, "off", 3) == 0))
    {
        return NGX_DECLINED;
    }

    caches = u->caches->elts;

    for (i = 0; i < u->caches->nelts; i++) {
        name = &caches[i]->shm_zone->shm.name;
        if (name->len == val.len
            && ngx_strncmp(name->data, val.data, val.len) == 0)
        {
            *cache = caches[i];
            return NGX_OK;
        }
    }

    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "ngx_http_cache_purge: cache zone \"%V\" not found", &val);

    return NGX_ERROR;
}
#endif


#if (NGX_HTTP_FASTCGI || NGX_HTTP_PROXY || NGX_HTTP_SCGI || NGX_HTTP_UWSGI)
static ngx_int_t
ngx_http_cache_purge_init_response(ngx_http_request_t *r, ngx_int_t rc)
{
    if (rc >= NGX_HTTP_BAD_REQUEST) {
        /* Core rejected the body before the module could execute. */
        return rc;
    }
    return ngx_http_cache_purge_reply(
        r, rc == NGX_BUSY     ? NGX_HTTP_CACHE_PURGE_RESULT_INVALID_SELECTOR :
           rc == NGX_DECLINED ? NGX_HTTP_CACHE_PURGE_RESULT_KEY_MISMATCH :
                                NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
}

#endif

/* -- request init ------------------------------------------------------- */

ngx_int_t
ngx_http_cache_purge_init(ngx_http_request_t *r, ngx_http_file_cache_t *cache,
    ngx_http_complex_value_t *cache_key)
{
    ngx_http_cache_t                 *c;
    ngx_str_t                        *key;
    ngx_int_t                         rc;
    ngx_http_cache_purge_loc_conf_t  *conf;
    ngx_str_t                         logical = ngx_null_string;
    ngx_str_t                         encoded = ngx_null_string;

    rc = ngx_http_discard_request_body(r);
    if (rc != NGX_OK) {
        return rc;
    }

    c = ngx_pcalloc(r->pool, sizeof(ngx_http_cache_t));
    if (c == NULL) {
        return NGX_ERROR;
    }

    rc = ngx_array_init(&c->keys, r->pool, 1, sizeof(ngx_str_t));
    if (rc != NGX_OK) {
        return NGX_ERROR;
    }

    key = ngx_array_push(&c->keys);
    if (key == NULL) {
        return NGX_ERROR;
    }

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    if (conf->namespace != NULL) {
        rc = ngx_http_cache_purge_generation_key(
            r, conf->namespace, &conf->logical_key, &logical, &encoded);
        if (rc != NGX_OK) {
            return rc == NGX_DECLINED ? NGX_BUSY : NGX_ERROR;
        }
    }
    rc = ngx_http_complex_value(r, cache_key, key);
    if (rc != NGX_OK) {
        return NGX_ERROR;
    }
    if (conf->namespace != NULL) {
        if (key->len != encoded.len
            || ngx_memcmp(key->data, encoded.data, encoded.len))
        {
            ngx_log_error(
                NGX_LOG_ERR, r->connection->log, 0,
                "ngx_http_cache_purge: native cache key for namespace "
                "\"%V\" must be $cache_purge_key",
                &conf->namespace->name);
            return NGX_DECLINED;
        }
        *key = logical;
    }

    r->cache = c;
    c->body_start = ngx_pagesize;
    c->file_cache = cache;
    c->file.log = r->connection->log;

    ngx_http_file_cache_create_key(r);

    return NGX_OK;
}


/* -- purge dispatch ----------------------------------------------------- */

void
ngx_http_cache_purge_handler(ngx_http_request_t *r)
{
    ngx_http_cache_purge_main_conf_t  *cmcf;
    ngx_http_cache_purge_walk_ctx_t    ctx;
    ngx_int_t                          rc, walk_rc;

#if (NGX_HAVE_FILE_AIO)
    if (r->aio) {
        return;
    }
#endif
    cmcf = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    rc = ngx_http_file_cache_purge(r);
    switch (rc) {
    case NGX_OK:
    case NGX_DECLINED:
        r->write_event_handler = ngx_http_request_empty_handler;
        ngx_memzero(&ctx, sizeof(ctx));
        walk_rc = NGX_OK;
        if (cmcf->vary_aware) {
            walk_rc = ngx_http_cache_purge_delete_variants(
                r, r->cache->file_cache, &ctx);
        }
        if (rc == NGX_OK && ctx.files_deleted != (ngx_uint_t) -1) {
            ctx.files_deleted++;
        }
        ngx_http_finalize_request(
            r, ngx_http_cache_purge_walk_reply(r, &ctx, walk_rc));
        return;
#if (NGX_HAVE_FILE_AIO)
    case NGX_AGAIN:
        r->write_event_handler = ngx_http_cache_purge_handler;
        return;
#endif
    case NGX_BUSY:
        ngx_http_finalize_request(
            r, ngx_http_cache_purge_reply(
                   r, NGX_HTTP_CACHE_PURGE_RESULT_DELETE_FAILED));
        return;
    default:
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "ngx_http_cache_purge: cache open returned %i", rc);
        ngx_http_finalize_request(
            r, ngx_http_cache_purge_reply(
                   r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR));
    }
}


/* -- file cache purge --------------------------------------------------- */

ngx_int_t
ngx_http_file_cache_purge(ngx_http_request_t *r)
{
    ngx_http_file_cache_t  *cache;
    ngx_http_cache_t       *c;
    ngx_int_t               rc;

    switch (ngx_http_file_cache_open(r)) {
    case NGX_OK:
    case NGX_HTTP_CACHE_STALE:
#if (nginx_version >= 8001)                                                    \
    || ((nginx_version < 8000) && (nginx_version >= 7060))
    case NGX_HTTP_CACHE_UPDATING:
#endif
        break;

    case NGX_DECLINED:
        return NGX_DECLINED;

#if (NGX_HAVE_FILE_AIO)
    case NGX_AGAIN:
        return NGX_AGAIN;
#endif

    default:
        return NGX_ERROR;
    }

    c = r->cache;
    cache = c->file_cache;

    ngx_shmtx_lock(&cache->shpool->mutex);

    if (!c->node->exists) {
        ngx_shmtx_unlock(&cache->shpool->mutex);
        return NGX_DECLINED;
    }

#if (nginx_version >= 1000001)
    cache->sh->size -= c->node->fs_size;
    c->node->fs_size = 0;
#else
    cache->sh->size -= (c->node->length + cache->bsize - 1) / cache->bsize;
    c->node->length = 0;
#endif

    c->node->exists = 0;
#if (nginx_version >= 8001)                                                    \
    || ((nginx_version < 8000) && (nginx_version >= 7060))
    c->node->updating = 0;
#endif

    ngx_shmtx_unlock(&cache->shpool->mutex);

    rc = ngx_http_cache_purge_unlink(r->connection->log, c->file.name.data,
                                     NULL);
    if (rc == NGX_OK) {
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "ngx_http_cache_purge: deleted \"%V\"", &c->file.name);
    }

    return rc == NGX_ERROR ? NGX_BUSY : rc;
}


static ngx_int_t
ngx_http_cache_purge_walk_reply(ngx_http_request_t *r,
    ngx_http_cache_purge_walk_ctx_t *ctx, ngx_int_t rc)
{
    ngx_http_cache_purge_result_t result;

    ngx_memzero(&result, sizeof(result));
    result.counted = 1;
    result.deleted = ctx->files_deleted;
    result.delete_errors = ctx->delete_errors;
    result.scan_errors = ctx->scan_errors;
    result.code = rc == NGX_ABORT ? NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR :
                  ctx->delete_errors ?
                                    NGX_HTTP_CACHE_PURGE_RESULT_DELETE_FAILED :
                  (ctx->scan_errors || rc != NGX_OK) ?
                                    NGX_HTTP_CACHE_PURGE_RESULT_SCAN_FAILED :
                  ctx->files_deleted ? NGX_HTTP_CACHE_PURGE_RESULT_PURGED :
                                       NGX_HTTP_CACHE_PURGE_RESULT_NO_MATCH;
    return ngx_http_cache_purge_send_result(r, &result);
}


/* -- bulk walk helpers -------------------------------------------------- */

ngx_int_t
ngx_http_cache_purge_all(ngx_http_request_t *r, ngx_http_file_cache_t *cache)
{
    ngx_http_cache_purge_main_conf_t  *cmcf;
    ngx_http_cache_purge_walk_ctx_t    ctx;
    ngx_int_t                          rc;

    cmcf = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    ngx_http_cache_purge_walk_init(&ctx, &cache->path->name, cmcf->protection,
                                   cmcf->paths, ngx_cycle->log);
    rc = ngx_http_cache_purge_walk(&ctx);
    return ngx_http_cache_purge_walk_reply(r, &ctx, rc);
}


ngx_int_t
ngx_http_cache_purge_partial(ngx_http_request_t *r,
    ngx_http_file_cache_t *cache)
{
    ngx_http_cache_purge_main_conf_t  *cmcf;
    ngx_http_cache_purge_walk_ctx_t    ctx;
    ngx_str_t                         *key;
    ngx_int_t                          rc;

    key = r->cache->keys.elts;
    cmcf = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    ngx_http_cache_purge_walk_init(&ctx, &cache->path->name, cmcf->protection,
                                   cmcf->paths, ngx_cycle->log);
    ctx.key_partial = key[0].data;
    ctx.key_len = key[0].len - 1;
    rc = ngx_http_cache_purge_walk(&ctx);
    return ngx_http_cache_purge_walk_reply(r, &ctx, rc);
}


ngx_int_t
ngx_http_cache_purge_is_partial(ngx_http_request_t *r)
{
    ngx_http_cache_t  *c = r->cache;
    ngx_str_t         *key = c->keys.elts;

    return c->keys.nelts > 0 && key[0].len > 0
           && key[0].data[key[0].len - 1] == '*';
}


#if (NGX_HTTP_FASTCGI)
ngx_int_t
ngx_http_fastcgi_cache_purge_handler(ngx_http_request_t *r)
{
    ngx_int_t                     rc;
    ngx_http_file_cache_t        *cache;
    ngx_http_fastcgi_loc_conf_t  *flcf;
#if (nginx_version >= 1007009)
    ngx_http_fastcgi_main_conf_t *fmcf;
#endif

    if (ngx_http_upstream_create(r) != NGX_OK) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }

    flcf = ngx_http_get_module_loc_conf(r, ngx_http_fastcgi_module);
    r->upstream->conf = &flcf->upstream;

#if (nginx_version >= 1007009)
    fmcf = ngx_http_get_module_main_conf(r, ngx_http_fastcgi_module);
    r->upstream->caches = &fmcf->caches;

    rc = ngx_http_cache_purge_cache_get(r, r->upstream, &cache);
    if (rc != NGX_OK) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_CACHE_UNAVAILABLE);
    }
#else
    cache = flcf->upstream.cache->data;
#endif

    rc = ngx_http_cache_purge_init(r, cache, &flcf->cache_key);
    if (rc != NGX_OK) {
        return ngx_http_cache_purge_init_response(r, rc);
    }

    return ngx_http_cache_purge_dispatch(r);
}
#endif

#if (NGX_HTTP_PROXY)
ngx_int_t
ngx_http_proxy_cache_purge_handler(ngx_http_request_t *r)
{
    ngx_int_t                         rc;
    ngx_http_file_cache_t            *cache;
    ngx_http_proxy_loc_conf_t        *plcf;
    ngx_http_cache_purge_loc_conf_t  *cplcf;
#if (nginx_version >= 1007009)
    ngx_http_proxy_main_conf_t  *pmcf;
    ngx_uint_t                   i;
    ngx_str_t                   *name;
    ngx_http_file_cache_t      **caches;
    ngx_str_t                    cv_val;
#endif

    cplcf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);

    /* Resolve separate-location state independently of proxy_pass. */
    if (cplcf->proxy.enable == 0
        && (cplcf->proxy_separate_zone || cplcf->proxy_separate_value))
    {
        if (cplcf->proxy_separate_zone) {
            cache = cplcf->proxy_separate_zone->data;
        } else {
            /* dynamic zone name -- evaluate and walk the proxy caches list */
            if (ngx_http_complex_value(r, cplcf->proxy_separate_value, &cv_val)
                != NGX_OK)
            {
                return ngx_http_cache_purge_reply(
                    r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
            }

#if (nginx_version >= 1007009)
            pmcf = ngx_http_get_module_main_conf(r, ngx_http_proxy_module);
            caches = pmcf->caches.elts;
            cache = NULL;

            for (i = 0; i < pmcf->caches.nelts; i++) {
                name = &caches[i]->shm_zone->shm.name;
                if (name->len == cv_val.len
                    && ngx_strncmp(name->data, cv_val.data, cv_val.len) == 0)
                {
                    cache = caches[i];
                    break;
                }
            }

            if (cache == NULL) {
                ngx_log_error(
                    NGX_LOG_ERR, r->connection->log, 0,
                    "ngx_http_cache_purge: cache zone \"%V\" not found",
                    &cv_val);
                return ngx_http_cache_purge_reply(
                    r, NGX_HTTP_CACHE_PURGE_RESULT_CACHE_UNAVAILABLE);
            }
#else
            cache = cplcf->proxy_separate_zone->data;
#endif
        }

        rc = ngx_http_cache_purge_init(r, cache, &cplcf->proxy_separate_key);
        if (rc != NGX_OK) {
            return ngx_http_cache_purge_init_response(r, rc);
        }

    } else {
        /* Inline syntax (proxy_cache_purge METHOD from ...): use plcf->upstream
         * as before. */
        if (ngx_http_upstream_create(r) != NGX_OK) {
            return ngx_http_cache_purge_reply(
                r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
        }

        plcf = ngx_http_get_module_loc_conf(r, ngx_http_proxy_module);
        r->upstream->conf = &plcf->upstream;

#if (nginx_version >= 1007009)
        pmcf = ngx_http_get_module_main_conf(r, ngx_http_proxy_module);
        r->upstream->caches = &pmcf->caches;

        rc = ngx_http_cache_purge_cache_get(r, r->upstream, &cache);
        if (rc != NGX_OK) {
            return ngx_http_cache_purge_reply(
                r, NGX_HTTP_CACHE_PURGE_RESULT_CACHE_UNAVAILABLE);
        }
#else
        cache = plcf->upstream.cache->data;
#endif

        rc = ngx_http_cache_purge_init(r, cache, &plcf->cache_key);
        if (rc != NGX_OK) {
            return ngx_http_cache_purge_init_response(r, rc);
        }
    }

    return ngx_http_cache_purge_dispatch(r);
}
#endif

#if (NGX_HTTP_SCGI)
ngx_int_t
ngx_http_scgi_cache_purge_handler(ngx_http_request_t *r)
{
    ngx_int_t                  rc;
    ngx_http_file_cache_t     *cache;
    ngx_http_scgi_loc_conf_t  *slcf;
#if (nginx_version >= 1007009)
    ngx_http_scgi_main_conf_t *smcf;
#endif

    if (ngx_http_upstream_create(r) != NGX_OK) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }

    slcf = ngx_http_get_module_loc_conf(r, ngx_http_scgi_module);
    r->upstream->conf = &slcf->upstream;

#if (nginx_version >= 1007009)
    smcf = ngx_http_get_module_main_conf(r, ngx_http_scgi_module);
    r->upstream->caches = &smcf->caches;

    rc = ngx_http_cache_purge_cache_get(r, r->upstream, &cache);
    if (rc != NGX_OK) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_CACHE_UNAVAILABLE);
    }
#else
    cache = slcf->upstream.cache->data;
#endif

    rc = ngx_http_cache_purge_init(r, cache, &slcf->cache_key);
    if (rc != NGX_OK) {
        return ngx_http_cache_purge_init_response(r, rc);
    }

    return ngx_http_cache_purge_dispatch(r);
}
#endif

#if (NGX_HTTP_UWSGI)
ngx_int_t
ngx_http_uwsgi_cache_purge_handler(ngx_http_request_t *r)
{
    ngx_int_t                   rc;
    ngx_http_file_cache_t      *cache;
    ngx_http_uwsgi_loc_conf_t  *ulcf;
#if (nginx_version >= 1007009)
    ngx_http_uwsgi_main_conf_t *umcf;
#endif

    if (ngx_http_upstream_create(r) != NGX_OK) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }

    ulcf = ngx_http_get_module_loc_conf(r, ngx_http_uwsgi_module);
    r->upstream->conf = &ulcf->upstream;

#if (nginx_version >= 1007009)
    umcf = ngx_http_get_module_main_conf(r, ngx_http_uwsgi_module);
    r->upstream->caches = &umcf->caches;

    rc = ngx_http_cache_purge_cache_get(r, r->upstream, &cache);
    if (rc != NGX_OK) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_CACHE_UNAVAILABLE);
    }
#else
    cache = ulcf->upstream.cache->data;
#endif

    rc = ngx_http_cache_purge_init(r, cache, &ulcf->cache_key);
    if (rc != NGX_OK) {
        return ngx_http_cache_purge_init_response(r, rc);
    }

    return ngx_http_cache_purge_dispatch(r);
}
#endif


#endif


#if (NGX_HTTP_CACHE)

typedef struct {
    ngx_uint_t  status;
    ngx_str_t   code;
    ngx_str_t   title;
    ngx_str_t   detail;
} ngx_http_cache_purge_response_t;

#define ngx_http_cache_purge_response_entry(s, c, t, d) \
    { s, ngx_string(c), ngx_string(t), ngx_string(d) }

static const ngx_http_cache_purge_response_t
    ngx_http_cache_purge_responses[] = {
        ngx_http_cache_purge_response_entry(200, "purged", "OK",
                                      "Matching cache files were deleted."),
        ngx_http_cache_purge_response_entry(200, "no_match", "OK",
                                      "No matching cache entry was found."),
        ngx_http_cache_purge_response_entry(
            202, "accepted", "Accepted",
            "Cleanup was accepted; completion is not guaranteed."),
        ngx_http_cache_purge_response_entry(
            200, "invalidated", "OK",
            "Logical invalidation is complete; physical cleanup is pending."),
        ngx_http_cache_purge_response_entry(200, "status", "OK",
                                      "Queue status snapshot."),
        ngx_http_cache_purge_response_entry(
            200, "refresh_complete", "OK",
            "The refresh scan finished; inspect each entry result."),
        ngx_http_cache_purge_response_entry(
            200, "refresh_incomplete", "OK",
            "Refresh is incomplete; inspect the cursor and entry results."),
        ngx_http_cache_purge_response_entry(
            400, "invalid_selector", "Bad Request",
            "The selector exceeds the supported key length."),
        ngx_http_cache_purge_response_entry(
            400, "invalid_cursor", "Bad Request",
            "Provide one cursor containing exactly 32 hexadecimal digits."),
        ngx_http_cache_purge_response_entry(
            405, "method_not_allowed", "Method Not Allowed",
            "Use a method listed in the Allow header."),
        ngx_http_cache_purge_response_entry(
            403, "access_denied", "Forbidden",
            "The client is not permitted to purge this cache."),
        ngx_http_cache_purge_response_entry(
            500, "cache_unavailable", "Internal Server Error",
            "No usable cache zone is configured for this operation."),
        ngx_http_cache_purge_response_entry(500, "key_mismatch",
                                      "Internal Server Error",
                                      "The configured cache key does not "
                                      "satisfy the namespace contract."),
        ngx_http_cache_purge_response_entry(500, "delete_failed",
                                      "Internal Server Error",
                                      "At least one deletion failed; some "
                                      "files may already have been removed."),
        ngx_http_cache_purge_response_entry(500, "scan_failed",
                                      "Internal Server Error",
                                      "The cache scan failed; some files may "
                                      "already have been processed."),
        ngx_http_cache_purge_response_entry(500, "internal_error",
                                      "Internal Server Error",
                                      "The operation could not be completed; "
                                      "consult the server error log.")
    };

typedef char ngx_http_cache_purge_catalog_size
    [(sizeof(ngx_http_cache_purge_responses)
          / sizeof(ngx_http_cache_purge_responses[0])
      == NGX_HTTP_CACHE_PURGE_RESULT_COUNT) ?
         1 :
         -1];

static const ngx_http_cache_purge_response_t *
ngx_http_cache_purge_response(ngx_http_request_t *r,
    ngx_http_cache_purge_result_code_e code)
{
    if ((ngx_uint_t) code >= NGX_HTTP_CACHE_PURGE_RESULT_COUNT) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "ngx_http_cache_purge: unexpected result code %ui",
                      (ngx_uint_t) code);
        code = NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR;
    }
    return &ngx_http_cache_purge_responses[code];
}


static ngx_int_t
ngx_http_cache_purge_header(ngx_http_request_t *r, ngx_str_t *name,
    const ngx_str_t *value)
{
    ngx_list_part_t  *part;
    ngx_table_elt_t  *h;
    ngx_uint_t        i;

    part = &r->headers_out.headers.part;
    for (/* void */; /* void */; part = part->next) {
        h = part->elts;
        for (i = 0; i < part->nelts; i++) {
            if (h[i].key.len == name->len
                && ngx_strncasecmp(h[i].key.data, name->data, name->len) == 0)
            {
                h[i].hash = 1;
                h[i].value = *value;
                return NGX_OK;
            }
        }
        if (part->next == NULL) {
            break;
        }
    }
    h = ngx_list_push(&r->headers_out.headers);
    if (h == NULL) {
        return NGX_ERROR;
    }
    ngx_memzero(h, sizeof(*h));
    h->hash = 1;
    h->key = *name;
    h->value = *value;
    return NGX_OK;
}


ngx_int_t
ngx_http_cache_purge_response_headers(ngx_http_request_t *r,
    ngx_http_cache_purge_result_code_e code)
{
    const ngx_http_cache_purge_response_t *result;
    ngx_str_t name = ngx_string("Cache-Purge-Code");
    ngx_str_t cache_control = ngx_string("Cache-Control");
    ngx_str_t no_store = ngx_string("no-store");

    result = ngx_http_cache_purge_response(r, code);
    r->headers_out.status = result->status;
    if (ngx_http_cache_purge_header(r, &name, &result->code) != NGX_OK
        || ngx_http_cache_purge_header(r, &cache_control, &no_store) != NGX_OK)
    {
        return NGX_ERROR;
    }
    return NGX_OK;
}


ngx_int_t
ngx_http_cache_purge_method_response(ngx_http_request_t *r, ngx_str_t *allow)
{
    ngx_str_t name = ngx_string("Allow");

    if (ngx_http_cache_purge_header(r, &name, allow) != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    return ngx_http_cache_purge_reply(
        r, NGX_HTTP_CACHE_PURGE_RESULT_METHOD_NOT_ALLOWED);
}


/* Keep JSON UTF-8 and XML 1.0 valid even for a binary configured cache key. */
static ngx_uint_t
ngx_http_cache_purge_text_key(ngx_str_t *key)
{
    u_char   *p, *end;
    uint32_t  c;

    p = key->data;
    end = p + key->len;
    while (p < end) {
        c = *p;
        if (c < 0x80) {
            p++;
        } else {
            c = ngx_utf8_decode(&p, (size_t) (end - p));
        }
        if (c > 0x10ffff || (c >= 0xd800 && c <= 0xdfff) || c == 0xfffe
            || c == 0xffff || (c < 0x20 && c != '\t' && c != '\n' && c != '\r'))
        {
            return 0;
        }
    }
    return 1;
}


static uintptr_t
ngx_http_cache_purge_escape_key(u_char *dst, u_char *src, size_t len,
    ngx_uint_t type)
{
    size_t extra;

    if (len == 0) {
        return (uintptr_t) dst;
    }

    if (type == NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_JSON) {
        return ngx_escape_json(dst, src, len);
    }

    if (type == NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_HTML) {
        return ngx_escape_html(dst, src, len);
    }

    if (type == NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_TEXT) {
        return dst ? (uintptr_t) ngx_cpymem(dst, src, len) : 0;
    }

    /* Split a CDATA terminator without changing the decoded key. */
    extra = 0;
    while (len != 0) {
        if (len >= 3 && src[0] == ']' && src[1] == ']' && src[2] == '>') {
            if (dst != NULL) {
                dst = ngx_cpymem(dst, "]]]]><![CDATA[>", 15);
            }
            extra += 12;
            src += 3;
            len -= 3;

        } else {
            if (dst != NULL) {
                *dst++ = *src;
            }
            src++;
            len--;
        }
    }

    return dst ? (uintptr_t) dst : extra;
}


ngx_int_t
ngx_http_cache_purge_reply(ngx_http_request_t *r,
    ngx_http_cache_purge_result_code_e code)
{
    ngx_http_cache_purge_result_t result;

    ngx_memzero(&result, sizeof(result));
    result.code = code;
    return ngx_http_cache_purge_send_result(r, &result);
}


ngx_int_t
ngx_http_cache_purge_send_result(ngx_http_request_t *r,
    ngx_http_cache_purge_result_t *result)
{
    const ngx_http_cache_purge_response_t  *desc;
    ngx_http_cache_purge_loc_conf_t        *conf;
    ngx_str_t                               key = ngx_null_string;
    ngx_str_t                              encoding = ngx_string("utf-8");
    ngx_str_t    raw;
    ngx_buf_t   *b;
    ngx_chain_t  out;
    ngx_uint_t   type, complete, pending;
    ngx_int_t    rc;
    size_t       len;

    desc = ngx_http_cache_purge_response(r, result->code);
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    type = conf->response_type;
    if (conf->queue_status_cache_path.data != NULL) {
        type = NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_JSON;
    }
#if (NGX_HTTP_PROXY)
    if (conf->refresh) {
        type = NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_JSON;
    }
#endif
    if (ngx_http_cache_purge_response_headers(r, result->code) != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    if (type == NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_NONE) {
        if (desc->status == NGX_HTTP_OK) {
            r->headers_out.status = NGX_HTTP_NO_CONTENT;
        }
        r->headers_out.content_length_n = 0;
        r->header_only = 1;
        return ngx_http_send_header(r);
    }
    if (r->cache != NULL && r->cache->keys.nelts != 0) {
        key = ((ngx_str_t *) r->cache->keys.elts)[0];
    }
    if (desc->status >= 400) {
        key.len = 0;
        type = NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_JSON;
    }
    if (key.len > ((size_t) -1 - 2048) / 8) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    if (key.len && !ngx_http_cache_purge_text_key(&key)) {
        raw = key;
        key.len = ngx_base64_encoded_length(raw.len);
        key.data = ngx_pnalloc(r->pool, key.len);
        if (key.data == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        ngx_encode_base64(&key, &raw);
        ngx_str_set(&encoding, "base64");
    }
    len = 2048 + key.len * 6;
    b = ngx_create_temp_buf(r->pool, len);
    if (b == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    complete = desc->status == NGX_HTTP_OK;
    pending = result->code == NGX_HTTP_CACHE_PURGE_RESULT_ACCEPTED
              || result->code == NGX_HTTP_CACHE_PURGE_RESULT_INVALIDATED;
    if (desc->status >= 400) {
        ngx_str_set(&r->headers_out.content_type, "application/problem+json");
        b->last = ngx_sprintf(
            b->last,
            "{\"type\":\"about:blank\",\"title\":\"%V\",\"status\":%ui,"
            "\"detail\":\"%V\",\"code\":\"%V\"",
            &desc->title, desc->status, &desc->detail, &desc->code);
    } else {
        switch (type) {
        case NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_HTML:
            ngx_str_set(&r->headers_out.content_type, "text/html");
            b->last = ngx_sprintf(
                b->last,
                "<!doctype html><html><head><title>Cache Purge</title></head>"
                "<body><h1>Cache Purge</h1><p>Key: ");
            break;
        case NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_XML:
            ngx_str_set(&r->headers_out.content_type, "application/xml");
            b->last = ngx_sprintf(
                b->last, "<?xml version=\"1.0\" "
                         "encoding=\"UTF-8\"?><response><Key><![CDATA[");
            break;
        case NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_TEXT:
            ngx_str_set(&r->headers_out.content_type, "text/plain");
            b->last = ngx_sprintf(b->last, "Key: ");
            break;
        default:
            type = NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_JSON;
            ngx_str_set(&r->headers_out.content_type, "application/json");
            b->last = ngx_sprintf(b->last, "{\"key\":\"");
        }
        b->last = (u_char *) ngx_http_cache_purge_escape_key(b->last, key.data,
                                                             key.len, type);
        switch (type) {
        case NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_HTML:
            b->last = ngx_sprintf(
                b->last,
                "</p><p>Code: %V</p><p>Key encoding: %V</p></body></html>",
                &desc->code, &encoding);
            break;
        case NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_XML:
            b->last = ngx_sprintf(b->last,
                                  "]]></Key><Code>%V</Code><KeyEncoding>%V</"
                                  "KeyEncoding></response>",
                                  &desc->code, &encoding);
            break;
        case NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_TEXT:
            b->last = ngx_sprintf(b->last, "\nCode: %V\nKey encoding: %V\n",
                                  &desc->code, &encoding);
            break;
        default:
            b->last = ngx_sprintf(
                b->last,
                "\",\"key_encoding\":\"%V\",\"code\":\"%V\",\"complete\":%s,"
                "\"cleanup_pending\":%s",
                &encoding, &desc->code, complete ? "true" : "false",
                pending ? "true" : "false");
        }
    }
    if (type == NGX_HTTP_CACHE_PURGE_RESPONSE_TYPE_JSON) {
        if (result->counted) {
            b->last = ngx_sprintf(
                b->last,
                ",\"deleted\":%ui,\"delete_errors\":%ui,\"scan_errors\":%ui",
                result->deleted, result->delete_errors, result->scan_errors);
        }
        *b->last++ = '}';
    }
    b->last_buf = (r == r->main);
    b->last_in_chain = 1;
    r->headers_out.content_length_n = b->last - b->pos;
    rc = ngx_http_send_header(r);
    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }
    out.buf = b;
    out.next = NULL;
    return ngx_http_output_filter(r, &out);
}

#endif


#if (NGX_RESTY_EXT && NGX_EXPR)
#include <ngx_http_expr_module.h>
#endif

#if (NGX_HTTP_CACHE)
/* -- bounded conditional REFRESH ---------------------------------------- */

#if (NGX_HTTP_PROXY)

enum {
    NGX_REFRESH_PENDING,
    NGX_REFRESH_KEPT,
    NGX_REFRESH_INVALIDATED,
    NGX_REFRESH_TRANSPORT,
    NGX_REFRESH_STATUS,
    NGX_REFRESH_VARY,
    NGX_REFRESH_PROFILE,
    NGX_REFRESH_REPLACED,
    NGX_REFRESH_ERROR
};

typedef struct {
    u_char                        hash[32];
    u_char                        key[NGX_HTTP_CACHE_PURGE_ENTRY_KEY_MAX + 1];
    size_t                        key_len;
    ngx_http_file_cache_header_t  header;
    ngx_uint_t                    status, result;
} ngx_http_cache_refresh_entry_t;

typedef struct {
    /* The visitor receives this first member, never a live HTTP request. */
    ngx_http_cache_purge_walk_ctx_t  walk;
    ngx_http_cache_purge_cursor_t    cursor;
    ngx_path_t                       path;
    ngx_str_t                        filter;
    u_char                           after[32];

    ngx_http_cache_refresh_entry_t  entries[NGX_HTTP_CACHE_PURGE_REFRESH_BATCH];

    ngx_uint_t    count;
    ngx_flag_t    more, complete, failed, timed_out;
    ngx_msec_t    deadline;
    ngx_atomic_t  cancelled;
} ngx_http_cache_refresh_scan_t;

typedef struct {
    ngx_pool_t                       *pool;
    ngx_http_request_t               *request;
    ngx_http_cleanup_t               *cleanup;
    ngx_event_t                       event;
    ngx_http_cache_refresh_scan_t    *scan;
    ngx_http_cache_purge_queue_t     *queue;
    ngx_http_file_cache_t            *cache;
    ngx_http_cache_purge_loc_conf_t  *conf;
    ngx_msec_t                        deadline;
    ngx_uint_t                        position;
    ngx_flag_t                        scanning, active, credit, decision,
                                      timed_out;
    u_char                            after[32];
    ngx_str_t                         uri, etag, modified;
    u_char     date[sizeof("Mon, 28 Sep 1970 06:00:00 GMT")];
#if (NGX_THREADS && !NGX_WIN32)
    ngx_thread_task_t  *task;
    ngx_thread_pool_t  *thread_pool;
#endif
} ngx_http_cache_refresh_t;

/* One bounded batch per worker; additional HTTP requests wait without I/O. */
#if (NGX_HTTP_PROXY)
static ngx_uint_t
ngx_http_cache_refresh_canonical(ngx_http_cache_refresh_scan_t *scan,
    ngx_str_t *path);
#endif
#if (NGX_HTTP_PROXY)
static void ngx_http_cache_refresh_collect(void *data, ngx_str_t *path);
#endif
#if (NGX_HTTP_PROXY)
static ngx_int_t ngx_http_cache_refresh_compare(const void *left,
    const void *right);
#endif
#if (NGX_HTTP_PROXY)
static ngx_int_t
ngx_http_cache_refresh_scan(ngx_http_cache_refresh_scan_t *scan);
#endif
#if (NGX_HTTP_PROXY)
static ngx_int_t ngx_http_cache_refresh_credit(ngx_http_cache_refresh_t *job,
    ngx_flag_t take);
#endif
#if (NGX_HTTP_PROXY)
static void ngx_http_cache_refresh_destroy(ngx_http_cache_refresh_t *job);
#endif
#if (NGX_HTTP_PROXY)
static void ngx_http_cache_refresh_cleanup(void *data);
#endif
#if (NGX_HTTP_PROXY && NGX_THREADS && !NGX_WIN32)
static void ngx_http_cache_refresh_thread(void *data, ngx_log_t *log);
#endif
#if (NGX_HTTP_PROXY && NGX_THREADS && !NGX_WIN32)
static void ngx_http_cache_refresh_thread_done(ngx_event_t *ev);
#endif
#if (NGX_HTTP_PROXY)
static ngx_int_t ngx_http_cache_refresh_reply(ngx_http_cache_refresh_t *job);
#endif
#if (NGX_HTTP_PROXY)
static ngx_int_t
ngx_http_cache_refresh_probe(ngx_http_cache_refresh_t *job,
    ngx_http_cache_refresh_entry_t *entry);
#endif
#if (NGX_HTTP_PROXY)
static ngx_int_t ngx_http_cache_refresh_done(ngx_http_request_t *sr, void *data,
    ngx_int_t rc);
#endif
#if (NGX_HTTP_PROXY)
static void ngx_http_cache_refresh_step(ngx_event_t *ev);
#endif

static ngx_http_cache_refresh_t *ngx_cache_refresh_current;
static void                      ngx_http_cache_refresh_step(ngx_event_t *ev);
static ngx_int_t ngx_http_cache_refresh_done(ngx_http_request_t *sr, void *data,
    ngx_int_t rc);


char *
ngx_http_cache_refresh_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_cache_purge_loc_conf_t  *location = conf;
    ngx_http_core_loc_conf_t         *core;
    char                             *rc;

    rc = ngx_http_proxy_cache_purge_conf(cf, cmd, conf);
    if (rc != NGX_CONF_OK) {
        return rc;
    }
    location->refresh = 1;
    core = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    core->handler = ngx_http_cache_refresh_handler;
    return NGX_CONF_OK;
}


char *
ngx_http_cache_refresh_profile(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_cache_purge_loc_conf_t  *location = conf;
    ngx_str_t                        *value = cf->args->elts;

    (void) cmd;
    if (location->refresh_location.data != NULL) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "ngx_http_cache_purge: \"%V\" directive "
                           "is duplicate",
                           &cmd->name);
        return NGX_CONF_ERROR;
    }
    if (value[1].len == 0 || value[1].data[0] != '/'
        || ngx_strchr(value[1].data, '?') || ngx_strchr(value[1].data, '$'))
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "ngx_http_cache_purge: \"%V\" directive "
                           "requires a literal internal location URI "
                           "without arguments or variables; got \"%V\"",
                           &cmd->name, &value[1]);
        return NGX_CONF_ERROR;
    }
    location->refresh_location = value[1];
    location->refresh_prefix = value[2];
    return NGX_CONF_OK;
}


ngx_int_t
ngx_http_cache_refresh_handler(ngx_http_request_t *r)
{
    ngx_str_t allow = ngx_string("REFRESH");

    if (r != r->main || r->method_name.len != 7
        || ngx_memcmp(r->method_name.data, "REFRESH", 7))
    {
        return ngx_http_cache_purge_method_response(r, &allow);
    }
    return ngx_http_proxy_cache_purge_handler(r);
}


/* Admit only the canonical native hash path: at most one record per cursor. */
static ngx_uint_t
ngx_http_cache_refresh_canonical(ngx_http_cache_refresh_scan_t *scan,
    ngx_str_t *path)
{
    size_t   i, level, offset, tail;
    u_char  *hash;

    if (path->len != scan->path.name.len + 1 + scan->path.len + 32) {
        return 0;
    }
    hash = path->data + path->len - 32;
    offset = scan->path.name.len + 1;
    tail = 32;
    for (i = 0; i < NGX_MAX_PATH_LEVEL; i++) {
        level = scan->path.level[i];
        if (level == 0) {
            break;
        }
        tail -= level;
        if (ngx_memcmp(path->data + offset, hash + tail, level)
            || path->data[offset + level] != '/')
        {
            return 0;
        }
        offset += level + 1;
    }
    return 1;
}


static void
ngx_http_cache_refresh_collect(void *data, ngx_str_t *path)
{
    ngx_http_cache_refresh_scan_t    *scan = data;
    ngx_http_cache_refresh_entry_t   *entry;
    ngx_http_cache_purge_walk_ctx_t  *walk = &scan->walk;
    ngx_str_t                         key, logical;
    ngx_uint_t                        i, largest;
    ssize_t                           rc;
    size_t                            len;
    u_char                           *hash;

    if (!ngx_http_cache_purge_is_cache_file(path)
        || ngx_http_cache_purge_is_protected_path(walk, path)
        || !ngx_http_cache_refresh_canonical(scan, path))
    {
        return;
    }
    hash = path->data + path->len - 32;
    if (ngx_memcmp(hash, scan->after, 32) <= 0) {
        return;
    }
    rc = ngx_http_cache_purge_read_key(walk, path, 0);
    if (rc != 0) {
        if (rc == NGX_ERROR) {
            walk->scan_errors++;
        }
        return;
    }
    key.data = walk->key_buffer;
    key.len = walk->entry_key_len;
    if (ngx_http_cache_purge_generation_current(walk->namespace, &key, &logical)
        != NGX_OK)
    {
        return;
    }
    len = scan->filter.len;
    if (len != 0 && scan->filter.data[len - 1] == '*') {
        len--;
        if (logical.len < len
            || ngx_strncasecmp(logical.data, scan->filter.data, len))
        {
            return;
        }
    } else if (logical.len != len
               || ngx_memcmp(logical.data, scan->filter.data, len))
    {
        return;
    }

    largest = 0;
    for (i = 0; i < scan->count; i++) {
        if (ngx_memcmp(hash, scan->entries[i].hash, 32) == 0) {
            return;
        }
        if (ngx_memcmp(scan->entries[i].hash, scan->entries[largest].hash, 32)
            > 0)
        {
            largest = i;
        }
    }
    if (scan->count == NGX_HTTP_CACHE_PURGE_REFRESH_BATCH) {
        scan->more = 1;
        if (ngx_memcmp(hash, scan->entries[largest].hash, 32) >= 0) {
            return;
        }
        entry = &scan->entries[largest];
    } else {
        entry = &scan->entries[scan->count++];
    }
    ngx_memcpy(entry->hash, hash, 32);
    ngx_memcpy(entry->key, key.data, key.len);
    entry->key_len = key.len;
    entry->header = walk->header;
}


static ngx_int_t
ngx_http_cache_refresh_compare(const void *left, const void *right)
{
    const ngx_http_cache_refresh_entry_t *a = left, *b = right;
    return ngx_memcmp(a->hash, b->hash, 32);
}


static ngx_int_t
ngx_http_cache_refresh_scan(ngx_http_cache_refresh_scan_t *scan)
{
    ngx_int_t rc;

    ngx_memory_barrier();
    if (scan->cancelled
        || (ngx_msec_int_t) (ngx_current_msec - scan->deadline) >= 0)
    {
        scan->timed_out = 1;
        return NGX_DONE;
    }
    rc = ngx_http_cache_purge_walk_step(&scan->cursor,
                                        NGX_HTTP_CACHE_PURGE_WALK_ENTRIES);
    if (rc != NGX_AGAIN) {
        scan->complete = rc == NGX_OK;
        scan->failed = rc != NGX_OK;
        if (scan->complete) {
            ngx_sort(scan->entries, scan->count, sizeof(scan->entries[0]),
                     ngx_http_cache_refresh_compare);
        }
    }
    return rc;
}


static ngx_int_t
ngx_http_cache_refresh_credit(ngx_http_cache_refresh_t *job, ngx_flag_t take)
{
    ngx_int_t rc;

    if (!take && !job->credit) {
        return NGX_OK;
    }
    rc = ngx_http_cache_purge_probe_credit(job->queue, take);
    if (rc == NGX_OK) {
        job->credit = take;
    }
    return rc;
}


static void
ngx_http_cache_refresh_destroy(ngx_http_cache_refresh_t *job)
{
    if (job->event.timer_set) {
        ngx_del_timer(&job->event);
    }
    if (job->scan != NULL) {
        while (job->scan->cursor.top != NULL) {
            ngx_http_cache_purge_walk_close(&job->scan->cursor);
        }
        if (job->scan->cursor.path.data != NULL) {
            ngx_free(job->scan->cursor.path.data);
        }
    }
    if (ngx_cache_refresh_current == job) {
        ngx_cache_refresh_current = NULL;
    }
    ngx_destroy_pool(job->pool);
}


static void
ngx_http_cache_refresh_cleanup(void *data)
{
    ngx_http_cache_refresh_t *job = data;

    job->request = NULL;
    (void) ngx_http_cache_refresh_credit(job, 0);
    if (job->scan != NULL) {
        job->scan->cancelled = 1;
        ngx_memory_barrier();
    }
#if (NGX_THREADS && !NGX_WIN32)
    if (job->task != NULL && job->task->event.active) {
        return;
    }
#endif
    ngx_http_cache_refresh_destroy(job);
}


#if (NGX_THREADS && !NGX_WIN32)
static void
ngx_http_cache_refresh_thread(void *data, ngx_log_t *log)
{
    ngx_http_cache_refresh_scan_t *scan = data;
    (void) log;
    while (ngx_http_cache_refresh_scan(scan)
           == NGX_AGAIN) { /* bounded slices */
    }
}


static void
ngx_http_cache_refresh_thread_done(ngx_event_t *ev)
{
    ngx_http_cache_refresh_t *job = ev->data;

    job->scanning = 0;
    if (job->request == NULL) {
        ngx_http_cache_refresh_destroy(job);
        return;
    }
    if (job->event.timer_set) {
        ngx_del_timer(&job->event);
    }
    ngx_http_cache_refresh_step(&job->event);
}
#endif


static ngx_int_t
ngx_http_cache_refresh_reply(ngx_http_cache_refresh_t *job)
{
    ngx_http_request_t              *r = job->request;
    ngx_http_cache_refresh_scan_t   *scan = job->scan;
    ngx_http_cache_refresh_entry_t  *entry;
    ngx_uint_t                       i, count, complete;
    ngx_buf_t                       *b;
    ngx_chain_t                      out;
    ngx_http_cache_purge_result_code_e code;
    ngx_int_t                          rc;
    static const char *result[] = { "undispatched",      "kept",
                                    "invalidated",       "transport_error",
                                    "upstream_status",   "skipped_vary",
                                    "skipped_profile",   "raced_replaced",
                                    "invalidation_error" };

    if (scan != NULL && scan->failed) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_SCAN_FAILED);
    }
    count = scan != NULL && scan->complete ? scan->count : 0;
    complete =
        scan != NULL && scan->complete && !scan->more && job->position == count;
    code = complete ? NGX_HTTP_CACHE_PURGE_RESULT_REFRESH_COMPLETE :
                      NGX_HTTP_CACHE_PURGE_RESULT_REFRESH_INCOMPLETE;
    b = ngx_create_temp_buf(r->pool, 1024 + count * 160);
    if (b == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    b->last = ngx_sprintf(b->last,
                          "{\"code\":\"%s\",\"scan_complete\":%s,\"complete\":%"
                          "s,\"timed_out\":%s,"
                          "\"scan_error\":%s,\"undispatched\":%ui,\"next_"
                          "cursor\":\"%*s\",\"results\":[",
                          complete ? "refresh_complete" : "refresh_incomplete",
                          scan != NULL && scan->complete ? "true" : "false",
                          complete ? "true" : "false",
                          job->timed_out ? "true" : "false",
                          scan != NULL && scan->failed ? "true" : "false",
                          (scan != NULL ? scan->count : 0) - job->position,
                          (size_t) 32, job->after);
    for (i = 0; i < count; i++) {
        entry = &scan->entries[i];
        b->last = ngx_sprintf(
            b->last,
            "%s{\"hash\":\"%*s\",\"upstream_status\":%ui,\"result\":\"%s\"}",
            i ? "," : "", (size_t) 32, entry->hash, entry->status,
            entry->result < sizeof(result) / sizeof(result[0]) ?
                result[entry->result] :
                "internal_error");
    }
    b->last = ngx_cpymem(b->last, "]}", 2);
    b->last_buf = 1;
    b->last_in_chain = 1;
    if (ngx_http_cache_purge_response_headers(r, code) != NGX_OK) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    ngx_str_set(&r->headers_out.content_type, "application/json");
    r->headers_out.content_length_n = b->last - b->pos;
    rc = ngx_http_send_header(r);
    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }
    out.buf = b;
    out.next = NULL;
    return ngx_http_output_filter(r, &out);
}


static ngx_int_t
ngx_http_cache_refresh_probe(ngx_http_cache_refresh_t *job,
    ngx_http_cache_refresh_entry_t *entry)
{
    ngx_http_post_subrequest_t  *post;
    ngx_http_request_t          *sr, *r = job->request;
    ngx_str_t                    encoded, logical;
    size_t                       i;

    encoded.data = entry->key;
    encoded.len = entry->key_len;
    if (ngx_http_cache_purge_generation_current(job->conf->namespace, &encoded,
                                                &logical)
        != NGX_OK)
    {
        entry->result = NGX_REFRESH_REPLACED;
        return NGX_DECLINED;
    }
    /* This profile describes one representation and cannot reconstruct the
     * request headers of a Vary entry from the native cache header. */
#ifdef NGX_HTTP_CACHE_VARY_LEN
    if (entry->header.vary_len != 0) {
        entry->result = NGX_REFRESH_VARY;
        return NGX_DECLINED;
    }
#endif
    if (logical.len <= job->conf->refresh_prefix.len
        || ngx_memcmp(logical.data, job->conf->refresh_prefix.data,
                      job->conf->refresh_prefix.len))
    {
        entry->result = NGX_REFRESH_PROFILE;
        return NGX_DECLINED;
    }
    job->uri.data = logical.data + job->conf->refresh_prefix.len;
    job->uri.len = logical.len - job->conf->refresh_prefix.len;
    if (job->uri.data[0] != '/') {
        entry->result = NGX_REFRESH_PROFILE;
        return NGX_DECLINED;
    }
    for (i = 0; i < job->uri.len; i++) {
        if (job->uri.data[i] <= ' ' || job->uri.data[i] == '#'
            || job->uri.data[i] == 127)
        {
            entry->result = NGX_REFRESH_PROFILE;
            return NGX_DECLINED;
        }
    }
    job->etag.data = entry->header.etag;
    job->etag.len = entry->header.etag_len;
    for (i = 0; i < job->etag.len; i++) {
        if (job->etag.data[i] < ' ' || job->etag.data[i] == 127) {
            entry->result = NGX_REFRESH_PROFILE;
            return NGX_DECLINED;
        }
    }
    job->modified.data = job->date;
    job->modified.len = 0;
    if (entry->header.last_modified > 0) {
        job->modified.len =
            ngx_http_time(job->date, entry->header.last_modified) - job->date;
    }
    if (ngx_http_cache_refresh_credit(job, 1) != NGX_OK) {
        return NGX_AGAIN;
    }
    post = ngx_palloc(r->pool, sizeof(*post));
    if (post == NULL) {
        (void) ngx_http_cache_refresh_credit(job, 0);
        return NGX_ERROR;
    }
    post->handler = ngx_http_cache_refresh_done;
    post->data = job;
    if (ngx_http_subrequest(r, &job->conf->refresh_location, NULL, &sr, post,
                            NGX_HTTP_SUBREQUEST_WAITED)
        != NGX_OK)
    {
        (void) ngx_http_cache_refresh_credit(job, 0);
        return NGX_ERROR;
    }
    sr->method = NGX_HTTP_HEAD;
    ngx_str_set(&sr->method_name, "HEAD");
    sr->header_only = 1;
    sr->request_body = NULL;
    job->active = 1;
    return NGX_OK;
}


/* Every terminal probe path releases its one credit and schedules progress. */
static ngx_int_t
ngx_http_cache_refresh_done(ngx_http_request_t *sr, void *data, ngx_int_t rc)
{
    ngx_http_cache_refresh_t        *job = data;
    ngx_http_cache_refresh_entry_t  *entry;

    (void) rc;
    if (!job->active) {
        return NGX_OK;
    }
    job->active = 0;
    (void) ngx_http_cache_refresh_credit(job, 0);
    entry = &job->scan->entries[job->position];
    entry->status =
        sr->upstream != NULL ? sr->upstream->headers_in.status_n : 0;
    if (entry->result == NGX_REFRESH_PROFILE) {
        /* A guard failure must not become a transport error or an upstream
         * decision after an error_page internal redirect. */
    } else if (entry->status == 304) {
        entry->result = NGX_REFRESH_KEPT;
    } else if (entry->status == 200 || entry->status == 404
               || entry->status == 410)
    {
        job->decision = 1;
    } else {
        entry->result =
            entry->status == 0 ? NGX_REFRESH_TRANSPORT : NGX_REFRESH_STATUS;
    }
    if (!job->event.timer_set) {
        ngx_add_timer(&job->event, 1);
    }
    /* Consume the probe's local error status. It belongs in the result row,
     * not in the main request's error_page/termination path. */
    return NGX_OK;
}


static void
ngx_http_cache_refresh_step(ngx_event_t *ev)
{
    ngx_http_cache_refresh_t          *job = ev->data;
    ngx_http_cache_refresh_scan_t     *scan;
    ngx_http_cache_refresh_entry_t    *entry;
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_http_request_t                *r = job->request;
    ngx_str_t                         *filter, encoded, logical;
    ngx_int_t                          rc;

    if (r == NULL) {
        return;
    }
    if ((ngx_msec_int_t) (ngx_current_msec - job->deadline) >= 0 || ngx_exiting
        || ngx_quit || ngx_terminate)
    {
        job->timed_out = 1;
        if (job->scan != NULL) {
            job->scan->cancelled = 1;
            ngx_memory_barrier();
        }
    }
    if (job->scanning || job->active) {
        ngx_add_timer(ev, 10);
        return;
    }
    if (job->timed_out) {
        goto finished;
    }
    if (ngx_cache_refresh_current != NULL && ngx_cache_refresh_current != job) {
        ngx_add_timer(ev, 10);
        return;
    }
    if (job->scan == NULL) {
        scan = ngx_pcalloc(job->pool, sizeof(*scan));
        if (scan == NULL) {
            goto failed;
        }
        job->scan = scan;
        ngx_cache_refresh_current = job;
        main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
        scan->path = *job->cache->path;
        scan->deadline = job->deadline;
        ngx_memcpy(scan->after, job->after, 32);
        filter = r->cache->keys.elts;
        scan->filter.len = filter[0].len;
        scan->filter.data = ngx_pnalloc(job->pool, filter[0].len);
        if (scan->filter.data == NULL) {
            goto failed;
        }
        ngx_memcpy(scan->filter.data, filter[0].data, filter[0].len);
        ngx_http_cache_purge_walk_init(&scan->walk, &scan->path.name,
                                       main->protection, main->paths,
                                       ngx_cycle->log);
        scan->walk.namespace = job->conf->namespace;
        scan->walk.visit = ngx_http_cache_refresh_collect;
        scan->cursor.ctx = &scan->walk;
#if (NGX_THREADS && !NGX_WIN32)
        if (ngx_http_cache_purge_threaded()) {
            job->thread_pool = ngx_thread_pool_get(
                (ngx_cycle_t *) ngx_cycle, &job->conf->thread_pool_name);
            job->task = ngx_thread_task_alloc(job->pool, 0);
            if (job->task == NULL) {
                goto failed;
            }
            job->task->ctx = scan;
            job->task->handler = ngx_http_cache_refresh_thread;
            job->task->event.handler = ngx_http_cache_refresh_thread_done;
            job->task->event.data = job;
            job->task->event.log = ngx_cycle->log;
        }
#endif
    }
    scan = job->scan;
    if (!scan->complete && !scan->failed && !scan->timed_out) {
#if (NGX_THREADS && !NGX_WIN32)
        if (job->thread_pool != NULL) {
            if (ngx_thread_task_post(job->thread_pool, job->task) == NGX_OK) {
                job->scanning = 1;
            } else {
                job->task->event.active = 0;
            }
            ngx_add_timer(ev, 10);
            return;
        }
#endif
        if (ngx_http_cache_refresh_scan(scan) == NGX_AGAIN) {
            ngx_add_timer(ev, 1);
            return;
        }
    }
    if (!scan->complete) {
        job->timed_out = scan->timed_out;
        goto finished;
    }
    if (job->position == scan->count) {
        goto finished;
    }
    entry = &scan->entries[job->position];
    if (job->decision) {
        encoded.data = entry->key;
        encoded.len = entry->key_len;
        if (ngx_http_cache_purge_generation_current(job->conf->namespace,
                                                    &encoded, &logical)
            != NGX_OK)
        {
            rc = NGX_DECLINED;
        } else {
            rc = ngx_http_cache_purge_enqueue(r, job->cache, &logical, 0,
                                              &encoded);
        }
        if (rc == NGX_AGAIN) {
            ngx_add_timer(ev, 10);
            return;
        }
        entry->result = rc == NGX_OK       ? NGX_REFRESH_INVALIDATED :
                        rc == NGX_DECLINED ? NGX_REFRESH_REPLACED :
                                             NGX_REFRESH_ERROR;
        job->decision = 0;
    }
    if (entry->result == NGX_REFRESH_PENDING) {
        rc = ngx_http_cache_refresh_probe(job, entry);
        if (rc == NGX_OK || rc == NGX_AGAIN) {
            ngx_add_timer(ev, 10);
            if (rc == NGX_OK) {
                /* Timer/thread callbacks do not drain HTTP posted requests. */
                ngx_http_run_posted_requests(r->connection);
            }
            return;
        }
        if (rc == NGX_ERROR) {
            entry->result = NGX_REFRESH_ERROR;
        }
    }
    ngx_memcpy(job->after, entry->hash, 32);
    job->position++;
    ngx_add_timer(ev, 1);
    return;

finished:
    rc = ngx_http_cache_refresh_reply(job);
    job->cleanup->handler = NULL;
    ngx_http_cache_refresh_destroy(job);
    ngx_http_finalize_request(r, rc);
    return;

failed:
    job->cleanup->handler = NULL;
    ngx_http_cache_refresh_destroy(job);
    ngx_http_finalize_request(
        r, ngx_http_cache_purge_reply(
               r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR));
}


ngx_int_t
ngx_http_cache_refresh_start(ngx_http_request_t *r)
{
    ngx_http_cache_refresh_t          *job;
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_pool_t                        *pool;
    ngx_http_cleanup_t                *cleanup;
    ngx_list_part_t                   *part;
    ngx_table_elt_t                   *header;
    ngx_uint_t                         i, n, found;

    pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, ngx_cycle->log);
    if (pool == NULL) {
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }
    job = ngx_pcalloc(pool, sizeof(*job));
    if (job == NULL) {
        ngx_destroy_pool(pool);
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }
    job->pool = pool;
    job->request = r;
    job->cache = r->cache->file_cache;
    job->conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    job->queue = main->queue;
    job->deadline = ngx_current_msec + job->conf->refresh_timeout;
    ngx_memset(job->after, '0', 32);
    found = 0;
    part = &r->headers_in.headers.part;
    for (/* void */; /* void */; part = part->next) {
        header = part->elts;
        for (i = 0; i < part->nelts; i++) {
            if (header[i].key.len == sizeof("X-Cache-Purge-Cursor") - 1
                && ngx_strncasecmp(header[i].key.data,
                                   (u_char *) "X-Cache-Purge-Cursor",
                                   header[i].key.len)
                       == 0)
            {
                if (found++ || header[i].value.len != 32) {
                    goto invalid;
                }
                for (n = 0; n < 32; n++) {
                    if (ngx_hextoi(header[i].value.data + n, 1) == NGX_ERROR) {
                        goto invalid;
                    }
                    job->after[n] = ngx_tolower(header[i].value.data[n]);
                }
            }
        }
        if (part->next == NULL) {
            break;
        }
    }
    cleanup = ngx_http_cleanup_add(r, 0);
    if (cleanup == NULL) {
        ngx_destroy_pool(pool);
        return ngx_http_cache_purge_reply(
            r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
    }
    job->cleanup = cleanup;
    cleanup->handler = ngx_http_cache_refresh_cleanup;
    cleanup->data = job;
    job->event.handler = ngx_http_cache_refresh_step;
    job->event.data = job;
    job->event.log = ngx_cycle->log;
    job->event.cancelable = 0;
    if (!r->discard_body) {
        r->read_event_handler = ngx_http_test_reading;
        if (r->connection->read->ready) {
            ngx_post_event(r->connection->read, &ngx_posted_events);
        } else if (ngx_handle_read_event(r->connection->read, 0) != NGX_OK) {
            cleanup->handler = NULL;
            ngx_destroy_pool(pool);
            return ngx_http_cache_purge_reply(
                r, NGX_HTTP_CACHE_PURGE_RESULT_INTERNAL_ERROR);
        }
    }
    r->write_event_handler = ngx_http_request_empty_handler;
    r->main->count++;
    ngx_add_timer(&job->event, 1);
    return NGX_DONE;

invalid:
    ngx_destroy_pool(pool);
    return ngx_http_cache_purge_reply(
        r, NGX_HTTP_CACHE_PURGE_RESULT_INVALID_CURSOR);
}


/* Pure getters. They never mutate the upstream method or cache policy. */
ngx_int_t
ngx_http_cache_refresh_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
    ngx_http_cache_refresh_t  *job;
    ngx_str_t                  value;

    if (r->post_subrequest == NULL
        || r->post_subrequest->handler != ngx_http_cache_refresh_done)
    {
        v->not_found = 1;
        return NGX_OK;
    }
    job = r->post_subrequest->data;
    value = data == 0 ? job->uri : data == 1 ? job->etag : job->modified;
    v->data = value.data;
    v->len = value.len;
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}


/* Validate the actual target after location lookup, including redirects. */
ngx_int_t
ngx_http_cache_refresh_guard(ngx_http_request_t *r)
{
    ngx_http_proxy_loc_conf_t  *proxy;
    ngx_http_core_loc_conf_t   *core;
    ngx_http_cache_refresh_t   *job;
    ngx_keyval_t               *headers;
    ngx_str_t                   method;
    ngx_uint_t                  i, nheaders, host, etag, modified;
    const char                 *reason;

    if (r->post_subrequest == NULL
        || r->post_subrequest->handler != ngx_http_cache_refresh_done)
    {
        return NGX_DECLINED;
    }
    job = r->post_subrequest->data;
    core = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
    proxy = ngx_http_get_module_loc_conf(r, ngx_http_proxy_module);
    if (!core->internal) {
        reason = "location must be internal";
        goto invalid;
    }
    if (r->uri.len != job->conf->refresh_location.len
        || ngx_memcmp(r->uri.data, job->conf->refresh_location.data,
                      r->uri.len))
    {
        reason = "internal redirect changed the profile URI";
        goto invalid;
    }
    if (proxy->upstream.pass_request_headers) {
        reason = "\"proxy_pass_request_headers\" must be off";
        goto invalid;
    }
    if (proxy->upstream.pass_request_body) {
        reason = "\"proxy_pass_request_body\" must be off";
        goto invalid;
    }
    if (proxy->upstream.intercept_errors) {
        reason = "\"proxy_intercept_errors\" must be off";
        goto invalid;
    }
#if (nginx_version >= 1007009)
    if (proxy->upstream.cache)
#else
    if (proxy->upstream.cache != NULL)
#endif
    {
        reason = "\"proxy_cache\" must be off";
        goto invalid;
    }
    ngx_str_set(&method, "HEAD");
#if (nginx_version >= 1011006)
#if (NGX_RESTY_EXT && NGX_EXPR)
    if (proxy->method != NULL
        && ngx_http_get_expr_complex_value(r, proxy->method, &method)
               == NGX_ERROR)
#else
    if (proxy->method != NULL
        && ngx_http_complex_value(r, proxy->method, &method) != NGX_OK)
#endif
    {
        reason = "could not evaluate \"proxy_method\"";
        goto invalid;
    }
#else
    if (proxy->method.len != 0) {
        method = proxy->method;
        /* Before 1.11.6 the core appends the request-line space at merge. */
        if (method.len != 0 && method.data[method.len - 1] == ' ') {
            method.len--;
        }
    }
#endif
    if (method.len != 4 || ngx_memcmp(method.data, "HEAD", 4)) {
        reason = "\"proxy_method\" must be HEAD";
        goto invalid;
    }
    host = etag = modified = 0;
    headers = NULL;
    nheaders = 0;
    if (proxy->headers_source != NULL) {
        headers = proxy->headers_source->elts;
        nheaders = proxy->headers_source->nelts;
    }
    for (i = 0; headers != NULL && i < nheaders; i++) {
        /* No client-controlled representation headers may leak into HEAD.
         * Only the two dedicated conditional getters may be dynamic. */
        if (ngx_strchr(headers[i].value.data, '$') != NULL
            && !((headers[i].key.len == sizeof("If-None-Match") - 1
                  && ngx_strncasecmp(headers[i].key.data,
                                     (u_char *) "If-None-Match",
                                     headers[i].key.len)
                         == 0)
                 || (headers[i].key.len == sizeof("If-Modified-Since") - 1
                     && ngx_strncasecmp(headers[i].key.data,
                                        (u_char *) "If-Modified-Since",
                                        headers[i].key.len)
                            == 0)))
        {
            ngx_log_error(
                NGX_LOG_ERR, r->connection->log, 0,
                "ngx_http_cache_purge: REFRESH profile \"%V\" rejected: "
                "header \"%V\" must have a literal value",
                &job->conf->refresh_location, &headers[i].key);
            job->scan->entries[job->position].result = NGX_REFRESH_PROFILE;
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
        if (headers[i].key.len == 4
            && ngx_strncasecmp(headers[i].key.data, (u_char *) "Host", 4) == 0)
        {
            host = headers[i].value.len != 0
                   && ngx_strchr(headers[i].value.data, '$') == NULL;
        }
        if (headers[i].key.len == sizeof("If-None-Match") - 1
            && ngx_strncasecmp(headers[i].key.data, (u_char *) "If-None-Match",
                               headers[i].key.len)
                   == 0)
        {
            etag =
                headers[i].value.len == sizeof("$cache_purge_refresh_etag") - 1
                && ngx_memcmp(headers[i].value.data,
                              "$cache_purge_refresh_etag", headers[i].value.len)
                       == 0;
        }
        if (headers[i].key.len == sizeof("If-Modified-Since") - 1
            && ngx_strncasecmp(headers[i].key.data,
                               (u_char *) "If-Modified-Since",
                               headers[i].key.len)
                   == 0)
        {
            modified = headers[i].value.len
                           == sizeof("$cache_purge_refresh_last_modified") - 1
                       && ngx_memcmp(headers[i].value.data,
                                     "$cache_purge_refresh_last_modified",
                                     headers[i].value.len)
                              == 0;
        }
    }
    if (!host) {
        reason = "a non-empty literal \"Host\" header is required";
        goto invalid;
    }
    if (!etag) {
        reason = "\"If-None-Match\" must be $cache_purge_refresh_etag";
        goto invalid;
    }
    if (!modified) {
        reason =
            "\"If-Modified-Since\" must be $cache_purge_refresh_last_modified";
        goto invalid;
    }
    return NGX_DECLINED;

invalid:
    job->scan->entries[job->position].result = NGX_REFRESH_PROFILE;
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "ngx_http_cache_purge: REFRESH profile \"%V\" rejected at "
                  "\"%V\": %s",
                  &job->conf->refresh_location, &r->uri, reason);
    return NGX_HTTP_INTERNAL_SERVER_ERROR;
}
#endif


#endif
