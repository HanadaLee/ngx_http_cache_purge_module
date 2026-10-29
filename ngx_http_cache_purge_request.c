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




#include <sys/random.h>
#include <fcntl.h>
#include <ngx_md5.h>

#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
#include <ngx_http_proxy_filter_module.h>
#endif

#if (NGX_EXPR)
#include <ngx_http_expr_module.h>
#endif

#define NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES   (2 * 1024 * 1024)
#define NGX_HTTP_CACHE_PURGE_CAPTURE_COUNT   64
#define NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD  (128 * 1024)
#define NGX_HTTP_CACHE_PURGE_CAPTURE_TIMEOUT 3000
#define NGX_HTTP_CACHE_PURGE_PROPAGATION_LINE 8192
#define NGX_HTTP_CACHE_PURGE_RETIRE_BYTES (256 * 1024)
#define NGX_HTTP_CACHE_PURGE_RETIRE_COUNT 32

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
    NGX_HTTP_CACHE_PURGE_ADMIT_REVALIDATE,
    NGX_HTTP_CACHE_PURGE_ADMIT_ERROR
};
typedef struct {
#if (NGX_EXPR)
    ngx_array_t               *tag;
    ngx_array_t               *identity[3];
    ngx_array_t               *protocol[4];
#else
    ngx_http_complex_value_t  *tag;
    ngx_http_complex_value_t  *identity[3];
    ngx_flag_t                 protocol[4];
#endif
    ngx_array_t               *proxy_headers;
} ngx_http_cache_purge_capture_conf_t;
typedef struct {
    ngx_array_t               locations;
    ngx_flag_t                enabled;
    ngx_http_complex_value_t  empty;
    ngx_str_t                 read_pool_name;
    ngx_thread_pool_t        *read_pool;
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
    ngx_str_t                     payload;
    size_t                        input_bytes;
    u_char                        transaction[16];
    unsigned                      replace_tags : 1;
} ngx_http_cache_purge_revalidation_t;

typedef struct {
    size_t       reserved;
    ngx_str_t    value[3];
} ngx_http_cache_purge_propagation_t;

typedef struct {
    ngx_buf_t     original;
    ngx_chain_t  *pending;
    ngx_chain_t **last;
    ngx_buf_t    *body;
    u_char       *data;
    size_t        capacity, reserved, links;
    unsigned      held : 1;
    unsigned      changed : 1;
} ngx_http_cache_purge_send_t;

typedef struct {
    /* The pool cleanup uses this first member's shared budget accounting. */
    ngx_http_cache_purge_propagation_t  allocation;
    ngx_http_cache_purge_fence_t        fence;
    ngx_str_t                           headers[3];
} ngx_http_cache_purge_incoming_t;

typedef struct {
    ngx_queue_t                     queue;
    ngx_event_t                     event;
    ngx_http_cache_purge_client_t  *client;
    ngx_http_cache_purge_call_t    *call;
    ngx_str_t                       payload;
    ngx_msec_t                      deadline;
    size_t                          bytes;
    int                             fd;
} ngx_http_cache_purge_retire_t;

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
    ngx_int_t                    (*create_request)(ngx_http_request_t *);
    void (*error_finalize)(ngx_http_request_t *, ngx_int_t);
    ngx_http_cache_purge_redirect_t      *redirect;
    ngx_http_cache_purge_revalidation_t  *validation;
    ngx_http_cache_purge_propagation_t   *propagation;
    ngx_http_cache_purge_propagation_t   *propagation_shared;
    ngx_http_cache_purge_send_t          *send;
    ngx_output_chain_filter_pt           send_next;
    void                                *send_data;
    ngx_buf_t                           *send_header;
    ngx_http_cache_purge_incoming_t      *incoming, *incoming_shared;
    ngx_http_cache_purge_fence_t           local_fence;
    ngx_http_cache_purge_watermark_t       local_watermark;
    uint64_t                               local_publication;
    uint64_t                              incoming_invalidate, incoming_delete;
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    ngx_list_t                          *propagation_headers;
#endif
    ngx_event_t                           read_event;
    ngx_event_t                           publication_retry;
    ngx_str_t                             publication_payload;
    ngx_pool_t                           *publication_pool;
    ngx_temp_file_t                      *temporary;

    ngx_str_t                       key, url, host, namespace, root, zone, path;
    ngx_str_t                       method;
    ngx_str_t                       misc, tags[64];
    ngx_uint_t                      tag_count, rpc;
    ngx_uint_t                      read_state;
    ngx_thread_task_t              *read_task;
    ngx_uint_t                      proof_retries;
    size_t                          reserved, input_bytes;
    ngx_msec_t                              deadline;
    ngx_http_cache_purge_watermark_t        origin;
    uint64_t                                origin_publication;
    ngx_queue_t                             origin_queue;
    ngx_http_cache_purge_control_reader_t  *origin_reader;
    uint64_t                                writer_sequence;
    int                                     lease;
    u_char                          id[16], previous[16], native_main[16];
    uint64_t  revision;
    unsigned  admitted : 1;
    unsigned  failed : 1;
    unsigned  need_finish : 1;
    unsigned  read_held : 1;
    unsigned  read_ref : 1;
    unsigned  read_header_only : 1;
    unsigned  read_bypass : 1;
    unsigned  local_ready : 1;
    unsigned  local_failed : 1;
    unsigned  incoming_error : 1;
    unsigned  incoming_retired : 1;
    unsigned  origin_valid : 1;
    unsigned  origin_registered : 1;
    unsigned  invalidation_retry : 1;
    unsigned  error_held : 1;
    unsigned  send_started : 1;
    unsigned  send_enabled : 1;
    unsigned  detached : 1;
} ngx_http_cache_purge_capture_t;

typedef struct {
    ngx_http_cache_purge_file_proof_t      proof;
    ngx_http_file_cache_header_t           header;
    ngx_http_cache_purge_watermark_t       watermark;
    ngx_http_cache_purge_capture_t        *capture;
    ngx_str_t                              path;
    uint64_t                               publication;
    int                                    fd, result;
} ngx_http_cache_purge_proof_task_t;

typedef struct {
    uint64_t   covered, validated, required;
    ngx_int_t  fenced;
} ngx_http_cache_purge_read_match_t;
typedef struct {
    ngx_http_script_code_pt code;
    ngx_array_t  *predicates;
    ngx_uint_t    protocol;
    uintptr_t     end;
} ngx_http_cache_purge_capture_code_t;

typedef struct {
    ngx_http_script_code_pt     code;
    ngx_http_upstream_local_t *original;
    uintptr_t                  end;
} ngx_http_cache_purge_bind_code_t;

extern ngx_module_t ngx_http_cache_purge_index_module;
static ngx_http_output_header_filter_pt
    ngx_http_cache_purge_capture_next_header;
static size_t     ngx_http_cache_purge_retained_bytes;
static ngx_uint_t ngx_http_cache_purge_retained_count;
static ngx_uint_t ngx_http_cache_purge_read_count;
static size_t ngx_http_cache_purge_propagation_bytes;
static ngx_queue_t ngx_http_cache_purge_retire_queue;
static ngx_uint_t ngx_http_cache_purge_retire_initialized;
static ngx_uint_t ngx_http_cache_purge_retire_count;
static size_t ngx_http_cache_purge_retire_bytes;
static ngx_queue_t ngx_http_cache_purge_origins;
static ngx_uint_t ngx_http_cache_purge_origins_initialized;
static u_char ngx_http_cache_purge_origin_ledger[16];
static ngx_int_t ngx_http_cache_purge_capture_origin(
    ngx_http_cache_purge_capture_t *ctx);
static void ngx_http_cache_purge_capture_origin_release(
    ngx_http_cache_purge_capture_t *ctx);
static void ngx_http_cache_purge_read_event(ngx_event_t *event);
static ngx_int_t ngx_http_cache_purge_proof_start(
    ngx_http_cache_purge_capture_t *ctx);
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
static u_char *ngx_http_cache_purge_capture_number(u_char *p, uint64_t value,
    unsigned width);
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
static char *ngx_http_cache_purge_identity_conf(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_cache_purge_capture_post(ngx_conf_t *cf);
static ngx_int_t ngx_http_cache_purge_capture_header(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_propagation_variables(ngx_conf_t *cf);
static ngx_int_t ngx_http_cache_purge_propagation_create(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_send_output(void *data, ngx_chain_t *in);
static void ngx_http_cache_purge_send_event(ngx_event_t *event);
static void ngx_http_cache_purge_send_cancel(
    ngx_http_cache_purge_capture_t *ctx);
static void ngx_http_cache_purge_send_clear(
    ngx_http_cache_purge_capture_t *ctx);
static ngx_int_t ngx_http_cache_purge_incoming_parse(
    ngx_http_cache_purge_capture_t *ctx);
static ngx_uint_t ngx_http_cache_purge_incoming_mode(
    ngx_http_cache_purge_capture_t *ctx, time_t date);
static ngx_int_t ngx_http_cache_purge_retire_start(
    ngx_http_cache_purge_capture_t *ctx);
static void ngx_http_cache_purge_retire_event(ngx_event_t *event);
static void ngx_http_cache_purge_retire_exit(ngx_cycle_t *cycle);
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
static ngx_int_t ngx_http_cache_purge_propagation_filter(ngx_http_request_t *r,
    ngx_http_proxy_filter_ctx_t *filter);
#endif

#if (NGX_EXPR)
#define NGX_HTTP_CACHE_PURGE_SCOPE                                           \
    (NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF                    \
     |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF|NGX_HTTP_LOC_WHEN_CONF)
#define NGX_HTTP_CACHE_PURGE_FLAG_SLOT ngx_conf_set_conditional_flag_slot
#else
#define NGX_HTTP_CACHE_PURGE_SCOPE                                           \
    (NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF)
#define NGX_HTTP_CACHE_PURGE_FLAG_SLOT ngx_conf_set_flag_slot
#endif

#define ngx_http_cache_purge_identity_command(name, n)                       \
    { ngx_string(name), NGX_HTTP_CACHE_PURGE_SCOPE|NGX_CONF_TAKE1,            \
      ngx_http_cache_purge_identity_conf, NGX_HTTP_LOC_CONF_OFFSET,          \
      offsetof(ngx_http_cache_purge_capture_conf_t, identity)                \
          + (n) * sizeof(void *), NULL }

#define ngx_http_cache_purge_protocol_command(name, n)                       \
    { ngx_string(name), NGX_HTTP_CACHE_PURGE_SCOPE|NGX_CONF_FLAG,             \
      NGX_HTTP_CACHE_PURGE_FLAG_SLOT, NGX_HTTP_LOC_CONF_OFFSET,              \
      offsetof(ngx_http_cache_purge_capture_conf_t, protocol)                \
          + (n) * sizeof(((ngx_http_cache_purge_capture_conf_t *) 0)         \
                         ->protocol[0]), NULL }

static ngx_command_t  ngx_http_cache_purge_capture_commands[] = {

    { ngx_string("cache_purge_read_pool"), NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_str_slot, NGX_HTTP_MAIN_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_capture_main_t, read_pool_name), NULL },

    ngx_http_cache_purge_identity_command("cache_purge_key_url", 0),
    ngx_http_cache_purge_identity_command("cache_purge_key_misc", 1),
    ngx_http_cache_purge_identity_command("cache_purge_key_namespace", 2),

    ngx_http_cache_purge_protocol_command("proxy_cache_purge", 0),
    ngx_http_cache_purge_protocol_command("fastcgi_cache_purge", 1),
    ngx_http_cache_purge_protocol_command("scgi_cache_purge", 2),
    ngx_http_cache_purge_protocol_command("uwsgi_cache_purge", 3),

    { ngx_string("cache_purge_tag"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
      |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF|NGX_HTTP_LOC_WHEN_CONF
#endif
      |NGX_CONF_TAKE1,
#if (NGX_EXPR)
      ngx_http_set_expr_complex_value_slot,
#else
      ngx_http_set_complex_value_slot,
#endif
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_purge_capture_conf_t, tag), NULL },

      ngx_null_command
};

static ngx_http_module_t  ngx_http_cache_purge_capture_context = {
    ngx_http_cache_purge_propagation_variables,
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
    ngx_http_cache_purge_retire_exit,
    NULL,
    NGX_MODULE_V1_PADDING
};

static ngx_int_t
ngx_http_cache_purge_capture_equal(ngx_str_t value, const char *literal)
{
    size_t length = ngx_strlen(literal);
    return value.len == length && ngx_memcmp(value.data, literal, length) == 0;
}

static ngx_str_t ngx_http_cache_purge_propagation_names[] = {
    ngx_string("cache_purge_propagation_time"),
    ngx_string("cache_purge_propagation_mode"),
    ngx_string("cache_purge_propagation_fence")
};

static void
ngx_http_cache_purge_retire_free(ngx_http_cache_purge_retire_t *job)
{
    if (job->call) {
        ngx_http_cache_purge_client_cancel(job->call);
    }
    if (job->event.timer_set) {
        ngx_del_timer(&job->event);
    }
    if (job->event.posted) {
        ngx_delete_posted_event(&job->event);
    }
    (void) close(job->fd);
    ngx_queue_remove(&job->queue);
    ngx_http_cache_purge_retire_count--;
    ngx_http_cache_purge_retire_bytes -= job->bytes;
    ngx_free(job);
}

static void
ngx_http_cache_purge_retire_retry(ngx_http_cache_purge_retire_t *job)
{
    if (ngx_terminate
        || (ngx_msec_int_t) (job->deadline - ngx_current_msec) <= 10)
    {
        ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
                      "cache purge selected retirement was not confirmed");
        ngx_http_cache_purge_retire_free(job);
        return;
    }
    ngx_add_timer(&job->event, 10);
}

static void
ngx_http_cache_purge_retire_reply(ngx_http_cache_purge_call_t *call, void *data,
    const ngx_http_cache_purge_reply_t *reply)
{
    ngx_http_cache_purge_retire_t  *job = data;

    job->call = NULL;
    if (reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_STOPPED) {
        ngx_http_cache_purge_retire_free(job);
        return;
    }
    if (reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK) {
        if ((reply->result == 0 && reply->payload.len == 25
             && reply->payload.data[24] == NGX_HTTP_CACHE_PURGE_INDEX_RETIRED)
            || reply->result == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
            || reply->result == NGX_HTTP_CACHE_ROCKSDB_CONFLICT)
        {
            /* Replaced or already retired instances must not be reselected. */
            ngx_http_cache_purge_retire_free(job);
            return;
        }
    }
    ngx_http_cache_purge_retire_retry(job);
}

static void
ngx_http_cache_purge_retire_event(ngx_event_t *event)
{
    ngx_http_cache_purge_retire_t  *job = event->data;
    ngx_msec_int_t                 left;

    left = (ngx_msec_int_t) (job->deadline - ngx_current_msec);
    if (left <= 0 || ngx_terminate) {
        ngx_http_cache_purge_retire_retry(job);
        return;
    }
    if (ngx_http_cache_purge_client_retire_selected(job->client, &job->payload,
            job->fd, (ngx_msec_t) left, ngx_http_cache_purge_retire_reply, job,
            &job->call) != NGX_OK)
    {
        ngx_http_cache_purge_retire_retry(job);
    }
}

static void
ngx_http_cache_purge_retire_exit(ngx_cycle_t *cycle)
{
    ngx_http_cache_purge_retire_t  *job;

    if (!ngx_http_cache_purge_retire_initialized) {
        return;
    }
    while (!ngx_queue_empty(&ngx_http_cache_purge_retire_queue)) {
        job = ngx_queue_data(ngx_queue_head(&ngx_http_cache_purge_retire_queue),
                             ngx_http_cache_purge_retire_t, queue);
        ngx_http_cache_purge_retire_free(job);
    }
}

static ngx_int_t
ngx_http_cache_purge_retire_start(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_retire_t  *job;
    ngx_http_cache_t               *cache;
    ngx_str_t                       fields[4];
    size_t                          i, size;
    u_char                         *p;

    if (ctx->incoming_retired) {
        return NGX_OK;
    }
    cache = ctx->cache;
    if (cache->file.fd == NGX_INVALID_FILE || cache->buf == NULL
        || cache->buf->start == NULL
        || cache->buf->last - cache->buf->start
           < (ptrdiff_t) sizeof(ngx_http_file_cache_header_t)
        || ngx_http_cache_purge_retire_count
           >= NGX_HTTP_CACHE_PURGE_RETIRE_COUNT
        || ngx_http_cache_purge_runtime_client(ctx->request, &ctx->client)
           != NGX_OK)
    {
        return NGX_ERROR;
    }
    fields[0] = cache->file_cache->path->name;
    fields[1] = cache->file_cache->shm_zone->shm.name;
    fields[2] = cache->file.name;
    if (fields[2].len <= fields[0].len + 1
        || ngx_memcmp(fields[2].data, fields[0].data, fields[0].len) != 0
        || fields[2].data[fields[0].len] != '/')
    {
        return NGX_ERROR;
    }
    fields[2].data += fields[0].len + 1;
    fields[2].len -= fields[0].len + 1;
    fields[3].data = cache->buf->start;
    fields[3].len = sizeof(ngx_http_file_cache_header_t);
    size = sizeof(*job) + 16;
    for (i = 0; i < 4; i++) {
        if (fields[i].len > 32768) {
            return NGX_ERROR;
        }
        size += fields[i].len;
    }
    if (size > NGX_HTTP_CACHE_PURGE_RETIRE_BYTES
               - ngx_http_cache_purge_retire_bytes)
    {
        return NGX_ERROR;
    }
    job = ngx_alloc(size, ngx_cycle->log);
    if (job == NULL) {
        return NGX_ERROR;
    }
    ngx_memzero(job, sizeof(*job));
    job->fd = fcntl(cache->file.fd, F_DUPFD_CLOEXEC, 0);
    if (job->fd < 0) {
        ngx_free(job);
        return NGX_ERROR;
    }
    job->bytes = size;
    job->client = ctx->client;
    job->deadline = ngx_current_msec + 5000;
    job->payload.data = (u_char *) (job + 1);
    job->payload.len = size - sizeof(*job);
    p = job->payload.data;
    for (i = 0; i < 4; i++) {
        p = ngx_http_cache_purge_capture_string(p, fields[i]);
    }
    job->event.data = job;
    job->event.handler = ngx_http_cache_purge_retire_event;
    job->event.log = ngx_cycle->log;
    if (!ngx_http_cache_purge_retire_initialized) {
        ngx_queue_init(&ngx_http_cache_purge_retire_queue);
        ngx_http_cache_purge_retire_initialized = 1;
    }
    ngx_http_cache_purge_retire_bytes += size;
    ngx_http_cache_purge_retire_count++;
    ngx_queue_insert_tail(&ngx_http_cache_purge_retire_queue, &job->queue);
    /* Own copied bytes and an independent FD; the request may now end. */
    ctx->incoming_retired = 1;
    ngx_post_event(&job->event, &ngx_posted_events);
    return NGX_OK;
}

static void
ngx_http_cache_purge_propagation_cleanup(void *data)
{
    ngx_http_cache_purge_propagation_t  *state = data;

    ngx_http_cache_purge_propagation_bytes -= state->reserved;
    state->reserved = 0;
}

static void
ngx_http_cache_purge_propagation_reply(ngx_http_cache_purge_call_t *call,
    void *data, const ngx_http_cache_purge_reply_t *reply)
{
    ngx_http_cache_purge_capture_t         *ctx = data;
    ngx_http_cache_purge_control_reader_t *reader;
    ngx_http_cache_purge_watermark_t       watermark;
    ngx_http_cache_rocksdb_error_t         error;
    uint64_t                               publication, current;
    ngx_uint_t                             retry;

    ctx->call = NULL;
    retry = reply->transport != NGX_HTTP_CACHE_PURGE_CLIENT_OK
            || reply->result == NGX_HTTP_CACHE_ROCKSDB_BUSY
            || reply->result == NGX_HTTP_CACHE_ROCKSDB_LIMIT
            || reply->result == NGX_HTTP_CACHE_ROCKSDB_EXPIRED;
    if (reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK
        && reply->result == 0 && reply->payload.len == 24)
    {
        publication = ngx_http_cache_purge_capture_read_number(
            reply->payload.data + 16, 8);
        if (publication != 0
            && ngx_http_cache_purge_runtime_reader(ctx->request, &reader,
                                                   &error) == 0
            && ngx_http_cache_purge_control_watermark(reader, &watermark,
                                                       &current, &error) == 0
            && current == publication)
        {
            ctx->local_fence.invalidate_ms =
                ngx_http_cache_purge_capture_read_number(
                    reply->payload.data, 8);
            ctx->local_fence.delete_ms =
                ngx_http_cache_purge_capture_read_number(
                    reply->payload.data + 8, 8);
            ctx->local_publication = publication;
            ctx->local_watermark = watermark;
            ctx->local_ready = 1;
            ngx_post_event(&ctx->read_event, &ngx_posted_events);
            return;
        }
        retry = 1;
    }
    if (retry && !ctx->request->main->terminated
        && (ngx_msec_int_t) (ctx->deadline - ngx_current_msec) > 10)
    {
        ngx_add_timer(&ctx->read_event, 10);
        return;
    }
    ngx_log_error(NGX_LOG_WARN, ctx->request->connection->log, 0,
                  "cache purge object query failed: transport:%ui result:%i",
                  (ngx_uint_t) reply->transport, (ngx_int_t) reply->result);
    ctx->local_failed = 1;
    ngx_post_event(&ctx->read_event, &ngx_posted_events);
}

static int
ngx_http_cache_purge_propagation_match(void *data,
    const ngx_http_cache_purge_control_match_t *match)
{
    ngx_http_cache_purge_fence_t  *fence = data;
    uint64_t                      *cutoff;

    if (match->flags & NGX_HTTP_CACHE_PURGE_CONTROL_TAG_CANDIDATE) {
        /* The shared image cannot resolve this representation's TAGs. */
        return NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
    }
    cutoff = match->mode == NGX_HTTP_CACHE_PURGE_INVALIDATE
             ? &fence->invalidate_ms : &fence->delete_ms;
    *cutoff = ngx_max(*cutoff, match->issued_at_ms);
    return 0;
}

static ngx_int_t
ngx_http_cache_purge_propagation_query(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_client_t          *client;
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_purge_control_context_t  context;
    ngx_http_cache_purge_watermark_t        watermark;
    ngx_http_cache_purge_fence_t            fence;
    ngx_http_cache_rocksdb_error_t          error;
    ngx_http_request_t                     *r;
    ngx_list_part_t                        *part;
    ngx_table_elt_t                        *headers;
    ngx_time_t                             *now;
    ngx_str_t                               payload, fields[4];
    uint64_t                                publication;
    ngx_uint_t                              i, count;
    ngx_msec_t                              timeout;
    size_t                                  size;
    u_char                                 *p;
    ngx_int_t                               rc;
    int                                     needed;

    r = ctx->request;
    timeout = ctx->deadline - ngx_current_msec;
    if (r->main->terminated || (ngx_msec_int_t) timeout <= 0) {
        return NGX_ERROR;
    }
    now = ngx_timeofday();
    ngx_memzero(&fence, sizeof(fence));
    rc = ngx_http_cache_purge_runtime_reader(r, &reader, &error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_control_propagation_needed(reader,
            (uint64_t) now->sec * 1000 + now->msec, &needed,
            &watermark, &publication, &error);
        if (rc == 0 && needed) {
            ngx_memzero(&context, sizeof(context));
            context.key.data = ctx->key.data;
            context.key.len = ctx->key.len;
            context.url.data = ctx->url.data;
            context.url.len = ctx->url.len;
            context.host.data = ctx->host.data;
            context.host.len = ctx->host.len;
            context.tag_namespace.data = ctx->namespace.data;
            context.tag_namespace.len = ctx->namespace.len;
            context.now_ms = (uint64_t) now->sec * 1000 + now->msec;
            context.proxy = 1;
            rc = ngx_http_cache_purge_control_propagation(reader, &context,
                0, 64, 65536, ngx_http_cache_purge_propagation_match, &fence,
                &watermark, &publication, &error);
        }
    }
    if (rc == 0) {
        /* Local admission and incoming requirements remain independent. A
         * new accepted sequence is checked again before the first send. */
        ctx->local_fence = fence;
        ctx->local_watermark = watermark;
        ctx->local_publication = publication;
        ctx->local_ready = 1;
        ngx_post_event(&ctx->read_event, &ngx_posted_events);
        return NGX_OK;
    }
    /* Discard partial matches on a TAG candidate, limit or image change.
     * Only the complete executor query may then supply local requirements. */
    if (ngx_http_cache_purge_runtime_read_client(r, &client) != NGX_OK) {
        return NGX_ERROR;
    }
    fields[0] = ctx->key;
    fields[1] = ctx->url;
    fields[2] = ctx->host;
    fields[3] = ctx->namespace;
    size = 36;
    count = 0;
    for (i = 0; i < 4; i++) {
        if (fields[i].len > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - size) {
            return NGX_ERROR;
        }
        size += fields[i].len;
    }
    for (part = &r->headers_in.headers.part; part; part = part->next) {
        headers = part->elts;
        for (i = 0; i < part->nelts; i++) {
            if (!headers[i].hash) {
                continue;
            }
            if (size > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - 8
                || headers[i].key.len
                   > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - size - 8)
            {
                return NGX_ERROR;
            }
            size += 8 + headers[i].key.len;
            if (headers[i].value.len
                > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - size)
            {
                return NGX_ERROR;
            }
            size += headers[i].value.len;
            count++;
        }
    }
    if (size > NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES
               - ngx_http_cache_purge_retained_bytes)
    {
        return NGX_BUSY;
    }
    payload.data = ngx_alloc(size, r->connection->log);
    if (payload.data == NULL) {
        return NGX_ERROR;
    }
    ngx_http_cache_purge_retained_bytes += size;
    payload.len = size;
    p = payload.data;
    for (i = 0; i < 4; i++) {
        p = ngx_http_cache_purge_capture_string(p, fields[i]);
    }
    p = ngx_cpymem(p, ctx->native_main, 16);
    p = ngx_http_cache_purge_capture_number(p, count, 4);
    for (part = &r->headers_in.headers.part; part; part = part->next) {
        headers = part->elts;
        for (i = 0; i < part->nelts; i++) {
            if (headers[i].hash) {
                p = ngx_http_cache_purge_capture_string(p, headers[i].key);
                p = ngx_http_cache_purge_capture_string(p, headers[i].value);
            }
        }
    }
    rc = NGX_ERROR;
    if ((size_t) (p - payload.data) == size) {
        rc = ngx_http_cache_purge_client_submit(client,
            NGX_HTTP_CACHE_PURGE_OP_OBJECT_PROPAGATION, &payload, timeout,
            ngx_http_cache_purge_propagation_reply, ctx, &ctx->call);
    }
    /* The IPC queue owns its copy; retries retain no request-pool payloads. */
    ngx_free(payload.data);
    ngx_http_cache_purge_retained_bytes -= size;
    return rc;
}

static ngx_int_t
ngx_http_cache_purge_propagation_snapshot(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_control_reader_t    *reader;
    ngx_http_cache_purge_watermark_t          watermark;
    ngx_http_cache_rocksdb_error_t            error;
    ngx_http_cache_purge_fence_t              fence;
    ngx_http_cache_purge_propagation_t       *state;
    ngx_http_cache_purge_loc_conf_t          *conf;
    ngx_http_cache_purge_capture_t           *main;
    ngx_pool_cleanup_t                       *cln;
    ngx_http_request_t                       *r;
    ngx_str_t                                 raw, encoded;
    uint64_t                                  publication;
    size_t                                    size;
    uint64_t                                  issued;
    u_char                                    raw_data[20], base64[28];
    u_char                                   *p;
    ngx_int_t                                 result;
    int                                       rc;

    r = ctx->request;
    if (ctx->failed || ctx->local_failed) {
        return NGX_ERROR;
    }
    if (!ctx->local_ready) {
        /* Resolve propagation only when an upstream request is actually sent.
         * Cached responses do not need the representation index query. */
        return NGX_AGAIN;
    }
    if (ngx_http_cache_purge_runtime_reader(r, &reader, &error) != 0) {
        return NGX_ERROR;
    }
    fence = ctx->local_fence;
    result = NGX_ERROR;
    rc = ngx_http_cache_purge_control_watermark(reader, &watermark,
                                                 &publication, &error);
    /* Maintenance can republish the same accepted operations. Their original
     * headers may outlive the local window; a new operation still requires a
     * new lookup, as does a different ledger. */
    if (rc != 0) {
        return NGX_AGAIN;
    }
    if (publication != ctx->local_publication
        && (watermark.sequence != ctx->local_watermark.sequence
            || ngx_memcmp(watermark.ledger,
                           ctx->local_watermark.ledger, 16) != 0))
    {
        return NGX_AGAIN;
    }
    if (ctx->incoming) {
        fence.invalidate_ms = ngx_max(fence.invalidate_ms,
                                      ctx->incoming->fence.invalidate_ms);
        fence.delete_ms = ngx_max(fence.delete_ms,
                                  ctx->incoming->fence.delete_ms);
    }
    if (!fence.invalidate_ms && !fence.delete_ms) {
        result = NGX_OK;
        goto done;
    }
    rc = ngx_http_cache_purge_fence_encode(&fence, NULL, 0, &raw.len, &error);
    if (rc != 0) {
        goto done;
    }
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    size = ngx_base64_encoded_length(raw.len);
    if (size + conf->propagation_header[2].len + 4
        > NGX_HTTP_CACHE_PURGE_PROPAGATION_LINE)
    {
        goto done;
    }
    raw.data = raw_data;
    encoded.data = base64;
    if (ngx_http_cache_purge_fence_encode(&fence, raw.data, sizeof(raw_data),
                                            &raw.len, &error) != 0)
    {
        goto done;
    }
    ngx_encode_base64(&encoded, &raw);
    main = ngx_http_get_module_ctx(r->main, ngx_http_cache_purge_index_module);
    if (main && main->propagation_shared
        && main->propagation_shared->value[2].len == encoded.len
        && ngx_memcmp(main->propagation_shared->value[2].data, encoded.data,
                       encoded.len) == 0)
    {
        /* Slice subrequests share their parent's pool. Identical immutable
         * requirements must not retain one separate pool for every slice. */
        ctx->propagation = main->propagation_shared;
        result = NGX_OK;
        goto done;
    }
    if (size + NGX_INT64_LEN + sizeof(*state) + sizeof(*cln)
        > NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES
          - ngx_http_cache_purge_propagation_bytes)
    {
        goto done;
    }
    cln = ngx_pool_cleanup_add(r->main->pool, sizeof(*state));
    if (cln == NULL) {
        goto done;
    }
    state = cln->data;
    ngx_memzero(state, sizeof(*state));
    cln->handler = ngx_http_cache_purge_propagation_cleanup;
    p = ngx_pnalloc(r->main->pool, size + NGX_INT64_LEN);
    if (p == NULL) {
        goto done;
    }
    state->reserved = size + NGX_INT64_LEN + sizeof(*state) + sizeof(*cln);
    ngx_http_cache_purge_propagation_bytes += state->reserved;
    issued = ngx_max(fence.invalidate_ms, fence.delete_ms);
    state->value[0].data = p;
    state->value[0].len = ngx_sprintf(p, "%uL", issued) - p;
    if (!fence.delete_ms) {
        ngx_str_set(&state->value[1], "invalidate");
    } else if (!fence.invalidate_ms) {
        ngx_str_set(&state->value[1], "delete");
    } else {
        ngx_str_set(&state->value[1], "mixed");
    }
    state->value[2].data = p + NGX_INT64_LEN;
    state->value[2].len = encoded.len;
    ngx_memcpy(state->value[2].data, encoded.data, encoded.len);
    ctx->propagation = state;
    if (main) {
        main->propagation_shared = state;
    }
    result = NGX_OK;

done:
    return result;
}

static ngx_int_t
ngx_http_cache_purge_propagation_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *value, uintptr_t data)
{
    ngx_http_cache_purge_capture_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    value->valid = 1;
    value->not_found = 0;
    value->no_cacheable = 1;
    value->len = 0;
    value->data = (u_char *) "";
    if (ctx && ctx->read_upstream == r->upstream && ctx->propagation) {
        value->len = ctx->propagation->value[data].len;
        value->data = ctx->propagation->value[data].data;
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_propagation_variables(ngx_conf_t *cf)
{
    ngx_http_variable_t  *variable;
    ngx_uint_t            i;

    for (i = 0; i < 3; i++) {
        variable = ngx_http_add_variable(cf,
            &ngx_http_cache_purge_propagation_names[i],
            NGX_HTTP_VAR_NOCACHEABLE);
        if (variable == NULL) {
            return NGX_ERROR;
        }
        variable->get_handler = ngx_http_cache_purge_propagation_variable;
        variable->data = i;
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_propagation_role(ngx_http_cache_purge_loc_conf_t *conf,
    ngx_str_t name)
{
    ngx_str_t   *expected;
    ngx_uint_t   i;

    for (i = 0; i < 4; i++) {
        expected = i < 3 ? &conf->propagation_header[i] :
                          &conf->tag_namespace_header;
        if (name.len == expected->len
            && ngx_strncasecmp(name.data, expected->data, name.len) == 0)
        {
            return (ngx_int_t) i;
        }
    }
    return NGX_DECLINED;
}

static ngx_chain_t *
ngx_http_cache_purge_send_link(ngx_http_cache_purge_capture_t *ctx,
    ngx_buf_t *buffer)
{
    ngx_chain_t                  *link;
    ngx_http_cache_purge_send_t  *send;

    send = ctx->send;
    if (send->links > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - sizeof(*link)
        || sizeof(*link) > NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES
                           - ngx_http_cache_purge_propagation_bytes)
    {
        return NULL;
    }
    link = ngx_alloc(sizeof(*link), ctx->request->connection->log);
    if (link == NULL) {
        return NULL;
    }
    link->buf = buffer;
    link->next = NULL;
    send->links += sizeof(*link);
    send->reserved += sizeof(*link);
    ngx_http_cache_purge_propagation_bytes += sizeof(*link);
    return link;
}

static void
ngx_http_cache_purge_send_clear(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_chain_t                  *link;
    ngx_http_cache_purge_send_t  *send;

    send = ctx->send;
    if (send == NULL) {
        return;
    }
    while (send->pending) {
        link = send->pending;
        send->pending = link->next;
        ngx_free(link);
        send->reserved -= sizeof(*link);
        send->links -= sizeof(*link);
        ngx_http_cache_purge_propagation_bytes -= sizeof(*link);
    }
    send->last = &send->pending;
}

static void
ngx_http_cache_purge_send_release(ngx_http_cache_purge_capture_t *ctx)
{
    if (ctx->send->held) {
        ctx->send->held = 0;
        ctx->request->main->count--;
        ctx->request->main->blocked--;
        ngx_http_cache_purge_read_count--;
    }
}

static void
ngx_http_cache_purge_send_cancel(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_send_t  *send;

    send = ctx->send;
    if (send == NULL) {
        return;
    }
    if (send->held) {
        if (ctx->call) {
            ngx_http_cache_purge_client_cancel(ctx->call);
            ctx->call = NULL;
        }
        if (ctx->read_event.timer_set) {
            ngx_del_timer(&ctx->read_event);
        }
        if (ctx->read_event.posted) {
            ngx_delete_posted_event(&ctx->read_event);
        }
        ngx_http_cache_purge_send_release(ctx);
        ctx->read_event.handler = ngx_http_cache_purge_read_event;
    }
    ngx_http_cache_purge_send_clear(ctx);
    ngx_http_cache_purge_propagation_bytes -= send->reserved;
    ngx_free(send->data);
    ngx_free(send);
    ctx->send = NULL;
}

static ngx_int_t
ngx_http_cache_purge_send_grant(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_purge_watermark_t        watermark;
    ngx_http_cache_rocksdb_error_t          error;
    uint64_t                                publication;

    if (!ctx->send_enabled) {
        ctx->send_started = 1;
        return NGX_OK;
    }
    if (!ctx->local_ready) {
        return NGX_AGAIN;
    }
    if (ctx->failed) {
        ngx_log_error(NGX_LOG_WARN, ctx->request->connection->log, 0,
                      "cache purge send admission unavailable: "
                      "failed:%ui origin:%ui", (ngx_uint_t) ctx->failed,
                      (ngx_uint_t) ctx->origin_registered);
        return NGX_ERROR;
    }
    if (!ctx->detached && !ctx->origin_registered
        && ngx_http_cache_purge_capture_origin(ctx) != NGX_OK)
    {
        /* No bytes have been sent yet. Retry an initial registration that
         * collided with publication within the existing send deadline. */
        return NGX_AGAIN;
    }
    if (ngx_http_cache_purge_runtime_reader(ctx->request, &reader, &error)
        != 0) {
        ngx_log_error(NGX_LOG_WARN, ctx->request->connection->log, 0,
                      "cache purge send reader unavailable: %s",
                      error.message);
        return NGX_ERROR;
    }
    if (ngx_http_cache_purge_control_watermark(reader, &watermark,
                                               &publication, &error) != 0)
    {
        return NGX_AGAIN;
    }
    if (watermark.sequence != ctx->local_watermark.sequence
        || ngx_memcmp(watermark.ledger, ctx->local_watermark.ledger, 16) != 0)
    {
        return NGX_AGAIN;
    }
    if (!ctx->detached
        && ngx_memcmp(ctx->origin.ledger, watermark.ledger, 16) != 0)
    {
        ngx_log_error(NGX_LOG_WARN, ctx->request->connection->log, 0,
                      "cache purge send ledger changed");
        return NGX_ERROR;
    }
    if (ngx_http_cache_purge_control_guard_begin(reader, publication, &error)
        != 0)
    {
        return NGX_AGAIN;
    }
    /* Admission precedes the first send attempt, not cache lookup. Keep the
     * older writer pin for retention, but timestamp this origin at admission.
     * No IPC, allocation or network I/O occurs while the guard is held. */
    if (!ctx->detached) {
        ctx->origin = watermark;
        ctx->origin_publication = publication;
    }
    ctx->send_started = 1;
    if (ngx_http_cache_purge_control_guard_end(reader, &error) != 0) {
        ctx->send_started = 0;
        ngx_log_error(NGX_LOG_WARN, ctx->request->connection->log, 0,
                      "cache purge send admission release failed: %s",
                      error.message);
        return NGX_ERROR;
    }
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_send_buffer(ngx_http_cache_purge_capture_t *ctx,
    size_t length)
{
    ngx_http_cache_purge_send_t  *send;
    size_t                        capacity, size;

    send = ctx->send;
    if (length > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD) {
        return NGX_ERROR;
    }
    capacity = length + 1024;
    if (send->data) {
        return capacity <= send->capacity ? NGX_OK : NGX_ERROR;
    }
    size = capacity * 2;
    if (size > NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES
               - ngx_http_cache_purge_propagation_bytes) {
        return NGX_ERROR;
    }
    send->data = ngx_alloc(size, ctx->request->connection->log);
    if (send->data == NULL) {
        return NGX_ERROR;
    }
    send->capacity = capacity;
    send->reserved += size;
    ngx_http_cache_purge_propagation_bytes += size;
    return NGX_OK;
}

#if (NGX_HTTP_V2)
static ngx_int_t ngx_http_cache_purge_send_h2(
    ngx_http_cache_purge_capture_t *ctx);
#endif

static ngx_int_t
ngx_http_cache_purge_send_headers(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_request_t               *r;
    ngx_buf_t                        *b, *body;
    ngx_chain_t                      *native, *pending;
    ngx_str_t                         name;
    ngx_http_cache_purge_loc_conf_t  *conf;
    u_char                           *p, *line, *end, *colon, *out, *last,
                                     *header_end;
    size_t                            capacity;
    ngx_uint_t                        i, keep, first;
    ngx_int_t                         role;

    r = ctx->request;
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    if (ctx->send->pending == NULL
        || ctx->send->pending->buf != ctx->send_header)
    {
        return NGX_ERROR;
    }
    b = ctx->send->pending->buf;
    *b = ctx->send->original;
    if (!ngx_buf_in_memory(b)) {
        return NGX_ERROR;
    }

#if (NGX_HTTP_V2)
    if (b->last - b->pos >= 24
        && ngx_memcmp(b->pos, "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", 24) == 0)
    {
        return ngx_http_cache_purge_send_h2(ctx);
    }
#endif

    header_end = NULL;
    for (p = b->pos; (size_t) (b->last - p) >= 4
         && (size_t) (p - b->pos) <= NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD; p++)
    {
        if (ngx_memcmp(p, CRLF CRLF, 4) == 0) {
            header_end = p + 4;
            break;
        }
    }
    if (header_end == NULL || header_end - b->pos > 131072) {
        return NGX_ERROR;
    }

    if (ngx_http_cache_purge_send_buffer(ctx, header_end - b->pos) != NGX_OK) {
        return NGX_ERROR;
    }
    capacity = ctx->send->capacity;
    out = ctx->send->data;
    last = out;
    p = b->pos;
    first = 1;
    while (p < header_end - 2) {
        line = p;
        end = ngx_strlchr(p, header_end, LF);
        if (end == NULL || end == line || end[-1] != CR) {
            return NGX_ERROR;
        }
        p = end + 1;
        keep = 1;
        if (!first) {
            colon = ngx_strlchr(line, end, ':');
            if (colon == NULL) {
                return NGX_ERROR;
            }
            name.data = line;
            name.len = colon - line;
            role = ngx_http_cache_purge_propagation_role(conf, name);
            keep = role == NGX_DECLINED;

        }
        if (keep) {
            last = ngx_cpymem(last, line, p - line);
        }
        first = 0;
    }
    if (ctx->propagation) {
        for (i = 0; i < 3; i++) {
            last = ngx_cpymem(last, conf->propagation_header[i].data,
                              conf->propagation_header[i].len);
            *last++ = ':';
            *last++ = ' ';
            last = ngx_cpymem(last, ctx->propagation->value[i].data,
                              ctx->propagation->value[i].len);
            last = ngx_cpymem(last, CRLF, 2);
        }
    }
    last = ngx_cpymem(last, CRLF, 2);

    if (header_end != b->last) {
        if (r->request_body_no_buffering) {
            return NGX_ERROR;
        }
        if (ctx->send->body == NULL) {
            body = ngx_alloc_buf(r->pool);
            native = ngx_alloc_chain_link(r->pool);
            if (body == NULL || native == NULL) {
                return NGX_ERROR;
            }
            pending = ngx_http_cache_purge_send_link(ctx, body);
            if (pending == NULL) {
                return NGX_ERROR;
            }
            *body = *b;
            body->start = body->pos = header_end;
            native->buf = pending->buf = body;
            native->next = ctx->read_upstream->request_bufs->next;
            ctx->read_upstream->request_bufs->next = native;
            pending->next = ctx->send->pending->next;
            if (ctx->send->last == &ctx->send->pending->next) {
                ctx->send->last = &pending->next;
            }
            ctx->send->pending->next = pending;
            ctx->send->body = body;
        }
        b->last_buf = 0;
        b->last_in_chain = 0;
        b->flush = 0;
    }

    b->start = b->pos = out;
    b->last = last;
    b->end = out + capacity;
    b->temporary = 1;
    b->memory = 0;
    return NGX_OK;
}


#if (NGX_HTTP_V2)
static ngx_int_t
ngx_http_cache_purge_send_integer(u_char **cursor, u_char *end, size_t mask,
    size_t *value)
{
    u_char     *p, ch;
    size_t      n;
    ngx_uint_t  shift;

    p = *cursor;
    if (p == end) {
        return NGX_ERROR;
    }
    n = *p++ & mask;
    if (n == mask) {
        shift = 0;
        do {
            if (p == end || shift > 14) {
                return NGX_ERROR;
            }
            ch = *p++;
            n += (size_t) (ch & 127) << shift;
            shift += 7;
            if (n > 131072) {
                return NGX_ERROR;
            }
        } while (ch & 128);
    }
    *cursor = p;
    *value = n;
    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_send_string(u_char **cursor, u_char *end)
{
    size_t  length;

    if (ngx_http_cache_purge_send_integer(cursor, end, 127, &length) != NGX_OK
        || length > (size_t) (end - *cursor))
    {
        return NGX_ERROR;
    }
    *cursor += length;
    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_send_h2(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_request_t               *r;
    ngx_buf_t                        *b;
    ngx_str_t                        *name, keys[4];
    ngx_http_cache_purge_loc_conf_t  *conf;
    u_char                           *p, *frame, *flat, *end, *field, *start,
                                     *out, *encoded;
    u_char                            first[9], names[4][132], scratch[128], ch;
    size_t                            prefix, length, size, names_len[4], index,
                                      capacity;
    ngx_uint_t                        i, remove, finished, count;

    r = ctx->request;
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    for (i = 0; i < 3; i++) {
        keys[i] = conf->propagation_header[i];
    }
    keys[3] = conf->tag_namespace_header;
    b = ctx->send_header;
    if (ngx_http_cache_purge_send_buffer(ctx, b->last - b->pos) != NGX_OK) {
        return NGX_ERROR;
    }
    p = b->pos + 24;
    while (b->last - p >= 9 && p[3] != NGX_HTTP_V2_HEADERS_FRAME) {
        length = ((size_t) p[0] << 16) | ((size_t) p[1] << 8) | p[2];
        if ((p[3] != NGX_HTTP_V2_SETTINGS_FRAME
             && p[3] != NGX_HTTP_V2_WINDOW_UPDATE_FRAME)
            || p[5] || p[6] || p[7] || p[8]
            || length > (size_t) (b->last - p - 9))
        {
            return NGX_ERROR;
        }
        p += 9 + length;
    }
    if (b->last - p < 9 || p[4] & ~(NGX_HTTP_V2_END_STREAM_FLAG
                                   | NGX_HTTP_V2_END_HEADERS_FLAG))
    {
        return NGX_ERROR;
    }
    prefix = p - b->pos;
    ngx_memcpy(first, p, 9);
    flat = ctx->send->data;
    end = flat;
    count = 0;
    do {
        if (b->last - p < 9) {
            return NGX_ERROR;
        }
        length = ((size_t) p[0] << 16) | ((size_t) p[1] << 8) | p[2];
        if (length > (size_t) (b->last - p - 9)
            || ngx_memcmp(p + 5, first + 5, 4) != 0
            || (count && (p[3] != NGX_HTTP_V2_CONTINUATION_FRAME
                         || p[4] & ~NGX_HTTP_V2_END_HEADERS_FLAG)))
        {
            return NGX_ERROR;
        }
        finished = p[4] & NGX_HTTP_V2_END_HEADERS_FLAG;
        end = ngx_cpymem(end, p + 9, length);
        p += 9 + length;
        count++;
    } while (!finished);
    if (p != b->last) {
        return NGX_ERROR;
    }

    for (i = 0; i < 4; i++) {
        names_len[i] = ngx_http_v2_write_name(names[i], keys[i].data,
                                             keys[i].len, scratch) - names[i];
    }
    p = out = flat;
    while (p < end) {
        field = p;
        ch = *p;
        remove = 0;
        if (ch & 128) {
            if (ngx_http_cache_purge_send_integer(&p, end, 127, &index)
                != NGX_OK || index == 0 || index > 61)
            {
                return NGX_ERROR;
            }
            name = ngx_http_v2_get_static_name(index);
            remove = ngx_http_cache_purge_propagation_role(conf, *name)
                     != NGX_DECLINED;

        } else if ((ch & 224) == 32) {
            if (ngx_http_cache_purge_send_integer(&p, end, 31, &index)
                != NGX_OK)
            {
                return NGX_ERROR;
            }

        } else {
            if (ngx_http_cache_purge_send_integer(&p, end,
                                                  ch & 64 ? 63 : 15, &index)
                != NGX_OK || index > 61)
            {
                return NGX_ERROR;
            }
            if (index == 0) {
                start = p;
                if (ngx_http_cache_purge_send_string(&p, end) != NGX_OK) {
                    return NGX_ERROR;
                }
                for (i = 0; i < 4; i++) {
                    if ((size_t) (p - start) == names_len[i]
                        && ngx_memcmp(start, names[i], names_len[i]) == 0)
                    {
                        remove = 1;
                        break;
                    }
                }

            } else {
                name = ngx_http_v2_get_static_name(index);
                for (i = 0; i < 4; i++) {
                    if (name->len == keys[i].len
                        && ngx_strncasecmp(name->data, keys[i].data,
                                           name->len) == 0)
                    {
                        remove = 1;
                        break;
                    }
                }
            }
            if (ngx_http_cache_purge_send_string(&p, end) != NGX_OK
                || (remove && (ch & 64)))
            {
                /* Dropping an indexed insertion would change later indexes. */
                return NGX_ERROR;
            }
        }
        if (!remove) {
            ngx_memmove(out, field, p - field);
            out += p - field;
        }
    }
    if (ctx->propagation) {
        for (i = 0; i < 3; i++) {
            *out++ = 0;
            out = ngx_cpymem(out, names[i], names_len[i]);
            out = ngx_http_v2_write_value(out, ctx->propagation->value[i].data,
                         ctx->propagation->value[i].len, scratch);
        }
    }
    size = out - flat;
    capacity = prefix + size + 9 * ((size + 16383) / 16384);
    if (capacity > ctx->send->capacity || size == 0) {
        return NGX_ERROR;
    }
    encoded = ctx->send->data + ctx->send->capacity;
    out = ngx_cpymem(encoded, b->pos, prefix);
    p = flat;
    count = 0;
    while (size) {
        length = ngx_min(size, 16384);
        frame = out;
        out = ngx_cpymem(out, first, 9);
        frame[0] = (u_char) (length >> 16);
        frame[1] = (u_char) (length >> 8);
        frame[2] = (u_char) length;
        frame[3] = count ? NGX_HTTP_V2_CONTINUATION_FRAME
                         : NGX_HTTP_V2_HEADERS_FRAME;
        frame[4] = count ? 0 : first[4] & NGX_HTTP_V2_END_STREAM_FLAG;
        if (length == size) {
            frame[4] |= NGX_HTTP_V2_END_HEADERS_FLAG;
        }
        out = ngx_cpymem(out, p, length);
        p += length;
        size -= length;
        count++;
    }
    b->start = b->pos = encoded;
    b->last = out;
    b->end = encoded + capacity;
    b->memory = 0;
    b->temporary = 1;
    return NGX_OK;
}


#endif

static ngx_int_t
ngx_http_cache_purge_send_output(void *data, ngx_chain_t *in)
{
    ngx_http_cache_purge_capture_t  *ctx = data;

    ngx_http_request_t           *r;
    ngx_http_cache_purge_send_t  *send;
    ngx_chain_t                  *link;
    ngx_int_t                     rc;

    r = ctx->request;
    if (ctx->incoming_error) {
        return NGX_HTTP_BAD_REQUEST;
    }
    if (ctx->send_started) {
        return ctx->send_next(ctx->send_data, in);
    }
    send = ctx->send;
    if (send == NULL) {
        rc = ctx->detached ? NGX_AGAIN : ngx_http_cache_purge_send_grant(ctx);
        if (rc == NGX_OK) {
            return ctx->send_next(ctx->send_data, in);
        }
        if (rc == NGX_ERROR
            || sizeof(*send) > NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES
                              - ngx_http_cache_purge_propagation_bytes)
        {
            return NGX_HTTP_SERVICE_UNAVAILABLE;
        }
        send = ngx_alloc(sizeof(*send), r->connection->log);
        if (send == NULL) {
            return NGX_HTTP_SERVICE_UNAVAILABLE;
        }
        ngx_memzero(send, sizeof(*send));
        send->original = *ctx->send_header;
        send->last = &send->pending;
        send->reserved = sizeof(*send);
        ngx_http_cache_purge_propagation_bytes += sizeof(*send);
        ctx->send = send;
        ctx->deadline = ngx_current_msec + NGX_HTTP_CACHE_PURGE_CAPTURE_TIMEOUT;
    }
    while (in) {
        link = ngx_http_cache_purge_send_link(ctx, in->buf);
        if (link == NULL) {
            return NGX_HTTP_SERVICE_UNAVAILABLE;
        }
        *send->last = link;
        send->last = &link->next;
        in = in->next;
    }
    if (ctx->local_failed) {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    if (ctx->local_ready && send->changed) {
        rc = ngx_http_cache_purge_send_grant(ctx);
        if (rc == NGX_OK) {
            rc = ctx->send_next(ctx->send_data, send->pending);
            ngx_http_cache_purge_send_clear(ctx);
            return rc;
        }
        if (rc == NGX_ERROR) {
            return NGX_HTTP_SERVICE_UNAVAILABLE;
        }
    }
    if (!send->held) {
        if (r->main->count == 65535
            || r->main->blocked >= NGX_HTTP_MAX_BLOCKED
            || ngx_http_cache_purge_read_count
               >= NGX_HTTP_CACHE_PURGE_CAPTURE_COUNT)
        {
            return NGX_HTTP_SERVICE_UNAVAILABLE;
        }
        send->held = 1;
        send->changed = 0;
        ctx->local_ready = !ctx->send_enabled;
        r->main->count++;
        r->main->blocked++;
        ngx_http_cache_purge_read_count++;
        ctx->read_event.data = ctx;
        ctx->read_event.handler = ngx_http_cache_purge_send_event;
        ctx->read_event.log = r->connection->log;
        ngx_post_event(&ctx->read_event, &ngx_posted_events);
    }
    return NGX_AGAIN;
}

static void
ngx_http_cache_purge_send_event(ngx_event_t *event)
{
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_http_request_t              *r;
    ngx_http_upstream_t             *u;
    ngx_connection_t                *connection;
    ngx_int_t                        rc, headers;

    ctx = event->data;
    r = ctx->request;
    u = ctx->read_upstream;
    connection = r->connection;
    if (r->main->terminated) {
        ngx_http_cache_purge_send_cancel(ctx);
        connection->write->handler(connection->write);
        return;
    }
    if (r->upstream != u) {
        ngx_http_cache_purge_send_cancel(ctx);
        return;
    }
    if (connection->error || u->peer.connection == NULL) {
        if (u->cleanup && *u->cleanup) {
            /* Keep the async reference until the upstream finalizer runs. */
            (*u->cleanup)(r);
        } else {
            ngx_http_cache_purge_send_cancel(ctx);
        }
        return;
    }
    if (ctx->call) {
        return;
    }
    if ((ngx_msec_int_t) (ctx->deadline - ngx_current_msec) <= 0) {
        ctx->local_failed = 1;
    }
    if (!ctx->local_ready && !ctx->local_failed) {
        rc = ngx_http_cache_purge_propagation_query(ctx);
        if (rc == NGX_OK) {
            return;
        }
        if (rc == NGX_BUSY
            && (ngx_msec_int_t) (ctx->deadline - ngx_current_msec) > 10)
        {
            ngx_add_timer(&ctx->read_event, 10);
            return;
        }
        ngx_log_error(NGX_LOG_WARN, connection->log, 0,
                      "cache purge send query submission failed: %i", rc);
        ctx->local_failed = 1;
    }
    if (ctx->local_ready && !ctx->local_failed) {
        ctx->propagation = NULL;
        rc = ctx->send_enabled ? ngx_http_cache_purge_propagation_snapshot(ctx)
                               : NGX_OK;
        if (rc == NGX_AGAIN
            && (ngx_msec_int_t) (ctx->deadline - ngx_current_msec) > 10)
        {
            ctx->local_ready = 0;
            ngx_add_timer(&ctx->read_event, 10);
            return;
        }
        headers = rc == NGX_OK ? ngx_http_cache_purge_send_headers(ctx)
                              : NGX_ERROR;
        if (rc != NGX_OK || headers != NGX_OK) {
            ngx_log_error(NGX_LOG_WARN, connection->log, 0,
                          "cache purge send refresh failed: "
                          "snapshot:%i headers:%i", rc, headers);
            ctx->local_failed = 1;
        } else {
            ctx->send->changed = 1;
        }
    }
    ngx_http_cache_purge_send_release(ctx);
    ctx->read_event.handler = ngx_http_cache_purge_read_event;
    u->write_event_handler(r, u);
    ngx_http_run_posted_requests(connection);
}

static ngx_int_t
ngx_http_cache_purge_incoming_parse(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_loc_conf_t     *conf;
    ngx_http_cache_purge_capture_t      *main;
    ngx_http_cache_purge_incoming_t     *incoming;
    ngx_http_cache_rocksdb_error_t       error;
    ngx_http_cache_rocksdb_bytes_t       bytes;
    ngx_http_cache_purge_fence_t         fence;
    ngx_pool_cleanup_t                  *cln;
    ngx_list_part_t                     *part;
    ngx_table_elt_t                     *headers;
    ngx_http_request_t                  *r;
    ngx_str_t                            values[3], raw, check;
    ngx_uint_t                           i, seen[3] = { 0, 0, 0 }, modes;
    ngx_int_t                            role;
    ngx_flag_t                           ignore;
    uint64_t                             issued, maximum;
    size_t                               size, at;
    unsigned                             digit;
    u_char                              *p, decoded[21], canonical[28];
    int                                  rc;

    r = ctx->request;
    ignore = ngx_http_cache_purge_ignore_propagation(r);
    if (ignore == 1) {
        return NGX_OK;
    }
    if (ignore != 0) {
        return NGX_ERROR;
    }
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    ngx_memzero(values, sizeof(values));
    for (part = &r->headers_in.headers.part; part; part = part->next) {
        headers = part->elts;
        for (i = 0; i < part->nelts; i++) {
            role = ngx_http_cache_purge_propagation_role(conf, headers[i].key);
            if (role < 0 || role >= 3) {
                continue;
            }
            if (seen[role]++) {
                return NGX_ERROR;
            }
            values[role] = headers[i].value;
        }
    }
    if (!seen[0] && !seen[1] && !seen[2]) {
        return NGX_OK;
    }
    if (!seen[0] || !seen[1] || !seen[2] || values[0].len == 0
        || values[0].len > 20 || values[0].data[0] == '0'
        || values[2].len != 28
        || values[2].len + conf->propagation_header[2].len + 4
           > NGX_HTTP_CACHE_PURGE_PROPAGATION_LINE)
    {
        return NGX_ERROR;
    }
    main = ngx_http_get_module_ctx(r->main, ngx_http_cache_purge_index_module);
    if (main && main->incoming_shared) {
        incoming = main->incoming_shared;
        for (i = 0; i < 3; i++) {
            if (incoming->headers[i].len != values[i].len
                || ngx_memcmp(incoming->headers[i].data, values[i].data,
                               values[i].len) != 0)
            {
                break;
            }
        }
        if (i == 3) {
            ctx->incoming = incoming;
            return NGX_OK;
        }
    }
    issued = 0;
    for (at = 0; at < values[0].len; at++) {
        digit = (unsigned) values[0].data[at] - '0';
        if (digit > 9 || issued > (UINT64_MAX - digit) / 10) {
            return NGX_ERROR;
        }
        issued = issued * 10 + digit;
    }
    raw.len = ngx_base64_decoded_length(values[2].len);
    raw.data = decoded;
    check.data = canonical;
    rc = NGX_ERROR;
    if (ngx_decode_base64(&raw, &values[2]) != NGX_OK || raw.len < 6) {
        goto done;
    }
    ngx_encode_base64(&check, &raw);
    if (check.len != values[2].len
        || ngx_memcmp(check.data, values[2].data, check.len) != 0)
    {
        goto done;
    }
    bytes.data = raw.data;
    bytes.len = raw.len;
    if (ngx_http_cache_purge_fence_decode(bytes, &fence, &error) != 0) {
        goto done;
    }
    maximum = ngx_max(fence.invalidate_ms, fence.delete_ms);
    modes = (fence.invalidate_ms ? NGX_HTTP_CACHE_PURGE_INVALIDATE : 0)
            | (fence.delete_ms ? NGX_HTTP_CACHE_PURGE_DELETE : 0);
    if (issued != maximum
        || !ngx_http_cache_purge_capture_equal(values[1],
            modes == NGX_HTTP_CACHE_PURGE_INVALIDATE ? "invalidate" :
            modes == NGX_HTTP_CACHE_PURGE_DELETE ? "delete" : "mixed"))
    {
        goto done;
    }
    size = sizeof(*incoming) + sizeof(*cln);
    for (i = 0; i < 3; i++) {
        size += values[i].len;
    }
    if (size > NGX_HTTP_CACHE_PURGE_CAPTURE_BYTES
               - ngx_http_cache_purge_propagation_bytes)
    {
        goto done;
    }
    cln = ngx_pool_cleanup_add(r->main->pool, size - sizeof(*cln));
    if (cln == NULL) {
        goto done;
    }
    incoming = cln->data;
    ngx_memzero(incoming, sizeof(*incoming));
    incoming->allocation.reserved = size;
    ngx_http_cache_purge_propagation_bytes += size;
    cln->handler = ngx_http_cache_purge_propagation_cleanup;
    incoming->fence = fence;
    p = (u_char *) (incoming + 1);
    for (i = 0; i < 3; i++) {
        incoming->headers[i].data = p;
        incoming->headers[i].len = values[i].len;
        p = ngx_cpymem(p, values[i].data, values[i].len);
    }
    ctx->incoming = incoming;
    if (main) {
        main->incoming_shared = incoming;
    }
    rc = NGX_OK;

done:
    return rc;
}

static void
ngx_http_cache_purge_incoming_cutoffs(ngx_http_cache_purge_capture_t *ctx)
{
    ctx->incoming_invalidate = ctx->incoming ?
                              ctx->incoming->fence.invalidate_ms : 0;
    ctx->incoming_delete = ctx->incoming ? ctx->incoming->fence.delete_ms : 0;
}

static ngx_uint_t
ngx_http_cache_purge_incoming_mode(ngx_http_cache_purge_capture_t *ctx,
    time_t date)
{
    if (ctx->incoming_delete
        && (date < 0 || (uint64_t) date < ctx->incoming_delete / 1000))
    {
        return NGX_HTTP_CACHE_PURGE_DELETE;
    }
    if (ctx->incoming_invalidate
        && (date < 0 || (uint64_t) date < ctx->incoming_invalidate / 1000))
    {
        return NGX_HTTP_CACHE_PURGE_INVALIDATE;
    }
    return 0;
}

static ngx_flag_t
ngx_http_cache_purge_propagation_incoming(ngx_http_request_t *r)
{
    ngx_http_cache_purge_loc_conf_t  *conf;
    ngx_list_part_t                  *part;
    ngx_table_elt_t                  *headers;
    ngx_uint_t                        i;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    for (part = &r->headers_in.headers.part; part; part = part->next) {
        headers = part->elts;
        for (i = 0; i < part->nelts; i++) {
            if (ngx_http_cache_purge_propagation_role(conf, headers[i].key)
                != NGX_DECLINED)
            {
                return 1;
            }
        }
    }
    return 0;
}

#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
static ngx_int_t
ngx_http_cache_purge_propagation_verify(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_loc_conf_t  *conf;
    ngx_list_part_t                  *part;
    ngx_table_elt_t                  *headers;
    ngx_uint_t                       i, seen[3] = { 0, 0, 0 };
    ngx_int_t                        role;
    ngx_str_t                       *expected;

    if (ctx->propagation_headers == NULL) {
        return ctx->propagation
               || ngx_http_cache_purge_propagation_incoming(ctx->request)
                   ? NGX_ERROR : NGX_OK;
    }
    conf = ngx_http_get_module_loc_conf(ctx->request,
                                       ngx_http_cache_purge_module);
    for (part = &ctx->propagation_headers->part; part; part = part->next) {
        headers = part->elts;
        for (i = 0; i < part->nelts; i++) {
            if (headers[i].hash == 0) {
                continue;
            }
            role = ngx_http_cache_purge_propagation_role(conf, headers[i].key);
            if (role == NGX_DECLINED) {
                continue;
            }
            if (ctx->propagation == NULL || role == 3 || seen[role]++) {
                return NGX_ERROR;
            }
            expected = &ctx->propagation->value[role];
            if (headers[i].value.len != expected->len
                || ngx_memcmp(headers[i].value.data, expected->data,
                               expected->len) != 0)
            {
                return NGX_ERROR;
            }
        }
    }
    return ctx->propagation == NULL || (seen[0] && seen[1] && seen[2])
           ? NGX_OK : NGX_ERROR;
}
#endif

#if !(NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
static ngx_int_t
ngx_http_cache_purge_propagation_headers(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_conf_t  *capture;
    ngx_http_cache_purge_loc_conf_t      *conf;
    ngx_keyval_t                         *headers;
    ngx_str_t                             value, name;
    ngx_uint_t                            i, seen[4] = { 0, 0, 0, 0 };
    ngx_int_t                             role;

    capture = ngx_http_get_module_loc_conf(r,
                                          ngx_http_cache_purge_index_module);
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    if (capture->proxy_headers == NULL
        || capture->proxy_headers == NGX_CONF_UNSET_PTR)
    {
        return NGX_ERROR;
    }
    headers = capture->proxy_headers->elts;
    for (i = 0; i < capture->proxy_headers->nelts; i++) {
        role = ngx_http_cache_purge_propagation_role(conf, headers[i].key);
        if (role == NGX_DECLINED) {
            continue;
        }
        if (seen[role]++) {
            return NGX_ERROR;
        }
        value = headers[i].value;
        if (role == 3) {
            if (value.len != 0) {
                return NGX_ERROR;
            }
            continue;
        }
        name = ngx_http_cache_purge_propagation_names[role];
        if ((value.len == name.len + 1 && value.data[0] == '$'
             && ngx_memcmp(value.data + 1, name.data, name.len) == 0)
            || (value.len == name.len + 3 && value.data[0] == '$'
                && value.data[1] == '{' && value.data[value.len - 1] == '}'
                && ngx_memcmp(value.data + 2, name.data, name.len) == 0))
        {
            continue;
        }
        return NGX_ERROR;
    }
    return seen[0] && seen[1] && seen[2] && seen[3] ? NGX_OK : NGX_ERROR;
}
#endif

static ngx_int_t
ngx_http_cache_purge_propagation_create(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_flag_t                       enabled;
    ngx_int_t                        rc;
    ngx_uint_t                       mode;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ctx == NULL || ctx->create_request == NULL
        || ctx->create_request == ngx_http_cache_purge_propagation_create
        || ctx->incoming_error)
    {
        return NGX_ERROR;
    }
    if (r->upstream->output.output_filter == ngx_http_cache_purge_send_output) {
        r->upstream->output.output_filter = ctx->send_next;
        r->upstream->output.filter_ctx = ctx->send_data;
    }
    ngx_http_cache_purge_send_cancel(ctx);
    ctx->send_started = 0;
    ctx->propagation = NULL;
    ngx_http_cache_purge_incoming_cutoffs(ctx);
    if (ctx->cache->file.fd != NGX_INVALID_FILE) {
        mode = ngx_http_cache_purge_incoming_mode(ctx, ctx->cache->date);
        if (mode == NGX_HTTP_CACHE_PURGE_DELETE) {
            if (ngx_http_cache_purge_retire_start(ctx) != NGX_OK) {
                return NGX_ERROR;
            }
            /* Native revalidation variables are empty outside EXPIRED. */
            if (r->upstream->cache_status == NGX_HTTP_CACHE_EXPIRED) {
                r->upstream->cache_status = NGX_HTTP_CACHE_BYPASS;
            }
        }
#if (NGX_RESTY_EXT)
        if (mode) {
            ctx->cache->purged = 1;
        }
#endif
    }
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    ctx->propagation_headers = NULL;
#endif
    enabled = ngx_http_cache_purge_propagation_enabled(r);
    if (enabled != 0 && enabled != 1) {
        return NGX_ERROR;
    }
    ctx->send_enabled = enabled;
#if !(NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    if (enabled || ngx_http_cache_purge_propagation_incoming(r)) {
        if (ngx_http_cache_purge_propagation_headers(r) != NGX_OK) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                "cache purge propagation requires all three proxy_set_header "
                "variables and an empty namespace header");
            return NGX_ERROR;
        }
    }
#endif
    if (enabled) {
        rc = ngx_http_cache_purge_propagation_snapshot(ctx);
        if (rc == NGX_AGAIN) {
            ctx->local_ready = 0;
        }
        if (rc != NGX_OK && rc != NGX_AGAIN) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                "cache purge propagation snapshot unavailable or over budget");
            return NGX_ERROR;
        }
    }
    /* Cache resumption can reinitialize u->method in nginx-ext. Preserve the
     * native HEAD-to-GET decision made before the cache-entry wait. */
    r->upstream->method = ctx->method;
    rc = ctx->create_request(r);
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    if (rc == NGX_OK && ngx_http_cache_purge_propagation_verify(ctx) != NGX_OK)
    {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "proxy filter bypassed or replaced cache purge headers");
        return NGX_ERROR;
    }
#endif
    if (rc == NGX_OK && enabled) {
        if (r->upstream->request_bufs == NULL) {
            return NGX_ERROR;
        }
        ctx->send_header = r->upstream->request_bufs->buf;
        ctx->send_next = r->upstream->output.output_filter;
        ctx->send_data = r->upstream->output.filter_ctx;
        if (ctx->send_next == NULL) {
            ctx->send_next = ngx_chain_writer;
            ctx->send_data = &r->upstream->writer;
        }
        r->upstream->output.output_filter = ngx_http_cache_purge_send_output;
        r->upstream->output.filter_ctx = ctx;
    }
    return rc;
}

#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
static ngx_int_t
ngx_http_cache_purge_propagation_filter(ngx_http_request_t *r,
    ngx_http_proxy_filter_ctx_t *filter)
{
    ngx_http_cache_purge_capture_t   *ctx;
    ngx_http_cache_purge_loc_conf_t  *conf;
    ngx_list_part_t                  *part;
    ngx_table_elt_t                  *headers, *header;
    ngx_uint_t                       i;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ctx == NULL || ctx->read_upstream != r->upstream) {
        return NGX_DECLINED;
    }
    if (filter->headers == NULL) {
        return NGX_ERROR;
    }
    ctx->propagation_headers = filter->headers;
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);
    for (part = &filter->headers->part; part; part = part->next) {
        headers = part->elts;
        for (i = 0; i < part->nelts; i++) {
            if (ngx_http_cache_purge_propagation_role(conf, headers[i].key)
                != NGX_DECLINED)
            {
                headers[i].hash = 0;
            }
        }
    }
    if (ctx->propagation == NULL) {
        return NGX_DECLINED;
    }
    for (i = 0; i < 3; i++) {
        header = ngx_list_push(filter->headers);
        if (header == NULL) {
            return NGX_ERROR;
        }
        ngx_memzero(header, sizeof(*header));
        header->key = conf->propagation_header[i];
        header->value = ctx->propagation->value[i];
        header->lowcase_key = ngx_pnalloc(r->pool, header->key.len);
        if (header->lowcase_key == NULL) {
            return NGX_ERROR;
        }
        header->hash = ngx_hash_strlow(header->lowcase_key, header->key.data,
                                       header->key.len);
    }
    return NGX_DECLINED;
}
#endif



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
    if (ctx->publication_retry.timer_set) {
        ngx_del_timer(&ctx->publication_retry);
    }
    if (ctx->publication_retry.posted) {
        ngx_delete_posted_event(&ctx->publication_retry);
    }
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
    ngx_http_cache_purge_send_cancel(ctx);
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

/* Configuration identity is independent of native cache key serialization. */
ngx_int_t
ngx_http_cache_purge_request_identity(ngx_http_request_t *r,
    ngx_str_t *url, ngx_str_t *misc, ngx_str_t *namespace, ngx_str_t *host,
    ngx_str_t *key)
{
    ngx_http_cache_purge_capture_conf_t   *conf;
    ngx_http_cache_rocksdb_bytes_t         input[3];
    ngx_http_cache_rocksdb_error_t         error;
    ngx_str_t                            *fields[3], value;
    ngx_url_t                             parsed;
    ngx_uint_t                            i;
    ngx_int_t                             rc;
    size_t                                total, prefix;
    u_char                                digest[32];

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_index_module);
    fields[0] = url;
    fields[1] = misc;
    fields[2] = namespace;
    if ((host != NULL && url == NULL)
        || (key != NULL && (url == NULL || misc == NULL || namespace == NULL)))
    {
        return NGX_ERROR;
    }
    total = 0;
    for (i = 0; i < 3; i++) {
        if (fields[i] == NULL) {
            continue;
        }
#if (NGX_EXPR)
        rc = ngx_http_get_expr_complex_value(r, conf->identity[i], &value);
#else
        rc = ngx_http_complex_value(r, conf->identity[i], &value);
#endif
        if (rc != NGX_OK || value.len > 32768
            || value.len > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - total)
        {
            return NGX_ERROR;
        }
        total += value.len;
        fields[i]->len = value.len;
        fields[i]->data = (u_char *) "";
        if (value.len) {
            fields[i]->data = ngx_pnalloc(r->pool, value.len);
            if (fields[i]->data == NULL) {
                return NGX_ERROR;
            }
            ngx_memcpy(fields[i]->data, value.data, value.len);
        }
        input[i].data = fields[i]->data;
        input[i].len = fields[i]->len;
    }
    if (namespace != NULL && namespace->len > 255) {
        return NGX_ERROR;
    }
    for (i = 0; namespace != NULL && i < namespace->len; i++) {
        if (namespace->data[i] <= 0x20 || namespace->data[i] >= 0x7f
            || namespace->data[i] == ',')
        {
            return NGX_ERROR;
        }
    }
    if (url == NULL) {
        return NGX_OK;
    }
    prefix = url->len >= 7
                 && ngx_strncmp(url->data, "http://", 7) == 0 ? 7 :
             url->len >= 8
                 && ngx_strncmp(url->data, "https://", 8) == 0 ? 8 : 0;
    if (prefix == 0 || url->len == prefix) {
        return NGX_ERROR;
    }
    for (i = prefix; i < url->len; i++) {
        if (url->data[i] <= 0x20 || url->data[i] == 0x7f
            || url->data[i] == '#')
        {
            return NGX_ERROR;
        }
    }
    ngx_memzero(&parsed, sizeof(parsed));
    parsed.url.data = url->data + prefix;
    parsed.url.len = url->len - prefix;
    parsed.default_port = prefix == 7 ? 80 : 443;
    parsed.uri_part = 1;
    parsed.no_resolve = 1;
    if (ngx_parse_url(r->pool, &parsed) != NGX_OK || parsed.host.len == 0
        || ngx_strlchr(parsed.host.data,
                      parsed.host.data + parsed.host.len, '@'))
    {
        return NGX_ERROR;
    }
    if (host != NULL) {
        host->len = parsed.host.len;
        host->data = ngx_pnalloc(r->pool, host->len);
        if (host->data == NULL) {
            return NGX_ERROR;
        }
        ngx_strlow(host->data, parsed.host.data, host->len);
    }
    if (key == NULL) {
        return NGX_OK;
    }
    key->len = 64;
    key->data = ngx_pnalloc(r->pool, key->len);
    if (key->data == NULL
        || ngx_http_cache_purge_identity_digest(input[0], input[2], input[1],
                                               digest, &error) != 0)
    {
        return NGX_ERROR;
    }
    ngx_hex_dump(key->data, digest, sizeof(digest));
    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_purge_capture_identity(ngx_http_cache_purge_capture_t *ctx)
{
    if (ngx_http_cache_purge_request_identity(ctx->request, &ctx->url,
            &ctx->misc, &ctx->namespace, &ctx->host, &ctx->key) != NGX_OK)
    {
        return NGX_ERROR;
    }
    ctx->input_bytes += ctx->url.len + ctx->misc.len + ctx->namespace.len
                        + ctx->host.len + ctx->key.len;
    return ctx->input_bytes <= NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD
           ? NGX_OK : NGX_ERROR;
}

static ngx_flag_t
ngx_http_cache_purge_protocol_enabled(ngx_http_request_t *r, ngx_uint_t n)
{
    ngx_http_cache_purge_capture_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_index_module);
#if (NGX_EXPR)
    return ngx_http_get_expr_flag_value(r, conf->protocol[n]);
#else
    return conf->protocol[n];
#endif
}

static ngx_flag_t
ngx_http_cache_purge_protocol_possible(
    ngx_http_cache_purge_capture_conf_t *conf, ngx_uint_t n)
{
#if (NGX_EXPR)
    ngx_conf_expr_flag_ctx_t  *values;
    ngx_uint_t                 i;

    if (conf->protocol[n] == NULL
        || conf->protocol[n] == NGX_CONF_UNSET_PTR)
    {
        return 0;
    }
    values = conf->protocol[n]->elts;
    for (i = 0; i < conf->protocol[n]->nelts; i++) {
        if (values[i].value == 1) {
            return 1;
        }
    }
    return 0;
#else
    return conf->protocol[n] == 1;
#endif
}

static ngx_int_t
ngx_http_cache_purge_method(ngx_http_request_t *r)
{
    ngx_uint_t  i;
    ngx_flag_t  enabled;

    if (r->method_name.len != 5
        || ngx_strncmp(r->method_name.data, "PURGE", 5) != 0)
    {
        return NGX_DECLINED;
    }
    for (i = 0; i < 4; i++) {
        enabled = ngx_http_cache_purge_protocol_enabled(r, i);
        if (enabled == 1) {
            r->write_event_handler = ngx_http_request_empty_handler;
            ngx_http_finalize_request(r,
                                      ngx_http_cache_purge_method_handler(r));
            return NGX_DONE;
        }
        if (enabled != 0) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }
    }
    return NGX_DECLINED;
}

static ngx_int_t
ngx_http_cache_purge_capture(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_main_t *main =
        ngx_http_get_module_main_conf(r, ngx_http_cache_purge_index_module);
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_pool_cleanup_t              *cln;

    if (!main->enabled) {
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
    ctx->method = r->upstream->method;
    ctx->read_process_header = r->upstream->process_header;
    ctx->read_reinit = r->upstream->reinit_request;
    ctx->error_finalize = r->upstream->finalize_request;
    if ((r->upstream->schema.len == 7
         && ngx_strncasecmp(r->upstream->schema.data, (u_char *) "http://", 7)
            == 0)
        || (r->upstream->schema.len == 8
            && ngx_strncasecmp(r->upstream->schema.data,
                              (u_char *) "https://", 8) == 0))
    {
        ctx->create_request = r->upstream->create_request;
        r->upstream->create_request = ngx_http_cache_purge_propagation_create;
    }
    r->upstream->process_header = ngx_http_cache_purge_read_process_header;
    r->upstream->reinit_request = ngx_http_cache_purge_read_reinit;
    r->upstream->finalize_request = ngx_http_cache_purge_error_finalize;

    ngx_memcpy(ctx->native_main, ctx->cache->main, 16);
    if (ngx_http_cache_purge_capture_identity(ctx) != NGX_OK) {
        return NGX_ERROR;
    }
    if (ctx->create_request
        && ngx_http_cache_purge_incoming_parse(ctx) != NGX_OK)
    {
        ctx->incoming_error = 1;
        return NGX_ERROR;
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
ngx_http_cache_purge_detached_key(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_request_t                  *r;
    ngx_http_cache_t                     cache;
    ngx_str_t                            uri, *keys;
    ngx_md5_t                            md5;
    ngx_uint_t                           i;
    ngx_int_t                            rc;
    size_t                               size;

    r = ctx->request;
    if (r->cache != NULL || r->upstream->create_key == NULL) {
        return NGX_ERROR;
    }
    ngx_memzero(&cache, sizeof(cache));
    cache.file.fd = NGX_INVALID_FILE;
    if (ngx_array_init(&cache.keys, r->pool, 4, sizeof(ngx_str_t)) != NGX_OK) {
        return NGX_ERROR;
    }

    /* The native builder only receives scratch key storage. Do not enter a
     * cache lookup or change the already-created upstream URI. */
    uri = r->upstream->uri;
    r->cache = &cache;
    rc = r->upstream->create_key(r);
    r->cache = NULL;
    r->upstream->uri = uri;
    if (rc == NGX_OK) {
        keys = cache.keys.elts;
        size = 0;
        ngx_md5_init(&md5);
        for (i = 0; i < cache.keys.nelts; i++) {
            if (keys[i].len > NGX_HTTP_CACHE_PURGE_CAPTURE_RECORD - size) {
                rc = NGX_ERROR;
                break;
            }
            size += keys[i].len;
            ngx_md5_update(&md5, keys[i].data, keys[i].len);
        }
        if (rc == NGX_OK && size) {
            ngx_md5_final(ctx->native_main, &md5);
            ctx->cache = &cache;
            rc = ngx_http_cache_purge_capture_identity(ctx);
            ctx->cache = NULL;
        } else {
            rc = NGX_ERROR;
        }
    }
    return rc;
}

static ngx_int_t
ngx_http_cache_purge_detached(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_pool_cleanup_t              *cleanup;
    ngx_flag_t                       enabled;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ngx_http_cache_purge_protocol_enabled(r, 0) == 0
        || r->upstream == NULL || r->cache != NULL
        || (ctx && ctx->read_upstream == r->upstream)
        || (!ngx_http_cache_purge_propagation_possible(r)
            && !ngx_http_cache_purge_propagation_incoming(r)))
    {
        return NGX_OK;
    }
    if (ngx_http_cache_purge_protocol_enabled(r, 0) != 1) {
        return NGX_ERROR;
    }
    enabled = ngx_http_cache_purge_propagation_enabled(r);
    if ((enabled != 0 && enabled != 1) || r->upstream->request_bufs == NULL) {
        return NGX_ERROR;
    }
    if (!enabled && !ngx_http_cache_purge_propagation_incoming(r)) {
        return NGX_OK;
    }
#if !(NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    if (ngx_http_cache_purge_propagation_headers(r) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
            "cache purge propagation requires all three proxy_set_header "
            "variables and an empty namespace header");
        return NGX_ERROR;
    }
#endif
    cleanup = ngx_pool_cleanup_add(r->pool, sizeof(*ctx));
    if (cleanup == NULL) {
        return NGX_ERROR;
    }
    ctx = cleanup->data;
    ngx_memzero(ctx, sizeof(*ctx));
    ctx->request = r;
    ctx->read_upstream = r->upstream;
    ctx->lease = -1;
    ctx->detached = 1;
    ctx->send_enabled = enabled;
    ctx->local_ready = !enabled;
    ctx->read_event.data = ctx;
    ctx->read_event.log = r->connection->log;
    cleanup->handler = ngx_http_cache_purge_capture_cleanup;
    ngx_http_set_ctx(r, ctx, ngx_http_cache_purge_index_module);
    if (enabled && ngx_http_cache_purge_detached_key(ctx) != NGX_OK) {
        return NGX_ERROR;
    }
    ctx->incoming_error = ngx_http_cache_purge_incoming_parse(ctx) != NGX_OK;
    ctx->read_reinit = r->upstream->reinit_request;
    ctx->error_finalize = r->upstream->finalize_request;
    r->upstream->reinit_request = ngx_http_cache_purge_read_reinit;
    r->upstream->finalize_request = ngx_http_cache_purge_error_finalize;
    ctx->send_header = r->upstream->request_bufs->buf;
    ctx->send_next = r->upstream->output.output_filter;
    ctx->send_data = r->upstream->output.filter_ctx;
    if (ctx->send_next == NULL) {
        ctx->send_next = ngx_chain_writer;
        ctx->send_data = &r->upstream->writer;
    }
    r->upstream->output.output_filter = ngx_http_cache_purge_send_output;
    r->upstream->output.filter_ctx = ctx;
    return NGX_OK;
}

static size_t
ngx_http_cache_purge_bind_length(ngx_http_script_engine_t *engine)
{
    engine->ip += sizeof(ngx_http_script_len_code_pt);
    return 0;
}

static void
ngx_http_cache_purge_bind_value(ngx_http_script_engine_t *engine)
{
    ngx_http_cache_purge_bind_code_t *code = (void *) engine->ip;
    ngx_str_t                        value = ngx_null_string;

    engine->ip += sizeof(*code) - sizeof(uintptr_t);
    if (code->original) {
        if (code->original->value) {
            if (ngx_http_complex_value(engine->request, code->original->value,
                                       &value) != NGX_OK)
            {
                engine->status = NGX_ERROR;
                return;
            }
        } else if (code->original->addr) {
            value = code->original->addr->name;
        }
    }
    if (ngx_http_cache_purge_detached(engine->request) != NGX_OK) {
        engine->status = NGX_ERROR;
        return;
    }
    if (value.len) {
        engine->buf = value;
        engine->pos = value.data + value.len;
    }
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
    ngx_http_cache_purge_watermark_t        origin;
    uint64_t                                publication;
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
    /* A failed read clears its outputs. Keep an existing registration intact
     * until its replacement has been validated. */
    if (ngx_http_cache_purge_control_watermark(reader, &origin,
                                                &publication, &error) != 0)
    {
        goto failed;
    }
    if (!ngx_queue_empty(&ngx_http_cache_purge_origins)) {
        last = ngx_queue_data(ngx_queue_last(&ngx_http_cache_purge_origins),
                             ngx_http_cache_purge_capture_t, origin_queue);
        if (ngx_memcmp(origin.ledger, ngx_http_cache_purge_origin_ledger,
                       16) != 0
            || origin.sequence < last->writer_sequence)
        {
            goto failed;
        }
    } else {
        ngx_memcpy(ngx_http_cache_purge_origin_ledger, origin.ledger, 16);
    }
    ctx->origin = origin;
    ctx->origin_publication = publication;
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
    ngx_flag_t                       enabled;

    engine->ip += sizeof(*code) - sizeof(uintptr_t);
    if (rc == NGX_ERROR) {
        engine->status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        return;
    }
    enabled = ngx_http_cache_purge_protocol_enabled(engine->request,
                                                   code->protocol);
    if (enabled == 0) {
        *engine->pos++ = rc == NGX_DECLINED ? '1' : '0';
        return;
    }
    if (enabled != 1) {
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
    if (ctx && ctx->incoming_error) {
        rc = NGX_OK;
    }
    if (ctx && !ctx->incoming_error && !ctx->origin_valid
        && ngx_http_cache_purge_capture_origin(ctx) != NGX_OK)
    {
        r->upstream->cacheable = 0;
        rc = NGX_DECLINED;
    }
    if (rc == NGX_OK
        || (ctx && !ctx->failed && ctx->origin_valid && rc == NGX_DECLINED))
    {
        if (ctx && ctx->read_state == NGX_HTTP_CACHE_PURGE_READ_OFF) {
            if ((!ctx->incoming_error
                 && ngx_http_cache_purge_read_request(ctx) != NGX_OK)
                || r->main->blocked >= NGX_HTTP_MAX_BLOCKED
                || r->main->count == 65535)
            {
                rc = NGX_DECLINED;
            } else {
                ctx->read_bypass = rc == NGX_DECLINED;
                rc = NGX_OK;
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

static ngx_http_complex_value_t *
ngx_http_cache_purge_tag_expression(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_conf_t  *conf;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_index_module);
#if (NGX_EXPR)
    return ngx_http_get_expr_ptr_value(r, conf->tag);
#else
    return conf->tag;
#endif
}

static ngx_int_t
ngx_http_cache_purge_capture_header(ngx_http_request_t *r)
{
    ngx_http_cache_purge_capture_t  *ctx;
    ngx_http_complex_value_t        *expression;
    ngx_str_t                        value;
    ngx_int_t                        rc;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_index_module);
    if (ctx && ctx->error_held && ctx->cache == r->cache
        && ctx->read_upstream == r->upstream && r->upstream->cleanup != NULL)
    {
        /* Native interception did not consume this response. Its ordinary
         * disk-cache path must retain the original cacheability decision. */
        ctx->cache->updated = 0;
        ctx->error_held = 0;
    }
    if (ctx && !ctx->detached && !ctx->failed
        && ctx->cache == r->cache && r->cached
        && ctx->read_state == NGX_HTTP_CACHE_PURGE_READ_READY)
    {
        return ngx_http_cache_purge_read_header(ctx);
    }
    if (ctx && !ctx->detached && !ctx->failed
        && ctx->cache == r->cache && !r->cached)
    {
        expression = ngx_http_cache_purge_tag_expression(r);
        rc = NGX_ERROR;
        if (expression == NULL
            || (ngx_http_complex_value(r,
                    expression, &value)
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
    expression = ngx_http_cache_purge_tag_expression(r);
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
        ngx_http_cache_purge_send_cancel(ctx);
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
    ngx_http_cache_purge_send_clear(ctx);
    if (ctx->detached) {
        return ctx->read_reinit(r);
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
    ngx_uint_t                 revalidate, failed;

    r = ctx->request;
    u = ctx->read_upstream;
    connection = r->connection;
    revalidate = allowed == NGX_HTTP_CACHE_PURGE_ADMIT_REVALIDATE
                 && !ctx->invalidation_retry;
    failed = allowed == NGX_HTTP_CACHE_PURGE_ADMIT_ERROR;
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
    if (allowed || failed || r->main->terminated) {
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
    if (failed) {
        ngx_http_file_cache_free(ctx->cache, NULL);
        ngx_http_finalize_request(r, NGX_HTTP_SERVICE_UNAVAILABLE);
        ngx_http_run_posted_requests(connection);
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
    ngx_http_cache_rocksdb_bytes_t           fields[11], tags[64], value;
    ngx_http_cache_purge_watermark_t        current, stored, validated;
    uint64_t                                publication, first, stamp;
    ngx_uint_t                              i, count;
    ngx_http_cache_purge_read_match_t       match;
    u_char                                 *p, *end;
    int                                     rc;
    ngx_uint_t                              incoming;
    time_t                                  date;

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
    for (i = 0; i < 11; i++) {
        if (ngx_http_cache_purge_read_field(&p, end, &fields[i]) != NGX_OK) {
            return 0;
        }
    }
    if (fields[0].len != ctx->key.len
        || ngx_memcmp(fields[0].data, ctx->key.data, ctx->key.len) != 0
        || fields[3].len != ctx->namespace.len
        || (ctx->namespace.len
            && ngx_memcmp(fields[3].data, ctx->namespace.data,
                          ctx->namespace.len) != 0)
        || fields[10].len != ctx->misc.len
        || (ctx->misc.len
            && ngx_memcmp(fields[10].data, ctx->misc.data,
                          ctx->misc.len) != 0)
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
    ngx_http_cache_purge_incoming_cutoffs(ctx);
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
    /* An unmet Delete requires a new entity, not a 304 header update. */
    if (ctx->validation && ctx->incoming_delete
        && (ctx->cache->date < 0
            || (uint64_t) ctx->cache->date < ctx->incoming_delete / 1000))
    {
        return ngx_http_cache_purge_retire_start(ctx) == NGX_OK
               ? NGX_HTTP_CACHE_PURGE_ADMIT_DENY
               : NGX_HTTP_CACHE_PURGE_ADMIT_ERROR;
    }
    date = ctx->validation ? ctx->validation->after.date : ctx->cache->date;
    incoming = ngx_http_cache_purge_incoming_mode(ctx, date);
    if (incoming == NGX_HTTP_CACHE_PURGE_DELETE) {
        return ngx_http_cache_purge_retire_start(ctx) == NGX_OK
               ? NGX_HTTP_CACHE_PURGE_ADMIT_DENY
               : NGX_HTTP_CACHE_PURGE_ADMIT_ERROR;
    }
#if !(NGX_RESTY_EXT)
    if (ctx->read_upstream->cache_status == NGX_HTTP_CACHE_STALE
        || ctx->read_upstream->cache_status == NGX_HTTP_CACHE_UPDATING)
    {
        return NGX_HTTP_CACHE_PURGE_ADMIT_ALLOW;
    }
#endif
    return match.required || incoming ? NGX_HTTP_CACHE_PURGE_ADMIT_REVALIDATE :
                            NGX_HTTP_CACHE_PURGE_ADMIT_ALLOW;
}

static ngx_int_t
ngx_http_cache_purge_proof_admit(ngx_http_cache_purge_proof_task_t *task)
{
    ngx_http_cache_purge_capture_t         *ctx;
    ngx_http_cache_purge_file_proof_t      *proof;
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_purge_control_context_t  context;
    ngx_http_cache_purge_watermark_t        current;
    ngx_http_cache_purge_read_match_t       match;
    ngx_http_cache_rocksdb_error_t          error;
    ngx_http_cache_rocksdb_bytes_t          tags[64];
    uint64_t                                publication, stamp;
    ngx_uint_t                              i, incoming;
    u_char                                  identity[64];
    int                                     rc;

    ctx = task->capture;
    proof = &task->proof;
    if (ngx_http_cache_purge_runtime_reader(ctx->request, &reader, &error)
        != 0)
    {
        return NGX_HTTP_CACHE_PURGE_ADMIT_ERROR;
    }
    rc = ngx_http_cache_purge_control_watermark(reader, &current, &stamp,
                                               &error);
    if (rc != 0 || stamp != task->publication) {
        return NGX_AGAIN;
    }
    if (proof->marker.state != NGX_HTTP_CACHE_PURGE_FILE_READY
        || ngx_memcmp(proof->ledger, current.ledger, 16) != 0
        || proof->entity > current.sequence
        || proof->validated > current.sequence
        || proof->required > current.sequence
        || proof->tag_count > 64)
    {
        return NGX_HTTP_CACHE_PURGE_ADMIT_DENY;
    }
    ngx_hex_dump(identity, proof->identity, sizeof(proof->identity));
    if (ctx->key.len != sizeof(identity)
        || ngx_memcmp(ctx->key.data, identity, sizeof(identity)) != 0)
    {
        return NGX_HTTP_CACHE_PURGE_ADMIT_DENY;
    }
    ngx_memzero(&context, sizeof(context));
    context.key.data = ctx->key.data;
    context.key.len = ctx->key.len;
    context.url.data = ctx->url.data;
    context.url.len = ctx->url.len;
    context.host.data = ctx->host.data;
    context.host.len = ctx->host.len;
    context.tag_namespace.data = ctx->namespace.data;
    context.tag_namespace.len = ctx->namespace.len;
    context.tag_count = proof->tag_count;
    context.tags = tags;
    context.now_ms = (uint64_t) ngx_time() * 1000;
    for (i = 0; i < proof->tag_count; i++) {
        tags[i].data = proof->tags[i];
        tags[i].len = NGX_HTTP_CACHE_PURGE_TAG_DIGEST_SIZE;
    }
    ngx_memzero(&match, sizeof(match));
    match.covered = proof->entity;
    match.validated = proof->validated;
    if (proof->required > proof->validated) {
        match.required = proof->required;
    }
    rc = ngx_http_cache_purge_control_match_digests(reader, &context, 64, 65536,
        ngx_http_cache_purge_read_match, &match, &publication, &error);
    if (rc != 0 || publication != stamp) {
        return NGX_AGAIN;
    }
    if (match.fenced) {
        return NGX_HTTP_CACHE_PURGE_ADMIT_DENY;
    }
    if (ngx_http_cache_purge_control_guard_begin(reader, publication, &error)
        != 0)
    {
        return NGX_AGAIN;
    }
    rc = ngx_http_cache_purge_control_guard_end(reader, &error);
    if (rc != 0) {
        return NGX_HTTP_CACHE_PURGE_ADMIT_ERROR;
    }
    ngx_http_cache_purge_incoming_cutoffs(ctx);
    incoming = ngx_http_cache_purge_incoming_mode(ctx, ctx->cache->date);
    if (incoming == NGX_HTTP_CACHE_PURGE_DELETE) {
        return ngx_http_cache_purge_retire_start(ctx) == NGX_OK
               ? NGX_HTTP_CACHE_PURGE_ADMIT_DENY
               : NGX_HTTP_CACHE_PURGE_ADMIT_ERROR;
    }
#if !(NGX_RESTY_EXT)
    if (ctx->read_upstream->cache_status == NGX_HTTP_CACHE_STALE
        || ctx->read_upstream->cache_status == NGX_HTTP_CACHE_UPDATING)
    {
        return NGX_HTTP_CACHE_PURGE_ADMIT_ALLOW;
    }
#endif
    return match.required || incoming ? NGX_HTTP_CACHE_PURGE_ADMIT_REVALIDATE :
                            NGX_HTTP_CACHE_PURGE_ADMIT_ALLOW;
}

static void
ngx_http_cache_purge_proof_read(void *data, ngx_log_t *log)
{
    ngx_http_cache_purge_proof_task_t  *task = data;
    ngx_http_cache_rocksdb_bytes_t  header, path;
    ngx_http_cache_rocksdb_error_t  error;
    uint64_t                        offset;
    u_char                          address[16];

    (void) log;
    header.data = (u_char *) &task->header;
    header.len = sizeof(task->header);
    task->result = ngx_http_cache_purge_file_proof_read_selected(task->fd,
        header, &task->proof, &error);
    if (task->result == 0) {
        path.data = task->path.data;
        path.len = task->path.len;
        task->result = ngx_http_cache_purge_file_address(path, address,
                                                        &offset, &error);
        if (task->result == 0
            && ngx_memcmp(address, task->proof.marker.address, 16) != 0)
        {
            task->result = NGX_HTTP_CACHE_ROCKSDB_CONFLICT;
        }
    }
    (void) close(task->fd);
    task->fd = -1;
}

static void
ngx_http_cache_purge_proof_done(ngx_event_t *event)
{
    ngx_http_cache_purge_proof_task_t  *task;
    ngx_http_cache_purge_capture_t     *ctx;
    ngx_int_t                           allowed;

    task = event->data;
    ctx = task->capture;
    if (ctx->read_event.timer_set) {
        ngx_del_timer(&ctx->read_event);
    }
    if (ctx->request->main->terminated) {
        ngx_http_cache_purge_read_complete(ctx,
                                           NGX_HTTP_CACHE_PURGE_ADMIT_DENY);
        return;
    }
    allowed = NGX_HTTP_CACHE_PURGE_ADMIT_ERROR;
    if ((ngx_msec_int_t) (ctx->deadline - ngx_current_msec) > 0) {
        if (task->result == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            ngx_add_timer(&ctx->read_event, 5);
            return;
        } else if (task->result == 0) {
            allowed = ngx_http_cache_purge_proof_admit(task);
        } else if (task->result == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
                   || task->result == NGX_HTTP_CACHE_ROCKSDB_CONFLICT)
        {
            allowed = NGX_HTTP_CACHE_PURGE_ADMIT_DENY;
        }
    }
    if (allowed == NGX_AGAIN) {
        if (++ctx->proof_retries < 4) {
            ngx_post_event(&ctx->read_event, &ngx_posted_events);
            return;
        }
        allowed = NGX_HTTP_CACHE_PURGE_ADMIT_ERROR;
    }
    ngx_http_cache_purge_read_complete(ctx, allowed);
}

static ngx_int_t
ngx_http_cache_purge_proof_start(ngx_http_cache_purge_capture_t *ctx)
{
    ngx_http_cache_purge_capture_main_t    *main;
    ngx_http_cache_purge_proof_task_t      *task;
    ngx_http_cache_purge_control_reader_t  *reader;
    ngx_http_cache_rocksdb_error_t          error;
    ngx_http_request_t                     *r;
    ngx_http_cache_t                       *cache;

    r = ctx->request;
    cache = ctx->cache;
    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_index_module);
    if (main->read_pool == NULL || cache->buf == NULL
        || cache->buf->last - cache->buf->start
           < (ptrdiff_t) sizeof(ngx_http_file_cache_header_t)
        || cache->file.name.len > 32768)
    {
        return NGX_ERROR;
    }
    if (ctx->read_task == NULL) {
        ctx->read_task = ngx_thread_task_alloc(r->pool, sizeof(*task)
                                                       + cache->file.name.len);
        if (ctx->read_task == NULL) {
            return NGX_ERROR;
        }
        task = ctx->read_task->ctx;
        task->capture = ctx;
        task->fd = -1;
        task->path.data = (u_char *) (task + 1);
        task->path.len = cache->file.name.len;
        ngx_memcpy(task->path.data, cache->file.name.data, task->path.len);
        ctx->read_task->handler = ngx_http_cache_purge_proof_read;
        ctx->read_task->event.data = task;
        ctx->read_task->event.handler = ngx_http_cache_purge_proof_done;
        ctx->read_task->event.log = r->connection->log;
    }
    task = ctx->read_task->ctx;
    if (ngx_http_cache_purge_runtime_reader(r, &reader, &error) != 0
        || ngx_http_cache_purge_control_watermark(reader, &task->watermark,
                                                 &task->publication, &error)
           != 0)
    {
        return NGX_AGAIN;
    }
    ngx_memcpy(&task->header, cache->buf->start, sizeof(task->header));
    task->fd = fcntl(cache->file.fd, F_DUPFD_CLOEXEC, 0);
    if (task->fd < 0) {
        return NGX_ERROR;
    }
    if (ngx_thread_task_post(main->read_pool, ctx->read_task) != NGX_OK) {
        close(task->fd);
        task->fd = -1;
        return NGX_ERROR;
    }
    /* Keep graceful shutdown in the event loop until the task completes.
     * blocked protects the pool; it does not keep the worker alive. */
    ngx_add_timer(&ctx->read_event, ctx->deadline - ngx_current_msec);
    return NGX_OK;
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
    if (v != NULL && !ctx->request->main->terminated
        && (ngx_msec_int_t) timeout > 10
        && reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK
        && (reply->result == NGX_HTTP_CACHE_ROCKSDB_BUSY
            || reply->result == NGX_HTTP_CACHE_ROCKSDB_LIMIT
            || reply->result == NGX_HTTP_CACHE_ROCKSDB_EXPIRED))
    {
        /* Keep the transaction and transferred lease while the executor
         * advances its bounded guards. Reuse the payload on every retry. */
        ngx_add_timer(&ctx->read_event, 10);
        return;
    }
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
    v->payload = payload;
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
    ctx->validation->payload = payload;
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
    if (allowed != NGX_HTTP_CACHE_PURGE_ADMIT_DENY
        && allowed != NGX_HTTP_CACHE_PURGE_ADMIT_ERROR
        && ctx->validation != NULL)
    {
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
    ngx_http_upstream_conf_t        *conf;
    ngx_connection_t                *connection;
    ngx_str_t                       payload, root, zone, path;
    ngx_msec_t  timeout;
    ngx_int_t   rc;
    u_char     *p;
    int         fds[2];

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
        if (ctx->incoming_error) {
            ngx_http_finalize_request(r, NGX_HTTP_BAD_REQUEST);
            ngx_http_run_posted_requests(connection);
            return;
        }
        if (ctx->local_failed) {
            ngx_http_finalize_request(r, NGX_HTTP_SERVICE_UNAVAILABLE);
            ngx_http_run_posted_requests(connection);
            return;
        }
        if (ctx->read_bypass) {
            /* Resume after the gate without entering the native file lookup.
             * Keep the original key and cacheable response-fill behavior. */
            conf = ngx_palloc(r->pool, sizeof(*conf));
            if (conf == NULL) {
                ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
                ngx_http_run_posted_requests(connection);
                return;
            }
            *conf = *ctx->read_upstream->conf;
            conf->cache = 0;
            ctx->read_upstream->conf = conf;
            ctx->read_upstream->cache_status = NGX_HTTP_CACHE_BYPASS;
        }
        ctx->read_resume(r);
        ngx_http_run_posted_requests(connection);
        return;
    }
    if (ctx->read_task != NULL && ctx->read_task->event.active) {
        /* A running file syscall cannot be cancelled or lose its pool. */
        ngx_add_timer(&ctx->read_event, 100);
        return;
    }
    timeout = ctx->deadline - ngx_current_msec;
    if (r->main->terminated || (ngx_msec_int_t) timeout <= 0
        || c->file.fd == NGX_INVALID_FILE
        || (ctx->read_upstream->cache_status == NGX_HTTP_CACHE_REVALIDATED
            && ctx->validation == NULL))
    {
        ngx_http_cache_purge_read_complete(ctx, 0);
        return;
    }
    if (ctx->validation == NULL) {
        rc = ngx_http_cache_purge_proof_start(ctx);
        if (rc == NGX_AGAIN && (ngx_msec_int_t) timeout > 10) {
            ngx_add_timer(&ctx->read_event, 1);
        } else if (rc != NGX_OK) {
            ngx_http_cache_purge_read_complete(ctx,
                                              NGX_HTTP_CACHE_PURGE_ADMIT_ERROR);
        }
        return;
    }
    if (ngx_http_cache_purge_runtime_client(r, &ctx->client) != NGX_OK) {
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
    if (ctx->validation != NULL && ctx->validation->payload.len != 0) {
        payload = ctx->validation->payload;
        if (ctx->rpc == NGX_HTTP_CACHE_PURGE_RPC_VALIDATE) {
            fds[0] = ctx->lease;
            fds[1] = c->file.fd;
            rc = ngx_http_cache_purge_client_validate_file(ctx->client,
                &payload, fds, timeout,
                ngx_http_cache_purge_read_validation_reply, ctx, &ctx->call);
        } else {
            rc = ngx_http_cache_purge_client_submit(ctx->client,
                NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION, &payload, timeout,
                ngx_http_cache_purge_read_validation_reply, ctx, &ctx->call);
        }
        if (rc == NGX_BUSY && (ngx_msec_int_t) timeout > 10) {
            ngx_add_timer(&ctx->read_event, 10);
        } else if (rc != NGX_OK) {
            ngx_http_cache_purge_read_complete(ctx, 0);
        }
        return;
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
    payload.len = 24 + root.len + zone.len + path.len
                  + ctx->key.len + ctx->url.len + ctx->host.len;
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
    p = ngx_http_cache_purge_capture_string(p, ctx->url);
    ngx_http_cache_purge_capture_string(p, ctx->host);
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
        ctx->read_state = NGX_HTTP_CACHE_PURGE_READ_DONE;
        return NGX_HTTP_SERVICE_UNAVAILABLE;
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
        ctx->zone, ctx->path, ngx_null_string, ctx->cache->vary
    };
    size_t      size = 16 + 9 * 4 + 8 + 1 + 2 + 6 * 4 + 16 + 4;
    ngx_uint_t  i;
    ngx_msec_t timeout = ngx_http_cache_purge_capture_remaining(ctx);
    ngx_int_t  rc;
    u_char    *p;
    int        fds[2] = { ctx->lease, ctx->temporary->file.fd };
    u_char     watermark[NGX_HTTP_CACHE_PURGE_VARIANT_SIZE];

    if (!timeout || !ctx->origin_valid) {
        return NGX_ERROR;
    }
    ngx_http_cache_purge_watermark_encode(&ctx->origin, watermark);
    metadata.data = watermark;
    metadata.len = NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE;
    if (ctx->cache->vary.len) {
        ngx_http_cache_purge_variant_encode(&ctx->origin, ctx->cache->main,
            ctx->cache->variant, watermark);
        metadata.len = sizeof(watermark);
    }
    size += metadata.len + ctx->misc.len;
    for (i = 0; i < 9; i++) {
        if (fields[i].len > 32768) {
            return NGX_ERROR;
        }
        size += fields[i].len;
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
    p = ngx_http_cache_purge_capture_string(p, ctx->misc);
    *p++ = (u_char) ctx->tag_count;
    for (i = 0; i < ctx->tag_count; i++) {
        p = ngx_http_cache_purge_capture_string(p, ctx->tags[i]);
    }
    ngx_memzero(p, 2 + 6 * 4);
    p += 2 + 6 * 4;
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

static void
ngx_http_cache_purge_capture_retry(ngx_event_t *event)
{
    ngx_http_cache_purge_capture_t  *ctx = event->data;
    ngx_msec_t                       timeout;
    ngx_int_t                        rc;

    timeout = ngx_http_cache_purge_capture_remaining(ctx);
    if (timeout == 0 || ctx->request->main->terminated) {
        goto failed;
    }
    if (ctx->rpc == NGX_HTTP_CACHE_PURGE_RPC_FINISH) {
        rc = ngx_http_cache_purge_client_finish_file(ctx->client,
            &ctx->publication_payload, ctx->lease, timeout,
            ngx_http_cache_purge_capture_reply, ctx, &ctx->call);
    } else {
        rc = ngx_http_cache_purge_client_submit(
            ctx->client, NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION,
            &ctx->publication_payload, timeout,
            ngx_http_cache_purge_capture_reply, ctx, &ctx->call);
    }
    if (rc == NGX_OK) {
        return;
    }
    if (rc == NGX_BUSY && timeout > 10) {
        ngx_add_timer(event, 10);
        return;
    }

failed:

    if (ctx->rpc == NGX_HTTP_CACHE_PURGE_RPC_FINISH) {
        ngx_http_cache_purge_capture_release_publication(ctx);
        ngx_http_cache_purge_capture_origin_release(ctx);
    }
    ngx_http_cache_purge_fill_complete(ctx->fill, NGX_DECLINED);
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
        ctx->publication_payload = payload;
        ctx->rpc = NGX_HTTP_CACHE_PURGE_RPC_FINISH;
        rc = ngx_http_cache_purge_client_finish_file(
            ctx->client, &payload, ctx->lease, timeout,
            ngx_http_cache_purge_capture_reply, ctx, &ctx->call);
        if (rc == NGX_OK) {
            return NGX_AGAIN;
        }
        if (rc == NGX_BUSY && timeout > 10 && !r->main->terminated) {
            ngx_add_timer(&ctx->publication_retry, 10);
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
    if (ngx_http_cache_purge_runtime_publish_client(r, &ctx->client) != 0
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
    ctx->publication_payload = payload;
    ctx->publication_retry.data = ctx;
    ctx->publication_retry.handler = ngx_http_cache_purge_capture_retry;
    ctx->publication_retry.log = r->connection->log;
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
    if ((stage == NGX_HTTP_CACHE_PURGE_RPC_ACQUIRE
         || stage == NGX_HTTP_CACHE_PURGE_RPC_FINISH)
        && reply->transport == NGX_HTTP_CACHE_PURGE_CLIENT_OK
        && reply->result == NGX_HTTP_CACHE_ROCKSDB_BUSY
        && !ctx->request->main->terminated
        && ngx_http_cache_purge_capture_remaining(ctx) > 10)
    {
        ngx_add_timer(&ctx->publication_retry, 10);
        return;
    }
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
    main->empty.value.data = (u_char *) "";
    return main;
}

static char *
ngx_http_cache_purge_identity_conf(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_str_t   *value;
    ngx_uint_t   field, i;

    value = cf->args->elts;
    field = (cmd->offset
             - offsetof(ngx_http_cache_purge_capture_conf_t, identity))
            / sizeof(void *);
    if (ngx_http_script_variables_count(&value[1]) == 0) {
        if (field == 0 && (value[1].len == 0
            || !((value[1].len > 7
                  && ngx_strncmp(value[1].data, "http://", 7) == 0)
                 || (value[1].len > 8
                     && ngx_strncmp(value[1].data, "https://", 8) == 0))))
        {
            return "requires a nonempty absolute HTTP or HTTPS URL";
        }
        if (field == 2) {
            if (value[1].len > 255) {
                return "requires a namespace of at most 255 bytes";
            }
            for (i = 0; i < value[1].len; i++) {
                if (value[1].data[i] <= 0x20 || value[1].data[i] >= 0x7f
                    || value[1].data[i] == ',')
                {
                    return "requires a printable ASCII namespace without "
                           "spaces or commas";
                }
            }
        }
    }
#if (NGX_EXPR)
    return ngx_http_set_expr_complex_value_slot(cf, cmd, conf);
#else
    return ngx_http_set_complex_value_slot(cf, cmd, conf);
#endif
}

static void *
ngx_http_cache_purge_capture_create_conf(ngx_conf_t *cf)
{
    ngx_http_cache_purge_capture_conf_t  *conf;
    ngx_uint_t                            i;

    conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf == NULL) {
        return NULL;
    }
    conf->tag = NGX_CONF_UNSET_PTR;
    for (i = 0; i < 3; i++) {
        conf->identity[i] = NGX_CONF_UNSET_PTR;
    }
#if !(NGX_EXPR)
    for (i = 0; i < 4; i++) {
        conf->protocol[i] = NGX_CONF_UNSET;
    }
#endif
    return conf;
}

static char *
ngx_http_cache_purge_capture_merge(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_cache_purge_capture_conf_t  *prev = parent;
    ngx_http_cache_purge_capture_conf_t  *conf = child;
    ngx_http_cache_purge_capture_main_t  *main;
    void                               ***location;
    ngx_uint_t                            i;

    main = ngx_http_conf_get_module_main_conf(cf,
                                             ngx_http_cache_purge_index_module);
    for (i = 0; i < 3; i++) {
#if (NGX_EXPR)
        if (ngx_conf_merge_expr_ptr_value(cf, &conf->identity[i],
                prev->identity[i], &main->empty) != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }
#else
        ngx_conf_merge_ptr_value(conf->identity[i], prev->identity[i],
                                 &main->empty);
#endif
    }
    for (i = 0; i < 4; i++) {
#if (NGX_EXPR)
        if (ngx_conf_merge_expr_flag_value(cf, &conf->protocol[i],
                prev->protocol[i], 0) != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }
#else
        ngx_conf_merge_value(conf->protocol[i], prev->protocol[i], 0);
#endif
    }

#if (NGX_EXPR)
    if (ngx_conf_merge_expr_ptr_value(cf, &conf->tag, prev->tag, NULL)
        != NGX_OK)
    {
        return NGX_CONF_ERROR;
    }
#else
    ngx_conf_merge_ptr_value(conf->tag, prev->tag, NULL);
#endif

    main = ngx_http_conf_get_module_main_conf(cf,
                                             ngx_http_cache_purge_index_module);
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
    static struct {
        ngx_http_script_len_code_pt code;
        uintptr_t                   end;
    } bind_lengths = { ngx_http_cache_purge_bind_length, 0 };
    static const char *names[] = { "proxy_cache_bypass", "fastcgi_cache_bypass",
                                   "scgi_cache_bypass", "uwsgi_cache_bypass" };
    ngx_http_cache_purge_capture_main_t *main =
        ngx_http_conf_get_module_main_conf(cf,
                                           ngx_http_cache_purge_index_module);
    ngx_http_cache_purge_capture_conf_t  *conf;
    ngx_http_cache_purge_capture_main_t  *old;
    ngx_module_t                         *module;
    ngx_command_t                        *cmd;
    ngx_array_t                         **slot, *array;
    ngx_http_complex_value_t             *value;
    ngx_http_upstream_conf_t             *upstream;
    ngx_http_cache_purge_capture_code_t  *code;
    ngx_http_cache_purge_bind_code_t     *binding;
    ngx_http_upstream_local_t           **local_slot, *local;
    void                               ***locations = main->locations.elts;
    ngx_uint_t                            i, j, k;
    ngx_flag_t                            resources;
    ngx_http_core_main_conf_t            *core;
    ngx_http_handler_pt                 *phase;
#if (NGX_EXPR)
    ngx_http_expr_complex_value_ctx_t    *identity;
    ngx_uint_t                            n;
#endif
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    ngx_http_proxy_filter_main_conf_t  *filters;
    ngx_http_proxy_filter_pt           *handler;
#endif
    resources = ngx_http_cache_purge_runtime_configured(cf);
    main->enabled = 0;
    for (k = 0; k < main->locations.nelts; k++) {
        conf = locations[k][ngx_http_cache_purge_index_module.ctx_index];
        for (j = 0; j < 4; j++) {
            if (ngx_http_cache_purge_protocol_possible(conf, j)) {
                break;
            }
        }
        if (j == 4) {
            continue;
        }
        if (!resources) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "cache purge protocol switches require cache_purge_store "
                "and cache_purge_state_zone");
            return NGX_ERROR;
        }
        main->enabled = 1;
        value = NULL;
#if (NGX_EXPR)
        identity = conf->identity[0]->elts;
        for (n = 0; n < conf->identity[0]->nelts; n++) {
            if (identity[n].value
                && (identity[n].value->lengths
                    || identity[n].value->value.len))
            {
                value = identity[n].value;
                break;
            }
        }
#else
        if (conf->identity[0]->lengths || conf->identity[0]->value.len) {
            value = conf->identity[0];
        }
#endif
        if (value == NULL) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "enabled cache purge location requires cache_purge_key_url");
            return NGX_ERROR;
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
                "enabling/disabling cache purge indexing requires a full "
                "restart to replace memory-only cache error state");
            return NGX_ERROR;
        }
    }
    if (!main->enabled) {
        return NGX_OK;
    }

    if (main->read_pool_name.len) {
        main->read_pool = ngx_thread_pool_add(cf, &main->read_pool_name);
        if (main->read_pool == NULL) {
            return NGX_ERROR;
        }
    }

    core = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    phase = ngx_array_push(&core->phases[NGX_HTTP_PRECONTENT_PHASE].handlers);
    if (phase == NULL) {
        return NGX_ERROR;
    }
    *phase = ngx_http_cache_purge_method;

    for (i = 0; cf->cycle->modules[i]; i++) {
        module = cf->cycle->modules[i];
        if (module->type != NGX_HTTP_MODULE || module->commands == NULL) {
            continue;
        }
        for (cmd = module->commands; cmd->name.len; cmd++) {
            if (ngx_http_cache_purge_capture_equal(cmd->name, "proxy_bind")) {
                if (cmd->set != ngx_http_upstream_bind_set_slot
                    || cmd->conf != NGX_HTTP_LOC_CONF_OFFSET)
                {
                    return NGX_ERROR;
                }
                for (k = 0; k < main->locations.nelts; k++) {
                    conf = locations[k][
                        ngx_http_cache_purge_index_module.ctx_index];
                    if (!ngx_http_cache_purge_protocol_possible(conf, 0)
                        || locations[k][module->ctx_index] == NULL)
                    {
                        continue;
                    }
                    local_slot = (ngx_http_upstream_local_t **)
                        ((u_char *) locations[k][module->ctx_index]
                         + cmd->offset);
                    if (*local_slot && (*local_slot)->value
                        && (*local_slot)->value->lengths
                           == (void *) &bind_lengths)
                    {
                        continue;
                    }
                    local = ngx_pcalloc(cf->pool, sizeof(*local));
                    binding = ngx_pcalloc(cf->pool, sizeof(*binding));
                    value = ngx_pcalloc(cf->pool, sizeof(*value));
                    if (local == NULL || binding == NULL || value == NULL) {
                        return NGX_ERROR;
                    }
                    if (*local_slot) {
                        *local = **local_slot;
                    }
                    binding->code = ngx_http_cache_purge_bind_value;
                    binding->original = *local_slot;
                    value->lengths = (void *) &bind_lengths;
                    value->values = (void *) binding;
                    local->value = value;
                    *local_slot = local;
                }
            }
            if (ngx_http_cache_purge_capture_equal(cmd->name,
                                                   "proxy_set_header")
                && cmd->set == ngx_conf_set_keyval_slot
                && cmd->conf == NGX_HTTP_LOC_CONF_OFFSET)
            {
                for (k = 0; k < main->locations.nelts; k++) {
                    conf = locations[k][
                        ngx_http_cache_purge_index_module.ctx_index];
                    conf->proxy_headers = *(ngx_array_t **)
                        ((u_char *) locations[k][module->ctx_index]
                         + cmd->offset);
                }
            }
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
                conf = locations[k][
                    ngx_http_cache_purge_index_module.ctx_index];
                if (!ngx_http_cache_purge_protocol_possible(conf, j)
                    || locations[k][module->ctx_index] == NULL)
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
                if (main->read_pool == NULL) {
                    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                        "managed cache locations require cache_purge_read_pool "
                        "and an explicitly configured nginx thread_pool");
                    return NGX_ERROR;
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
                code->protocol = j;
                code->predicates = *slot == NGX_CONF_UNSET_PTR ? NULL : *slot;
                value->lengths = (void *) &lengths;
                value->values = (void *) code;
                *slot = array;
            }
        }
    }
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    filters = ngx_http_conf_get_module_main_conf(cf,
                                                 ngx_http_proxy_filter_module);
    handler = ngx_array_push(&filters->phases[NGX_HTTP_PROXY_REQUEST_FILTER]);
    if (handler == NULL) {
        return NGX_ERROR;
    }
    *handler = ngx_http_cache_purge_propagation_filter;
#endif
    ngx_http_cache_purge_capture_next_header = ngx_http_top_header_filter;
    ngx_http_top_header_filter = ngx_http_cache_purge_capture_header;
    return NGX_OK;
}
