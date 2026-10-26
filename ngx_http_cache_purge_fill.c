/* Copyright (C) Hanada */
#include "ngx_http_cache_purge_fill.h"

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
                                               ngx_int_t           rc);
static void ngx_http_cache_purge_fill_begin(ngx_event_t *event);
static void ngx_http_cache_purge_fill_step(ngx_http_cache_purge_fill_t *f,
                                           ngx_int_t                    rc);

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
ngx_http_cache_purge_fill_arm(ngx_http_request_t               *r,
                              ngx_http_cache_purge_fill_t     **slot,
                              ngx_http_cache_purge_fill_step_pt step,
                              void                             *data)
{
    ngx_http_cache_purge_fill_t *f;
    ngx_http_upstream_t         *u = r->upstream;
    ngx_pool_cleanup_t          *cleanup;

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
    ngx_pool_cleanup_t          *cleanup;
    ngx_http_cache_purge_fill_t *f = NULL;
    ngx_event_pipe_t            *p = r->upstream->pipe;

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
    ngx_http_cache_purge_fill_t *f = event->data;
    ngx_http_request_t          *r = f->request;

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
    ngx_http_request_t    *r = f->request;
    ngx_http_cache_t      *c = f->cache;
    ngx_http_file_cache_t *cache = c->file_cache;
    ngx_connection_t      *connection = r->connection;

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
