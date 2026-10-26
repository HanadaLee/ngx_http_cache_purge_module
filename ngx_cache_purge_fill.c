/* Copyright (C) Hanada */
#include "ngx_cache_purge_fill.h"

#if (NGX_HTTP_CACHE && NGX_RESTY_EXT && NGX_HTTP_CACHE_ADAPTER_VERSION >= 3)

extern ngx_module_t ngx_http_cache_purge_module;

struct ngx_http_cache_purge_fill_s {
    ngx_http_request_t                 *request;
    ngx_http_cache_t                   *cache;
    ngx_http_upstream_t                *upstream;
    ngx_temp_file_t                    *temp_file;
    ngx_http_cache_purge_fill_step_pt   step;
    void                              *data;
    ngx_http_upstream_handler_pt        read_handler;
    ngx_http_upstream_handler_pt        write_handler;
    void (*finalize)(ngx_http_request_t *r, ngx_int_t rc);
    ngx_uint_t                         phase;
    ngx_int_t                          result;
    ngx_int_t                          outcome;
    unsigned                           active:1;
    unsigned                           waiting:1;
    unsigned                           cancelled:1;
    unsigned                           native_update:1;
    unsigned                           paused:1;
    unsigned                           finalizing_ref:1;
};

static ngx_int_t ngx_http_cache_purge_fill_step(ngx_http_cache_purge_fill_t *f,
    ngx_int_t rc);
static void ngx_http_cache_purge_fill_finalize(ngx_http_request_t *r,
    ngx_int_t rc);

static void
ngx_http_cache_purge_fill_idle(ngx_http_request_t *r, ngx_http_upstream_t *u)
{
}

static void
ngx_http_cache_purge_fill_pause(ngx_http_cache_purge_fill_t *f)
{
    ngx_connection_t *c;

    if (f->paused) {
        return;
    }
    f->paused = 1;
    f->read_handler = f->upstream->read_event_handler;
    f->write_handler = f->upstream->write_event_handler;
    f->upstream->read_event_handler = ngx_http_cache_purge_fill_idle;
    f->upstream->write_event_handler = ngx_http_cache_purge_fill_idle;
    c = f->upstream->peer.connection;
    if (c == NULL) {
        return;
    }
    if (c->read->timer_set) {
        ngx_del_timer(c->read);
    }
    if (c->write->timer_set) {
        ngx_del_timer(c->write);
    }
    if (c->read->active && ngx_del_event(c->read, NGX_READ_EVENT, 0) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, f->request->connection->log, 0,
                      "could not suspend cache fill upstream read");
    }
    if (c->write->active && ngx_del_event(c->write, NGX_WRITE_EVENT, 0) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, f->request->connection->log, 0,
                      "could not suspend cache fill upstream write");
    }
}

ngx_int_t
ngx_http_cache_purge_fill_start(ngx_http_request_t *r, ngx_temp_file_t *tf,
    ngx_http_cache_purge_fill_t **slot, ngx_http_cache_purge_fill_step_pt step,
    void *data)
{
    ngx_http_cache_purge_fill_t *f;
    ngx_int_t rc;

    if (slot == NULL || step == NULL || tf == NULL || r->cache == NULL
        || r->upstream == NULL)
    {
        return NGX_ERROR;
    }
    if (*slot != NULL) {
        f = *slot;
        if (f->cache != r->cache || f->upstream != r->upstream) {
            return NGX_ERROR;
        }
        return f->active ? NGX_AGAIN : f->outcome;
    }
    if (r->cache->updated || r->cache->node == NULL) {
        return NGX_DECLINED;
    }
    if (r->aio) {
        /* Native AIO owns the continuation and its blocked reference. */
        return NGX_AGAIN;
    }
    if (r->main->blocked >= NGX_HTTP_MAX_BLOCKED
        || ngx_http_get_module_ctx(r, ngx_http_cache_purge_module) != NULL)
    {
        return NGX_ERROR;
    }
    f = ngx_pcalloc(r->pool, sizeof(*f));
    if (f == NULL) {
        return NGX_ERROR;
    }
    *slot = f;
    f->request = r;
    f->cache = r->cache;
    f->upstream = r->upstream;
    f->temp_file = tf;
    f->step = step;
    f->data = data;
    f->phase = NGX_CACHE_PURGE_FILL_PREPARE;
    f->active = 1;
    f->finalize = r->upstream->finalize_request;
    ngx_http_set_ctx(r, f, ngx_http_cache_purge_module);
    r->upstream->finalize_request = ngx_http_cache_purge_fill_finalize;

    /* Preserve the original node reference across native cancellation/free.
     * The module restores updated before invoking update/free itself. */
    f->cache->updated = 1;
    r->main->blocked++;
    r->aio = 1;
    rc = f->step(r, f->cache, tf, f->phase, NGX_OK, data);
    rc = ngx_http_cache_purge_fill_step(f, rc);
    if (rc == NGX_AGAIN) {
        ngx_http_cache_purge_fill_pause(f);
    }
    return rc;
}

static void
ngx_http_cache_purge_fill_finalize(ngx_http_request_t *r, ngx_int_t rc)
{
    ngx_http_cache_purge_fill_t *f;

    f = ngx_http_get_module_ctx(r, ngx_http_cache_purge_module);
    if (f == NULL || f->upstream != r->upstream) {
        ngx_log_error(NGX_LOG_ALERT, r->connection->log, 0,
                      "cache fill lost upstream context");
        return;
    }
    if (f->active) {
        f->cancelled = 1;
        /* blocked alone does not prevent normal HTTP/1 keepalive cleanup. */
        r->main->count++;
        f->finalizing_ref = 1;
    }
    f->upstream->finalize_request = f->finalize;
    f->finalize(r, rc);
}

static ngx_int_t
ngx_http_cache_purge_fill_step(ngx_http_cache_purge_fill_t *f, ngx_int_t rc)
{
    ngx_http_request_t *r = f->request;
    ngx_http_cache_t *c = f->cache;
    ngx_http_file_cache_t *cache = c->file_cache;

    if (rc == NGX_AGAIN) {
        f->waiting = 1;
        return NGX_AGAIN;
    }
    if (rc != NGX_OK && rc != NGX_DECLINED && rc != NGX_ABORT
        && rc < NGX_HTTP_BAD_REQUEST)
    {
        rc = NGX_ERROR;
    }
    if (f->phase == NGX_CACHE_PURGE_FILL_PREPARE) {
        if (f->cancelled || r->main->terminated || r->cache != c) {
            rc = NGX_ABORT;
        }
        if (rc == NGX_OK) {
            /* Native update releases its reference. Retain one for FINISH. */
            ngx_shmtx_lock(&cache->shpool->mutex);
            c->node->count++;
            ngx_shmtx_unlock(&cache->shpool->mutex);
            f->native_update = 1;
            c->updated = 0;
            rc = ngx_http_file_cache_update(r, f->temp_file);
        }
        f->result = rc;
        f->phase = NGX_CACHE_PURGE_FILL_FINISH;
        rc = f->step(r, c, f->temp_file, f->phase, rc, f->data);
        if (rc == NGX_AGAIN) {
            f->waiting = 1;
            return NGX_AGAIN;
        }
    }
    f->outcome = rc == NGX_OK ? f->result : NGX_ERROR;
    f->active = 0;
    f->upstream->finalize_request = f->finalize;
    if (f->native_update) {
        ngx_shmtx_lock(&cache->shpool->mutex);
        c->node->count--;
        ngx_shmtx_unlock(&cache->shpool->mutex);
    } else {
        c->updated = 0;
        ngx_http_file_cache_free(c, f->temp_file);
    }
    r->main->blocked--;
    r->aio = 0;
    return f->outcome;
}

void
ngx_http_cache_purge_fill_complete(ngx_http_cache_purge_fill_t *f, ngx_int_t rc)
{
    ngx_http_request_t *r = f->request;
    ngx_connection_t *connection = r->connection;
    ngx_http_upstream_t *u = f->upstream;

    if (!f->active || !f->waiting) {
        ngx_log_error(NGX_LOG_ALERT, connection->log, 0,
                      "unexpected cache fill completion");
        return;
    }
    f->waiting = 0;
    if (ngx_http_cache_purge_fill_step(f, rc == NGX_AGAIN ? NGX_ERROR : rc)
        == NGX_AGAIN)
    {
        return;
    }
    if (f->finalizing_ref) {
        f->finalizing_ref = 0;
        if (!r->main->terminated) {
            if (r->main->blocked) {
                ngx_http_close_request(r, 0);
            } else {
                ngx_http_finalize_request(r->main, NGX_DONE);
            }
            return;
        }
        r->main->count--;
    }
    if (r->done || r->main->terminated) {
        connection->write->handler(connection->write);
        return;
    }
    if (r->cache != f->cache || r->upstream != u || u->cleanup == NULL) {
        r->write_event_handler(r);
        ngx_http_run_posted_requests(connection);
        return;
    }
    u->read_event_handler = f->read_handler;
    u->write_event_handler = f->write_handler;
    u->pipe->aio = 0;
    /* Resume through the native handler installed before suspension. */
    u->read_event_handler(r, u);
    ngx_http_run_posted_requests(connection);
}

#endif
