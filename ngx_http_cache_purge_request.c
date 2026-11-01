
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


#include "ngx_http_cache_purge.h"
#include <ngx_md5.h>
#include <sys/random.h>

#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
#include <ngx_http_proxy_filter_module.h>
#endif


typedef struct {
    ngx_http_request_t              *request;
    ngx_http_cache_purge_message_t  *message;
    ngx_http_cache_purge_job_t       job;
    ngx_http_cache_purge_entry_t    *identity;
    ngx_event_t                      event;
    ngx_http_post_subrequest_t       post;
    ngx_http_post_subrequest_t      *previous;
    ngx_msec_t                       deadline;
    ngx_str_t                        operation_id;
    ngx_str_t                        method;
    uint64_t                         incoming;
    ngx_uint_t                       incoming_action;
    ngx_uint_t                       parsed;
    ngx_uint_t                       status;
    ngx_uint_t                       retry;
    ngx_uint_t                       header_only;
    ngx_uint_t                       resume;
    ngx_uint_t                       recorded;
    ngx_http_cache_purge_match_t     known;
    ngx_output_chain_filter_pt       send;
    void                            *send_data;
    ngx_chain_t                     *body;
    ngx_chain_t                     *pending;
    ngx_buf_t                       *header;
    ngx_uint_t                       lookup;
    ngx_uint_t                       lookup_error;
} ngx_http_cache_purge_ctx_t;


static ngx_uint_t ngx_http_cache_purge_equal(ngx_str_t a, const char *b);
static ngx_int_t ngx_http_cache_purge_get_protocol(ngx_http_request_t *r);
static void ngx_http_cache_purge_request_cleanup(void *data);
static ngx_http_cache_purge_ctx_t *ngx_http_cache_purge_get_ctx(
    ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_get_header(ngx_http_request_t *r,
    ngx_uint_t role, ngx_str_t *value);
static ngx_int_t ngx_http_cache_purge_parse_number(ngx_str_t value,
    uint64_t *number);
static ngx_int_t ngx_http_cache_purge_parse_propagation(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_parse_host(ngx_http_request_t *r,
    ngx_str_t value, ngx_http_cache_rocksdb_bytes_t *host);
static int ngx_http_cache_purge_tag_compare(const void *a, const void *b);
static ngx_int_t ngx_http_cache_purge_parse_tags(ngx_str_t value,
    ngx_http_cache_purge_entry_t *entry);
static ngx_int_t ngx_http_cache_purge_get_identity(ngx_http_request_t *r,
    ngx_http_cache_purge_entry_t *entry, ngx_uint_t tags, ngx_str_t *url);
static ngx_int_t ngx_http_cache_purge_match(ngx_http_request_t *r,
    ngx_http_cache_purge_entry_t *entry, ngx_http_cache_purge_match_t *match,
    ngx_uint_t tags);
static ngx_int_t ngx_http_cache_purge_send_response(ngx_http_request_t *r,
    ngx_uint_t status, ngx_str_t body);
static void ngx_http_cache_purge_id(ngx_str_t id, u_char output[16]);
static ngx_int_t ngx_http_cache_purge_id_valid(ngx_str_t id);
static ngx_int_t ngx_http_cache_purge_submit(ngx_http_request_t *r,
    ngx_http_cache_purge_ctx_t *ctx, ngx_uint_t type);
static ngx_uint_t ngx_http_cache_purge_flag_possible(
    ngx_http_cache_purge_loc_conf_t *conf, ngx_uint_t n);


static ngx_http_output_header_filter_pt  ngx_http_next_header_filter;
static ngx_int_t ngx_http_cache_purge_guard_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_http_cache_purge_propagation_variable(
    ngx_http_request_t *r, ngx_http_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_http_cache_purge_header_filter(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_log_handler(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_handler(ngx_http_request_t *r);
static void ngx_http_cache_purge_resume_handler(ngx_event_t *event);
static void ngx_http_cache_purge_poll_handler(ngx_event_t *event);
static void ngx_http_cache_purge_finalize_request(ngx_http_request_t *r,
    ngx_int_t rc);
static ngx_int_t ngx_http_cache_purge_create_request(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_purge_send_filter(void *data, ngx_chain_t *in);
static void ngx_http_cache_purge_lookup_handler(ngx_event_t *event);
static ngx_int_t ngx_http_cache_purge_subrequest_done(ngx_http_request_t *r,
    void *data, ngx_int_t rc);
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
static ngx_int_t ngx_http_cache_purge_proxy_request_filter(
    ngx_http_request_t *r, ngx_http_proxy_filter_ctx_t *filter);
#endif


static ngx_uint_t
ngx_http_cache_purge_equal(ngx_str_t a, const char *b)
{
    return a.len == ngx_strlen(b)
           && ngx_strncasecmp(a.data, (u_char *) b, a.len) == 0;
}


static ngx_int_t
ngx_http_cache_purge_get_protocol(ngx_http_request_t *r)
{
    ngx_uint_t                         i;
    ngx_http_cache_purge_main_conf_t  *main;

    if (r->upstream == NULL) {
        return NGX_DECLINED;
    }

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);

    if (main->path == NULL) {
        return NGX_DECLINED;
    }

    for (i = 0; i < 4; i++) {
        if (main->protocol[i] != NULL
            && r->upstream->output.tag == main->protocol[i]
            && ngx_http_cache_purge_get_flag(r, i) == 1)
        {
            return (ngx_int_t) i;
        }
    }

    return NGX_DECLINED;
}


static void
ngx_http_cache_purge_request_cleanup(void *data)
{
    ngx_http_cache_purge_ctx_t        *ctx = data;
    ngx_http_cache_purge_main_conf_t  *main;

    if (ctx->event.timer_set) {
        ngx_del_timer(&ctx->event);
    }

    if (ctx->event.posted) {
        ngx_delete_posted_event(&ctx->event);
    }

    main = ngx_http_get_module_main_conf(ctx->request,
                                         ngx_http_cache_purge_module);

    if (ctx->message != NULL) {
        ngx_http_cache_purge_state_detach(&main->state, &ctx->message);
    }
}


static ngx_http_cache_purge_ctx_t *
ngx_http_cache_purge_get_ctx(ngx_http_request_t *r)
{
    ngx_pool_cleanup_t          *cleanup;
    ngx_http_cache_purge_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_module);

    if (ctx != NULL) {
        return ctx;
    }

    cleanup = ngx_pool_cleanup_add(r->pool, sizeof(*ctx));

    if (cleanup == NULL) {
        return NULL;
    }

    ctx = cleanup->data;
    ngx_memzero(ctx, sizeof(*ctx));
    ctx->request = r;
    ctx->event.data = ctx;
    ctx->event.log = r->connection->log;
    cleanup->handler = ngx_http_cache_purge_request_cleanup;
    ngx_http_set_ctx(r, ctx, ngx_http_cache_purge_module);

    return ctx;
}


static ngx_int_t
ngx_http_cache_purge_get_header(ngx_http_request_t *r, ngx_uint_t role,
    ngx_str_t *value)
{
    ngx_str_t         name;
    ngx_uint_t        i, found;
    ngx_list_part_t  *part;
    ngx_table_elt_t  *h;

    ngx_str_null(value);

    if (ngx_http_cache_purge_get_value(r, role, &name) != NGX_OK) {
        return NGX_ERROR;
    }

    found = 0;

    for (part = &r->headers_in.headers.part; part; part = part->next) {
        h = part->elts;

        for (i = 0; i < part->nelts; i++) {
            if (h[i].hash != 0 && h[i].key.len == name.len
                && ngx_strncasecmp(h[i].key.data, name.data, name.len) == 0)
            {
                if (found++) {
                    return NGX_ERROR;
                }

                *value = h[i].value;
            }
        }
    }

    return found ? NGX_OK : NGX_DECLINED;
}


static ngx_int_t
ngx_http_cache_purge_parse_number(ngx_str_t value, uint64_t *number)
{
    size_t    i;
    uint64_t  n;

    n = 0;

    if (value.len == 0 || value.len > 20) {
        return NGX_ERROR;
    }

    for (i = 0; i < value.len; i++) {
        if (value.data[i] < '0' || value.data[i] > '9'
            || n > (UINT64_MAX - (value.data[i] - '0')) / 10)
        {
            return NGX_ERROR;
        }

        n = n * 10 + value.data[i] - '0';
    }

    *number = n;

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_parse_propagation(ngx_http_request_t *r)
{
    uint64_t                     time;
    ngx_int_t                    a, b;
    ngx_str_t                    stamp, action;
    ngx_http_cache_purge_ctx_t  *ctx;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_module);

    if (ctx != NULL && ctx->parsed) {
        return NGX_OK;
    }

    if (ngx_http_cache_purge_get_flag(r, 5) == 1) {
        return NGX_OK;
    }

    a = ngx_http_cache_purge_get_header(r, NGX_HTTP_CACHE_PURGE_TIME_HEADER,
                                        &stamp);
    b = ngx_http_cache_purge_get_header(
        r, NGX_HTTP_CACHE_PURGE_PROPAGATION_ACTION_HEADER, &action);

    if (a == NGX_DECLINED && b == NGX_DECLINED) {
        return NGX_OK;
    }

    if (a != NGX_OK || b != NGX_OK
        || ngx_http_cache_purge_parse_number(stamp, &time) != NGX_OK
        || time == 0
        || (!ngx_http_cache_purge_equal(action, "delete")
            && !ngx_http_cache_purge_equal(action, "invalidate")))
    {
        return NGX_ERROR;
    }

    ctx = ngx_http_cache_purge_get_ctx(r);

    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->parsed = 1;
    ctx->incoming = time / 1000;
    ctx->incoming_action = ngx_http_cache_purge_equal(action, "delete") ?
                               NGX_HTTP_CACHE_PURGE_DELETE :
                               NGX_HTTP_CACHE_PURGE_INVALIDATE;

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_parse_host(ngx_http_request_t *r, ngx_str_t value,
    ngx_http_cache_rocksdb_bytes_t *host)
{
    size_t     offset;
    u_char    *p;
    ngx_url_t  url;

    if (value.len > 7
        && ngx_strncasecmp(value.data, (u_char *) "http://", 7) == 0)
    {
        offset = 7;
    } else if (value.len > 8
               && ngx_strncasecmp(value.data, (u_char *) "https://", 8) == 0)
    {
        offset = 8;
    } else {
        return NGX_ERROR;
    }

    ngx_memzero(&url, sizeof(url));
    url.url.data = value.data + offset;
    url.url.len = value.len - offset;
    url.no_resolve = 1;
    url.uri_part = 1;

    if (ngx_parse_url(r->pool, &url) != NGX_OK || url.host.len == 0) {
        return NGX_ERROR;
    }

    p = ngx_pnalloc(r->pool, url.host.len);

    if (p == NULL) {
        return NGX_ERROR;
    }

    ngx_strlow(p, url.host.data, url.host.len);
    host->data = p;
    host->len = url.host.len;

    return NGX_OK;
}


static int
ngx_http_cache_purge_tag_compare(const void *a, const void *b)
{
    const ngx_http_cache_rocksdb_bytes_t  *left = a;
    const ngx_http_cache_rocksdb_bytes_t  *right = b;

    return ngx_memn2cmp((u_char *) left->data, (u_char *) right->data,
                        left->len, right->len);
}


static ngx_int_t
ngx_http_cache_purge_parse_tags(ngx_str_t value,
    ngx_http_cache_purge_entry_t *entry)
{
    size_t  start, end, i, j;

    entry->tag_count = 0;

    for (i = 0; i < value.len; i = end + 1) {
        start = i;

        for (end = i; end < value.len && value.data[end] != ','; end++) {
            if (value.data[end] == 0 || value.data[end] == '\r'
                || value.data[end] == '\n')
            {
                return NGX_ERROR;
            }
        }

        while (start < end
               && (value.data[start] == ' ' || value.data[start] == '\t'))
        {
            start++;
        }

        j = end;

        while (j > start
               && (value.data[j - 1] == ' ' || value.data[j - 1] == '\t'))
        {
            j--;
        }

        if (j == start) {
            continue;
        }

        if (j - start > 1024
            || entry->tag_count == NGX_HTTP_CACHE_PURGE_TAG_MAX)
        {
            return NGX_ERROR;
        }

        entry->tags[entry->tag_count].data = value.data + start;
        entry->tags[entry->tag_count++].len = j - start;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_get_identity(ngx_http_request_t *r,
    ngx_http_cache_purge_entry_t *entry, ngx_uint_t tags, ngx_str_t *url)
{
    ngx_str_t                        value;
    ngx_uint_t                       i;
    ngx_http_cache_rocksdb_bytes_t  *fields[3];

    ngx_memzero(entry, sizeof(*entry));
    fields[0] = &entry->url;
    fields[1] = &entry->misc;
    fields[2] = &entry->namespace_value;

    if (url != NULL) {
        entry->url.data = url->data;
        entry->url.len = url->len;
    }

    for (i = url != NULL ? 1 : 0; i < 3; i++) {
        if (ngx_http_cache_purge_get_value(r, i, &value) != NGX_OK
            || value.len > 32768 || (i == 0 && value.len == 0))
        {
            return NGX_ERROR;
        }

        fields[i]->data = value.data;
        fields[i]->len = value.len;
    }

    value.data = (u_char *) entry->url.data;
    value.len = entry->url.len;

    if (ngx_http_cache_purge_parse_host(r, value, &entry->host) != NGX_OK) {
        return NGX_ERROR;
    }

    if (tags) {
        if (ngx_http_cache_purge_get_value(r, NGX_HTTP_CACHE_PURGE_TAG, &value)
            != NGX_OK
            || ngx_http_cache_purge_parse_tags(value, entry) != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_match(ngx_http_request_t *r,
    ngx_http_cache_purge_entry_t *entry, ngx_http_cache_purge_match_t *match,
    ngx_uint_t tags)
{
    uint64_t                          *local, *propagate;
    ngx_int_t                          rc;
    ngx_str_t                          url, *resolved;
    ngx_http_cache_purge_ctx_t        *ctx;
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    ngx_memzero(entry, sizeof(*entry));
    ngx_memzero(match, sizeof(*match));

    if (ngx_http_cache_purge_state_active(&main->state)) {
        rc = NGX_AGAIN;
        resolved = NULL;

        if (tags && !ngx_http_cache_purge_state_complex(&main->state)) {
            if (ngx_http_cache_purge_get_value(r, NGX_HTTP_CACHE_PURGE_URL,
                                               &url)
                != NGX_OK
                || url.len == 0 || url.len > 32768)
            {
                return NGX_ERROR;
            }

            entry->url.data = url.data;
            entry->url.len = url.len;
            resolved = &url;
            rc = ngx_http_cache_purge_state_match_url(
                &main->state, entry, (uint64_t) ngx_time(), match);
        }

        if (rc == NGX_AGAIN) {
            if (ngx_http_cache_purge_get_identity(r, entry, tags, resolved)
                != NGX_OK)
            {
                return NGX_ERROR;
            }

            if (tags) {
                rc = ngx_http_cache_purge_state_match_local(
                    &main->state, entry, (uint64_t) ngx_time(), match);
            } else {
                rc = ngx_http_cache_purge_state_match(
                    &main->state, entry, (uint64_t) ngx_time(), match);
            }
        }

        if (rc == NGX_ERROR) {
            return rc;
        }
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_module);

    if (ctx != NULL && ctx->incoming != 0) {
        local = ctx->incoming_action == NGX_HTTP_CACHE_PURGE_DELETE ?
                    &match->delete_before :
                    &match->invalidate_before;
        propagate = ctx->incoming_action == NGX_HTTP_CACHE_PURGE_DELETE ?
                        &match->propagate_delete :
                        &match->propagate_invalidate;
        *local = ngx_max(*local, ctx->incoming);
        *propagate = ngx_max(*propagate, ctx->incoming);
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_guard_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
    ngx_http_cache_purge_main_conf_t  *main;
#if (NGX_RESTY_EXT)
    ngx_int_t          purged;
    ngx_http_cache_t  *cache;

#endif

    *v = ngx_http_variable_null_value;

    if (r->upstream == NULL || ngx_http_cache_purge_get_flag(r, data) != 1) {
        return NGX_OK;
    }

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);

    if (r->upstream->create_request != ngx_http_cache_purge_create_request) {
        if (main->create[data] != NULL
            && main->create[data] != r->upstream->create_request)
        {
            return NGX_ERROR;
        }

        main->create[data] = r->upstream->create_request;
        r->upstream->create_request = ngx_http_cache_purge_create_request;
    }

    if (r->upstream->finalize_request != ngx_http_cache_purge_finalize_request)
    {
        if (main->finalize[data] != NULL
            && main->finalize[data] != r->upstream->finalize_request)
        {
            return NGX_ERROR;
        }

        main->finalize[data] = r->upstream->finalize_request;
        r->upstream->finalize_request = ngx_http_cache_purge_finalize_request;
    }
#if (NGX_RESTY_EXT)
    cache = r->cache;

    if (cache != NULL && cache->file_cache != NULL) {
        purged = ngx_http_cache_purge_native_node(
            cache->file_cache, cache->key, 0, NGX_HTTP_CACHE_PURGE_NODE_READ);
        cache->purged |= purged == 1;
    }
#endif
    if (data == 0 && ngx_http_cache_purge_parse_propagation(r) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


static void
ngx_http_cache_purge_finalize_request(ngx_http_request_t *r, ngx_int_t rc)
{
    ngx_uint_t                         i;
    ngx_http_cache_purge_main_conf_t  *main;

    if (r->cache != NULL
        && (r->cache->error != 0 || rc == NGX_HTTP_BAD_GATEWAY
            || rc == NGX_HTTP_GATEWAY_TIME_OUT))
    {
        r->upstream->cacheable = 0;
        r->cache->error = 0;
        r->cache->valid_sec = 0;

        if (r->cache->file_cache != NULL) {
            (void) ngx_http_cache_purge_native_node(
                r->cache->file_cache, r->cache->key, 0,
                NGX_HTTP_CACHE_PURGE_NODE_CLEAR_ERROR);
        }
    }

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);

    for (i = 0; i < 4; i++) {
        if (r->upstream->output.tag == main->protocol[i]
            && main->finalize[i] != NULL)
        {
            main->finalize[i](r, rc);

            return;
        }
    }
}


static ngx_int_t
ngx_http_cache_purge_create_request(ngx_http_request_t *r)
{
    ngx_int_t                          protocol;
    ngx_int_t                          rc;
    ngx_http_cache_t                  *cache;
    ngx_http_cache_purge_ctx_t        *ctx;
    ngx_http_cache_purge_entry_t       entry;
    ngx_http_cache_purge_match_t       match;
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);

    for (protocol = 0; protocol < 4; protocol++) {
        if (r->upstream->output.tag == main->protocol[protocol]) {
            break;
        }
    }

    if (protocol == 4 || main->create[protocol] == NULL) {
        return NGX_ERROR;
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_module);

    if (r != r->main && r->cache != NULL
        && (r->post_subrequest == NULL
            || r->post_subrequest->handler
               != ngx_http_cache_purge_subrequest_done))
    {
        ctx = ngx_http_cache_purge_get_ctx(r);

        if (ctx == NULL) {
            return NGX_ERROR;
        }

        ctx->previous = r->post_subrequest;
        ctx->post.handler = ngx_http_cache_purge_subrequest_done;
        ctx->post.data = ctx;
        r->post_subrequest = &ctx->post;
    }

    if (ctx != NULL && ctx->retry) {
        r->upstream->method = ctx->method;
    }

    cache = r->cache;
    ngx_memzero(&entry, sizeof(entry));
#if (NGX_RESTY_EXT)
    if (cache != NULL && cache->secondary && cache->file_cache != NULL) {
        cache->purged =
            ngx_http_cache_purge_native_node(cache->file_cache, cache->key, 0,
                                             NGX_HTTP_CACHE_PURGE_NODE_READ)
            == 1;
    }
#endif
    if (cache != NULL
        && (ngx_http_cache_purge_state_active(&main->state)
            || (ctx != NULL && ctx->incoming)))
    {
        if (ngx_http_cache_purge_match(r, &entry, &match, 0) != NGX_OK) {
            return NGX_ERROR;
        }

        if ((uint64_t) cache->date < match.delete_before) {
            r->upstream->cache_status = NGX_HTTP_CACHE_MISS;
            cache->valid_sec = 0;
        }
#if (NGX_RESTY_EXT)
        if ((uint64_t) cache->date < match.delete_before
            || (uint64_t) cache->date < match.invalidate_before)
        {
            cache->purged = 1;
        }
#endif
    }

    if (protocol == 0 && cache != NULL
        && ngx_http_cache_purge_state_active(&main->state)
        && ngx_http_cache_purge_get_flag(r, 4) == 1
        && (ctx == NULL || (ctx->identity == NULL && ctx->lookup == 0))
        && ngx_http_cache_purge_state_tagged(&main->state, &entry,
                                             (uint64_t) ngx_time()))
    {
        ctx = ngx_http_cache_purge_get_ctx(r);

        if (ctx == NULL
            || ngx_http_cache_purge_state_enqueue(&main->state,
                                                  NGX_HTTP_CACHE_PURGE_LOOKUP,
                                                  &entry, &ctx->message)
               != NGX_OK)
        {
            return NGX_ERROR;
        }

        ctx->lookup = 1;
        ctx->body = r->upstream->request_bufs;
        ctx->deadline = ngx_current_msec + 10000;
        ctx->event.handler = ngx_http_cache_purge_lookup_handler;
        ngx_add_timer(&ctx->event, 1);
    }

    rc = main->create[protocol](r);

    if (rc == NGX_OK && ctx != NULL && ctx->lookup == 1) {
        ctx->header = r->upstream->request_bufs->buf;
        ctx->send = r->upstream->output.output_filter;
        ctx->send_data = r->upstream->output.filter_ctx;

        if (ctx->send == NULL) {
            ctx->send = ngx_chain_writer;
            ctx->send_data = &r->upstream->writer;
        }

        r->upstream->output.output_filter = ngx_http_cache_purge_send_filter;
        r->upstream->output.filter_ctx = ctx;
    }

    return rc;
}


static void
ngx_http_cache_purge_lookup_handler(ngx_event_t *event)
{
    ngx_http_cache_purge_ctx_t        *ctx = event->data;
    int                                result;
    ngx_int_t                          rc;
    ngx_connection_t                  *connection;
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_get_module_main_conf(ctx->request,
                                         ngx_http_cache_purge_module);
    rc = ngx_http_cache_purge_state_poll(&main->state, &ctx->message, &result,
                                         &ctx->known);

    if (rc == NGX_AGAIN
        && (ngx_msec_int_t) (ngx_current_msec - ctx->deadline) < 0)
    {
        ngx_add_timer(event, 1);

        return;
    }

    ctx->lookup = 2;
    ctx->lookup_error = rc != NGX_OK || result != 0;
    connection = ctx->request->upstream->peer.connection;

    if (connection != NULL) {
        ngx_post_event(connection->write, &ngx_posted_events);
    }
}


/* Only a MISS with scoped TAG rules waits here, before sending any bytes. */
static ngx_int_t
ngx_http_cache_purge_send_filter(void *data, ngx_chain_t *in)
{
    ngx_http_cache_purge_ctx_t        *ctx = data;
    ngx_http_request_t                *r;
    ngx_http_upstream_t               *u;
    ngx_output_chain_filter_pt         send;
    ngx_http_cache_purge_main_conf_t  *main;

    r = ctx->request;
    u = r->upstream;

    if (ngx_chain_add_copy(r->pool, &ctx->pending, in) != NGX_OK) {
        return NGX_ERROR;
    }

    if (ctx->lookup == 1) {
        return NGX_AGAIN;
    }

    if (ctx->lookup_error) {
        return NGX_ERROR;
    }

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    u->output.output_filter = ctx->send;
    u->output.filter_ctx = ctx->send_data;
    u->request_bufs = ctx->body;

    if (r->cache != NULL) {
        if ((uint64_t) r->cache->date < ctx->known.delete_before) {
            u->cache_status = NGX_HTTP_CACHE_MISS;
        }
#if (NGX_RESTY_EXT)
        if ((uint64_t) r->cache->date < ctx->known.delete_before
            || (uint64_t) r->cache->date < ctx->known.invalidate_before)
        {
            r->cache->purged = 1;
        }
#endif
    }

    if (main->create[0](r) != NGX_OK) {
        return NGX_ERROR;
    }

    /* The original header buffer is still owned by the unsent output chain. */
    *ctx->header = *u->request_bufs->buf;
    send = u->output.output_filter;

    return send(u->output.filter_ctx, ctx->pending);
}


static void
ngx_http_cache_purge_resume_handler(ngx_event_t *event)
{
    ngx_connection_t            *connection;
    ngx_http_request_t          *r;
    ngx_http_upstream_t         *u;
    ngx_http_cache_purge_ctx_t  *ctx;

    ctx = event->data;
    r = ctx->request;
    u = r->upstream;
    connection = r->connection;
    ctx->resume = 0;
    r->header_only = ctx->header_only;

    if (u->reinit_request(r) != NGX_OK) {
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);

        return;
    }

    ngx_memzero(&u->headers_in, sizeof(u->headers_in));
    u->headers_in.content_length_n = -1;
    u->headers_in.last_modified_time = -1;
    ngx_memzero(&r->headers_out, sizeof(r->headers_out));
    r->headers_out.content_length_n = -1;
    r->headers_out.last_modified_time = -1;

    if (ngx_list_init(&r->headers_out.headers, r->pool, 20,
                      sizeof(ngx_table_elt_t))
        != NGX_OK
        || ngx_list_init(&r->headers_out.trailers, r->pool, 4,
                         sizeof(ngx_table_elt_t))
           != NGX_OK)
    {
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);

        return;
    }

    u->buffer.start = NULL;
    u->request_sent = 1;
    r->cached = 0;
    ngx_http_upstream_init(r);
    ngx_http_run_posted_requests(connection);
}


static ngx_int_t
ngx_http_cache_purge_header_filter(ngx_http_request_t *r)
{
    uint64_t                           date;
    ngx_uint_t                         remove, invalidate;
    ngx_http_upstream_t               *u;
    ngx_http_upstream_conf_t          *conf;
    ngx_http_cache_purge_ctx_t        *ctx;
    ngx_http_cache_purge_entry_t       entry;
    ngx_http_cache_purge_match_t       match;
    ngx_http_cache_purge_main_conf_t  *main;

    if (r->cache == NULL || r->err_status != 0) {
        return ngx_http_next_header_filter(r);
    }

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_module);
    date = (uint64_t) (r->cached ? r->cache->date : r->start_sec);

    if (!ngx_http_cache_purge_state_filtering(&main->state, date)
        && (ctx == NULL || date >= ctx->incoming))
    {
        return ngx_http_next_header_filter(r);
    }

    if (ngx_http_cache_purge_get_protocol(r) == NGX_DECLINED) {
        return ngx_http_next_header_filter(r);
    }

    u = r->upstream;

    if (u->cache_status == NGX_HTTP_CACHE_REVALIDATED) {
        /* Native 304 processing updates cache date after cache_send(). */

        return ngx_http_next_header_filter(r);
    }

    if (ngx_http_cache_purge_match(r, &entry, &match, 1) != NGX_OK) {
        return NGX_ERROR;
    }

    if (!r->cached) {
        if ((uint64_t) r->start_sec < match.delete_before
            || (uint64_t) r->start_sec < match.invalidate_before)
        {
            u->cacheable = 0;
        }

        return ngx_http_next_header_filter(r);
    }

    remove = date < match.delete_before;
    invalidate = date < match.invalidate_before;

    if (!remove && !invalidate) {
        return ngx_http_next_header_filter(r);
    }

    if (u->cache_status == NGX_HTTP_CACHE_STALE) {
#if !(NGX_RESTY_EXT)
        if (!remove) {
            return ngx_http_next_header_filter(r);
        }
#endif
        return u->state != NULL && u->state->status >= 400 ?
                   u->state->status :
                   NGX_HTTP_BAD_GATEWAY;
    }
#if !(NGX_RESTY_EXT)
    if (!remove && u->cache_status == NGX_HTTP_CACHE_UPDATING) {
        return ngx_http_next_header_filter(r);
    }
#endif
    ctx = ngx_http_cache_purge_get_ctx(r);

    if (ctx == NULL || ctx->retry || u->cleanup != NULL) {
        return NGX_HTTP_BAD_GATEWAY;
    }

    conf = ngx_palloc(r->pool, sizeof(*conf));

    if (conf == NULL) {
        return NGX_ERROR;
    }

    *conf = *u->conf;
    conf->cache = 0;
    u->conf = conf;
    u->cacheable = 1;
    u->cache_status = remove ? NGX_HTTP_CACHE_MISS : NGX_HTTP_CACHE_EXPIRED;
    r->cache->valid_sec = 0;
    r->cache->updating_sec = 0;
    r->cache->error_sec = 0;
#if (NGX_RESTY_EXT)
    r->cache->purged = 1;
#endif
    ctx->retry = 1;

    if (entry.tag_count != 0) {
        ctx->identity = ngx_palloc(r->pool, sizeof(entry));

        if (ctx->identity == NULL) {
            return NGX_ERROR;
        }

        *ctx->identity = entry;
    }

    ctx->method = u->method;
    ctx->header_only = r->header_only;
    ctx->resume = 1;
    ctx->event.handler = ngx_http_cache_purge_resume_handler;
    r->header_only = 1;
    ngx_post_event(&ctx->event, &ngx_posted_events);

    return NGX_DONE;
}


static ngx_int_t
ngx_http_cache_purge_log_handler(ngx_http_request_t *r)
{
    ngx_str_t                         *root;
    ngx_file_uniq_t                    uniq;
    ngx_http_cache_t                  *c;
    ngx_http_upstream_t               *u;
    ngx_http_cache_purge_ctx_t        *ctx;
    ngx_http_cache_purge_entry_t       entry;
    ngx_http_cache_purge_message_t    *message;
    ngx_http_cache_purge_main_conf_t  *main;

    c = r->cache;
    u = r->upstream;

    if (c == NULL || u == NULL || c->file_cache == NULL
        || (u->cache_status != NGX_HTTP_CACHE_REVALIDATED
            && (!c->updated || r->cached || !u->cacheable || u->pipe == NULL
                || !u->pipe->upstream_done)))
    {
        return NGX_OK;
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_module);

    if ((ctx != NULL && ctx->recorded)
        || ngx_http_cache_purge_get_protocol(r) == NGX_DECLINED)
    {
        return NGX_OK;
    }

    root = &c->file_cache->path->name;

    if (c->file.name.len <= root->len
        || ngx_http_cache_purge_native_identity(c->file_cache, c->key, &uniq)
           != NGX_OK)
    {
        return NGX_OK;
    }

    if (ngx_http_cache_purge_get_identity(r, &entry, 1, NULL) != NGX_OK) {
        ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                      "cache purge fill identity is invalid");

        return NGX_OK;
    }

    (void) ngx_http_cache_purge_native_node(c->file_cache, c->key, uniq,
                                            NGX_HTTP_CACHE_PURGE_NODE_CLEAR);
    ngx_memcpy(entry.hash, c->key, 16);
    entry.uniq = uniq;
    entry.created = (uint64_t) c->date;
    entry.root.data = root->data;
    entry.root.len = root->len;
    entry.zone.data = c->file_cache->shm_zone->shm.name.data;
    entry.zone.len = c->file_cache->shm_zone->shm.name.len;
    entry.path.data = c->file.name.data + root->len + 1;
    entry.path.len = c->file.name.len - root->len - 1;
    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);

    if (ngx_http_cache_purge_state_enqueue(
            &main->state, NGX_HTTP_CACHE_PURGE_RECORD, &entry, &message)
        != NGX_OK)
    {
        ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                      "cache purge fill index queue is full");
    } else if (ctx != NULL) {
        ctx->recorded = 1;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_purge_subrequest_done(ngx_http_request_t *r, void *data,
    ngx_int_t rc)
{
    ngx_http_cache_purge_ctx_t  *ctx = data;

    (void) ngx_http_cache_purge_log_handler(r);

    if (ctx->previous != NULL) {
        return ctx->previous->handler(r, ctx->previous->data, rc);
    }

    return rc;
}


static ngx_int_t
ngx_http_cache_purge_propagation_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
    u_char                            *p;
    uint64_t                           stamp;
    ngx_http_cache_purge_ctx_t        *ctx;
    ngx_http_cache_purge_entry_t       entry;
    ngx_http_cache_purge_match_t       match;
    ngx_http_cache_purge_match_t       known;
    ngx_http_cache_purge_main_conf_t  *main;

    *v = ngx_http_variable_null_value;

    if (ngx_http_cache_purge_get_protocol(r) != 0
        || ngx_http_cache_purge_get_flag(r, 4) != 1)
    {
        return NGX_OK;
    }

    if (ngx_http_cache_purge_parse_propagation(r) != NGX_OK
        || ngx_http_cache_purge_match(r, &entry, &match, 0) != NGX_OK)
    {
        return NGX_ERROR;
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_purge_module);

    if (ctx != NULL) {
        match.propagate_delete =
            ngx_max(match.propagate_delete, ctx->known.propagate_delete);
        match.propagate_invalidate = ngx_max(match.propagate_invalidate,
                                             ctx->known.propagate_invalidate);
    }

    if (ctx != NULL && ctx->identity != NULL) {
        main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
        (void) ngx_http_cache_purge_state_match(&main->state, ctx->identity,
                                                (uint64_t) ngx_time(), &known);
        match.propagate_delete =
            ngx_max(match.propagate_delete, known.propagate_delete);
        match.propagate_invalidate =
            ngx_max(match.propagate_invalidate, known.propagate_invalidate);
    }

    stamp = ngx_max(match.propagate_delete, match.propagate_invalidate);

    if (stamp == 0) {
        return NGX_OK;
    }

    if (data) {
        v->data = match.propagate_delete ? (u_char *) "delete" :
                                           (u_char *) "invalidate";
        v->len = match.propagate_delete ? 6 : 10;
    } else {
        p = ngx_pnalloc(r->pool, NGX_INT64_LEN + 1);

        if (p == NULL) {
            return NGX_ERROR;
        }

        v->data = p;
        v->len = ngx_sprintf(p, "%uL", stamp * 1000) - p;
    }

    return NGX_OK;
}


#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
static ngx_int_t
ngx_http_cache_purge_proxy_request_filter(ngx_http_request_t *r,
    ngx_http_proxy_filter_ctx_t *filter)
{
    ngx_uint_t                 i, j;
    ngx_list_part_t           *part;
    ngx_table_elt_t           *h, *header;
    ngx_str_t                  names[2];
    ngx_http_variable_value_t  value[2];

    if (ngx_http_cache_purge_get_protocol(r) != 0) {
        return NGX_DECLINED;
    }

    for (i = 0; i < 2; i++) {
        if (ngx_http_cache_purge_get_value(r,
                                           NGX_HTTP_CACHE_PURGE_TIME_HEADER + i,
                                           &names[i])
            != NGX_OK
            || ngx_http_cache_purge_propagation_variable(r, &value[i], i)
               != NGX_OK)
        {
            return NGX_ERROR;
        }
    }

    for (part = &filter->headers->part; part; part = part->next) {
        h = part->elts;

        for (i = 0; i < part->nelts; i++) {
            for (j = 0; j < 2; j++) {
                if (h[i].key.len == names[j].len
                    && ngx_strncasecmp(h[i].key.data, names[j].data,
                                       names[j].len)
                       == 0)
                {
                    h[i].hash = 0;
                }
            }
        }
    }

    for (i = 0; i < 2; i++) {
        if (value[i].len == 0) {
            continue;
        }

        header = ngx_list_push(filter->headers);

        if (header == NULL) {
            return NGX_ERROR;
        }

        ngx_memzero(header, sizeof(*header));
        header->key = names[i];
        header->value.data = value[i].data;
        header->value.len = value[i].len;
        header->lowcase_key = ngx_pnalloc(r->pool, names[i].len);

        if (header->lowcase_key == NULL) {
            return NGX_ERROR;
        }

        header->hash =
            ngx_hash_strlow(header->lowcase_key, names[i].data, names[i].len);
    }

    return NGX_DECLINED;
}


#endif


static ngx_int_t
ngx_http_cache_purge_send_response(ngx_http_request_t *r, ngx_uint_t status,
    ngx_str_t body)
{
    ngx_buf_t   *buffer;
    ngx_int_t    rc;
    ngx_chain_t  out;

    r->headers_out.status = status;
    r->headers_out.content_length_n = body.len;
    ngx_str_set(&r->headers_out.content_type, "application/json");
    buffer = ngx_calloc_buf(r->pool);

    if (buffer == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    buffer->pos = body.data;
    buffer->last = body.data + body.len;
    buffer->memory = 1;
    buffer->last_buf = 1;
    rc = ngx_http_send_header(r);

    if (rc != NGX_OK || r->header_only) {
        return rc;
    }

    out.buf = buffer;
    out.next = NULL;

    return ngx_http_output_filter(r, &out);
}


static void
ngx_http_cache_purge_id(ngx_str_t id, u_char output[16])
{
    ngx_md5_t  md5;

    ngx_md5_init(&md5);
    ngx_md5_update(&md5, id.data, id.len);
    ngx_md5_final(output, &md5);
}


static ngx_int_t
ngx_http_cache_purge_id_valid(ngx_str_t id)
{
    size_t  i;
    u_char  ch;

    if (id.len == 0 || id.len > 128) {
        return NGX_ERROR;
    }

    for (i = 0; i < id.len; i++) {
        ch = id.data[i];

        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
              || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_'
              || ch == '.'))
        {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static void
ngx_http_cache_purge_poll_handler(ngx_event_t *event)
{
    int                                result;
    u_char                            *p;
    ngx_int_t                          rc;
    ngx_str_t                          body;
    ngx_uint_t                         status;
    ngx_http_request_t                *r;
    ngx_http_cache_purge_ctx_t        *ctx;
    ngx_http_cache_purge_job_t         job;
    ngx_http_cache_purge_main_conf_t  *main;

    ctx = event->data;
    r = ctx->request;
    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    rc = ngx_http_cache_purge_state_poll(&main->state, &ctx->message, &result,
                                         &job);

    if (rc == NGX_AGAIN
        && (ngx_msec_int_t) (ngx_current_msec - ctx->deadline) < 0)
    {
        ngx_add_timer(event, 5);

        return;
    }

    if (rc != NGX_OK || result != 0) {
        status = NGX_HTTP_SERVICE_UNAVAILABLE;

        if (rc == NGX_OK) {
            if (result == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
                status = NGX_HTTP_NOT_FOUND;
            } else if (result == NGX_HTTP_CACHE_ROCKSDB_CONFLICT) {
                status = NGX_HTTP_CONFLICT;
            } else if (result == NGX_HTTP_CACHE_ROCKSDB_INVALID) {
                status = NGX_HTTP_BAD_REQUEST;
            }
        }

        ngx_http_finalize_request(r, status);

        return;
    }

    p = ngx_pnalloc(r->pool, 768 + ctx->operation_id.len);

    if (p == NULL) {
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);

        return;
    }

    body.data = p;
    p = ngx_sprintf(
        p,
        "{\"operation_id\":\"%V\",\"effective\":true,"
        "\"durable\":true,\"ready\":true,\"effective_at\":%uL,"
        "\"propagation_until\":%uL,\"cleanup\":{\"state\":\"%s\","
        "\"scan_done\":%s,\"visited\":%uL,\"affected\":%uL,\"error\":%ui}}",
        &ctx->operation_id, job.issued * 1000, job.retain_until * 1000,
        job.done ? "done" : "pending", job.done ? "true" : "false",
        job.visited, job.affected, (ngx_uint_t) job.error);
    body.len = p - body.data;
    rc = ngx_http_cache_purge_send_response(r, ctx->status, body);
    ngx_http_finalize_request(r, rc);
}


static ngx_int_t
ngx_http_cache_purge_submit(ngx_http_request_t *r,
    ngx_http_cache_purge_ctx_t *ctx, ngx_uint_t type)
{
    ngx_int_t                          rc;
    ngx_http_cache_purge_main_conf_t  *main;

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);

    if (main->path == NULL || !ngx_http_cache_purge_state_ready(&main->state))
    {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }

    rc = ngx_http_cache_purge_state_enqueue(&main->state, type, &ctx->job,
                                            &ctx->message);

    if (rc != NGX_OK) {
        return rc == NGX_DECLINED ? NGX_HTTP_CONFLICT :
                                    NGX_HTTP_SERVICE_UNAVAILABLE;
    }

    ctx->status =
        type == NGX_HTTP_CACHE_PURGE_SUBMIT ? NGX_HTTP_ACCEPTED : NGX_HTTP_OK;
    ctx->deadline = ngx_current_msec + 10000;
    ctx->event.handler = ngx_http_cache_purge_poll_handler;
    r->main->count++;
    r->read_event_handler = ngx_http_test_reading;
    r->write_event_handler = ngx_http_request_empty_handler;
    ngx_add_timer(&ctx->event, 1);

    return NGX_DONE;
}


static ngx_int_t
ngx_http_cache_purge_handler(ngx_http_request_t *r)
{
    uint64_t                           issued;
    ngx_int_t                          a, b, rc;
    ngx_str_t                          type, action, tags, stamp, value;
    ngx_uint_t                         i, enabled;
    ngx_http_cache_purge_ctx_t        *ctx;
    ngx_http_cache_purge_entry_t       entry;
    ngx_http_cache_purge_main_conf_t  *main;
    u_char                             random[16];

    if (!ngx_http_cache_purge_equal(r->method_name, "PURGE")) {
        return NGX_DECLINED;
    }

    enabled = 0;

    for (i = 0; i < 4; i++) {
        enabled |= ngx_http_cache_purge_get_flag(r, i) == 1;
    }

    if (!enabled) {
        return NGX_DECLINED;
    }

    if (r != r->main || r->headers_in.content_length_n > 0
        || r->headers_in.chunked)
    {
        return NGX_HTTP_BAD_REQUEST;
    }

    ctx = ngx_http_cache_purge_get_ctx(r);

    if (ctx == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    if (ngx_http_cache_purge_get_header(r, NGX_HTTP_CACHE_PURGE_TYPE_HEADER,
                                        &type)
        == NGX_ERROR
        || ngx_http_cache_purge_get_header(r,
                                           NGX_HTTP_CACHE_PURGE_ACTION_HEADER,
                                           &action)
           == NGX_ERROR)
    {
        return NGX_HTTP_BAD_REQUEST;
    }

    ctx->job.select.type = NGX_HTTP_CACHE_PURGE_SELECT_URL;

    if (type.len == 0 || ngx_http_cache_purge_equal(type, "url")) {
        /* URL selects all misc and namespace variants. */
    } else if (ngx_http_cache_purge_equal(type, "key")) {
        ctx->job.select.type = NGX_HTTP_CACHE_PURGE_SELECT_KEY;
    } else if (ngx_http_cache_purge_equal(type, "prefix")) {
        ctx->job.select.type = NGX_HTTP_CACHE_PURGE_SELECT_PREFIX;
    } else if (ngx_http_cache_purge_equal(type, "host")) {
        ctx->job.select.type = NGX_HTTP_CACHE_PURGE_SELECT_HOST;
    } else if (ngx_http_cache_purge_equal(type, "tag")
               || ngx_http_cache_purge_equal(type, "host-tag"))
    {
        ctx->job.select.type = NGX_HTTP_CACHE_PURGE_SELECT_TAG;
        ctx->job.select.host_scope =
            ngx_http_cache_purge_equal(type, "host-tag");
    } else {
        return NGX_HTTP_BAD_REQUEST;
    }

    if (action.len == 0 || ngx_http_cache_purge_equal(action, "delete")) {
        ctx->job.action = NGX_HTTP_CACHE_PURGE_DELETE;
    } else if (ngx_http_cache_purge_equal(action, "invalidate")) {
        ctx->job.action = NGX_HTTP_CACHE_PURGE_INVALIDATE;
    } else {
        return NGX_HTTP_BAD_REQUEST;
    }

    ngx_memzero(&entry, sizeof(entry));

    if (ctx->job.select.type != NGX_HTTP_CACHE_PURGE_SELECT_TAG
        || ctx->job.select.host_scope)
    {
        if (ctx->job.select.type == NGX_HTTP_CACHE_PURGE_SELECT_KEY) {
            if (ngx_http_cache_purge_get_identity(r, &entry, 0, NULL) != NGX_OK)
            {
                return NGX_HTTP_BAD_REQUEST;
            }
        } else {
            if (ngx_http_cache_purge_get_value(r, NGX_HTTP_CACHE_PURGE_URL,
                                               &value)
                != NGX_OK
                || value.len > 32768
                || ngx_http_cache_purge_parse_host(r, value, &entry.host)
                   != NGX_OK)
            {
                return NGX_HTTP_BAD_REQUEST;
            }

            entry.url.data = value.data;
            entry.url.len = value.len;
        }

        ctx->job.select.value =
            ctx->job.select.type == NGX_HTTP_CACHE_PURGE_SELECT_HOST ?
                entry.host :
                entry.url;

        if (ctx->job.select.type == NGX_HTTP_CACHE_PURGE_SELECT_KEY) {
            ctx->job.select.namespace_value = entry.namespace_value;
            ctx->job.select.misc = entry.misc;
        }
    }

    if (ctx->job.select.type == NGX_HTTP_CACHE_PURGE_SELECT_TAG) {
        if (ngx_http_cache_purge_get_header(r, NGX_HTTP_CACHE_PURGE_TAGS_HEADER,
                                            &tags)
            != NGX_OK
            || ngx_http_cache_purge_parse_tags(tags, &entry) != NGX_OK
            || entry.tag_count == 0
            || entry.tag_count > NGX_HTTP_CACHE_PURGE_TAG_SELECT)
        {
            return NGX_HTTP_BAD_REQUEST;
        }

        ngx_qsort(entry.tags, entry.tag_count, sizeof(entry.tags[0]),
                  ngx_http_cache_purge_tag_compare);
        value.len = 0;

        for (i = 0; i < entry.tag_count; i++) {
            value.len += entry.tags[i].len + (i != 0);
        }

        value.data = ngx_pnalloc(r->pool, value.len);

        if (value.data == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        ctx->job.select.value.data = value.data;
        value.len = 0;

        for (i = 0; i < entry.tag_count; i++) {
            if (i != 0
                && ngx_http_cache_purge_tag_compare(&entry.tags[i - 1],
                                                    &entry.tags[i])
                   == 0)
            {
                continue;
            }

            if (value.len != 0) {
                value.data[value.len++] = ',';
            }

            ngx_memcpy(value.data + value.len, entry.tags[i].data,
                       entry.tags[i].len);
            value.len += entry.tags[i].len;
        }

        ctx->job.select.value.len = value.len;

        if (ctx->job.select.host_scope) {
            ctx->job.select.namespace_value = entry.host;
        } else {
            if (ngx_http_cache_purge_get_value(r,
                                               NGX_HTTP_CACHE_PURGE_NAMESPACE,
                                               &value)
                != NGX_OK)
            {
                return NGX_HTTP_BAD_REQUEST;
            }

            ctx->job.select.namespace_value.data = value.data;
            ctx->job.select.namespace_value.len = value.len;
        }
    }

    a = ngx_http_cache_purge_get_header(r, NGX_HTTP_CACHE_PURGE_ID_HEADER,
                                        &ctx->operation_id);
    b = ngx_http_cache_purge_get_header(r, NGX_HTTP_CACHE_PURGE_ISSUED_HEADER,
                                        &stamp);

    if (a == NGX_ERROR || b == NGX_ERROR || a != b) {
        return NGX_HTTP_BAD_REQUEST;
    }

    if (a == NGX_DECLINED) {
        if (getrandom(random, sizeof(random), 0) != sizeof(random)) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        ctx->operation_id.data = ngx_pnalloc(r->pool, 32);

        if (ctx->operation_id.data == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        ctx->operation_id.len = 32;
        ngx_hex_dump(ctx->operation_id.data, random, 16);
        issued = (uint64_t) ngx_time() * 1000;
    } else if (ngx_http_cache_purge_id_valid(ctx->operation_id) != NGX_OK
               || ngx_http_cache_purge_parse_number(stamp, &issued) != NGX_OK
               || issued < 1000)
    {
        return NGX_HTTP_BAD_REQUEST;
    }

    ctx->job.issued = issued / 1000;
    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    ctx->job.retain_until =
        (uint64_t) ngx_time() + (main->propagation + 999) / 1000;
    ngx_http_cache_purge_id(ctx->operation_id, ctx->job.id);
    rc = ngx_http_cache_purge_submit(r, ctx, NGX_HTTP_CACHE_PURGE_SUBMIT);
    ngx_http_finalize_request(r, rc);

    return NGX_DONE;
}


ngx_int_t
ngx_http_cache_purge_api_handler(ngx_http_request_t *r)
{
    u_char                            *p;
    ngx_str_t                          tail, body;
    ngx_http_cache_purge_ctx_t        *ctx;
    ngx_http_cache_purge_loc_conf_t   *conf;
    ngx_http_cache_purge_main_conf_t  *main;
    uint64_t                           stats[3];

    main = ngx_http_get_module_main_conf(r, ngx_http_cache_purge_module);
    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_module);

    if (r->uri.len < conf->api.len
        || !(r->method & (NGX_HTTP_GET | NGX_HTTP_HEAD)))
    {
        return NGX_HTTP_NOT_ALLOWED;
    }

    tail.data = r->uri.data + conf->api.len;
    tail.len = r->uri.len - conf->api.len;

    while (tail.len && tail.data[0] == '/') {
        tail.data++;
        tail.len--;
    }

    if (ngx_http_cache_purge_equal(tail, "capabilities")) {
        ngx_http_cache_purge_state_cleanup_stats(&main->state, stats);
        body.data = ngx_pnalloc(r->pool, 512);

        if (body.data == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        p = ngx_sprintf(
            body.data,
            "{\"version\":\"ngx-http-cache-purge/v3\",\"ready\":%s,"
            "\"authentication\":\"nginx-configuration\",\"json_submit\":false,"
            "\"propagation_timeout_ms\":%M,\"index_cleanup\":{"
            "\"checked\":%uL,\"retained\":%uL,\"errors\":%uL}}",
            main->path && ngx_http_cache_purge_state_ready(&main->state) ?
                "true" :
                "false",
            main->propagation, stats[0], stats[1], stats[2]);
        body.len = p - body.data;

        return ngx_http_cache_purge_send_response(r, NGX_HTTP_OK, body);
    }

    if (tail.len <= 11 || ngx_strncmp(tail.data, "operations/", 11) != 0) {
        return NGX_HTTP_NOT_FOUND;
    }

    ctx = ngx_http_cache_purge_get_ctx(r);

    if (ctx == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ctx->operation_id.data = tail.data + 11;
    ctx->operation_id.len = tail.len - 11;

    if (ngx_http_cache_purge_id_valid(ctx->operation_id) != NGX_OK) {
        return NGX_HTTP_BAD_REQUEST;
    }

    ngx_http_cache_purge_id(ctx->operation_id, ctx->job.id);

    return ngx_http_cache_purge_submit(r, ctx, NGX_HTTP_CACHE_PURGE_STATUS);
}


ngx_int_t
ngx_http_cache_purge_add_variables(ngx_conf_t *cf)
{
    static ngx_str_t  names[] = {
        ngx_string("cache_purge_proxy_guard"),
        ngx_string("cache_purge_fastcgi_guard"),
        ngx_string("cache_purge_scgi_guard"),
        ngx_string("cache_purge_uwsgi_guard"),
        ngx_string("cache_purge_propagation_time"),
        ngx_string("cache_purge_propagation_action")
    };

    ngx_uint_t            i;
    ngx_http_variable_t  *v;

    for (i = 0; i < 6; i++) {
        v = ngx_http_add_variable(cf, &names[i], NGX_HTTP_VAR_NOCACHEABLE);

        if (v == NULL) {
            return NGX_ERROR;
        }

        v->get_handler = i < 4 ? ngx_http_cache_purge_guard_variable :
                                 ngx_http_cache_purge_propagation_variable;
        v->data = i < 4 ? i : i - 4;
    }

    return NGX_OK;
}


static ngx_uint_t
ngx_http_cache_purge_flag_possible(ngx_http_cache_purge_loc_conf_t *conf,
    ngx_uint_t n)
{
#if (NGX_EXPR)
    ngx_uint_t                 i;
    ngx_conf_expr_flag_ctx_t  *values;

    if (conf->flag[n] == NULL || conf->flag[n] == NGX_CONF_UNSET_PTR) {
        return 0;
    }

    values = conf->flag[n]->elts;

    for (i = 0; i < conf->flag[n]->nelts; i++) {
        if (values[i].value == 1) {
            return 1;
        }
    }

    return 0;
#else
    return conf->flag[n] == 1;
#endif
}


ngx_int_t
ngx_http_cache_purge_init_handlers(ngx_conf_t *cf)
{
    void        ***locations;
    ngx_uint_t     i, j, k, n;
    ngx_array_t  **slot, *array;

    ngx_module_t                      *module;
    ngx_command_t                     *cmd;
    ngx_http_handler_pt               *handler;
    ngx_http_complex_value_t          *values;
    ngx_http_core_main_conf_t         *core;
    ngx_http_cache_purge_loc_conf_t   *conf;
    ngx_http_cache_purge_main_conf_t  *main;
    ngx_http_compile_complex_value_t   compile;
    static const char                 *bypass[] = {
        "proxy_cache_bypass",
        "fastcgi_cache_bypass",
        "scgi_cache_bypass",
        "uwsgi_cache_bypass"
    };
    static ngx_str_t                   guard[] = {
        ngx_string("$cache_purge_proxy_guard"),
        ngx_string("$cache_purge_fastcgi_guard"),
        ngx_string("$cache_purge_scgi_guard"),
        ngx_string("$cache_purge_uwsgi_guard")
    };
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    ngx_http_proxy_filter_pt           *filter;
    ngx_http_proxy_filter_main_conf_t  *filters;

#endif

    main = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_purge_module);
    locations = main->locations.elts;

    for (k = 0; k < main->locations.nelts; k++) {
        conf = locations[k][ngx_http_cache_purge_module.ctx_index];

        for (i = 0; i < 4; i++) {
            if (ngx_http_cache_purge_flag_possible(conf, i)
                && (!conf->url_set || main->path == NULL))
            {
                ngx_conf_log_error(
                    NGX_LOG_EMERG, cf, 0,
                    "cache purge locations require cache_purge_key_url, "
                    "cache_purge_index_store and cache_purge_state_zone");

                return NGX_ERROR;
            }
        }
    }

    if (main->path == NULL) {
        return NGX_OK;
    }

    for (i = 0; i < 4; i++) {
        ngx_memzero(&compile, sizeof(compile));
        compile.cf = cf;
        compile.value = &guard[i];
        compile.complex_value = &main->guard[i];

        if (ngx_http_compile_complex_value(&compile) != NGX_OK) {
            return NGX_ERROR;
        }
    }

    for (i = 0; cf->cycle->modules[i]; i++) {
        module = cf->cycle->modules[i];

        if (module->type != NGX_HTTP_MODULE || module->commands == NULL) {
            continue;
        }

        for (cmd = module->commands; cmd->name.len; cmd++) {
            for (j = 0; j < 4; j++) {
                if (ngx_http_cache_purge_equal(cmd->name, bypass[j])) {
                    break;
                }
            }

            if (j == 4 || cmd->set != ngx_http_set_predicate_slot
                || cmd->conf != NGX_HTTP_LOC_CONF_OFFSET)
            {
                continue;
            }

            main->protocol[j] = (ngx_buf_tag_t) module;

            for (k = 0; k < main->locations.nelts; k++) {
                conf = locations[k][ngx_http_cache_purge_module.ctx_index];

                if (!ngx_http_cache_purge_flag_possible(conf, j)) {
                    continue;
                }

                slot = (ngx_array_t **) ((u_char *)
                                             locations[k][module->ctx_index]
                                         + cmd->offset);

                if (*slot && *slot != NGX_CONF_UNSET_PTR && (*slot)->nelts) {
                    values = (*slot)->elts;

                    if (values[0].values == main->guard[j].values) {
                        continue;
                    }
                }

                n = *slot && *slot != NGX_CONF_UNSET_PTR ? (*slot)->nelts : 0;
                array = ngx_array_create(cf->pool, n + 1, sizeof(*values));

                if (array == NULL) {
                    return NGX_ERROR;
                }

                values = ngx_array_push_n(array, n + 1);

                if (values == NULL) {
                    return NGX_ERROR;
                }

                values[0] = main->guard[j];

                if (n) {
                    ngx_memcpy(values + 1, (*slot)->elts, n * sizeof(*values));
                }

                *slot = array;
            }
        }
    }

    core = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    handler =
        ngx_array_push(&core->phases[NGX_HTTP_PRECONTENT_PHASE].handlers);

    if (handler == NULL) {
        return NGX_ERROR;
    }

    *handler = ngx_http_cache_purge_handler;
    handler = ngx_array_push(&core->phases[NGX_HTTP_LOG_PHASE].handlers);

    if (handler == NULL) {
        return NGX_ERROR;
    }

    *handler = ngx_http_cache_purge_log_handler;
#if (NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER)
    filters =
        ngx_http_conf_get_module_main_conf(cf, ngx_http_proxy_filter_module);
    filter = ngx_array_push(&filters->phases[NGX_HTTP_PROXY_REQUEST_FILTER]);

    if (filter == NULL) {
        return NGX_ERROR;
    }

    *filter = ngx_http_cache_purge_proxy_request_filter;
#endif
    ngx_http_next_header_filter = ngx_http_top_header_filter;
    ngx_http_top_header_filter = ngx_http_cache_purge_header_filter;

    return NGX_OK;
}
