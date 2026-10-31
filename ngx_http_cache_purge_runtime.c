/* Copyright (C) Hanada */

#include "ngx_http_cache_purge.h"
#include <pthread.h>
#include <sched.h>


#define NGX_HTTP_CACHE_PURGE_STATE_VERSION  4
#define NGX_HTTP_CACHE_PURGE_STATE_LIMIT    256
#define NGX_HTTP_CACHE_PURGE_FILE_BATCH     8


typedef struct ngx_http_cache_purge_rule_s  ngx_http_cache_purge_rule_t;


typedef struct {
    uint64_t    bits[8];
    ngx_uint_t  checked;
} ngx_http_cache_purge_tags_t;


struct ngx_http_cache_purge_rule_s {
    ngx_queue_t                    queue;
    ngx_http_cache_purge_rule_t   *parent;
    u_char                         id[16];
    ngx_http_cache_purge_select_t  select;
    uint64_t                       issued, retain_until;
    ngx_uint_t                     action, references, published, local;
};


struct ngx_http_cache_purge_message_s {
    ngx_queue_t                  all, pending;
    ngx_pid_t                    owner, executor;
    ngx_uint_t                   state;
    size_t                       bytes;
    int                          result;
    ngx_http_cache_purge_work_t  work;
};


struct ngx_http_cache_purge_shared_s {
    ngx_uint_t                   version;
    ngx_atomic_t                 published;
    ngx_atomic_t                 filtering;
    ngx_atomic_t                 complex;
    ngx_atomic_t                 ready;
    ngx_pid_t                    owner;
    ngx_queue_t                  rules, messages, pending;
    ngx_uint_t                   rule_count, message_count;
    ngx_uint_t                   url_count;
    ngx_http_cache_purge_rule_t *urls[NGX_HTTP_CACHE_PURGE_STATE_LIMIT];
    size_t                       message_bytes, capacity;
    uint64_t                     collected;
};


static ngx_uint_t
ngx_http_cache_purge_state_fields(ngx_http_cache_purge_work_t *work,
    ngx_http_cache_rocksdb_bytes_t **fields)
{
    ngx_http_cache_purge_entry_t  *entry;
    ngx_http_cache_purge_job_t    *job;
    ngx_uint_t                     i;

    if (work->type == NGX_HTTP_CACHE_PURGE_STATUS) {
        return 0;
    }

    if (work->type == NGX_HTTP_CACHE_PURGE_SUBMIT) {
        job = &work->data.job;
        fields[0] = &job->select.value;
        fields[1] = &job->select.namespace_value;
        fields[2] = &job->select.misc;
        return 3;
    }

    entry = &work->data.entry;
    fields[0] = &entry->url;
    fields[1] = &entry->host;
    fields[2] = &entry->namespace_value;
    fields[3] = &entry->misc;
    fields[4] = &entry->root;
    fields[5] = &entry->zone;
    fields[6] = &entry->path;
    for (i = 0; i < entry->tag_count; i++) {
        fields[i + 7] = &entry->tags[i];
    }

    return 7 + entry->tag_count;
}


static ngx_int_t
ngx_http_cache_purge_state_size(ngx_http_cache_purge_work_t *work)
{
    ngx_http_cache_rocksdb_bytes_t  *fields[71];
    ngx_http_cache_purge_job_t      *job;
    ngx_uint_t                       i, count;
    size_t                           maximum;

    if ((work->type == NGX_HTTP_CACHE_PURGE_RECORD
         || work->type == NGX_HTTP_CACHE_PURGE_LOOKUP)
        && work->data.entry.tag_count > 64)
    {
        return NGX_ERROR;
    }

    if (work->type == NGX_HTTP_CACHE_PURGE_SUBMIT) {
        job = &work->data.job;
        if (job->issued == 0 || job->select.value.len == 0
            || job->action > NGX_HTTP_CACHE_PURGE_INVALIDATE
            || job->select.type > NGX_HTTP_CACHE_PURGE_SELECT_TAG
            || job->select.host_scope > 1
            || (job->select.host_scope
                && job->select.type != NGX_HTTP_CACHE_PURGE_SELECT_TAG))
        {
            return NGX_ERROR;
        }
    }

    work->size = sizeof(*work);
    count = ngx_http_cache_purge_state_fields(work, fields);
    for (i = 0; i < count; i++) {
        maximum = work->type == NGX_HTTP_CACHE_PURGE_RECORD && i >= 7
                  ? 1024 : 32768;
        if (fields[i]->len > maximum
            || (fields[i]->len != 0 && fields[i]->data == NULL)
            || fields[i]->len > NGX_HTTP_CACHE_PURGE_MESSAGE_MAX - work->size)
        {
            return NGX_ERROR;
        }

        work->size += fields[i]->len;
    }

    return NGX_OK;
}


static void
ngx_http_cache_purge_state_copy(ngx_http_cache_purge_work_t *dst,
    ngx_http_cache_purge_work_t *src)
{
    ngx_http_cache_rocksdb_bytes_t  *fields[71];
    ngx_uint_t                       i, count;
    u_char                          *p;

    *dst = *src;
    p = (u_char *) (dst + 1);
    count = ngx_http_cache_purge_state_fields(dst, fields);
    for (i = 0; i < count; i++) {
        if (fields[i]->len != 0) {
            ngx_memcpy(p, fields[i]->data, fields[i]->len);
        }

        fields[i]->data = p;
        p += fields[i]->len;
    }

    if (dst->type == NGX_HTTP_CACHE_PURGE_SUBMIT
        || dst->type == NGX_HTTP_CACHE_PURGE_STATUS)
    {
        dst->data.job.bookmark.data = NULL;
        dst->data.job.bookmark.len = 0;
    }
}


static ngx_uint_t
ngx_http_cache_purge_state_equal(ngx_http_cache_rocksdb_bytes_t a,
    ngx_http_cache_rocksdb_bytes_t b)
{
    return a.len == b.len && (a.len == 0 || ngx_memcmp(a.data, b.data, a.len)
                                           == 0);
}


static ngx_http_cache_purge_rule_t *
ngx_http_cache_purge_state_find(ngx_http_cache_purge_state_t *state,
    const u_char  id[16])
{
    ngx_queue_t                  *q;
    ngx_http_cache_purge_rule_t  *rule;

    for (q = ngx_queue_head(&state->shared->rules);
         q != ngx_queue_sentinel(&state->shared->rules);
         q = ngx_queue_next(q))
    {
        rule = ngx_queue_data(q, ngx_http_cache_purge_rule_t, queue);
        if (ngx_memcmp(rule->id, id, 16) == 0) {
            return rule;
        }
    }

    return NULL;
}


static ngx_int_t
ngx_http_cache_purge_state_reserve(ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_job_t  *job, ngx_http_cache_purge_rule_t **out)
{
    ngx_http_cache_purge_rule_t    *rule;
    ngx_http_cache_purge_select_t  *s;
    u_char                         *p;
    size_t                          size;

    rule = ngx_http_cache_purge_state_find(state, job->id);
    if (rule != NULL) {
        s = &rule->select;
        if (rule->issued != job->issued || rule->action != job->action
            || s->type != job->select.type
            || s->host_scope != job->select.host_scope
            || !ngx_http_cache_purge_state_equal(s->value, job->select.value)
            || !ngx_http_cache_purge_state_equal(s->namespace_value,
                                                  job->select.namespace_value)
            || !ngx_http_cache_purge_state_equal(s->misc, job->select.misc))
        {
            return NGX_DECLINED;
        }

        *out = rule;
        return NGX_OK;
    }

    if (state->shared->rule_count == NGX_HTTP_CACHE_PURGE_STATE_LIMIT) {
        return NGX_BUSY;
    }

    size = sizeof(*rule) + job->select.value.len
           + job->select.namespace_value.len + job->select.misc.len;
    rule = ngx_slab_calloc_locked(state->pool, size);
    if (rule == NULL) {
        return NGX_BUSY;
    }

    ngx_memcpy(rule->id, job->id, 16);
    rule->issued = job->issued;
    rule->retain_until = job->retain_until;
    rule->action = job->action;
    rule->select = job->select;
    p = (u_char *) (rule + 1);
    rule->select.value.data = p;
    p = ngx_cpymem(p, job->select.value.data, job->select.value.len);
    rule->select.namespace_value.data = p;
    if (job->select.namespace_value.len != 0) {
        p = ngx_cpymem(p, job->select.namespace_value.data,
                       job->select.namespace_value.len);
    }

    rule->select.misc.data = p;
    if (job->select.misc.len != 0) {
        ngx_memcpy(p, job->select.misc.data, job->select.misc.len);
    }

    ngx_queue_insert_tail(&state->shared->rules, &rule->queue);
    state->shared->rule_count++;
    *out = rule;

    return NGX_OK;
}


static int
ngx_http_cache_purge_state_compare(const void *a, const void *b)
{
    ngx_http_cache_purge_rule_t  *left, *right;

    left = *(ngx_http_cache_purge_rule_t * const *) a;
    right = *(ngx_http_cache_purge_rule_t * const *) b;
    return ngx_memn2cmp((u_char *) left->select.value.data,
                       (u_char *) right->select.value.data,
                       left->select.value.len, right->select.value.len);
}


static void
ngx_http_cache_purge_state_index(ngx_http_cache_purge_state_t *state)
{
    ngx_http_cache_purge_rule_t  *rule, *parent, **urls;
    ngx_queue_t                  *q;
    ngx_uint_t                    i, n;

    urls = state->shared->urls;
    n = 0;
    for (q = ngx_queue_head(&state->shared->rules);
         q != ngx_queue_sentinel(&state->shared->rules);
         q = ngx_queue_next(q))
    {
        rule = ngx_queue_data(q, ngx_http_cache_purge_rule_t, queue);
        if (!rule->published || !rule->local
            || (rule->select.type != NGX_HTTP_CACHE_PURGE_SELECT_URL
                && rule->select.type != NGX_HTTP_CACHE_PURGE_SELECT_PREFIX))
        {
            continue;
        }

        urls[n++] = rule;
    }
    ngx_qsort(urls, n, sizeof(*urls), ngx_http_cache_purge_state_compare);

    /* Sorted predecessors and their prefix ancestors cover every match. */
    parent = NULL;
    for (i = 0; i < n; i++) {
        rule = urls[i];
        while (parent != NULL
               && (parent->select.value.len > rule->select.value.len
                   || ngx_memcmp(parent->select.value.data,
                                 rule->select.value.data,
                                 parent->select.value.len) != 0))
        {
            parent = parent->parent;
        }
        rule->parent = parent;
        parent = rule;
    }
    state->shared->url_count = n;
}


static void
ngx_http_cache_purge_state_publish(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_rule_t  *rule, const ngx_http_cache_purge_job_t *job)
{
    ngx_uint_t  local;

    local = rule->published && rule->local;
    rule->local = !job->done;
    rule->retain_until = job->retain_until;
    if (!rule->published) {
        rule->published = 1;
        ngx_memory_barrier();
        (void) ngx_atomic_fetch_add(&state->shared->published, 1);
    }
    if (local != rule->local) {
        ngx_memory_barrier();
        (void) ngx_atomic_fetch_add(&state->shared->filtering,
                                     rule->local ? 1 : -1);
        if (rule->select.type != NGX_HTTP_CACHE_PURGE_SELECT_URL
            && rule->select.type != NGX_HTTP_CACHE_PURGE_SELECT_PREFIX)
        {
            (void) ngx_atomic_fetch_add(&state->shared->complex,
                                         rule->local ? 1 : -1);
        } else {
            ngx_http_cache_purge_state_index(state);
        }
    }
}


static void
ngx_http_cache_purge_state_remove(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_rule_t *rule)
{
    ngx_queue_remove(&rule->queue);
    state->shared->rule_count--;
    if (rule->published) {
        (void) ngx_atomic_fetch_add(&state->shared->published, -1);
        if (rule->local) {
            (void) ngx_atomic_fetch_add(&state->shared->filtering, -1);
            if (rule->select.type != NGX_HTTP_CACHE_PURGE_SELECT_URL
                && rule->select.type != NGX_HTTP_CACHE_PURGE_SELECT_PREFIX)
            {
                (void) ngx_atomic_fetch_add(&state->shared->complex, -1);
            } else {
                ngx_http_cache_purge_state_index(state);
            }
        }
    }

    ngx_slab_free_locked(state->pool, rule);
}


ngx_int_t
ngx_http_cache_purge_state_init(ngx_shm_zone_t *zone, void *old)
{
    ngx_http_cache_purge_state_t   *state;
    ngx_http_cache_purge_shared_t  *shared;

    state = zone->data;
    state->channel[0] = state->channel[1] = -1;
    state->pool = (ngx_slab_pool_t *) zone->shm.addr;
    if (zone->shm.size < 1024 * 1024) {
        return NGX_ERROR;
    }

    if (old != NULL || zone->shm.exists) {
        shared = state->pool->data;
        if (shared == NULL
            || shared->version != NGX_HTTP_CACHE_PURGE_STATE_VERSION
            || shared->capacity != zone->shm.size / 2)
        {
            return NGX_ERROR;
        }

    } else {
        shared = ngx_slab_calloc(state->pool, sizeof(*shared));
        if (shared == NULL) {
            return NGX_ERROR;
        }

        shared->version = NGX_HTTP_CACHE_PURGE_STATE_VERSION;
        shared->capacity = zone->shm.size / 2;
        ngx_queue_init(&shared->rules);
        ngx_queue_init(&shared->messages);
        ngx_queue_init(&shared->pending);
        state->pool->data = shared;
    }

    state->shared = shared;

    return NGX_OK;
}


ngx_int_t
ngx_http_cache_purge_state_enqueue(ngx_http_cache_purge_state_t *state,
    ngx_uint_t  type, const void *input, ngx_http_cache_purge_message_t **out)
{
    ngx_http_cache_purge_work_t      work;
    ngx_http_cache_purge_message_t  *message;
    ngx_http_cache_purge_rule_t     *rule;
    ngx_http_cache_purge_shared_t   *shared;
    size_t                           size;
    ngx_int_t                        rc;

    *out = NULL;
    if (type > NGX_HTTP_CACHE_PURGE_LOOKUP || input == NULL) {
        return NGX_ERROR;
    }

    ngx_memzero(&work, sizeof(work));
    work.type = type;
    if (type == NGX_HTTP_CACHE_PURGE_RECORD
        || type == NGX_HTTP_CACHE_PURGE_LOOKUP)
    {
        work.data.entry = *(const ngx_http_cache_purge_entry_t *) input;
    } else if (type == NGX_HTTP_CACHE_PURGE_SUBMIT) {
        work.data.job = *(const ngx_http_cache_purge_job_t *) input;
    } else {
        ngx_memcpy(work.data.job.id,
                   ((const ngx_http_cache_purge_job_t *) input)->id, 16);
    }

    if (ngx_http_cache_purge_state_size(&work) != NGX_OK) {
        return NGX_ERROR;
    }

    size = offsetof(ngx_http_cache_purge_message_t, work) + work.size;
    shared = state->shared;
    ngx_shmtx_lock(&state->pool->mutex);
    if (shared->message_count == NGX_HTTP_CACHE_PURGE_STATE_LIMIT
        || size > shared->capacity - shared->message_bytes)
    {
        ngx_shmtx_unlock(&state->pool->mutex);
        return NGX_BUSY;
    }

    message = ngx_slab_alloc_locked(state->pool, size);
    if (message == NULL) {
        ngx_shmtx_unlock(&state->pool->mutex);
        return NGX_BUSY;
    }

    if (type == NGX_HTTP_CACHE_PURGE_SUBMIT) {
        rc = ngx_http_cache_purge_state_reserve(state, &work.data.job, &rule);
        if (rc != NGX_OK) {
            ngx_slab_free_locked(state->pool, message);
            ngx_shmtx_unlock(&state->pool->mutex);
            return rc;
        }

        rule->references++;
    }

    shared->message_count++;
    shared->message_bytes += size;
    ngx_shmtx_unlock(&state->pool->mutex);

    /* This allocation is private until it is linked; copy outside the lock. */
    message->owner = type == NGX_HTTP_CACHE_PURGE_RECORD ? 0 : ngx_pid;
    message->executor = 0;
    message->state = 0;
    message->bytes = size;
    message->result = 0;
    ngx_http_cache_purge_state_copy(&message->work, &work);

    ngx_shmtx_lock(&state->pool->mutex);
    ngx_queue_insert_tail(&shared->messages, &message->all);
    ngx_queue_insert_tail(&shared->pending, &message->pending);
    ngx_shmtx_unlock(&state->pool->mutex);
    if (type != NGX_HTTP_CACHE_PURGE_RECORD) {
        *out = message;
    }

    if (state->channel[1] != -1) {
        (void) write(state->channel[1], "q", 1);
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_cache_purge_state_take(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_work_t  *work, ngx_http_cache_purge_message_t **out)
{
    ngx_http_cache_purge_message_t  *message;
    ngx_queue_t                     *q;

    *out = NULL;
    ngx_shmtx_lock(&state->pool->mutex);
    if (ngx_queue_empty(&state->shared->pending)) {
        ngx_shmtx_unlock(&state->pool->mutex);
        return NGX_DECLINED;
    }

    q = ngx_queue_head(&state->shared->pending);
    message = ngx_queue_data(q, ngx_http_cache_purge_message_t, pending);
    ngx_queue_remove(q);
    message->state = 1;
    message->executor = ngx_pid;
    ngx_shmtx_unlock(&state->pool->mutex);

    ngx_http_cache_purge_state_copy(work, &message->work);
    *out = message;

    return NGX_OK;
}


static void
ngx_http_cache_purge_state_free(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_message_t *message)
{
    ngx_queue_remove(&message->all);
    state->shared->message_count--;
    state->shared->message_bytes -= message->bytes;
    ngx_slab_free_locked(state->pool, message);
}


static void
ngx_http_cache_purge_state_result(ngx_http_cache_purge_job_t *dst,
    const ngx_http_cache_purge_job_t *src)
{
    *dst = *src;
    ngx_memzero(&dst->select, sizeof(dst->select));
    ngx_memzero(&dst->bookmark, sizeof(dst->bookmark));
}


void
ngx_http_cache_purge_state_complete(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_message_t  *message, int result,
    const void *output)
{
    ngx_http_cache_purge_rule_t       *rule;
    const ngx_http_cache_purge_job_t  *job;

    job = message->work.type == NGX_HTTP_CACHE_PURGE_SUBMIT
          || message->work.type == NGX_HTTP_CACHE_PURGE_STATUS ? output : NULL;

    if (result == 0 && (message->work.type == NGX_HTTP_CACHE_PURGE_SUBMIT
                       || message->work.type == NGX_HTTP_CACHE_PURGE_STATUS)
        && (job == NULL
            || ngx_memcmp(job->id, message->work.data.job.id, 16) != 0))
    {
        result = NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
        job = NULL;
    }

    ngx_shmtx_lock(&state->pool->mutex);
    if (message->work.type == NGX_HTTP_CACHE_PURGE_SUBMIT) {
        rule = ngx_http_cache_purge_state_find(state,
                                              message->work.data.job.id);
        if (rule != NULL) {
            rule->references--;
            if (result == 0 && job != NULL) {
                ngx_http_cache_purge_state_publish(state, rule, job);
            } else if (!rule->published && rule->references == 0) {
                ngx_http_cache_purge_state_remove(state, rule);
            }
        }
    }

    message->result = result;
    if (message->work.type == NGX_HTTP_CACHE_PURGE_LOOKUP && output != NULL) {
        message->work.match = *(const ngx_http_cache_purge_match_t *) output;
    }
    if (job != NULL && message->work.type != NGX_HTTP_CACHE_PURGE_RECORD) {
        ngx_http_cache_purge_state_result(&message->work.data.job, job);
    }

    message->state = 2;
    if (message->owner == 0) {
        ngx_http_cache_purge_state_free(state, message);
    }

    ngx_shmtx_unlock(&state->pool->mutex);
}


ngx_int_t
ngx_http_cache_purge_state_poll(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_message_t  **message, int *result,
    void *output)
{
    ngx_http_cache_purge_message_t  *m;

    m = *message;
    if (m == NULL) {
        return NGX_DECLINED;
    }

    ngx_shmtx_lock(&state->pool->mutex);
    if (m->state != 2) {
        ngx_shmtx_unlock(&state->pool->mutex);
        return NGX_AGAIN;
    }

    *result = m->result;
    if (m->work.type == NGX_HTTP_CACHE_PURGE_LOOKUP) {
        *(ngx_http_cache_purge_match_t *) output = m->work.match;
    } else {
        ngx_http_cache_purge_state_result(output, &m->work.data.job);
    }
    ngx_http_cache_purge_state_free(state, m);
    *message = NULL;
    ngx_shmtx_unlock(&state->pool->mutex);

    return NGX_OK;
}


void
ngx_http_cache_purge_state_detach(ngx_http_cache_purge_state_t *state,
    ngx_http_cache_purge_message_t **message)
{
    ngx_http_cache_purge_message_t  *m;

    m = *message;
    if (m == NULL) {
        return;
    }

    ngx_shmtx_lock(&state->pool->mutex);
    m->owner = 0;
    if (m->state == 2) {
        ngx_http_cache_purge_state_free(state, m);
    }

    *message = NULL;
    ngx_shmtx_unlock(&state->pool->mutex);
}


ngx_int_t
ngx_http_cache_purge_state_install(ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_job_t *job)
{
    ngx_http_cache_purge_work_t   work;
    ngx_http_cache_purge_rule_t  *rule;
    ngx_int_t                     rc;

    ngx_memzero(&work, sizeof(work));
    work.type = NGX_HTTP_CACHE_PURGE_SUBMIT;
    work.data.job = *job;
    if (ngx_http_cache_purge_state_size(&work) != NGX_OK) {
        return NGX_ERROR;
    }

    ngx_shmtx_lock(&state->pool->mutex);
    rc = ngx_http_cache_purge_state_reserve(state, job, &rule);
    if (rc == NGX_OK) {
        ngx_http_cache_purge_state_publish(state, rule, job);
    }

    ngx_shmtx_unlock(&state->pool->mutex);

    return rc;
}


void
ngx_http_cache_purge_state_finish(ngx_http_cache_purge_state_t *state,
    const u_char  id[16])
{
    ngx_http_cache_purge_rule_t  *rule;

    ngx_shmtx_lock(&state->pool->mutex);
    rule = ngx_http_cache_purge_state_find(state, id);
    if (rule != NULL && rule->local) {
        rule->local = 0;
        if (rule->published) {
            (void) ngx_atomic_fetch_add(&state->shared->filtering, -1);
            if (rule->select.type != NGX_HTTP_CACHE_PURGE_SELECT_URL
                && rule->select.type != NGX_HTTP_CACHE_PURGE_SELECT_PREFIX)
            {
                (void) ngx_atomic_fetch_add(&state->shared->complex, -1);
            } else {
                ngx_http_cache_purge_state_index(state);
            }
        }
    }

    ngx_shmtx_unlock(&state->pool->mutex);
}


static ngx_uint_t
ngx_http_cache_purge_state_matches(const ngx_http_cache_purge_select_t *s,
    const ngx_http_cache_purge_entry_t *entry,
    ngx_http_cache_purge_tags_t *tags)
{
    size_t      i;
    ngx_uint_t  key;

    switch (s->type) {
    case NGX_HTTP_CACHE_PURGE_SELECT_URL:
        return ngx_http_cache_purge_state_equal(s->value, entry->url);

    case NGX_HTTP_CACHE_PURGE_SELECT_KEY:
        return ngx_http_cache_purge_state_equal(s->value, entry->url)
               && ngx_http_cache_purge_state_equal(s->misc, entry->misc)
               && ngx_http_cache_purge_state_equal(s->namespace_value,
                                                     entry->namespace_value);

    case NGX_HTTP_CACHE_PURGE_SELECT_PREFIX:
        return s->value.len <= entry->url.len
               && ngx_memcmp(s->value.data, entry->url.data, s->value.len) == 0;

    case NGX_HTTP_CACHE_PURGE_SELECT_HOST:
        return ngx_http_cache_purge_state_equal(s->value, entry->host);

    case NGX_HTTP_CACHE_PURGE_SELECT_TAG:
        if (!ngx_http_cache_purge_state_equal(s->namespace_value,
                    s->host_scope ? entry->host : entry->namespace_value))
        {
            return 0;
        }

        /* Avoid bitmap setup when only one scoped TAG rule needs this set. */
        if (entry->tag_count > 1 && tags->checked++) {
            if (tags->checked == 2) {
                ngx_memzero(tags->bits, sizeof(tags->bits));
                for (i = 0; i < entry->tag_count; i++) {
                    key = ngx_hash_key((u_char *) entry->tags[i].data,
                                       entry->tags[i].len) & 511;
                    tags->bits[key / 64] |= (uint64_t) 1 << (key % 64);
                }
            }
            key = ngx_hash_key((u_char *) s->value.data, s->value.len) & 511;
            if (!(tags->bits[key / 64] & ((uint64_t) 1 << (key % 64)))) {
                return 0;
            }
        }

        for (i = 0; i < entry->tag_count; i++) {
            if (ngx_http_cache_purge_state_equal(s->value, entry->tags[i])) {
                return 1;
            }
        }
    }

    return 0;
}


static void
ngx_http_cache_purge_state_apply(ngx_http_cache_purge_rule_t *rule,
    const ngx_http_cache_purge_entry_t *entry, uint64_t now,
    ngx_http_cache_purge_match_t *match, ngx_uint_t local_only,
    ngx_http_cache_purge_tags_t *tags)
{
    uint64_t  *local, *propagate;

    if (!rule->published
        || (!rule->local && (local_only || rule->retain_until <= now))
        || !ngx_http_cache_purge_state_matches(&rule->select, entry, tags))
    {
        return;
    }

    if (rule->action == NGX_HTTP_CACHE_PURGE_DELETE) {
        local = &match->delete_before;
        propagate = &match->propagate_delete;
    } else {
        local = &match->invalidate_before;
        propagate = &match->propagate_invalidate;
    }

    if (rule->local) {
        *local = ngx_max(*local, rule->issued);
    }
    if (rule->retain_until > now) {
        *propagate = ngx_max(*propagate, rule->issued);
    }
}


static ngx_int_t
ngx_http_cache_purge_state_select(ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_entry_t  *entry, uint64_t now,
    ngx_http_cache_purge_match_t *match, ngx_uint_t url_only,
    ngx_uint_t local_only)
{
    ngx_http_cache_purge_rule_t  *rule;
    ngx_http_cache_purge_tags_t   tags;
    ngx_queue_t                  *q;
    ngx_uint_t                    low, high, middle;

    ngx_memzero(match, sizeof(*match));
    if (state->shared->published == 0) {
        return NGX_DECLINED;
    }

    if (entry->tag_count > 64) {
        return NGX_ERROR;
    }

    tags.checked = 0;
    ngx_shmtx_lock(&state->pool->mutex);
    if (url_only) {
        if (state->shared->complex != 0) {
            ngx_shmtx_unlock(&state->pool->mutex);
            return NGX_AGAIN;
        }
        low = 0;
        high = state->shared->url_count;
        while (low < high) {
            middle = (low + high) / 2;
            rule = state->shared->urls[middle];
            if (ngx_memn2cmp((u_char *) rule->select.value.data,
                            (u_char *) entry->url.data,
                            rule->select.value.len, entry->url.len) <= 0)
            {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        rule = low == 0 ? NULL : state->shared->urls[low - 1];
        for ( /* void */; rule != NULL; rule = rule->parent) {
            ngx_http_cache_purge_state_apply(rule, entry, now, match, 1, &tags);
        }
    } else {
        for (q = ngx_queue_head(&state->shared->rules);
             q != ngx_queue_sentinel(&state->shared->rules);
             q = ngx_queue_next(q))
        {
            rule = ngx_queue_data(q, ngx_http_cache_purge_rule_t, queue);
            ngx_http_cache_purge_state_apply(rule, entry, now, match,
                                              local_only, &tags);
        }
    }

    ngx_shmtx_unlock(&state->pool->mutex);

    return match->delete_before || match->invalidate_before
           || match->propagate_delete || match->propagate_invalidate
           ? NGX_OK : NGX_DECLINED;
}


ngx_int_t
ngx_http_cache_purge_state_match(ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_entry_t  *entry, uint64_t now,
    ngx_http_cache_purge_match_t *match)
{
    return ngx_http_cache_purge_state_select(state, entry, now, match, 0, 0);
}


ngx_int_t
ngx_http_cache_purge_state_match_local(ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_entry_t  *entry, uint64_t now,
    ngx_http_cache_purge_match_t *match)
{
    return ngx_http_cache_purge_state_select(state, entry, now, match, 0, 1);
}


/* NGX_AGAIN requires the caller to supply the remaining identity fields. */
ngx_int_t
ngx_http_cache_purge_state_match_url(ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_entry_t  *entry, uint64_t now,
    ngx_http_cache_purge_match_t *match)
{
    return ngx_http_cache_purge_state_select(state, entry, now, match, 1, 1);
}


ngx_uint_t
ngx_http_cache_purge_state_tagged(ngx_http_cache_purge_state_t *state,
    const ngx_http_cache_purge_entry_t *entry, uint64_t now)
{
    ngx_http_cache_purge_rule_t  *rule;
    ngx_queue_t                  *q;
    ngx_uint_t                    found;

    if (state->shared->published == 0) {
        return 0;
    }
    found = 0;
    ngx_shmtx_lock(&state->pool->mutex);
    for (q = ngx_queue_head(&state->shared->rules);
         q != ngx_queue_sentinel(&state->shared->rules);
         q = ngx_queue_next(q))
    {
        rule = ngx_queue_data(q, ngx_http_cache_purge_rule_t, queue);
        if (rule->published && rule->retain_until > now
            && rule->select.type == NGX_HTTP_CACHE_PURGE_SELECT_TAG
            && ngx_http_cache_purge_state_equal(rule->select.namespace_value,
                     rule->select.host_scope ? entry->host
                                             : entry->namespace_value))
        {
            found = 1;
            break;
        }
    }
    ngx_shmtx_unlock(&state->pool->mutex);
    return found;
}


void
ngx_http_cache_purge_state_collect(ngx_http_cache_purge_state_t *state,
    uint64_t now)
{
    ngx_queue_t                     *q, *next;
    ngx_http_cache_purge_rule_t     *rule;
    ngx_http_cache_purge_message_t  *message;
    ngx_pid_t                        pids[2 * NGX_HTTP_CACHE_PURGE_STATE_LIMIT];
    ngx_uint_t                       i, count, dead;

    ngx_shmtx_lock(&state->pool->mutex);
    if (state->shared->collected == now) {
        ngx_shmtx_unlock(&state->pool->mutex);
        return;
    }

    state->shared->collected = now;
    for (q = ngx_queue_head(&state->shared->rules);
         q != ngx_queue_sentinel(&state->shared->rules); q = next)
    {
        next = ngx_queue_next(q);
        rule = ngx_queue_data(q, ngx_http_cache_purge_rule_t, queue);
        if (!rule->local && rule->references == 0 && rule->retain_until <= now)
        {
            ngx_http_cache_purge_state_remove(state, rule);
        }
    }

    count = 0;
    for (q = ngx_queue_head(&state->shared->messages);
         q != ngx_queue_sentinel(&state->shared->messages);
         q = ngx_queue_next(q))
    {
        message = ngx_queue_data(q, ngx_http_cache_purge_message_t, all);
        if (message->owner != 0) {
            pids[count++] = message->owner;
        }

        if (message->state == 1) {
            pids[count++] = message->executor;
        }
    }

    ngx_shmtx_unlock(&state->pool->mutex);

    /* Process probes must not hold the lock used by request rule matching. */
    dead = 0;
    for (i = 0; i < count; i++) {
        if (kill(pids[i], 0) == -1 && ngx_errno == NGX_ESRCH) {
            pids[dead++] = pids[i];
        }
    }

    if (dead == 0) {
        return;
    }

    ngx_shmtx_lock(&state->pool->mutex);
    for (q = ngx_queue_head(&state->shared->messages);
         q != ngx_queue_sentinel(&state->shared->messages); q = next)
    {
        next = ngx_queue_next(q);
        message = ngx_queue_data(q, ngx_http_cache_purge_message_t, all);
        for (i = 0; i < dead; i++) {
            if (message->owner == pids[i]) {
                message->owner = 0;
            }

            if (message->state == 1 && message->executor == pids[i]) {
                message->state = 0;
                message->executor = 0;
                ngx_queue_insert_tail(&state->shared->pending,
                                      &message->pending);
            }
        }

        if (message->state == 2 && message->owner == 0) {
            ngx_http_cache_purge_state_free(state, message);
        }
    }

    ngx_shmtx_unlock(&state->pool->mutex);
}


void
ngx_http_cache_purge_state_stats(ngx_http_cache_purge_state_t *state,
    ngx_uint_t  *rules, ngx_uint_t *messages, size_t *bytes)
{
    ngx_shmtx_lock(&state->pool->mutex);
    *rules = state->shared->rule_count;
    *messages = state->shared->message_count;
    *bytes = state->shared->message_bytes;
    ngx_shmtx_unlock(&state->pool->mutex);
}


/*
 * Production work runs in the native cache manager; single mode uses a thread.
 */
typedef struct {
    ngx_http_cache_rocksdb_bytes_t  root, zone;
} ngx_http_cache_purge_target_t;


typedef struct {
    ngx_uint_t       target, action;
    ngx_file_uniq_t  uniq;
    u_char           hash[16];
} ngx_http_cache_purge_delta_t;


enum {
    NGX_HTTP_CACHE_PURGE_OWNER_OPEN = 0,
    NGX_HTTP_CACHE_PURGE_OWNER_MESSAGE,
    NGX_HTTP_CACHE_PURGE_OWNER_NEXT,
    NGX_HTTP_CACHE_PURGE_OWNER_STEP,
    NGX_HTTP_CACHE_PURGE_OWNER_MAINTAIN
};


struct ngx_http_cache_purge_owner_s {
    ngx_http_cache_purge_state_t    *state;
    ngx_http_file_cache_t          **caches;
    ngx_http_cache_purge_target_t   *targets;
    ngx_uint_t                       target_count;
    ngx_log_t                       *log;
    ngx_connection_t                *connection;
    ngx_event_t                      timer;
    ngx_http_cache_purge_message_t  *ticket;
    ngx_msec_t                       next_at, maintain_at;
    ngx_uint_t                       opened, restoring, restored, pending;
    ngx_uint_t                       selected, after_set, quota, threaded;
    int                              last_error;

    pthread_t                         thread;
    pthread_mutex_t                   mutex;
    pthread_cond_t                    condition;
    int                               phase, stopping, operation, result;
    int                               notify_fd, have_job;
    uint64_t                          now;
    u_char                            after[16], current[16];
    char                             *path;
    ngx_http_cache_rocksdb_options_t  options;
    ngx_http_cache_rocksdb_t         *store;
    ngx_http_cache_purge_catalog_t   *catalog;
    ngx_http_cache_purge_jobs_t      *jobs;
    ngx_http_cache_purge_work_t      *work;
    ngx_http_cache_rocksdb_error_t    error;
    ngx_http_cache_purge_delta_t      changes[NGX_HTTP_CACHE_PURGE_FILE_BATCH];
    ngx_uint_t                        change_count;
};


static void ngx_http_cache_purge_owner_drive(ngx_http_cache_purge_owner_t *o);
static void ngx_http_cache_purge_owner_notify(ngx_event_t *event);
static void ngx_http_cache_purge_owner_timer(ngx_event_t *event);
static void ngx_http_cache_purge_owner_exit(void);

static ngx_http_cache_purge_owner_t *ngx_http_cache_purge_manager_owner;


ngx_int_t
ngx_http_cache_purge_channel_init(ngx_cycle_t *cycle,
    ngx_http_cache_purge_state_t *state)
{
    ngx_pool_cleanup_t  *cleanup;

    if (pipe(state->channel) == -1) {
        return NGX_ERROR;
    }

    if (ngx_nonblocking(state->channel[0]) == -1
        || ngx_nonblocking(state->channel[1]) == -1
        || fcntl(state->channel[0], F_SETFD, FD_CLOEXEC) == -1
        || fcntl(state->channel[1], F_SETFD, FD_CLOEXEC) == -1)
    {
        ngx_http_cache_purge_channel_close(state);
        return NGX_ERROR;
    }

    cleanup = ngx_pool_cleanup_add(cycle->pool, 0);
    if (cleanup == NULL) {
        ngx_http_cache_purge_channel_close(state);
        return NGX_ERROR;
    }

    cleanup->handler = ngx_http_cache_purge_channel_close;
    cleanup->data = state;
    return NGX_OK;
}


void
ngx_http_cache_purge_channel_close(void *data)
{
    ngx_http_cache_purge_state_t  *state;
    ngx_uint_t                     i;

    state = data;
    for (i = 0; i < 2; i++) {
        if (state->channel[i] != -1) {
            (void) close(state->channel[i]);
            state->channel[i] = -1;
        }
    }
}


ngx_uint_t
ngx_http_cache_purge_state_ready(ngx_http_cache_purge_state_t *state)
{
    return state->shared->ready;
}


ngx_uint_t
ngx_http_cache_purge_state_active(ngx_http_cache_purge_state_t *state)
{
    return state->shared->published;
}


ngx_uint_t
ngx_http_cache_purge_state_filtering(ngx_http_cache_purge_state_t *state)
{
    return state->shared->filtering;
}


/* Local rules that require fields other than the URL. */
ngx_uint_t
ngx_http_cache_purge_state_complex(ngx_http_cache_purge_state_t *state)
{
    return state->shared->complex;
}


static int
ngx_http_cache_purge_owner_file(void *data,
    const ngx_http_cache_purge_entry_t  *entry, unsigned action,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_owner_t       *o;
    ngx_http_cache_purge_native_file_t  file;
    ngx_http_cache_purge_delta_t       *delta;
    ngx_uint_t                          i;
    ngx_int_t                           rc;
    char                                name[NGX_MAX_PATH];
    size_t                              length;

    o = data;
    for (i = 0; i < o->target_count; i++) {
        if (ngx_http_cache_purge_state_equal(entry->root, o->targets[i].root)
            && ngx_http_cache_purge_state_equal(entry->zone,
                                                  o->targets[i].zone))
        {
            break;
        }
    }

    length = entry->root.len + entry->path.len + 2;
    if (i == o->target_count || length > sizeof(name)
        || o->change_count == NGX_HTTP_CACHE_PURGE_FILE_BATCH)
    {
        error->code = NGX_HTTP_CACHE_ROCKSDB_INVALID;
        snprintf(error->message, sizeof(error->message),
                 "purge file has an unknown cache root/zone or invalid path");
        return error->code;
    }

    memcpy(name, entry->root.data, entry->root.len);
    name[entry->root.len] = '/';
    memcpy(name + entry->root.len + 1, entry->path.data, entry->path.len);
    name[length - 1] = 0;
    rc = ngx_http_cache_purge_native_file(name,
                       action == NGX_HTTP_CACHE_PURGE_INVALIDATE,
                       (ngx_file_uniq_t) entry->uniq, &file);
    if (rc == NGX_ERROR) {
        error->code = NGX_HTTP_CACHE_ROCKSDB_IO;
        snprintf(error->message, sizeof(error->message),
                 "native cache file operation failed: errno %d", file.error);
        return error->code;
    }

    if (rc == NGX_DECLINED && file.error != NGX_ENOENT) {
        return NGX_HTTP_CACHE_ROCKSDB_CONFLICT;
    }

    delta = &o->changes[o->change_count++];
    delta->target = i;
    delta->uniq = (ngx_file_uniq_t) entry->uniq;
    memcpy(delta->hash, entry->hash, 16);
    delta->action = rc == NGX_DECLINED || action == NGX_HTTP_CACHE_PURGE_DELETE
                    ? NGX_HTTP_CACHE_PURGE_NODE_DELETE
                    : NGX_HTTP_CACHE_PURGE_NODE_INVALIDATE;
    return rc == NGX_DECLINED ? NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND : 0;
}


static int
ngx_http_cache_purge_owner_result(ngx_http_cache_purge_owner_t *o,
    ngx_http_cache_purge_job_t *job)
{
    ngx_http_cache_purge_work_t  source;

    memset(&source, 0, sizeof(source));
    source.type = NGX_HTTP_CACHE_PURGE_SUBMIT;
    source.data.job = *job;
    if (ngx_http_cache_purge_state_size(&source) != NGX_OK) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }

    ngx_http_cache_purge_state_copy(o->work, &source);
    o->have_job = 1;
    return 0;
}


static void
ngx_http_cache_purge_owner_close(ngx_http_cache_purge_owner_t *o)
{
    ngx_http_cache_purge_jobs_close(o->jobs);
    ngx_http_cache_purge_catalog_close(o->catalog);
    o->jobs = NULL;
    o->catalog = NULL;
    if (o->store != NULL) {
        (void) ngx_http_cache_rocksdb_close(&o->store, NULL);
    }
}


static int
ngx_http_cache_purge_owner_tag(void *data, ngx_http_cache_rocksdb_bytes_t tag)
{
    ngx_http_cache_purge_owner_t  *o = data;
    ngx_http_cache_purge_match_t   match, *result;

    o->work->data.entry.tag_count = 1;
    o->work->data.entry.tags[0] = tag;
    (void) ngx_http_cache_purge_state_match(o->state, &o->work->data.entry,
                                            o->now, &match);
    result = &o->work->match;
    result->delete_before = ngx_max(result->delete_before, match.delete_before);
    result->invalidate_before = ngx_max(result->invalidate_before,
                                        match.invalidate_before);
    result->propagate_delete = ngx_max(result->propagate_delete,
                                       match.propagate_delete);
    result->propagate_invalidate = ngx_max(result->propagate_invalidate,
                                           match.propagate_invalidate);
    return 0;
}


static int
ngx_http_cache_purge_owner_records(ngx_http_cache_purge_owner_t *o)
{
    ngx_http_cache_purge_message_t  *message;
    ngx_queue_t                     *pending;
    ngx_uint_t                       i, ready;
    int                              rc;

    for (i = 0; /* void */; i++) {
        rc = ngx_http_cache_purge_catalog_put(o->catalog,
                                               &o->work->data.entry, &o->error);
        ngx_http_cache_purge_state_complete(o->state, o->ticket, rc, NULL);
        o->ticket = NULL;
        if (rc != 0 || i == 15) {
            return rc;
        }

        /* Only this owner consumes the queue. Stop before a management job. */
        pending = &o->state->shared->pending;
        ready = 0;
        ngx_shmtx_lock(&o->state->pool->mutex);
        if (!ngx_queue_empty(pending)) {
            message = ngx_queue_data(ngx_queue_head(pending),
                                      ngx_http_cache_purge_message_t, pending);
            ready = message->work.type == NGX_HTTP_CACHE_PURGE_RECORD;
        }
        ngx_shmtx_unlock(&o->state->pool->mutex);
        if (!ready
            || ngx_http_cache_purge_state_take(o->state, o->work, &o->ticket)
               != NGX_OK)
        {
            return 0;
        }
    }
}


static int
ngx_http_cache_purge_owner_execute(ngx_http_cache_purge_owner_t *o)
{
    ngx_http_cache_purge_job_t  job;
    size_t                      removed;
    int                         rc, duplicate, copied;

    memset(&o->error, 0, sizeof(o->error));
    memset(&job, 0, sizeof(job));
    o->have_job = 0;
    o->change_count = 0;
    rc = 0;

    switch (o->operation) {
    case NGX_HTTP_CACHE_PURGE_OWNER_OPEN:
        rc = ngx_http_cache_rocksdb_open(o->path, &o->options, &o->store,
                                         &o->error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_catalog_open(o->store, &o->catalog,
                                                   &o->error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_jobs_open(o->store, o->catalog,
                                                 &o->jobs, &o->error);
        }
        if (rc != 0) {
            ngx_http_cache_purge_owner_close(o);
        }
        return rc;

    case NGX_HTTP_CACHE_PURGE_OWNER_MESSAGE:
        if (o->work->type == NGX_HTTP_CACHE_PURGE_LOOKUP) {
            ngx_memzero(&o->work->match, sizeof(o->work->match));
            return ngx_http_cache_purge_catalog_tags(o->catalog,
                       &o->work->data.entry, ngx_http_cache_purge_owner_tag, o,
                       &o->error);
        }
        if (o->work->type == NGX_HTTP_CACHE_PURGE_RECORD) {
            return ngx_http_cache_purge_owner_records(o);
        }
        if (o->work->type == NGX_HTTP_CACHE_PURGE_SUBMIT) {
            rc = ngx_http_cache_purge_job_submit(o->jobs, &o->work->data.job,
                                                 &duplicate, &o->error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_job_get(o->jobs, o->work->data.job.id,
                                              &job, &o->error);
        }
        if (rc == 0) {
            rc = ngx_http_cache_purge_owner_result(o, &job);
        }
        return rc;

    case NGX_HTTP_CACHE_PURGE_OWNER_NEXT:
        rc = ngx_http_cache_purge_job_next(o->jobs,
                      o->after_set ? o->after : NULL, &job, &o->error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_owner_result(o, &job);
        }
        return rc;

    case NGX_HTTP_CACHE_PURGE_OWNER_STEP:
        rc = ngx_http_cache_purge_job_step(o->jobs, o->current, o->now,
                       NGX_HTTP_CACHE_PURGE_FILE_BATCH,
                       ngx_http_cache_purge_owner_file, o, &job, &o->error);
        if (job.issued != 0) {
            copied = ngx_http_cache_purge_owner_result(o, &job);
            if (copied != 0) {
                rc = copied;
            }
        }
        return rc;

    case NGX_HTTP_CACHE_PURGE_OWNER_MAINTAIN:
        rc = ngx_http_cache_purge_catalog_collect(o->catalog, o->now, 32,
                                                   &removed, &o->error);
        if (rc == 0) {
            rc = ngx_http_cache_rocksdb_sync(o->store, &o->error);
        }
        return rc;
    }

    return NGX_HTTP_CACHE_ROCKSDB_INVALID;
}


static void *
ngx_http_cache_purge_owner_thread(void *data)
{
    ngx_http_cache_purge_owner_t  *o;
    sigset_t                       set;
    struct sched_param             priority;

    o = data;
    sigfillset(&set);
    sigdelset(&set, SIGILL);
    sigdelset(&set, SIGFPE);
    sigdelset(&set, SIGSEGV);
    sigdelset(&set, SIGBUS);
    (void) pthread_sigmask(SIG_BLOCK, &set, NULL);
    (void) setpriority(PRIO_PROCESS, 0, 10);
    ngx_memzero(&priority, sizeof(priority));
    (void) pthread_setschedparam(pthread_self(), SCHED_BATCH, &priority);

    pthread_mutex_lock(&o->mutex);
    for ( ;; ) {
        while (o->phase != 1 && !o->stopping) {
            pthread_cond_wait(&o->condition, &o->mutex);
        }
        if (o->phase != 1 && o->stopping) {
            break;
        }
        o->phase = 2;
        pthread_mutex_unlock(&o->mutex);
        o->result = ngx_http_cache_purge_owner_execute(o);
        pthread_mutex_lock(&o->mutex);
        o->phase = 3;
        pthread_mutex_unlock(&o->mutex);
        (void) write(o->notify_fd, "d", 1);
        pthread_mutex_lock(&o->mutex);
    }
    pthread_mutex_unlock(&o->mutex);
    ngx_http_cache_purge_owner_close(o);
    return NULL;
}


static void
ngx_http_cache_purge_owner_deliver(ngx_http_cache_purge_owner_t *o)
{
    ngx_http_cache_purge_job_t    *job;
    ngx_http_cache_purge_delta_t  *delta;
    ngx_uint_t                     i, nodes_ok;
    ngx_int_t                      rc;

    nodes_ok = 1;
    for (i = 0; i < o->change_count; i++) {
        delta = &o->changes[i];
        rc = ngx_http_cache_purge_native_node(o->caches[delta->target],
                     delta->hash, delta->uniq, delta->action);
        if (rc == NGX_ERROR) {
            nodes_ok = 0;
            ngx_log_error(NGX_LOG_ALERT, o->log, 0,
                          "cache purge native node accounting failed");
        }
    }

    if (o->result != 0 && o->result != NGX_HTTP_CACHE_ROCKSDB_DONE
        && o->result != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
        && o->result != NGX_HTTP_CACHE_ROCKSDB_BUSY
        && o->last_error != o->result)
    {
        ngx_log_error(NGX_LOG_WARN, o->log, 0,
                      "cache purge background error %d: %s",
                      o->result, o->error.message);
    }
    o->last_error = o->result;
    job = o->have_job ? &o->work->data.job : NULL;

    switch (o->operation) {
    case NGX_HTTP_CACHE_PURGE_OWNER_OPEN:
        o->opened = o->result == 0;
        o->next_at = ngx_current_msec + (o->opened ? 0 : 1000);
        if (o->opened) {
            o->restoring = 1;
            o->pending = 1;
            ngx_shmtx_lock(&o->state->pool->mutex);
            o->state->shared->owner = ngx_pid;
            o->state->shared->ready = 0;
            ngx_shmtx_unlock(&o->state->pool->mutex);
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OWNER_MESSAGE:
        if (o->ticket != NULL) {
            ngx_http_cache_purge_state_complete(o->state, o->ticket, o->result,
                      o->work->type == NGX_HTTP_CACHE_PURGE_LOOKUP
                      ? (const void *) &o->work->match : (const void *) job);
            o->ticket = NULL;
        }
        if (o->result == 0 && job != NULL && !job->done) {
            o->pending = 1;
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OWNER_NEXT:
        if (o->result == 0 && job != NULL) {
            rc = ngx_http_cache_purge_state_install(o->state, job);
            if (rc != NGX_OK) {
                o->next_at = ngx_current_msec + 1000;
                break;
            }
            if (o->restoring) {
                o->restored = 1;
                ngx_memcpy(o->after, job->id, 16);
                o->after_set = 1;
            } else {
                ngx_memcpy(o->current, job->id, 16);
                o->selected = 1;
            }
        } else if (o->result == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            if (o->restoring) {
                o->restoring = 0;
                o->pending = o->restored;
                ngx_memory_barrier();
                o->state->shared->ready = 1;
            } else if (!o->after_set) {
                o->pending = 0;
            }
            o->after_set = 0;
        } else {
            o->next_at = ngx_current_msec + 1000;
        }
        break;

    case NGX_HTTP_CACHE_PURGE_OWNER_STEP:
        if (job != NULL && job->done && nodes_ok) {
            ngx_http_cache_purge_state_finish(o->state, job->id);
        }
        ngx_memcpy(o->after, o->current, 16);
        o->after_set = 1;
        o->selected = 0;
        /* File cleanup may be slow; foreground workers must keep running. */
        o->next_at = ngx_current_msec + (o->result == 0 ? 50 : 1000);
        break;

    case NGX_HTTP_CACHE_PURGE_OWNER_MAINTAIN:
        o->maintain_at = ngx_current_msec + 1000;
        break;
    }
}


static void
ngx_http_cache_purge_owner_drive(ngx_http_cache_purge_owner_t *o)
{
    ngx_uint_t  complete, queued;
    ngx_int_t   rc;
    ngx_msec_t  delay;
    int         operation;

    if (o->timer.timer_set) {
        ngx_del_timer(&o->timer);
    }
    pthread_mutex_lock(&o->mutex);
    complete = o->phase == 3;
    if (complete) {
        o->phase = 0;
    }
    rc = o->phase == 0 ? NGX_OK : NGX_AGAIN;
    pthread_mutex_unlock(&o->mutex);
    if (complete) {
        ngx_http_cache_purge_owner_deliver(o);
    }
    if (rc != NGX_OK) {
        ngx_add_timer(&o->timer, 100);
        return;
    }

    ngx_http_cache_purge_state_collect(o->state, (uint64_t) ngx_time());
    operation = -1;
    if (!o->opened) {
        if ((ngx_msec_int_t) (ngx_current_msec - o->next_at) >= 0) {
            operation = NGX_HTTP_CACHE_PURGE_OWNER_OPEN;
        }
    } else if ((ngx_msec_int_t) (ngx_current_msec - o->maintain_at) >= 0) {
        operation = NGX_HTTP_CACHE_PURGE_OWNER_MAINTAIN;
    } else if (o->pending && (o->restoring || o->quota >= 16)
               && (ngx_msec_int_t) (ngx_current_msec - o->next_at) >= 0)
    {
        operation = o->selected ? NGX_HTTP_CACHE_PURGE_OWNER_STEP
                               : NGX_HTTP_CACHE_PURGE_OWNER_NEXT;
        o->quota = 0;
    } else if (ngx_http_cache_purge_state_take(o->state, o->work, &o->ticket)
               == NGX_OK)
    {
        operation = NGX_HTTP_CACHE_PURGE_OWNER_MESSAGE;
        o->quota++;
    } else if (o->pending
               && (ngx_msec_int_t) (ngx_current_msec - o->next_at) >= 0)
    {
        operation = o->selected ? NGX_HTTP_CACHE_PURGE_OWNER_STEP
                               : NGX_HTTP_CACHE_PURGE_OWNER_NEXT;
        o->quota = 0;
    }

    if (operation != -1) {
        pthread_mutex_lock(&o->mutex);
        o->operation = operation;
        o->now = (uint64_t) ngx_time();
        o->phase = 1;
        if (o->threaded) {
            pthread_cond_signal(&o->condition);
        }
        pthread_mutex_unlock(&o->mutex);
        if (!o->threaded) {
            o->result = ngx_http_cache_purge_owner_execute(o);
            if (o->store != NULL && ngx_http_cache_purge_manager_owner == NULL)
            {
                /* Register after RocksDB initializes its process globals. */
                if (atexit(ngx_http_cache_purge_owner_exit) != 0) {
                    ngx_log_error(NGX_LOG_ALERT, o->log, 0,
                                  "cache purge manager cleanup failed");
                    ngx_http_cache_purge_owner_close(o);
                    exit(2);
                }
                ngx_http_cache_purge_manager_owner = o;
            }
            o->phase = 0;
            ngx_http_cache_purge_owner_deliver(o);
        }
    }
    delay = o->pending ? 10 : 100;
    if (operation != -1 && !o->threaded) {
        ngx_shmtx_lock(&o->state->pool->mutex);
        queued = !ngx_queue_empty(&o->state->shared->pending);
        ngx_shmtx_unlock(&o->state->pool->mutex);
        if (queued
            || (o->pending
                && (ngx_msec_int_t) (ngx_current_msec - o->next_at) >= 0))
        {
            delay = 1;
        }
    }
    ngx_add_timer(&o->timer, delay);
}


static void
ngx_http_cache_purge_owner_notify(ngx_event_t *event)
{
    ngx_connection_t              *c;
    ngx_http_cache_purge_owner_t  *o;
    u_char                         buffer[256];
    ssize_t                        n;

    c = event->data;
    o = c->data;
    do {
        n = read(c->fd, buffer, sizeof(buffer));
    } while (n > 0 || (n == -1 && ngx_errno == NGX_EINTR));
    (void) ngx_handle_read_event(event, 0);
    ngx_http_cache_purge_owner_drive(o);
}


static void
ngx_http_cache_purge_owner_timer(ngx_event_t *event)
{
    ngx_http_cache_purge_owner_drive(event->data);
}


ngx_int_t
ngx_http_cache_purge_owner_start(ngx_cycle_t *cycle,
    ngx_http_cache_purge_state_t            *state, const ngx_str_t *path,
    const ngx_http_cache_rocksdb_options_t  *options,
    ngx_http_file_cache_t                  **caches, ngx_uint_t count,
    ngx_uint_t threaded, ngx_http_cache_purge_owner_t **out)
{
    ngx_http_cache_purge_owner_t    *o;
    ngx_str_t                        names[2];
    ngx_uint_t                       i, j;
    ngx_http_cache_rocksdb_bytes_t  *field;
    u_char                          *p;
    pthread_attr_t                   attr;
    int                              rc;

    *out = NULL;
    o = ngx_pcalloc(cycle->pool, sizeof(*o));
    if (o == NULL) {
        return NGX_ERROR;
    }
    o->state = state;
    o->threaded = threaded;
    o->log = cycle->log;
    o->options = *options;
    o->notify_fd = state->channel[1];
    o->next_at = ngx_current_msec;
    o->maintain_at = ngx_current_msec + 1000;
    o->path = ngx_pnalloc(cycle->pool, path->len + 1);
    o->targets = ngx_pcalloc(cycle->pool, count * sizeof(*o->targets));
    o->caches = ngx_palloc(cycle->pool, count * sizeof(*o->caches));
    if (o->path == NULL || o->targets == NULL || o->caches == NULL) {
        return NGX_ERROR;
    }
    ngx_memcpy(o->path, path->data, path->len);
    o->path[path->len] = 0;
    o->target_count = count;
    for (i = 0; i < count; i++) {
        o->caches[i] = caches[i];
        names[0] = caches[i]->path->name;
        names[1] = caches[i]->shm_zone->shm.name;
        for (j = 0; j < 2; j++) {
            field = j ? &o->targets[i].zone : &o->targets[i].root;
            p = ngx_pnalloc(cycle->pool, names[j].len);
            if (p == NULL) {
                return NGX_ERROR;
            }
            ngx_memcpy(p, names[j].data, names[j].len);
            field->data = p;
            field->len = names[j].len;
        }
    }
    o->work = ngx_alloc(NGX_HTTP_CACHE_PURGE_MESSAGE_MAX, cycle->log);
    if (o->work == NULL) {
        return NGX_ERROR;
    }
    if (pthread_mutex_init(&o->mutex, NULL) != 0) {
        ngx_free(o->work);
        return NGX_ERROR;
    }
    if (pthread_cond_init(&o->condition, NULL) != 0) {
        pthread_mutex_destroy(&o->mutex);
        ngx_free(o->work);
        return NGX_ERROR;
    }
    if (threaded) {
        if (pthread_attr_init(&attr) != 0) {
            goto failed;
        }
        rc = pthread_attr_setstacksize(&attr, 1024 * 1024);
        if (rc == 0) {
            rc = pthread_create(&o->thread, &attr,
                                 ngx_http_cache_purge_owner_thread, o);
        }
        pthread_attr_destroy(&attr);
        if (rc != 0) {
            goto failed;
        }
    }

    o->connection = ngx_get_connection(state->channel[0], cycle->log);
    if (o->connection == NULL) {
        ngx_http_cache_purge_owner_stop(o);
        return NGX_ERROR;
    }
    o->connection->data = o;
    o->connection->read->handler = ngx_http_cache_purge_owner_notify;
    o->connection->read->log = cycle->log;
    o->timer.data = o;
    o->timer.handler = ngx_http_cache_purge_owner_timer;
    o->timer.log = cycle->log;
    o->timer.cancelable = 1;
    if (ngx_handle_read_event(o->connection->read, 0) != NGX_OK) {
        ngx_http_cache_purge_owner_stop(o);
        return NGX_ERROR;
    }
    *out = o;
    ngx_http_cache_purge_owner_drive(o);
    return NGX_OK;

failed:

    pthread_cond_destroy(&o->condition);
    pthread_mutex_destroy(&o->mutex);
    ngx_free(o->work);
    return NGX_ERROR;
}


void
ngx_http_cache_purge_owner_stop(ngx_http_cache_purge_owner_t *o)
{
    if (o == NULL) {
        return;
    }
    if (o->timer.timer_set) {
        ngx_del_timer(&o->timer);
    }
    pthread_mutex_lock(&o->mutex);
    o->stopping = 1;
    pthread_cond_signal(&o->condition);
    pthread_mutex_unlock(&o->mutex);
    if (o->threaded) {
        pthread_join(o->thread, NULL);
    } else {
        ngx_http_cache_purge_owner_close(o);
    }
    if (o->phase == 3) {
        ngx_http_cache_purge_owner_deliver(o);
    }
    ngx_shmtx_lock(&o->state->pool->mutex);
    if (o->state->shared->owner == ngx_pid) {
        o->state->shared->ready = 0;
        o->state->shared->owner = 0;
    }
    ngx_shmtx_unlock(&o->state->pool->mutex);
    if (o->connection != NULL) {
        ngx_close_connection(o->connection);
        o->state->channel[0] = -1;
    }
    pthread_cond_destroy(&o->condition);
    pthread_mutex_destroy(&o->mutex);
    ngx_free(o->work);
}


static void
ngx_http_cache_purge_owner_exit(void)
{
    ngx_http_cache_purge_owner_stop(ngx_http_cache_purge_manager_owner);
    ngx_http_cache_purge_manager_owner = NULL;
}
