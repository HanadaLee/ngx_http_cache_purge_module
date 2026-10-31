/* Copyright (C) Hanada */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_http_cache_purge.h"


/* The caller holds the native cache-zone mutex. */
static ngx_http_file_cache_node_t *
ngx_http_cache_purge_native_lookup(ngx_http_file_cache_t *cache,
    const u_char *key)
{
    ngx_rbtree_node_t           *node, *sentinel;
    ngx_http_file_cache_node_t  *file;
    ngx_rbtree_key_t             hash;
    ngx_int_t                    compare;

    ngx_memcpy(&hash, key, sizeof(hash));
    node = cache->sh->rbtree.root;
    sentinel = cache->sh->rbtree.sentinel;
    while (node != sentinel) {
        if (hash != node->key) {
            node = hash < node->key ? node->left : node->right;
            continue;
        }
        file = (ngx_http_file_cache_node_t *) node;
        compare = ngx_memcmp(key + sizeof(hash), file->key,
                            NGX_HTTP_CACHE_KEY_LEN - sizeof(hash));
        if (compare == 0) {
            return file;
        }
        node = compare < 0 ? node->left : node->right;
    }
    return NULL;
}


/* Read fill identity without retaining a node pointer or statting a file. */
ngx_int_t
ngx_http_cache_purge_native_identity(ngx_http_file_cache_t *cache,
    const u_char  *key, ngx_file_uniq_t *uniq)
{
    ngx_http_file_cache_node_t  *file;
    ngx_int_t                    rc;

    *uniq = 0;
    rc = NGX_DECLINED;
    ngx_shmtx_lock(&cache->shpool->mutex);
    file = ngx_http_cache_purge_native_lookup(cache, key);
    if (file != NULL && file->exists && file->uniq != 0) {
        *uniq = file->uniq;
        rc = NGX_OK;
    }
    ngx_shmtx_unlock(&cache->shpool->mutex);
    return rc;
}


/* Node state is touched only by a worker, under the native cache-zone lock. */
ngx_int_t
ngx_http_cache_purge_native_node(ngx_http_file_cache_t *cache,
    const u_char  *key, ngx_file_uniq_t expected, ngx_uint_t action)
{
    ngx_http_file_cache_node_t  *file;
    ngx_int_t                    rc;

    ngx_shmtx_lock(&cache->shpool->mutex);
    file = ngx_http_cache_purge_native_lookup(cache, key);
    if (file == NULL || (expected != 0 && expected != file->uniq)) {
        ngx_shmtx_unlock(&cache->shpool->mutex);
        return NGX_DECLINED;
    }
    rc = NGX_OK;
    switch (action) {
    case NGX_HTTP_CACHE_PURGE_NODE_READ:
        rc = file->purged;
        break;

    case NGX_HTTP_CACHE_PURGE_NODE_INVALIDATE:
        file->valid_sec = 0;
        file->error = 0;
        file->purged = 1;
        break;

    case NGX_HTTP_CACHE_PURGE_NODE_DELETE:
        if (file->fs_size < 0 || file->fs_size > cache->sh->size) {
            rc = NGX_ERROR;
            break;
        }
        cache->sh->size -= file->fs_size;
        file->fs_size = 0;
        file->exists = 0;
        file->error = 0;
        file->purged = 0;
        break;

    case NGX_HTTP_CACHE_PURGE_NODE_CLEAR:
        file->purged = 0;
        break;

    case NGX_HTTP_CACHE_PURGE_NODE_CLEAR_ERROR:
        if (file->error) {
            file->error = 0;
            file->valid_sec = 0;
        }
        break;

    default:
        rc = NGX_ERROR;
        break;
    }
    ngx_shmtx_unlock(&cache->shpool->mutex);
    return rc;
}


/* These operations run in the background executor, never on a cache HIT. */
ngx_int_t
ngx_http_cache_purge_native_file(const char *name, ngx_uint_t invalidate,
    ngx_file_uniq_t  expected, ngx_http_cache_purge_native_file_t *result)
{
    ngx_fd_t                      fd;
    ngx_file_info_t               info;
    ngx_http_file_cache_header_t  header;
    ngx_err_t                     err;
    ssize_t                       n;
    size_t                        size;
    time_t                        expired;
    u_char                       *p;
    ngx_int_t                     rc;

    ngx_memzero(result, sizeof(*result));

    fd = open(name, (invalidate ? O_RDWR : O_RDONLY) | O_CLOEXEC
                    | O_NOFOLLOW | O_NONBLOCK);
    if (fd == NGX_INVALID_FILE) {
        result->error = ngx_errno;
        return result->error == NGX_ENOENT ? NGX_DECLINED : NGX_ERROR;
    }

    rc = NGX_ERROR;
    err = 0;

    if (ngx_fd_info(fd, &info) == NGX_FILE_ERROR) {
        err = ngx_errno;
        goto done;
    }

    if (!ngx_is_file(&info)) {
        err = NGX_EINVAL;
        goto done;
    }

    result->uniq = ngx_file_uniq(&info);
    result->size = ngx_file_fs_size(&info);

    if (expected != 0 && expected != result->uniq) {
        rc = NGX_DECLINED;
        goto done;
    }

    if (!invalidate) {
        if (ngx_delete_file(name) == NGX_FILE_ERROR) {
            err = ngx_errno;
            rc = err == NGX_ENOENT ? NGX_DECLINED : NGX_ERROR;
        } else {
            rc = NGX_OK;
        }

        goto done;
    }

    p = (u_char *) &header;
    size = 0;

    while (size != sizeof(header)) {
        n = pread(fd, p + size, sizeof(header) - size, (off_t) size);
        if (n > 0) {
            size += n;
            continue;
        }

        if (n < 0 && ngx_errno == NGX_EINTR) {
            continue;
        }

        err = n < 0 ? ngx_errno : NGX_EINVAL;
        goto done;
    }

    if (header.version != NGX_HTTP_CACHE_VERSION
        || header.header_start < sizeof(header)
        || header.body_start < header.header_start
        || (off_t) header.body_start > ngx_file_size(&info))
    {
        err = NGX_EINVAL;
        goto done;
    }

    expired = 0;
    p = (u_char *) &expired;
    size = 0;

    while (size != sizeof(expired)) {
        n = pwrite(fd, p + size, sizeof(expired) - size,
                   (off_t) offsetof(ngx_http_file_cache_header_t, valid_sec)
                   + size);
        if (n > 0) {
            size += n;
            continue;
        }

        if (n < 0 && ngx_errno == NGX_EINTR) {
            continue;
        }

        err = n < 0 ? ngx_errno : EIO;
        goto done;
    }

    rc = NGX_OK;

done:

    if (ngx_close_file(fd) == NGX_FILE_ERROR && err == 0 && rc == NGX_OK) {
        err = ngx_errno;
        rc = NGX_ERROR;
    }

    result->error = err;

    return rc;
}
