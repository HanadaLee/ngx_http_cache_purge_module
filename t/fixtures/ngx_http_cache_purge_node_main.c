/* Exercise native node accounting with the real nginx structure layout. */
#include "../../ngx_http_cache_purge_runtime.c"

#include <assert.h>

static ngx_uint_t  ngx_http_cache_purge_test_busy;
static ngx_uint_t  ngx_http_cache_purge_test_locked;

ngx_uint_t
ngx_shmtx_trylock(ngx_shmtx_t *mutex)
{
    (void) mutex;
    assert(!ngx_http_cache_purge_test_locked);
    if (ngx_http_cache_purge_test_busy) {
        return 0;
    }
    ngx_http_cache_purge_test_locked = 1;
    return 1;
}

void
ngx_shmtx_unlock(ngx_shmtx_t *mutex)
{
    (void) mutex;
    assert(ngx_http_cache_purge_test_locked);
    ngx_http_cache_purge_test_locked = 0;
}

ngx_int_t
ngx_hextoi(u_char *line, size_t size)
{
    ngx_int_t  value, digit;

    value = 0;
    while (size--) {
        digit = *line++;
        if (digit >= '0' && digit <= '9') {
            digit -= '0';
        } else if (digit >= 'a' && digit <= 'f') {
            digit = digit - 'a' + 10;
        } else {
            return NGX_ERROR;
        }
        value = value * 16 + digit;
    }
    return value;
}

void
ngx_log_error_core(ngx_uint_t level, ngx_log_t *log, ngx_err_t error,
    const char *format, ...)
{
    (void) level;
    (void) log;
    (void) error;
    (void) format;
}

int
main(void)
{
    ngx_http_file_cache_t           cache;
    ngx_http_file_cache_sh_t        shared;
    ngx_http_file_cache_node_t      node;
    ngx_slab_pool_t                 pool;
    ngx_rbtree_node_t               sentinel;
    ngx_http_cache_purge_cleanup_t  cleanup;
    ngx_log_t                       log;
    u_char                          hash[16];
    ngx_uint_t                      i;
    int                             rc;
    static const unsigned char path[] =
        "a/00/000102030405060708090a0b0c0d0e0f";

    ngx_memzero(&cache, sizeof(cache));
    ngx_memzero(&shared, sizeof(shared));
    ngx_memzero(&node, sizeof(node));
    ngx_memzero(&pool, sizeof(pool));
    ngx_memzero(&sentinel, sizeof(sentinel));
    ngx_memzero(&cleanup, sizeof(cleanup));
    ngx_memzero(&log, sizeof(log));
    for (i = 0; i < 16; i++) {
        hash[i] = (u_char) i;
    }
    ngx_memcpy(&node.node.key, hash, sizeof(node.node.key));
    ngx_memcpy(node.key, hash + sizeof(node.node.key), sizeof(node.key));
    node.node.left = &sentinel;
    node.node.right = &sentinel;
    shared.rbtree.root = &node.node;
    shared.rbtree.sentinel = &sentinel;
    shared.size = 100;
    node.fs_size = 7;
    node.exists = 1;
    node.uniq = 42;
    cache.sh = &shared;
    cache.shpool = &pool;
    cleanup.inode = 42;
    cleanup.outcome = NGX_HTTP_CACHE_PURGE_REMOVED;
    cleanup.path.data = path;
    cleanup.path.len = sizeof(path) - 1;

    ngx_http_cache_purge_test_busy = 1;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == NGX_HTTP_CACHE_ROCKSDB_BUSY && shared.size == 100);
    ngx_http_cache_purge_test_busy = 0;
    node.count = 1;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == NGX_HTTP_CACHE_ROCKSDB_BUSY && shared.size == 100);
    node.count = 0;
    node.deleting = 1;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == NGX_HTTP_CACHE_ROCKSDB_BUSY && shared.size == 100);
    node.deleting = 0;
    shared.loading = 1;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == NGX_HTTP_CACHE_ROCKSDB_BUSY && shared.size == 100);
    shared.loading = 0;
    cleanup.inode = 43;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == 0 && shared.size == 100 && node.exists);
    cleanup.inode = 42;
    cleanup.outcome = NGX_HTTP_CACHE_PURGE_REPLACED;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == 0 && shared.size == 100 && node.exists);
    cleanup.outcome = NGX_HTTP_CACHE_PURGE_MISSING;
    node.fs_size = 101;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == NGX_HTTP_CACHE_ROCKSDB_INVALID && shared.size == 100);
    node.fs_size = 7;
    node.key[sizeof(node.key) - 1]++;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == 0 && shared.size == 100 && node.exists);
    node.key[sizeof(node.key) - 1]--;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == 0 && shared.size == 93 && !node.exists && !node.fs_size);
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == 0 && shared.size == 93);
    node.uniq = 0;
    node.exists = 1;
    node.fs_size = 3;
    rc = ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log);
    assert(rc == 0 && shared.size == 90 && !node.exists);
    cleanup.path.len = 31;
    assert(ngx_http_cache_purge_runtime_node(&cache, &cleanup, &log)
           == NGX_HTTP_CACHE_ROCKSDB_INVALID);
    assert(!ngx_http_cache_purge_test_locked);
    puts("PASS native node identity, accounting, contention and idempotence");
    return 0;
}
