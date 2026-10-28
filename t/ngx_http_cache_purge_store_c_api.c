#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>


static unsigned  ngx_http_cache_purge_test_syncs;
static unsigned  ngx_http_cache_purge_test_sync_error;

int ngx_http_cache_purge_test_fdatasync(int fd) __asm__("fdatasync");
int ngx_http_cache_purge_test_fsync(int fd) __asm__("fsync");


unsigned
ngx_http_cache_purge_test_sync_count(void)
{
    return __atomic_load_n(&ngx_http_cache_purge_test_syncs, __ATOMIC_RELAXED);
}


void
ngx_http_cache_purge_test_sync_fail(unsigned fail)
{
    __atomic_store_n(&ngx_http_cache_purge_test_sync_error, fail,
                     __ATOMIC_RELAXED);
}


static int
ngx_http_cache_purge_test_sync(int fd, long operation)
{
    char     name[64], target[4096];
    ssize_t  n;

    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    n = readlink(name, target, sizeof(target));
    if (n >= 4 && memcmp(target + n - 4, ".log", 4) == 0) {
        __atomic_add_fetch(&ngx_http_cache_purge_test_syncs, 1,
                           __ATOMIC_RELAXED);
        if (__atomic_load_n(&ngx_http_cache_purge_test_sync_error,
                            __ATOMIC_RELAXED))
        {
            errno = EIO;
            return -1;
        }
    }

    return (int) syscall(operation, fd);
}


/* Test executable only: count real WAL sync calls, including RocksDB calls. */
int
ngx_http_cache_purge_test_fdatasync(int fd)
{
    return ngx_http_cache_purge_test_sync(fd, SYS_fdatasync);
}


int
ngx_http_cache_purge_test_fsync(int fd)
{
    return ngx_http_cache_purge_test_sync(fd, SYS_fsync);
}

/* Compiled as C, so the test verifies the bridge's actual C ABI. */
int
ngx_http_cache_purge_store_c_probe(ngx_http_cache_rocksdb_t *store)
{
    static const unsigned char      key[] = "C:abi";
    unsigned char                   value[16];
    ngx_http_cache_rocksdb_bytes_t  input = { key, sizeof(key) - 1 };
    ngx_http_cache_rocksdb_buffer_t output = { value, sizeof(value), 0 };
    ngx_http_cache_rocksdb_error_t  error;
    int rc = ngx_http_cache_rocksdb_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                        input, &output, &error);
    return rc == NGX_HTTP_CACHE_ROCKSDB_OK && output.length == 5
                   && memcmp(value, "C ABI", 5) == 0 ?
               0 :
               1;
}
