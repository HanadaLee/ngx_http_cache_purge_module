#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>


static unsigned  ngx_http_cache_purge_test_syncs;
static unsigned  ngx_http_cache_purge_test_sync_error;

typedef struct {
    dev_t     device;
    ino_t     inode;
    unsigned  enabled, count, fail;
    unsigned  marker_fail;
    void    (*hook)(int, void *);
    void    (*marker_hook)(int, void *);
    void     *data;
} ngx_http_cache_purge_test_file_sync_t;

static __thread ngx_http_cache_purge_test_file_sync_t
    ngx_http_cache_purge_test_file_sync;

int ngx_http_cache_purge_test_fdatasync(int fd) __asm__("fdatasync");
int ngx_http_cache_purge_test_fsync(int fd) __asm__("fsync");
int ngx_http_cache_purge_test_fsetxattr(int fd, const char *name,
    const void *value, size_t size, int flags) __asm__("fsetxattr");


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

int
ngx_http_cache_purge_test_watch_file(int fd, unsigned fail,
    void (*hook)(int, void *), void *data)
{
    struct stat  st;

    memset(&ngx_http_cache_purge_test_file_sync, 0,
           sizeof(ngx_http_cache_purge_test_file_sync));
    if (fd < 0) {
        return 0;
    }
    if (fstat(fd, &st) != 0) {
        return -1;
    }
    ngx_http_cache_purge_test_file_sync.device = st.st_dev;
    ngx_http_cache_purge_test_file_sync.inode = st.st_ino;
    ngx_http_cache_purge_test_file_sync.enabled = 1;
    ngx_http_cache_purge_test_file_sync.fail = fail;
    ngx_http_cache_purge_test_file_sync.hook = hook;
    ngx_http_cache_purge_test_file_sync.data = data;
    return 0;
}

unsigned
ngx_http_cache_purge_test_file_sync_count(void)
{
    return ngx_http_cache_purge_test_file_sync.count;
}

void
ngx_http_cache_purge_test_watch_marker(unsigned fail,
    void (*hook)(int, void *))
{
    ngx_http_cache_purge_test_file_sync.marker_fail = fail;
    ngx_http_cache_purge_test_file_sync.marker_hook = hook;
}

int
ngx_http_cache_purge_test_fsetxattr(int fd, const char *name,
    const void *value, size_t size, int flags)
{
    ngx_http_cache_purge_test_file_sync_t  *watch;
    struct stat                             st;

    watch = &ngx_http_cache_purge_test_file_sync;
    if (watch->enabled && strcmp(name, "user.ngx_cache_purge.id") == 0
        && fstat(fd, &st) == 0 && st.st_dev == watch->device
        && st.st_ino == watch->inode)
    {
        if (watch->marker_hook != NULL) {
            watch->marker_hook(fd, watch->data);
        }
        if (watch->marker_fail) {
            errno = EIO;
            return -1;
        }
    }
    return (int) syscall(SYS_fsetxattr, fd, name, value, size, flags);
}


static int
ngx_http_cache_purge_test_sync(int fd, long operation)
{
    ngx_http_cache_purge_test_file_sync_t  *watch;
    struct stat                             st;
    char                                    name[64], target[4096];
    ssize_t                                 n;

    watch = &ngx_http_cache_purge_test_file_sync;
    if (watch->enabled && fstat(fd, &st) == 0
        && st.st_dev == watch->device && st.st_ino == watch->inode)
    {
        watch->count++;
        if (watch->hook != NULL) {
            watch->hook(fd, watch->data);
        }
        if (watch->count == watch->fail) {
            errno = EIO;
            return -1;
        }
    }

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
