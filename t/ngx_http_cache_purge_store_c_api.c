
/*
 * Copyright (C) Hanada
 */


#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>


static int ngx_http_cache_purge_test_sync(int fd, long operation);


static unsigned           ngx_http_cache_purge_test_syncs;
static unsigned           ngx_http_cache_purge_test_sync_error;
static __thread unsigned  ngx_http_cache_purge_test_direct_mode;
static __thread unsigned  ngx_http_cache_purge_test_direct_seen;
static __thread unsigned  ngx_http_cache_purge_test_direct_ready;
static __thread int       ngx_http_cache_purge_test_direct_fd = -1;

int ngx_http_cache_purge_test_fdatasync(int fd) __asm__("fdatasync");
int ngx_http_cache_purge_test_fsync(int fd) __asm__("fsync");
int ngx_http_cache_purge_test_open(const char *path, int flags,
    ...) __asm__("open");
ssize_t ngx_http_cache_purge_test_pwrite(int fd, const void *data, size_t size,
    off_t offset) __asm__("pwrite");
ssize_t ngx_http_cache_purge_test_pread(int fd, void *data, size_t size,
    off_t offset) __asm__("pread");


void
ngx_http_cache_purge_test_direct_fail(unsigned mode)
{
    ngx_http_cache_purge_test_direct_mode = mode;
    ngx_http_cache_purge_test_direct_seen = 0;
    ngx_http_cache_purge_test_direct_ready = 0;
    ngx_http_cache_purge_test_direct_fd = -1;
}


unsigned
ngx_http_cache_purge_test_direct_count(void)
{
    return ngx_http_cache_purge_test_direct_seen;
}


unsigned
ngx_http_cache_purge_test_direct_supported(void)
{
    return ngx_http_cache_purge_test_direct_ready;
}


int
ngx_http_cache_purge_test_open(const char *path, int flags, ...)
{
    int      fd, probe;
    mode_t   mode;
    va_list  args;

    mode = 0;

    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_start(args, flags);
        mode = va_arg(args, int);
        va_end(args);
    }

    probe = (flags & O_TMPFILE) == O_TMPFILE && (flags & O_DIRECT);

    if (probe && ngx_http_cache_purge_test_direct_mode == 1) {
        ngx_http_cache_purge_test_direct_seen++;
        errno = EOPNOTSUPP;

        return -1;
    }

    fd = (int) syscall(SYS_openat, AT_FDCWD, path, flags, mode);

    if (probe) {
        ngx_http_cache_purge_test_direct_fd = fd;
    }

    return fd;
}


ssize_t
ngx_http_cache_purge_test_pwrite(int fd, const void *data, size_t size,
    off_t offset)
{
    ssize_t  n;

    if (fd == ngx_http_cache_purge_test_direct_fd
        && ngx_http_cache_purge_test_direct_mode == 2)
    {
        ngx_http_cache_purge_test_direct_seen++;
        ngx_http_cache_purge_test_direct_fd = -1;
        errno = EINVAL;

        return -1;
    }

    n = syscall(SYS_pwrite64, fd, data, size, offset);

    if (fd == ngx_http_cache_purge_test_direct_fd && n != (ssize_t) size) {
        ngx_http_cache_purge_test_direct_fd = -1;
    }

    return n;
}


ssize_t
ngx_http_cache_purge_test_pread(int fd, void *data, size_t size, off_t offset)
{
    ssize_t  n;

    if (fd == ngx_http_cache_purge_test_direct_fd
        && ngx_http_cache_purge_test_direct_mode == 3)
    {
        ngx_http_cache_purge_test_direct_seen++;
        ngx_http_cache_purge_test_direct_fd = -1;
        errno = EINVAL;

        return -1;
    }

    n = syscall(SYS_pread64, fd, data, size, offset);

    if (fd == ngx_http_cache_purge_test_direct_fd) {
        if (n == (ssize_t) size) {
            ngx_http_cache_purge_test_direct_ready = 1;
        }

        ngx_http_cache_purge_test_direct_fd = -1;
    }

    return n;
}


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
    ssize_t  length;
    char     name[64], target[4096];

    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    length = readlink(name, target, sizeof(target));

    if (length >= 4 && memcmp(target + length - 4, ".log", 4) == 0) {
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
    static const unsigned char       key[] = "C:abi";
    unsigned char                    value[16];
    ngx_http_cache_rocksdb_bytes_t   input = { key, sizeof(key) - 1 };
    ngx_http_cache_rocksdb_buffer_t  output = { value, sizeof(value), 0 };
    ngx_http_cache_rocksdb_error_t   error;
    int                              rc =
        ngx_http_cache_rocksdb_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                   input, &output, &error);

    return rc == NGX_HTTP_CACHE_ROCKSDB_OK && output.length == 5
           && memcmp(value, "C ABI", 5) == 0 ?
               0 :
               1;
}
