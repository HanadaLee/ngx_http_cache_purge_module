#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/xattr.h>
#include <unistd.h>


static unsigned  ngx_http_cache_purge_test_syncs;
static unsigned  ngx_http_cache_purge_test_sync_error;
static unsigned  ngx_http_cache_purge_test_sync_cut;
static __thread unsigned ngx_http_cache_purge_test_direct_mode;
static __thread unsigned ngx_http_cache_purge_test_direct_seen;
static __thread unsigned ngx_http_cache_purge_test_direct_ready;
static __thread int ngx_http_cache_purge_test_direct_fd = -1;
static __thread unsigned ngx_http_cache_purge_test_wal_armed;
static __thread unsigned ngx_http_cache_purge_test_wal_paused;

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

typedef struct {
    unsigned char  header[24];
    size_t         received, total;
    int            fd, armed, paused, sent;
} ngx_http_cache_purge_test_reply_t;

static __thread ngx_http_cache_purge_test_reply_t
    ngx_http_cache_purge_test_reply;

int ngx_http_cache_purge_test_fdatasync(int fd) __asm__("fdatasync");
int ngx_http_cache_purge_test_open(const char *path, int flags, ...)
    __asm__("open");
ssize_t ngx_http_cache_purge_test_pwrite(int fd, const void *data, size_t size,
    off_t offset) __asm__("pwrite");
ssize_t ngx_http_cache_purge_test_pread(int fd, void *data, size_t size,
    off_t offset) __asm__("pread");
int ngx_http_cache_purge_test_fsync(int fd) __asm__("fsync");
int ngx_http_cache_purge_test_fsetxattr(int fd, const char *name,
    const void *value, size_t size, int flags) __asm__("fsetxattr");
int ngx_http_cache_purge_test_poll(struct pollfd *fds, nfds_t count,
    int timeout) __asm__("poll");
ssize_t ngx_http_cache_purge_test_recvmsg(int fd, struct msghdr *msg,
    int flags) __asm__("recvmsg");
ssize_t ngx_http_cache_purge_test_send(int fd, const void *data, size_t size,
    int flags) __asm__("send");
ssize_t ngx_http_cache_purge_test_sendmsg(int fd, const struct msghdr *msg,
    int flags) __asm__("sendmsg");

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
    va_list  args;
    mode_t   mode;
    int      fd, probe;

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

ssize_t
ngx_http_cache_purge_test_recvmsg(int fd, struct msghdr *msg, int flags)
{
    ngx_http_cache_purge_test_reply_t  *reply;
    const unsigned char                *data;
    size_t                              i;
    ssize_t                             n;

    n = syscall(SYS_recvmsg, fd, msg, flags);
    if (n <= 0 || getenv("CACHE_PURGE_TEST_REPLY_GATE") == NULL) {
        return n;
    }
    reply = &ngx_http_cache_purge_test_reply;
    if (reply->armed || msg->msg_iovlen != 1) {
        return n;
    }
    reply->fd = fd;
    data = msg->msg_iov[0].iov_base;
    for (i = 0; i < (size_t) n; i++) {
        if (reply->received < sizeof(reply->header)) {
            reply->header[reply->received] = data[i];
        }
        reply->received++;
        if (reply->received == sizeof(reply->header)) {
            reply->total = sizeof(reply->header)
                + ((size_t) reply->header[16] << 24)
                + ((size_t) reply->header[17] << 16)
                + ((size_t) reply->header[18] << 8) + reply->header[19];
        }
    }
    if (reply->received >= sizeof(reply->header)
        && reply->received == reply->total
        && memcmp(reply->header, "NCP1", 4) == 0)
    {
        reply->armed = 1;
    }
    return n;
}

int
ngx_http_cache_purge_test_poll(struct pollfd *fds, nfds_t count, int timeout)
{
    ngx_http_cache_purge_test_reply_t  *reply;
    const char                         *directory;
    char                                path[4096];
    int                                 marker;

    reply = &ngx_http_cache_purge_test_reply;
    directory = getenv("CACHE_PURGE_TEST_REPLY_GATE");
    if (directory != NULL && reply->armed && !reply->paused) {
        reply->paused = 1;
        snprintf(path, sizeof(path), "%s/paused", directory);
        marker = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0600);
        if (marker >= 0) {
            close(marker);
        }
        snprintf(path, sizeof(path), "%s/release", directory);
        while (access(path, F_OK) != 0) {
            usleep(1000);
        }
    }
    directory = getenv("CACHE_PURGE_TEST_PUBLICATION_WAL_GATE");
    if (directory != NULL && ngx_http_cache_purge_test_wal_armed
        && !ngx_http_cache_purge_test_wal_paused && timeout > 0)
    {
        snprintf(path, sizeof(path), "%s/idle-poll", directory);
        marker = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0600);
        if (marker >= 0) {
            close(marker);
        }
    }
    return (int) syscall(SYS_poll, fds, count, timeout);
}

static int
ngx_http_cache_purge_test_reply_fault(int fd, size_t *size)
{
    ngx_http_cache_purge_test_reply_t  *reply;
    const char                         *mode;

    reply = &ngx_http_cache_purge_test_reply;
    if (!reply->armed || fd != reply->fd || reply->sent++) {
        return 0;
    }
    mode = getenv("CACHE_PURGE_TEST_REPLY_MODE");
    if (mode != NULL && (strcmp(mode, "again") == 0
                        || strcmp(mode, "interrupted") == 0))
    {
        errno = strcmp(mode, "again") == 0 ? EAGAIN : EINTR;
        return -1;
    }
    if (mode != NULL && strcmp(mode, "partial") == 0 && *size > 7) {
        *size = 7;
    }
    return 0;
}

ssize_t
ngx_http_cache_purge_test_send(int fd, const void *data, size_t size, int flags)
{
    if (ngx_http_cache_purge_test_reply_fault(fd, &size) != 0) {
        return -1;
    }
    return syscall(SYS_sendto, fd, data, size, flags, NULL, 0);
}

ssize_t
ngx_http_cache_purge_test_sendmsg(int fd, const struct msghdr *msg, int flags)
{
    size_t  size;

    size = 0;
    if (ngx_http_cache_purge_test_reply_fault(fd, &size) != 0) {
        return -1;
    }
    return syscall(SYS_sendmsg, fd, msg, flags);
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

void
ngx_http_cache_purge_test_sync_exit_after(unsigned count)
{
    ngx_http_cache_purge_test_sync_cut =
        ngx_http_cache_purge_test_sync_count() + count;
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
    char                                    path[4096];
    const char                             *directory;
    unsigned char                           marker[40];
    ssize_t                                 n;
    int                                     notify;

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

    directory = getenv("CACHE_PURGE_TEST_PUBLICATION_WAL_GATE");
    if (directory != NULL && !ngx_http_cache_purge_test_wal_armed
        && fgetxattr(fd, "user.ngx_cache_purge.id", marker, sizeof(marker))
           == sizeof(marker)
        && memcmp(marker, "NCF1", 4) == 0 && marker[7] == 3)
    {
        ngx_http_cache_purge_test_wal_armed = 1;
        snprintf(path, sizeof(path), "%s/armed", directory);
        notify = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0600);
        if (notify >= 0) {
            close(notify);
        }
    }

    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    n = readlink(name, target, sizeof(target));
    if (n >= 4 && memcmp(target + n - 4, ".log", 4) == 0) {
        if (directory != NULL && ngx_http_cache_purge_test_wal_armed
            && !ngx_http_cache_purge_test_wal_paused)
        {
            ngx_http_cache_purge_test_wal_paused = 1;
            snprintf(path, sizeof(path), "%s/paused", directory);
            notify = open(path, O_CREAT | O_WRONLY | O_CLOEXEC, 0600);
            if (notify >= 0) {
                close(notify);
            }
            snprintf(path, sizeof(path), "%s/release", directory);
            while (access(path, F_OK) != 0) {
                usleep(1000);
            }
        }
        __atomic_add_fetch(&ngx_http_cache_purge_test_syncs, 1,
                           __ATOMIC_RELAXED);
        if (__atomic_load_n(&ngx_http_cache_purge_test_sync_error,
                            __ATOMIC_RELAXED))
        {
            errno = EIO;
            return -1;
        }
        if (ngx_http_cache_purge_test_sync_cut != 0
            && ngx_http_cache_purge_test_sync_count()
               == ngx_http_cache_purge_test_sync_cut)
        {
            if (syscall(operation, fd) == 0) {
                _exit(86);
            }
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
