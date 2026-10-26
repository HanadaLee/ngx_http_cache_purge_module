/* Copyright (C) Hanada */
#include "ngx_http_cache_purge_internal.h"
#include <openssl/sha.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/xattr.h>
#include <unistd.h>

static const char ngx_http_cache_purge_attribute[] = "user.ngx_cache_purge.id";
static const uint64_t ngx_http_cache_purge_last_offset =
    UINT64_C(0x3fffffffffffffff);
static int
ngx_http_cache_purge_result(ngx_http_cache_rocksdb_error_t *error, int code,
    const char *message)
{
    if (error) {
        error->code = code;
        snprintf(error->message, sizeof(error->message), "%s", message);
    }
    return code;
}
static int
ngx_http_cache_purge_io(ngx_http_cache_rocksdb_error_t *error,
    const char *operation)
{
    int saved = errno;
    int code = saved == EMFILE || saved == ENFILE || saved == ENOMEM
                       || saved == ENOLCK ?
                   NGX_HTTP_CACHE_ROCKSDB_LIMIT :
                   NGX_HTTP_CACHE_ROCKSDB_IO;
    if (error) {
        error->code = code;
        snprintf(error->message, sizeof(error->message), "%s: errno %d",
                 operation, saved);
    }
    return code;
}
static int
ngx_http_cache_purge_zero(const unsigned char *id)
{
    size_t    i;
    unsigned  value;

    value = 0;
    for (i = 0; i < 16; ++i) {
        value |= id[i];
    }
    return value == 0;
}
static int
ngx_http_cache_purge_sync_file(int fd, int durable,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (durable && fsync(fd) != 0) {
        return ngx_http_cache_purge_io(error, "sync file marker");
    }
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OK, "");
}
static void
ngx_http_cache_purge_encode(
    unsigned char raw[NGX_HTTP_CACHE_PURGE_FILE_MARKER_SIZE],
    const unsigned char *id, const unsigned char *address, unsigned state)
{
    memcpy(raw, "NCF1", 4);
    raw[4] = raw[5] = raw[6] = 0;
    raw[7] = state;
    memcpy(raw + 8, id, 16);
    memcpy(raw + 24, address, 16);
}
static int
ngx_http_cache_purge_lock_request(int fd, uint64_t offset,
    ngx_http_cache_rocksdb_error_t *error)
{
#if (defined(F_OFD_SETLK))
    struct flock lock;
#endif

    if (fd < 0 || offset > ngx_http_cache_purge_last_offset
        || offset > (((uint64_t) 1 << (sizeof(off_t) * CHAR_BIT - 1)) - 1))
    {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "invalid file lease descriptor/offset");
    }
#if (defined(F_OFD_SETLK))
    memset(&lock, 0, sizeof(lock));
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = (off_t) offset;
    lock.l_len = 1;
    if (fcntl(fd, F_OFD_SETLK, &lock) != 0) {
        if (errno == EAGAIN || errno == EACCES) {
            return ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_BUSY, "cache path lease is held");
        }
        if (errno == EINVAL || errno == EOPNOTSUPP) {
            return ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "filesystem/kernel lacks OFD cache leases");
        }
        return ngx_http_cache_purge_io(error, "file lease operation");
    }
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OK, "");
#else
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                       "build lacks OFD cache leases");
#endif
}
static int
ngx_http_cache_purge_inspect_locks(int fd, struct stat *st,
    ngx_http_cache_rocksdb_error_t *error)
{
    int flags;

    if (fd < 0) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid lease file descriptor");
    }
    if (fstat(fd, st) != 0) {
        return ngx_http_cache_purge_io(error, "inspect lease file");
    }
    if (!S_ISREG(st->st_mode) || st->st_nlink != 1 || st->st_size != 0
        || (st->st_mode & 0077))
    {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "lease file type, mode or links are invalid");
    }
    flags = fcntl(fd, F_GETFL);
    if (flags < 0) {
        return ngx_http_cache_purge_io(error, "inspect lease file access");
    }
    if ((flags & O_ACCMODE) != O_RDWR) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "lease file is not read/write");
    }
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OK, "");
}

int
ngx_http_cache_purge_file_address(ngx_http_cache_rocksdb_bytes_t address,
    unsigned char digest[16], uint64_t *offset,
    ngx_http_cache_rocksdb_error_t *error)
{
    size_t         i;
    uint64_t       value;
    unsigned char  hash[SHA256_DIGEST_LENGTH];

    if (!address.data || !address.len || address.len > 65536
        || address.data[0] != '/' || memchr(address.data, 0, address.len)
        || !digest || !offset)
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid canonical cache address");
    }
    if (!SHA256(address.data, address.len, hash)) {
        return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_IO,
                                           "address digest failed");
    }
    memcpy(digest, hash, 16);
    value = 0;
    for (i = 16; i < 24; ++i) {
        value = (value << 8) | hash[i];
    }
    *offset = value & ngx_http_cache_purge_last_offset;
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OK, "");
}

int
ngx_http_cache_purge_file_marker_read(int fd,
    ngx_http_cache_purge_file_marker_t *marker,
    ngx_http_cache_rocksdb_error_t *error)
{
    ssize_t        n;
    unsigned char  raw[NGX_HTTP_CACHE_PURGE_FILE_MARKER_SIZE];

    if (fd < 0 || !marker) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid file marker input");
    }
    memset(marker, 0, sizeof(*marker));
    n = fgetxattr(fd, ngx_http_cache_purge_attribute, raw, sizeof(raw));
    if (n < 0) {
        if (errno == ENODATA) {
            return ngx_http_cache_purge_result(error,
                                               NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
                                               "file has no instance marker");
        }
        if (errno == ERANGE) {
            return ngx_http_cache_purge_result(error,
                                               NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                               "file marker is oversized");
        }
        return ngx_http_cache_purge_io(error, "read file marker");
    }
    if (n != (ssize_t) sizeof(raw) || memcmp(raw, "NCF1", 4) || raw[4] || raw[5]
        || raw[6] || (raw[7] != 1 && raw[7] != 2)
        || ngx_http_cache_purge_zero(raw + 8))
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                           "unknown or malformed file marker");
    }
    marker->state = raw[7];
    memcpy(marker->id, raw + 8, 16);
    memcpy(marker->address, raw + 24, 16);
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OK, "");
}

int
ngx_http_cache_purge_file_marker_prepare(int fd, const unsigned char id[16],
    const unsigned char address[16], int durable,
    ngx_http_cache_rocksdb_error_t *error)
{
    int                                 rc;
    ngx_http_cache_purge_file_marker_t  current;

    unsigned char raw[NGX_HTTP_CACHE_PURGE_FILE_MARKER_SIZE];

    if (fd < 0 || !id || !address || ngx_http_cache_purge_zero(id)
        || (durable != 0 && durable != 1))
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid prepared file marker");
    }
    rc = ngx_http_cache_purge_file_marker_read(fd, &current, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_OK) {
        if (current.state != NGX_HTTP_CACHE_PURGE_FILE_PREPARED
            || memcmp(current.id, id, 16)
            || memcmp(current.address, address, 16))
        {
            return ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "file already has another identity or is readable");
        }
        return ngx_http_cache_purge_sync_file(fd, durable, error);
    }
    if (rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        return rc;
    }
    ngx_http_cache_purge_encode(raw, id, address,
                                NGX_HTTP_CACHE_PURGE_FILE_PREPARED);
    if (fsetxattr(fd, ngx_http_cache_purge_attribute, raw, sizeof(raw),
                  XATTR_CREATE)
        != 0)
    {
        if (errno == EEXIST) {
            return ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "file identity appeared concurrently");
        }
        return ngx_http_cache_purge_io(error, "prepare file marker");
    }
    return ngx_http_cache_purge_sync_file(fd, durable, error);
}

int
ngx_http_cache_purge_file_marker_publish(int fd, const unsigned char id[16],
    const unsigned char address[16], int durable,
    ngx_http_cache_rocksdb_error_t *error)
{
    int                                 rc;
    ngx_http_cache_purge_file_marker_t  current;

    unsigned char raw[NGX_HTTP_CACHE_PURGE_FILE_MARKER_SIZE];

    if (fd < 0 || !id || !address || ngx_http_cache_purge_zero(id)
        || (durable != 0 && durable != 1))
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid published file marker");
    }
    rc = ngx_http_cache_purge_file_marker_read(fd, &current, error);
    if (rc) {
        return rc;
    }
    if (memcmp(current.id, id, 16) || memcmp(current.address, address, 16)) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "file instance/address changed before publication");
    }
    if (current.state == NGX_HTTP_CACHE_PURGE_FILE_READY) {
        return ngx_http_cache_purge_sync_file(fd, durable, error);
    }
    ngx_http_cache_purge_encode(raw, id, address,
                                NGX_HTTP_CACHE_PURGE_FILE_READY);
    if (fsetxattr(fd, ngx_http_cache_purge_attribute, raw, sizeof(raw),
                  XATTR_REPLACE)
        != 0)
    {
        if (errno == ENODATA) {
            return ngx_http_cache_purge_result(error,
                                               NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                               "file identity disappeared");
        }
        return ngx_http_cache_purge_io(error, "publish file marker");
    }
    return ngx_http_cache_purge_sync_file(fd, durable, error);
}

int
ngx_http_cache_purge_file_locks_open(const char *path, uint32_t owner,
    int create, int *output, ngx_http_cache_rocksdb_error_t *error)
{
    int         created, fd, rc;
    struct stat st;

    if (output) {
        *output = -1;
    }
    if (!path || path[0] != '/' || !output || (create != 0 && create != 1)) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid lease file input");
    }
    created = 0;
    fd = -1;
    if (create) {
        fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                  0600);
        if (fd >= 0) {
            created = 1;
        } else if (errno != EEXIST) {
            return ngx_http_cache_purge_io(error, "create lease file");
        }
    }
    if (fd < 0) {
        fd = open(path, O_RDWR | O_NOFOLLOW | O_CLOEXEC);
    }
    if (fd < 0) {
        return ngx_http_cache_purge_io(error, "open lease file");
    }
    if (created
        && (fchown(fd, (uid_t) owner, (gid_t) -1) != 0
            || fchmod(fd, 0600) != 0))
    {
        rc = ngx_http_cache_purge_io(error, "set lease file ownership");
        close(fd);
        return rc;
    }
    if (fstat(fd, &st) != 0) {
        rc = ngx_http_cache_purge_io(error, "inspect lease file");
        close(fd);
        return rc;
    }
    if (!S_ISREG(st.st_mode) || st.st_nlink != 1 || st.st_size != 0
        || st.st_uid != owner || (st.st_mode & 0077))
    {
        close(fd);
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "lease file type, owner, mode or links are invalid");
    }
    *output = fd;
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OK, "");
}

int
ngx_http_cache_purge_file_lease_acquire(const char *path, int anchor_fd,
    uint64_t offset, int *output, ngx_http_cache_rocksdb_error_t *error)
{
    int         fd, rc;
    struct stat anchor;

    if (output) {
        *output = -1;
    }
    if (!output || offset > ngx_http_cache_purge_last_offset) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid lease output/offset");
    }
    rc = ngx_http_cache_purge_inspect_locks(anchor_fd, &anchor, error);
    if (rc) {
        return rc;
    }
    rc = ngx_http_cache_purge_file_locks_open(path, anchor.st_uid, 0, &fd,
                                              error);
    if (rc) {
        return rc;
    }
    rc = ngx_http_cache_purge_file_lease_adopt(anchor_fd, fd, offset, error);
    if (rc) {
        close(fd);
        return rc;
    }
    *output = fd;
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OK, "");
}

int
ngx_http_cache_purge_file_lease_adopt(int anchor_fd, int fd, uint64_t offset,
    ngx_http_cache_rocksdb_error_t *error)
{
    int         flags, rc;
    struct stat anchor, received;

    if (offset > ngx_http_cache_purge_last_offset) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid lease offset");
    }
    rc = ngx_http_cache_purge_inspect_locks(anchor_fd, &anchor, error);
    if (rc) {
        return rc;
    }
    rc = ngx_http_cache_purge_inspect_locks(fd, &received, error);
    if (rc) {
        return rc;
    }
    if (anchor.st_dev != received.st_dev || anchor.st_ino != received.st_ino
        || anchor.st_uid != received.st_uid)
    {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "lease descriptor names another lock file");
    }
    flags = fcntl(fd, F_GETFD);
    if (flags < 0 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return ngx_http_cache_purge_io(error, "set lease close-on-exec");
    }
    return ngx_http_cache_purge_lock_request(fd, offset, error);
}

int
ngx_http_cache_purge_file_lease_release(int *fd,
    ngx_http_cache_rocksdb_error_t *error)
{
    int value;

    if (!fd || *fd < 0) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid lease reference");
    }
    value = *fd;
    *fd = -1;
    /* On Linux close consumes the descriptor even on EINTR; never retry it. */
    if (close(value) != 0) {
        return ngx_http_cache_purge_io(error, "close lease reference");
    }
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OK, "");
}


ssize_t
ngx_http_cache_purge_transport_recv(int socket, void *data, size_t size,
    int allow_rights, int fds[NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS],
    size_t *count)
{
    union {
        struct cmsghdr aligned;
        unsigned char  bytes[CMSG_SPACE(3 * sizeof(int))];
    } control;
    struct iovec    iov = { data, size };
    struct msghdr    message;
    struct cmsghdr  *header;
    ssize_t          n;
    size_t          i, number;
    int             bad = 0, fd;

    if (!count || !fds || !data || !size || (allow_rights && size != 1)) {
        errno = EINVAL;
        return -1;
    }
    *count = 0;
    memset(&message, 0, sizeof(message));
    message.msg_iov = &iov;
    message.msg_iovlen = 1;
    message.msg_control = control.bytes;
    message.msg_controllen = sizeof(control.bytes);
    n = recvmsg(socket, &message, MSG_DONTWAIT | MSG_CMSG_CLOEXEC);
    if (n < 0) {
        return n;
    }
    bad = (message.msg_flags & (MSG_CTRUNC | MSG_TRUNC)) != 0;
    for (header = CMSG_FIRSTHDR(&message); header;
         header = CMSG_NXTHDR(&message, header))
    {
        if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS
            || header->cmsg_len < CMSG_LEN(0))
        {
            bad = 1;
            continue;
        }
        number = (header->cmsg_len - CMSG_LEN(0)) / sizeof(int);
        if (!number || (header->cmsg_len - CMSG_LEN(0)) % sizeof(int)) {
            bad = 1;
        }
        for (i = 0; i < number; i++) {
            memcpy(&fd, (unsigned char *) CMSG_DATA(header) + i * sizeof(int),
                   sizeof(fd));
            if (*count < NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS) {
                fds[(*count)++] = fd;
            } else {
                close(fd);
                bad = 1;
            }
        }
    }
    if ((!allow_rights || n != 1) && *count) {
        bad = 1;
    }
    if (bad) {
        for (i = 0; i < *count; i++) {
            close(fds[i]);
        }
        *count = 0;
        errno = EPROTO;
        return -1;
    }
    return n;
}

ssize_t
ngx_http_cache_purge_transport_send(int socket, const void *data, size_t size,
    const int *fds, size_t count)
{
    union {
        struct cmsghdr aligned;
        unsigned char  bytes[CMSG_SPACE(2 * sizeof(int))];
    } control;
    struct iovec    iov = { (void *) data, size };
    struct msghdr    message;
    struct cmsghdr  *header;
    if (count > NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS
        || (count && (!fds || size != 1)))
    {
        errno = EINVAL;
        return -1;
    }
    if (!count) {
        return send(socket, data, size, MSG_DONTWAIT | MSG_NOSIGNAL);
    }
    memset(&message, 0, sizeof(message));
    memset(&control, 0, sizeof(control));
    message.msg_iov = &iov;
    message.msg_iovlen = 1;
    message.msg_control = control.bytes;
    message.msg_controllen = CMSG_SPACE(count * sizeof(int));
    header = CMSG_FIRSTHDR(&message);
    header->cmsg_level = SOL_SOCKET;
    header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(count * sizeof(int));
    memcpy(CMSG_DATA(header), fds, count * sizeof(int));
    return sendmsg(socket, &message, MSG_DONTWAIT | MSG_NOSIGNAL);
}


typedef struct {
    ngx_http_cache_purge_root_t  configured;
    char                        *path;
    size_t                       length;
    int                          fd;
    void                        *storage;
} ngx_http_cache_purge_publication_root_t;

struct ngx_http_cache_purge_publications_s {
    ngx_http_cache_purge_publication_root_t  *roots;
    size_t                                    count;
    char                                     *database;
    pid_t                                     pid;
    pthread_t                                 thread;
};

typedef struct {
    uint64_t                        device;
    uint64_t                        inode;
    uint64_t                        size;
    uint64_t                        seconds;
    uint32_t                        nanoseconds;
    ngx_http_cache_rocksdb_bytes_t  temporary;
} ngx_http_cache_purge_publication_proof_t;

static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_publication_bytes(const char *value)
{
    ngx_http_cache_rocksdb_bytes_t result;

    result.data = (const unsigned char *) value;
    result.len = strlen(value);
    return result;
}

static int
ngx_http_cache_purge_publication_valid(ngx_http_cache_rocksdb_bytes_t value,
    size_t limit)
{
    return value.len <= limit && (value.len == 0 || value.data != NULL);
}

static int
ngx_http_cache_purge_publication_equal(ngx_http_cache_rocksdb_bytes_t a,
    ngx_http_cache_rocksdb_bytes_t b)
{
    return a.len == b.len && (a.len == 0 || memcmp(a.data, b.data, a.len) == 0);
}

static int
ngx_http_cache_purge_publication_relative(ngx_http_cache_rocksdb_bytes_t path)
{
    size_t at, end, length;

    if (!ngx_http_cache_purge_publication_valid(path, 32768) || path.len == 0
        || path.data[0] == '/' || path.data[path.len - 1] == '/'
        || memchr(path.data, 0, path.len))
    {
        return 0;
    }

    at = 0;
    while (at < path.len) {
        end = at;
        while (end < path.len && path.data[end] != '/') {
            end++;
        }
        length = end - at;
        if (length == 0 || (length == 1 && path.data[at] == '.')
            || (length == 2 && path.data[at] == '.'
                && path.data[at + 1] == '.'))
        {
            return 0;
        }
        at = end + 1;
    }
    return 1;
}

static int
ngx_http_cache_purge_publication_contains(ngx_http_cache_rocksdb_bytes_t root,
    ngx_http_cache_rocksdb_bytes_t path)
{
    return path.len > root.len && memcmp(root.data, path.data, root.len) == 0
           && path.data[root.len] == '/';
}

static int
ngx_http_cache_purge_publication_owner(ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (p == NULL || p->pid != getpid()
        || !pthread_equal(p->thread, pthread_self()))
    {
        return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_OWNER,
                                           "file publication owner mismatch");
    }
    return 0;
}

static void
ngx_http_cache_purge_publication_free(ngx_http_cache_purge_publications_t *p)
{
    size_t i;

    for (i = 0; i < p->count; i++) {
        if (p->roots[i].fd >= 0) {
            close(p->roots[i].fd);
        }
        free(p->roots[i].path);
        free(p->roots[i].storage);
    }
    free(p->roots);
    free(p->database);
    free(p);
}

int
ngx_http_cache_purge_publications_open(const ngx_http_cache_purge_root_t *roots,
    size_t count, const char *database,
    ngx_http_cache_purge_publications_t **out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publications_t      *p;
    ngx_http_cache_purge_publication_root_t  *root;
    ngx_http_cache_rocksdb_bytes_t           relative, canonical, db;
    unsigned char                           *bytes;
    size_t                                   i, total, size;
    int                                      rc;

    ngx_http_cache_purge_result(error, 0, "");
    if (out != NULL) {
        *out = NULL;
    }
    if (out == NULL || database == NULL || database[0] != '/' || count > 256
        || (count && roots == NULL))
    {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid publication roots");
    }
    total = 0;
    for (i = 0; i < count; i++) {
        if (!ngx_http_cache_purge_publication_valid(roots[i].path, 4096)
            || roots[i].path.len < 2 || roots[i].path.data[0] != '/'
            || !ngx_http_cache_purge_publication_valid(roots[i].zone, 128)
            || (roots[i].zone.len
                && memchr(roots[i].zone.data, 0, roots[i].zone.len))
            || (roots[i].kind != NGX_HTTP_CACHE_PURGE_ROOT_CACHE
                && roots[i].kind != NGX_HTTP_CACHE_PURGE_ROOT_TEMP)
            || (roots[i].kind == NGX_HTTP_CACHE_PURGE_ROOT_CACHE
                && roots[i].zone.len == 0))
        {
            return ngx_http_cache_purge_result(error,
                                               NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                               "invalid publication root");
        }
        relative.data = roots[i].path.data + 1;
        relative.len = roots[i].path.len - 1;
        if (!ngx_http_cache_purge_publication_relative(relative)) {
            return ngx_http_cache_purge_result(error,
                                               NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                               "unsafe registered root path");
        }
        total += roots[i].path.len + roots[i].zone.len;
        if (total > 65536) {
            return ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "publication roots exceed budget");
        }
    }
    p = calloc(1, sizeof(*p));
    if (p == NULL) {
        return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                           "publication allocation failed");
    }
    p->pid = getpid();
    p->thread = pthread_self();
    if (count) {
        p->roots = calloc(count, sizeof(*p->roots));
        if (p->roots == NULL) {
            ngx_http_cache_purge_publication_free(p);
            return ngx_http_cache_purge_result(error,
                                               NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                               "publication allocation failed");
        }
    }
    p->count = count;
    for (i = 0; i < count; i++) {
        p->roots[i].fd = -1;
    }
    p->database = realpath(database, NULL);
    if (p->database == NULL) {
        rc = ngx_http_cache_purge_io(error, "resolve database directory");
        goto failed;
    }
    db = ngx_http_cache_purge_publication_bytes(p->database);
    for (i = 0; i < count; i++) {
        root = &p->roots[i];
        size = roots[i].path.len + roots[i].zone.len + 2;
        bytes = malloc(size);
        if (bytes == NULL) {
            rc =
                ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                            "publication allocation failed");
            goto failed;
        }
        root->storage = bytes;
        root->configured = roots[i];
        memcpy(bytes, roots[i].path.data, roots[i].path.len);
        bytes[roots[i].path.len] = 0;
        root->configured.path.data = bytes;
        bytes += roots[i].path.len + 1;
        if (roots[i].zone.len) {
            memcpy(bytes, roots[i].zone.data, roots[i].zone.len);
        }
        bytes[roots[i].zone.len] = 0;
        root->configured.zone.data = bytes;
        root->path = realpath((const char *) root->configured.path.data, NULL);
        if (root->path == NULL) {
            rc = ngx_http_cache_purge_io(error, "resolve registered directory");
            goto failed;
        }
        root->length = strlen(root->path);
        if (root->length == 1 && root->path[0] == '/') {
            rc = ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "filesystem root cannot be a cache/temp directory");
            goto failed;
        }
        root->fd =
            open(root->path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (root->fd < 0) {
            rc = ngx_http_cache_purge_io(error, "open registered directory");
            goto failed;
        }
        canonical = ngx_http_cache_purge_publication_bytes(root->path);
        if (root->configured.kind == NGX_HTTP_CACHE_PURGE_ROOT_CACHE
            && (ngx_http_cache_purge_publication_equal(canonical, db)
                || ngx_http_cache_purge_publication_contains(canonical, db)
                || ngx_http_cache_purge_publication_contains(db, canonical)))
        {
            rc = ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "database and cache roots overlap");
            goto failed;
        }
    }
    *out = p;
    return 0;

failed:
    ngx_http_cache_purge_publication_free(p);
    return rc;
}

int
ngx_http_cache_purge_publications_close(
    ngx_http_cache_purge_publications_t **out,
    ngx_http_cache_rocksdb_error_t *error)
{
    int rc;

    ngx_http_cache_purge_result(error, 0, "");
    if (out == NULL || *out == NULL) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null publication close");
    }
    rc = ngx_http_cache_purge_publication_owner(*out, error);
    if (rc != 0) {
        return rc;
    }
    ngx_http_cache_purge_publication_free(*out);
    *out = NULL;
    return 0;
}

static int
ngx_http_cache_purge_publication_verify(
    ngx_http_cache_purge_publication_root_t *root,
    ngx_http_cache_rocksdb_error_t *error)
{
    struct stat pinned, current;
    int         fd, rc;

    fd = open(root->path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "registered directory disappeared or became inaccessible");
    }
    rc = 0;
    if (fstat(root->fd, &pinned) != 0 || fstat(fd, &current) != 0) {
        rc = ngx_http_cache_purge_io(error, "inspect registered directory");
    } else if (pinned.st_dev != current.st_dev
               || pinned.st_ino != current.st_ino)
    {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "registered directory was replaced; restart required");
    }
    close(fd);
    return rc;
}

/*
 * Walk pinned directories with constant descriptor space; never follow links.
 */
static int
ngx_http_cache_purge_publication_beneath(int root,
    ngx_http_cache_rocksdb_bytes_t path, int missing, int writable, int *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    char *copy, *part, *slash;
    int   current, next, flags, rc;

    *out = -1;
    if (!ngx_http_cache_purge_publication_relative(path)) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "unsafe relative file path");
    }
    copy = malloc(path.len + 1);
    if (copy == NULL) {
        return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                           "publication allocation failed");
    }
    memcpy(copy, path.data, path.len);
    copy[path.len] = 0;
    current = fcntl(root, F_DUPFD_CLOEXEC, 0);
    if (current < 0) {
        rc = ngx_http_cache_purge_io(error, "duplicate registered directory");
        free(copy);
        return rc;
    }
    part = copy;
    rc = 0;
    for ( ;; ) {
        slash = strchr(part, '/');
        if (slash != NULL) {
            *slash = 0;
        }
        flags = slash == NULL && writable ? O_RDWR : O_RDONLY;
        flags |= O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK;
        if (slash != NULL) {
            flags |= O_DIRECTORY;
        }
        next = openat(current, part, flags);
        if (next < 0) {
            if (errno == ENOENT && missing) {
                break;
            }
            if (errno == ENOENT || errno == ELOOP || errno == ENOTDIR
                || errno == EACCES)
            {
                rc = ngx_http_cache_purge_result(
                    error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                    "file path is missing, inaccessible or contains a symlink");
            } else {
                rc =
                    ngx_http_cache_purge_io(error, "open registered file path");
            }
            break;
        }
        if (slash == NULL) {
            *out = next;
            break;
        }
        close(current);
        current = next;
        part = slash + 1;
    }
    close(current);
    free(copy);
    return rc;
}

static ngx_http_cache_purge_publication_root_t *
ngx_http_cache_purge_publication_find(ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_rocksdb_bytes_t root, ngx_http_cache_rocksdb_bytes_t zone,
    int configured)
{
    size_t i;

    for (i = 0; i < p->count; i++) {
        if (p->roots[i].configured.kind == NGX_HTTP_CACHE_PURGE_ROOT_CACHE
            && ngx_http_cache_purge_publication_equal(
                p->roots[i].configured.zone, zone)
            && (ngx_http_cache_purge_publication_equal(
                    ngx_http_cache_purge_publication_bytes(p->roots[i].path),
                    root)
                || (configured
                    && ngx_http_cache_purge_publication_equal(
                        p->roots[i].configured.path, root))))
        {
            return &p->roots[i];
        }
    }
    return NULL;
}

static int
ngx_http_cache_purge_publication_address(
    ngx_http_cache_purge_publication_root_t *root,
    ngx_http_cache_rocksdb_bytes_t path, ngx_http_cache_rocksdb_bytes_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char *value;

    if (!ngx_http_cache_purge_publication_relative(path)) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "unsafe target path");
    }
    value = malloc(root->length + 1 + path.len + 1);
    if (value == NULL) {
        return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                           "publication allocation failed");
    }
    memcpy(value, root->path, root->length);
    value[root->length] = '/';
    memcpy(value + root->length + 1, path.data, path.len);
    value[root->length + 1 + path.len] = 0;
    out->data = value;
    out->len = root->length + 1 + path.len;
    return 0;
}

void
ngx_http_cache_purge_publication_target_free(
    ngx_http_cache_purge_publication_target_t *target)
{
    if (target != NULL) {
        free(target->storage);
        memset(target, 0, sizeof(*target));
    }
}

int
ngx_http_cache_purge_publications_target(ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_bytes_t root,
    ngx_http_cache_rocksdb_bytes_t zone, ngx_http_cache_rocksdb_bytes_t path,
    ngx_http_cache_purge_publication_target_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_root_t  *target;
    ngx_http_cache_purge_index_match_t        current;
    int                                       rc;

    ngx_http_cache_purge_result(error, 0, "");
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (out == NULL || store == NULL
        || !ngx_http_cache_purge_publication_valid(root, 32768)
        || !ngx_http_cache_purge_publication_valid(zone, 32768)
        || !ngx_http_cache_purge_publication_relative(path))
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid publication target");
    }
    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    target = ngx_http_cache_purge_publication_find(p, root, zone, 1);
    if (target == NULL) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "unregistered cache root/zone");
    }
    rc = ngx_http_cache_purge_publication_verify(target, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_address(target, path,
                                                      &out->address, error);
    }
    if (rc != 0) {
        return rc;
    }
    out->storage = (void *) out->address.data;
    out->root.data = out->address.data;
    out->root.len = target->length;
    rc = ngx_http_cache_purge_index_current(store, out->root, path, &current,
                                            error);
    if (rc == 0) {
        memcpy(out->previous, current.id, 16);
    } else if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        rc = ngx_http_cache_purge_result(error, 0, "");
    } else {
        ngx_http_cache_purge_publication_target_free(out);
    }
    return rc;
}

static int
ngx_http_cache_purge_publication_sync_parents(
    ngx_http_cache_purge_publication_root_t *root,
    ngx_http_cache_rocksdb_bytes_t relative,
    ngx_http_cache_rocksdb_error_t *error)
{
    size_t slash;
    int    fd, rc;

    for ( ;; ) {
        slash = relative.len;
        while (slash && relative.data[slash - 1] != '/') {
            slash--;
        }
        if (slash == 0) {
            if (fsync(root->fd) != 0) {
                return ngx_http_cache_purge_io(error,
                                               "sync registered cache root");
            }
            return 0;
        }
        relative.len = slash - 1;
        rc = ngx_http_cache_purge_publication_beneath(root->fd, relative, 1, 0,
                                                      &fd, error);
        if (rc != 0) {
            return rc;
        }
        if (fd >= 0) {
            if (fsync(fd) != 0) {
                rc = ngx_http_cache_purge_io(error,
                                             "sync publication directory");
            }
            close(fd);
            if (rc != 0) {
                return rc;
            }
        }
    }
}

static uint64_t
ngx_http_cache_purge_publication_number(const unsigned char *p, unsigned n)
{
    uint64_t value;

    value = 0;
    while (n--) {
        value = (value << 8) | *p++;
    }
    return value;
}

static unsigned char *
ngx_http_cache_purge_publication_encode(unsigned char *p, uint64_t value,
    unsigned width)
{
    while (width) {
        *p++ = (unsigned char) (value >> (--width * 8));
    }
    return p;
}

static int
ngx_http_cache_purge_publication_proof(ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_publication_proof_t *proof,
    ngx_http_cache_rocksdb_error_t *error)
{
    size_t length, at;

    if (!ngx_http_cache_purge_publication_valid(metadata, 16384)
        || metadata.len < 48 || memcmp(metadata.data, "NFP1", 4) != 0)
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "file has no publication proof");
    }
    proof->device =
        ngx_http_cache_purge_publication_number(metadata.data + 4, 8);
    proof->inode =
        ngx_http_cache_purge_publication_number(metadata.data + 12, 8);
    proof->size =
        ngx_http_cache_purge_publication_number(metadata.data + 20, 8);
    proof->seconds =
        ngx_http_cache_purge_publication_number(metadata.data + 28, 8);
    proof->nanoseconds = (uint32_t) ngx_http_cache_purge_publication_number(
        metadata.data + 36, 4);
    length =
        (size_t) ngx_http_cache_purge_publication_number(metadata.data + 40, 4);
    if (length > metadata.len - 48) {
        return ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "truncated publication temporary path");
    }
    proof->temporary.data = metadata.data + 44;
    proof->temporary.len = length;
    at = 44 + length;
    length =
        (size_t) ngx_http_cache_purge_publication_number(metadata.data + at, 4);
    if (length != metadata.len - at - 4 || proof->nanoseconds >= 1000000000
        || proof->temporary.len == 0 || proof->temporary.data[0] != '/')
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid publication identity");
    }
    return 0;
}

static int
ngx_http_cache_purge_publication_same(
    const ngx_http_cache_purge_publication_proof_t *proof,
    const struct stat *st)
{
    return proof->device == (uint64_t) st->st_dev
           && proof->inode == (uint64_t) st->st_ino
           && proof->size == (uint64_t) st->st_size
           && proof->seconds == (uint64_t) st->st_mtim.tv_sec
           && proof->nanoseconds == (uint32_t) st->st_mtim.tv_nsec;
}

static int
ngx_http_cache_purge_publication_apply(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_purge_index_action_e action, const unsigned char *id,
    uint64_t revision, ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_expected_t expected;

    memcpy(expected.id, id, 16);
    expected.revision = revision;
    return ngx_http_cache_purge_index_apply(store, action, &expected, 1, out,
                                            error);
}

int
ngx_http_cache_purge_publications_inspect(
    ngx_http_cache_purge_publications_t *p, ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_bytes_t root, ngx_http_cache_rocksdb_bytes_t zone,
    ngx_http_cache_rocksdb_bytes_t path, int fd, uint32_t peer_uid,
    ngx_http_cache_purge_index_file_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_root_t   *target;
    ngx_http_cache_purge_publication_proof_t   proof;
    ngx_http_cache_purge_index_file_t          file;
    ngx_http_cache_purge_index_match_t         current;
    ngx_http_cache_purge_file_marker_t         marker, after;
    ngx_http_cache_rocksdb_bytes_t             address;
    struct stat                              received, named;
    unsigned char                            digest[16];
    uint64_t                                 offset;
    int                                      rc, flags, intent, named_fd;

    ngx_http_cache_purge_result(error, 0, "");
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (out == NULL || store == NULL
        || !ngx_http_cache_purge_publication_valid(root, 32768)
        || !ngx_http_cache_purge_publication_valid(zone, 32768)
        || !ngx_http_cache_purge_publication_relative(path))
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid cache file inspection");
    }
    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    target = ngx_http_cache_purge_publication_find(p, root, zone, 1);
    if (target == NULL) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "unregistered cache root/zone");
    }
    rc = ngx_http_cache_purge_publication_verify(target, error);
    if (rc != 0) {
        return rc;
    }
    flags = fcntl(fd, F_GETFL);
    if (flags < 0 || (flags & O_ACCMODE) == O_WRONLY || (flags & O_PATH)) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "cache descriptor is not readable");
    }
    if (fstat(fd, &received) != 0) {
        return ngx_http_cache_purge_io(error, "inspect cache descriptor");
    }
    if (!S_ISREG(received.st_mode) || received.st_uid != peer_uid) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "cache file type or owner mismatch");
    }
    if (received.st_nlink != 1) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                           "cache descriptor is not exclusive");
    }
    memset(&file, 0, sizeof(file));
    memset(&address, 0, sizeof(address));
    named_fd = -1;
    rc = ngx_http_cache_purge_file_marker_read(fd, &marker, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT) {
        rc = ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                           "cache file marker is malformed");
    }
    if (rc != 0) {
        goto done;
    }
    if (marker.state != NGX_HTTP_CACHE_PURGE_FILE_READY) {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
                                           "cache file publication is pending");
        goto done;
    }
    rc = ngx_http_cache_purge_publication_address(target, path, &address,
                                                  error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_address(address, digest, &offset, error);
    }
    if (rc != 0) {
        goto done;
    }
    if (memcmp(marker.address, digest, 16) != 0) {
        goto mismatch;
    }
    rc = ngx_http_cache_purge_index_get_file(store, marker.id, &file, error);
    if (rc != 0) {
        goto done;
    }
    if (file.state != NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED
        || !ngx_http_cache_purge_publication_equal(
            file.root, ngx_http_cache_purge_publication_bytes(target->path))
        || !ngx_http_cache_purge_publication_equal(file.zone, zone)
        || !ngx_http_cache_purge_publication_equal(file.path, path))
    {
        goto mismatch;
    }
    rc = ngx_http_cache_purge_index_has_intent(store, marker.id, &intent,
                                               error);
    if (rc != 0) {
        goto done;
    }
    if (intent) {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "cache index publication is pending");
        goto done;
    }
    rc = ngx_http_cache_purge_index_current(store, file.root, path, &current,
                                             error);
    if (rc != 0) {
        goto done;
    }
    if (memcmp(current.id, marker.id, 16) != 0
        || current.revision != file.revision || current.state != file.state)
    {
        goto mismatch;
    }
    rc = ngx_http_cache_purge_publication_proof(file.metadata, &proof, error);
    if (rc != 0) {
        goto done;
    }
    rc = ngx_http_cache_purge_publication_beneath(target->fd, path, 1, 0,
                                                  &named_fd, error);
    if (rc != 0) {
        goto done;
    }
    if (named_fd < 0) {
        goto mismatch;
    }
    if (fstat(fd, &received) != 0 || fstat(named_fd, &named) != 0) {
        rc = ngx_http_cache_purge_io(error, "recheck selected cache instance");
        goto done;
    }
    if (!ngx_http_cache_purge_publication_same(&proof, &received)
        || !ngx_http_cache_purge_publication_same(&proof, &named)
        || received.st_nlink != 1 || named.st_nlink != 1
        || received.st_uid != peer_uid || named.st_uid != peer_uid)
    {
        goto mismatch;
    }
    rc = ngx_http_cache_purge_file_marker_read(fd, &after, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
        || rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT)
    {
        goto mismatch;
    }
    if (rc != 0) {
        goto done;
    }
    if (after.state != marker.state || memcmp(after.id, marker.id, 16)
        || memcmp(after.address, marker.address, 16))
    {
        goto mismatch;
    }
    *out = file;
    memset(&file, 0, sizeof(file));
    goto done;

mismatch:
    rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                       "cache descriptor and index disagree");
done:
    if (named_fd >= 0) {
        close(named_fd);
    }
    free((void *) address.data);
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}

int
ngx_http_cache_purge_publications_prepare(
    ngx_http_cache_purge_publications_t *p, ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_fill_t *fill,
    ngx_http_cache_rocksdb_bytes_t temporary, int anchor, int lease,
    int temporary_fd, uint32_t peer_uid,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_root_t *target, *source;
    ngx_http_cache_purge_index_file_t   file;
    ngx_http_cache_purge_index_match_t  match;
    ngx_http_cache_purge_file_marker_t  marker;
    ngx_http_cache_rocksdb_bytes_t address, temp_address, relative, prefix;
    struct stat                    received, named, existing, after;
    unsigned char                  digest[16], *proof, *next;
    uint64_t                       offset;
    size_t                         i, pass, longest, size;
    int                            rc, flags, named_fd, current_fd;

    ngx_http_cache_purge_result(error, 0, "");
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (store == NULL || fill == NULL || out == NULL
        || !ngx_http_cache_purge_publication_valid(temporary, 32768)
        || !ngx_http_cache_purge_publication_valid(fill->file.root, 32768)
        || !ngx_http_cache_purge_publication_valid(fill->file.zone, 32768)
        || !ngx_http_cache_purge_publication_valid(fill->file.metadata, 16384))
    {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid publication preparation");
    }
    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    file = fill->file;
    target = ngx_http_cache_purge_publication_find(p, file.root, file.zone, 1);
    if (target == NULL) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "unregistered cache root/zone");
    }
    rc = ngx_http_cache_purge_publication_verify(target, error);
    if (rc != 0) {
        return rc;
    }
    memset(&address, 0, sizeof(address));
    memset(&temp_address, 0, sizeof(temp_address));
    memset(&relative, 0, sizeof(relative));
    proof = NULL;
    named_fd = -1;
    current_fd = -1;
    file.root = ngx_http_cache_purge_publication_bytes(target->path);
    rc = ngx_http_cache_purge_publication_address(target, file.path, &address,
                                                  error);
    if (rc != 0) {
        goto done;
    }
    if (ngx_http_cache_purge_publication_equal(temporary, address)) {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "temporary file cannot be the final cache path");
        goto done;
    }
    flags = fcntl(temporary_fd, F_GETFL);
    if (flags < 0 || (flags & O_ACCMODE) != O_RDWR) {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "temporary descriptor is not read/write");
        goto done;
    }
    if (fstat(temporary_fd, &received) != 0) {
        rc = ngx_http_cache_purge_io(error, "inspect temporary descriptor");
        goto done;
    }
    if (!S_ISREG(received.st_mode) || received.st_nlink != 1
        || received.st_uid != peer_uid)
    {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "temporary file type, links or owner mismatch");
        goto done;
    }
    source = NULL;
    longest = 0;
    for (i = 0; i < p->count; i++) {
        for (pass = 0; pass < 2; pass++) {
            prefix =
                pass ?
                    ngx_http_cache_purge_publication_bytes(p->roots[i].path) :
                    p->roots[i].configured.path;
            if (prefix.len > longest
                && ngx_http_cache_purge_publication_contains(prefix, temporary))
            {
                source = &p->roots[i];
                longest = prefix.len;
                relative.data = temporary.data + longest + 1;
                relative.len = temporary.len - longest - 1;
            }
        }
    }
    if (source == NULL) {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "temporary file is outside registered directories");
        goto done;
    }
    rc = ngx_http_cache_purge_publication_verify(source, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_address(source, relative,
                                                      &temp_address, error);
    }
    if (rc != 0) {
        goto done;
    }
    if (ngx_http_cache_purge_publication_contains(
            ngx_http_cache_purge_publication_bytes(p->database), temp_address)
        || ngx_http_cache_purge_publication_equal(
            ngx_http_cache_purge_publication_bytes(p->database), temp_address))
    {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "temporary descriptor cannot address database storage");
        goto done;
    }
    rc = ngx_http_cache_purge_publication_beneath(source->fd, relative, 0, 0,
                                                  &named_fd, error);
    if (rc != 0) {
        goto done;
    }
    if (fstat(named_fd, &named) != 0) {
        rc = ngx_http_cache_purge_io(error, "inspect named temporary file");
        goto done;
    }
    if (named.st_dev != received.st_dev || named.st_ino != received.st_ino) {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "temporary path and descriptor name different instances");
        goto done;
    }
    rc = ngx_http_cache_purge_publication_beneath(target->fd, file.path, 1, 0,
                                                  &current_fd, error);
    if (rc != 0) {
        goto done;
    }
    if (current_fd >= 0) {
        if (fstat(current_fd, &existing) != 0) {
            rc = ngx_http_cache_purge_io(error, "inspect current cache file");
            goto done;
        }
        if (!S_ISREG(existing.st_mode)
            || (existing.st_dev == received.st_dev
                && existing.st_ino == received.st_ino))
        {
            rc = ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "temporary file aliases the current cache instance");
            goto done;
        }
    }
    rc = ngx_http_cache_purge_file_address(address, digest, &offset, error);
    if (rc == 0) {
        rc =
            ngx_http_cache_purge_file_lease_adopt(anchor, lease, offset, error);
    }
    if (rc != 0) {
        goto done;
    }
    rc = ngx_http_cache_purge_file_marker_read(temporary_fd, &marker, error);
    if (rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        if (rc != 0) {
            goto done;
        }
        if (marker.state != NGX_HTTP_CACHE_PURGE_FILE_PREPARED
            || memcmp(marker.id, file.id, 16)
            || memcmp(marker.address, digest, 16))
        {
            rc = ngx_http_cache_purge_result(
                error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "temporary file already belongs to another publication");
            goto done;
        }
    }

    /* Bind retries to device/inode and size/mtime; xattr changes ctime. */
    size = 48 + temp_address.len + file.metadata.len;
    if (size > 16384) {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "publication proof exceeds metadata limit");
        goto done;
    }
    proof = malloc(size);
    if (proof == NULL) {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                         "publication allocation failed");
        goto done;
    }
    memcpy(proof, "NFP1", 4);
    next =
        ngx_http_cache_purge_publication_encode(proof + 4, received.st_dev, 8);
    next = ngx_http_cache_purge_publication_encode(next, received.st_ino, 8);
    next = ngx_http_cache_purge_publication_encode(next, received.st_size, 8);
    next = ngx_http_cache_purge_publication_encode(next,
                                                   received.st_mtim.tv_sec, 8);
    next = ngx_http_cache_purge_publication_encode(next,
                                                   received.st_mtim.tv_nsec, 4);
    next = ngx_http_cache_purge_publication_encode(next, temp_address.len, 4);
    memcpy(next, temp_address.data, temp_address.len);
    next += temp_address.len;
    next = ngx_http_cache_purge_publication_encode(next, file.metadata.len, 4);
    if (file.metadata.len) {
        memcpy(next, file.metadata.data, file.metadata.len);
    }
    file.metadata.data = proof;
    file.metadata.len = size;

    /*
     * Persist intent before writing the marker; errors never authorize rename.
     */
    rc = ngx_http_cache_purge_index_prepare(store, &file, fill->previous,
                                            &match, error);
    if (rc != 0) {
        goto done;
    }
    if (match.state != NGX_HTTP_CACHE_PURGE_INDEX_PREPARED) {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                         "file publication already advanced");
        goto done;
    }
    rc = ngx_http_cache_purge_file_marker_prepare(temporary_fd, file.id, digest,
                                                  1, error);
    if (rc != 0) {
        goto done;
    }
    if (fstat(temporary_fd, &after) != 0) {
        rc = ngx_http_cache_purge_io(error, "recheck prepared temporary file");
        goto done;
    }
    if (after.st_nlink != 1 || after.st_size != received.st_size
        || after.st_mtim.tv_sec != received.st_mtim.tv_sec
        || after.st_mtim.tv_nsec != received.st_mtim.tv_nsec)
    {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "temporary file changed during durable preparation");
        goto done;
    }
    *out = match;
    rc = ngx_http_cache_purge_result(error, 0, "");

done:
    if (named_fd >= 0) {
        close(named_fd);
    }
    if (current_fd >= 0) {
        close(current_fd);
    }
    free(proof);
    free((void *) address.data);
    free((void *) temp_address.data);
    return rc;
}

int
ngx_http_cache_purge_publications_finish(ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_rocksdb_t *store, const unsigned char id[16],
    uint64_t revision, int anchor, int lease, uint32_t peer_uid,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_file_t   file;
    ngx_http_cache_purge_index_match_t  result;
    ngx_http_cache_purge_file_marker_t  marker;
    ngx_http_cache_rocksdb_bytes_t           address, relative, prefix;
    ngx_http_cache_purge_publication_proof_t saved;
    ngx_http_cache_purge_publication_root_t *target, *source;
    struct stat                              st, opened, final_stat;
    unsigned char  digest[16];
    uint64_t       offset;
    size_t                                   i, longest;
    int                                      rc, intent, observed, fd, final;

    ngx_http_cache_purge_result(error, 0, "");
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (out == NULL || store == NULL || id == NULL || revision == 0) {
        return ngx_http_cache_purge_result(error,
                                           NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                           "invalid publication finish");
    }
    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_index_get_file(store, id, &file, error);
    if (rc != 0) {
        return rc;
    }
    memset(&address, 0, sizeof(address));
    observed = -1;
    fd = -1;
    final = -1;
    rc = ngx_http_cache_purge_publication_proof(file.metadata, &saved, error);
    if (rc != 0) {
        goto done;
    }
    if (file.revision < revision) {
        rc =
            ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                        "publication revision moved backwards");
        goto done;
    }
    rc = ngx_http_cache_purge_index_has_intent(store, id, &intent, error);
    if (rc != 0) {
        goto done;
    }
    if (!intent) {
        memcpy(result.id, id, 16);
        result.revision = file.revision;
        result.state = file.state;
        goto success;
    }
    if (file.revision != revision
        && !(revision != UINT64_MAX && file.revision == revision + 1
             && file.state == NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED))
    {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                         "publication revision changed");
        goto done;
    }
    target = NULL;
    source = NULL;
    longest = 0;
    for (i = 0; i < p->count; i++) {
        prefix = ngx_http_cache_purge_publication_bytes(p->roots[i].path);
        if (p->roots[i].configured.kind == NGX_HTTP_CACHE_PURGE_ROOT_CACHE
            && ngx_http_cache_purge_publication_equal(prefix, file.root)
            && ngx_http_cache_purge_publication_equal(
                p->roots[i].configured.zone, file.zone))
        {
            target = &p->roots[i];
        }
        if (prefix.len > longest
            && ngx_http_cache_purge_publication_contains(prefix,
                                                         saved.temporary))
        {
            source = &p->roots[i];
            longest = prefix.len;
        }
    }
    if (target == NULL || source == NULL) {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "publication directories are no longer registered");
        goto done;
    }
    rc = ngx_http_cache_purge_publication_verify(target, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_verify(source, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_address(target, file.path,
                                                      &address, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_address(address, digest, &offset, error);
    }
    if (rc == 0) {
        rc =
            ngx_http_cache_purge_file_lease_adopt(anchor, lease, offset, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_beneath(target->fd, file.path, 1,
                                                      0, &observed, error);
    }
    if (rc != 0) {
        goto done;
    }
    if (observed < 0) {
        goto abandon;
    }
    if (fstat(observed, &st) != 0) {
        rc = ngx_http_cache_purge_io(error, "inspect publication result");
        goto done;
    }
    if (!S_ISREG(st.st_mode)) {
        goto abandon;
    }
    if ((uint64_t) st.st_dev != saved.device) {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "cross-filesystem copy needs verified publication evidence");
        goto done;
    }
    rc = ngx_http_cache_purge_file_marker_read(observed, &marker, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        goto abandon;
    }
    if (rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT) {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                         "publication marker is malformed");
    }
    if (rc != 0) {
        goto done;
    }
    if (memcmp(marker.id, id, 16) || memcmp(marker.address, digest, 16)) {
        goto abandon;
    }
    if (!ngx_http_cache_purge_publication_same(&saved, &st) || st.st_nlink != 1
        || st.st_uid != peer_uid)
    {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "publication file differs from its prepared identity");
        goto done;
    }
    rc = ngx_http_cache_purge_publication_beneath(target->fd, file.path, 0, 1,
                                                  &fd, error);
    if (rc != 0) {
        goto done;
    }
    if (fstat(fd, &opened) != 0) {
        rc = ngx_http_cache_purge_io(error,
                                     "inspect writable publication result");
        goto done;
    }
    if (!ngx_http_cache_purge_publication_same(&saved, &opened)
        || opened.st_nlink != 1)
    {
        rc = ngx_http_cache_purge_result(
            error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "publication file changed while opening");
        goto done;
    }
    if (fsync(fd) != 0) {
        rc = ngx_http_cache_purge_io(error, "sync final cache file");
        goto done;
    }
    rc =
        ngx_http_cache_purge_publication_sync_parents(target, file.path, error);
    relative.data = saved.temporary.data + source->length + 1;
    relative.len = saved.temporary.len - source->length - 1;
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_sync_parents(source, relative,
                                                           error);
    }
    if (rc != 0) {
        goto done;
    }
    memcpy(result.id, id, 16);
    result.revision = file.revision;
    result.state = file.state;
    if (file.state == NGX_HTTP_CACHE_PURGE_INDEX_PREPARED) {
        rc = ngx_http_cache_purge_publication_apply(
            store, NGX_HTTP_CACHE_PURGE_INDEX_COMMIT_PENDING, id, file.revision,
            &result, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_marker_publish(fd, id, digest, 1, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_beneath(target->fd, file.path, 1,
                                                      0, &final, error);
    }
    if (rc != 0) {
        goto done;
    }
    if (final < 0) {
        goto discard;
    }
    if (fstat(final, &final_stat) != 0) {
        rc = ngx_http_cache_purge_io(error, "recheck published file");
        goto done;
    }
    if (!ngx_http_cache_purge_publication_same(&saved, &final_stat)
        || final_stat.st_nlink != 1)
    {
        goto discard;
    }
    rc = ngx_http_cache_purge_publication_apply(
        store, NGX_HTTP_CACHE_PURGE_INDEX_PUBLISH, id, result.revision, &result,
        error);
    goto success;

abandon:
    rc = ngx_http_cache_purge_publication_apply(
        store,
        file.state == NGX_HTTP_CACHE_PURGE_INDEX_PREPARED ?
            NGX_HTTP_CACHE_PURGE_INDEX_ABORT :
            NGX_HTTP_CACHE_PURGE_INDEX_DISCARD,
        id, file.revision, &result, error);
    goto success;

discard:
    rc = ngx_http_cache_purge_publication_apply(
        store, NGX_HTTP_CACHE_PURGE_INDEX_DISCARD, id, result.revision, &result,
        error);

success:
    if (rc == 0) {
        *out = result;
    }

done:
    if (observed >= 0) {
        close(observed);
    }
    if (fd >= 0) {
        close(fd);
    }
    if (final >= 0) {
        close(final);
    }
    free((void *) address.data);
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}
