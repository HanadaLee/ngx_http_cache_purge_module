/* Copyright (C) Hanada */
#include "ngx_http_cache_purge_internal.h"
#include <openssl/sha.h>
#include <errno.h>
#include <stdio.h>
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
    struct flock  lock;
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
    int  flags;

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

    unsigned char  raw[NGX_HTTP_CACHE_PURGE_FILE_MARKER_SIZE];

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

    unsigned char  raw[NGX_HTTP_CACHE_PURGE_FILE_MARKER_SIZE];

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
    int          created, fd, rc;
    struct stat  st;

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
    int          fd, rc;
    struct stat  anchor;

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
    int          flags, rc;
    struct stat  anchor, received;

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
    int  value;

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
    struct iovec     iov = { data, size };
    struct msghdr    message;
    struct cmsghdr  *header;
    ssize_t          n;
    size_t           i, number;
    int              bad = 0, fd;

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
