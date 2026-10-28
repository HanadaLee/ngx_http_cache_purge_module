/* Copyright (C) Hanada */
#include "ngx_http_cache_purge_internal.h"
#include <openssl/sha.h>
#include <openssl/evp.h>
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
    ngx_http_cache_purge_native_layout_t      native_layout;
    char                                     *lease_path;
    int                                       lease_anchor;
};

typedef struct {
    uint64_t                        device;
    uint64_t                        inode;
    uint64_t                        size;
    uint64_t                        seconds;
    uint32_t                        nanoseconds;
    ngx_http_cache_rocksdb_bytes_t  temporary;
    ngx_http_cache_rocksdb_bytes_t  metadata;
} ngx_http_cache_purge_publication_proof_t;

typedef struct {
    const unsigned char             *transaction, *digest;
    uint64_t                         revision;
    uint32_t                         uid;
    int                              pending, replace_tags;
    ngx_http_cache_rocksdb_bytes_t   base, before, after;
    ngx_http_cache_rocksdb_bytes_t   tags[64];
    size_t                           tag_count;
} ngx_http_cache_purge_validation_plan_t;

static int ngx_http_cache_purge_validation_plan(
    ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_validation_plan_t *plan,
    ngx_http_cache_rocksdb_error_t *error);
static int ngx_http_cache_purge_publication_same(
    const ngx_http_cache_purge_publication_proof_t *proof,
    const struct stat *st);

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
    free(p->lease_path);
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
            || (roots[i].kind < NGX_HTTP_CACHE_PURGE_ROOT_CACHE
                || roots[i].kind > NGX_HTTP_CACHE_PURGE_ROOT_PROXY)
            || (roots[i].kind != NGX_HTTP_CACHE_PURGE_ROOT_TEMP
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
    p->lease_anchor = -1;
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
        if (root->configured.kind != NGX_HTTP_CACHE_PURGE_ROOT_TEMP
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
ngx_http_cache_purge_publications_native_layout(
    ngx_http_cache_purge_publications_t *p,
    const ngx_http_cache_purge_native_layout_t *layout,
    const char *lease_path, int anchor_fd,
    ngx_http_cache_rocksdb_error_t *error)
{
    uint32_t     offsets[6], widths[6];
    size_t       i, j, length;
    char        *path;
    struct stat  st;
    int          rc;

    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    if (layout == NULL || !layout->version || layout->size > 4096
        || (layout->version_size != 4 && layout->version_size != 8)
        || !layout->vary_capacity || layout->vary_capacity > 255
        || lease_path == NULL || lease_path[0] != '/')
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native cache layout");
    }
    offsets[0] = layout->version_offset;
    offsets[1] = layout->header_start_offset;
    offsets[2] = layout->crc32_offset;
    offsets[3] = layout->vary_len_offset;
    offsets[4] = layout->vary_offset;
    offsets[5] = layout->variant_offset;
    widths[0] = layout->version_size;
    widths[1] = 2;
    widths[2] = 4;
    widths[3] = 1;
    widths[4] = layout->vary_capacity;
    widths[5] = 16;
    for (i = 0; i < 6; i++) {
        if (widths[i] > layout->size
            || offsets[i] > layout->size - widths[i])
        {
            return ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "native field exceeds header");
        }
        for (j = 0; j < i; j++) {
            if (offsets[i] < offsets[j] + widths[j]
                && offsets[j] < offsets[i] + widths[i])
            {
                return ngx_http_cache_purge_result(error,
                    NGX_HTTP_CACHE_ROCKSDB_INVALID, "native fields overlap");
            }
        }
    }
    length = strlen(lease_path);
    if (length > 4096) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "native lease path exceeds limit");
    }
    rc = ngx_http_cache_purge_inspect_locks(anchor_fd, &st, error);
    if (rc != 0) {
        return rc;
    }
    if (st.st_uid != geteuid()) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "native lease owner differs");
    }
    path = malloc(length + 1);
    if (path == NULL) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "native lease allocation failed");
    }
    memcpy(path, lease_path, length + 1);
    free(p->lease_path);
    p->lease_path = path;
    p->lease_anchor = anchor_fd;
    p->native_layout = *layout;
    return ngx_http_cache_purge_result(error, 0, "");
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
        if (p->roots[i].configured.kind != NGX_HTTP_CACHE_PURGE_ROOT_TEMP
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

    if (!ngx_http_cache_purge_publication_valid(metadata, 16384)) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid publication metadata");
    }

    if (metadata.len >= 4 && memcmp(metadata.data, "NFR1", 4) == 0) {
        if (metadata.len < 64
            || ngx_http_cache_purge_publication_number(metadata.data + 60, 4)
               != metadata.len - 64)
        {
            return ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid validation receipt");
        }
        metadata.data += 64;
        metadata.len -= 64;
    }

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
    proof->metadata.data = metadata.data + at + 4;
    proof->metadata.len = length;
    return 0;
}

void
ngx_http_cache_purge_watermark_encode(
    const ngx_http_cache_purge_watermark_t *watermark,
    unsigned char output[NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE])
{
    memcpy(output, "NFW1", 4);
    memcpy(output + 4, watermark->ledger, 16);
    ngx_http_cache_purge_publication_encode(
        output + 20, watermark->sequence, 8);
}

void
ngx_http_cache_purge_variant_encode(
    const ngx_http_cache_purge_watermark_t *watermark,
    const unsigned char main[16], const unsigned char variant[16],
    unsigned char output[NGX_HTTP_CACHE_PURGE_VARIANT_SIZE])
{
    ngx_http_cache_purge_watermark_encode(watermark, output);
    memcpy(output + 28, "NFK1", 4);
    memcpy(output + 32, main, 16);
    memcpy(output + 48, variant, 16);
}

int
ngx_http_cache_purge_watermark_decode(ngx_http_cache_rocksdb_bytes_t data,
    ngx_http_cache_purge_watermark_t *watermark,
    ngx_http_cache_rocksdb_error_t *error)
{
    if (watermark == NULL
        || !ngx_http_cache_purge_publication_valid(data, 16384))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid watermark output/input");
    }
    memset(watermark, 0, sizeof(*watermark));
    if (data.len >= 4 && memcmp(data.data, "NFW2", 4) == 0) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "conditional proof is not a full-entity origin");
    }
    if (data.len < 4 || memcmp(data.data, "NFW1", 4) != 0) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "file has no origin watermark");
    }
    if ((data.len != NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE
         && (data.len != NGX_HTTP_CACHE_PURGE_VARIANT_SIZE
             || memcmp(data.data + 28, "NFK1", 4) != 0))
        || ngx_http_cache_purge_zero(data.data + 4))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid origin watermark");
    }
    memcpy(watermark->ledger, data.data + 4, 16);
    watermark->sequence = ngx_http_cache_purge_publication_number(
        data.data + 20, 8);
    return ngx_http_cache_purge_result(error, 0, "");
}

static int
ngx_http_cache_purge_metadata_watermarks(ngx_http_cache_rocksdb_bytes_t data,
    ngx_http_cache_purge_watermark_t *origin,
    ngx_http_cache_purge_watermark_t *validated,
    ngx_http_cache_rocksdb_error_t *error)
{
    int  rc;

    memset(origin, 0, sizeof(*origin));
    memset(validated, 0, sizeof(*validated));
    if (data.len >= 4 && memcmp(data.data, "NFW2", 4) == 0) {
        if (data.len < 32 || ngx_http_cache_purge_zero(data.data + 4)
            || ngx_http_cache_purge_publication_number(data.data + 28, 4)
               != data.len - 32)
        {
            return ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "invalid conditional watermark");
        }
        memcpy(validated->ledger, data.data + 4, 16);
        validated->sequence = ngx_http_cache_purge_publication_number(
            data.data + 20, 8);
        data.data += 32;
        data.len -= 32;
        rc = ngx_http_cache_purge_watermark_decode(data, origin, error);
        if (rc != 0 && rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            return rc;
        }
        if (rc == 0 && memcmp(origin->ledger, validated->ledger, 16) == 0
            && origin->sequence > validated->sequence)
        {
            return ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "conditional watermark regressed");
        }
        return ngx_http_cache_purge_result(error, 0, "");
    }
    rc = ngx_http_cache_purge_watermark_decode(data, origin, error);
    if (rc == 0) {
        *validated = *origin;
    }
    return rc;
}

int
ngx_http_cache_purge_publication_watermarks(
    ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_watermark_t *origin,
    ngx_http_cache_purge_watermark_t *validated,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_proof_t  proof;
    ngx_http_cache_purge_validation_plan_t    plan;
    int                                       rc;

    if (origin == NULL || validated == NULL) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing watermark output");
    }
    memset(origin, 0, sizeof(*origin));
    memset(validated, 0, sizeof(*validated));
    if (metadata.data != NULL && metadata.len >= 4
        && memcmp(metadata.data, "NFV", 3) == 0)
    {
        rc = ngx_http_cache_purge_validation_plan(metadata, &plan, error);
        if (rc != 0) {
            return rc;
        }
        metadata = plan.base;
    }
    rc = ngx_http_cache_purge_publication_proof(metadata, &proof, error);
    return rc == 0 ? ngx_http_cache_purge_metadata_watermarks(proof.metadata,
        origin, validated, error) : rc;
}

int
ngx_http_cache_purge_publication_variant(
    ngx_http_cache_rocksdb_bytes_t metadata, unsigned char main[16],
    unsigned char variant[16], ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_proof_t  proof;
    ngx_http_cache_purge_validation_plan_t    plan;
    ngx_http_cache_purge_watermark_t          origin, validated;
    int                                       rc;

    if (main != NULL) {
        memset(main, 0, 16);
    }
    if (variant != NULL) {
        memset(variant, 0, 16);
    }
    if (main == NULL || variant == NULL
        || !ngx_http_cache_purge_publication_valid(metadata, 16384))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid variant metadata/output");
    }
    if (metadata.len >= 4 && memcmp(metadata.data, "NFV", 3) == 0) {
        rc = ngx_http_cache_purge_validation_plan(metadata, &plan, error);
        if (rc != 0) {
            return rc;
        }
        metadata = plan.base;
    }
    rc = ngx_http_cache_purge_publication_proof(metadata, &proof, error);
    if (rc != 0) {
        return rc;
    }
    metadata = proof.metadata;
    rc = ngx_http_cache_purge_metadata_watermarks(metadata, &origin,
        &validated, error);
    if (rc != 0) {
        return rc;
    }
    if (metadata.len >= 4 && memcmp(metadata.data, "NFW2", 4) == 0) {
        metadata.data += 32;
        metadata.len -= 32;
    }
    rc = ngx_http_cache_purge_watermark_decode(metadata, &origin, error);
    if (rc != 0) {
        return rc;
    }
    if (metadata.len != NGX_HTTP_CACHE_PURGE_VARIANT_SIZE) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "file has no stored Vary hashes");
    }
    memcpy(main, metadata.data + 32, 16);
    memcpy(variant, metadata.data + 48, 16);
    return ngx_http_cache_purge_result(error, 0, "");
}

int
ngx_http_cache_purge_publication_native(
    const ngx_http_cache_purge_index_file_t *file,
    unsigned char main[16], unsigned char variant[16],
    ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char  digest[16], digit;
    size_t         i;

    if (main != NULL) {
        memset(main, 0, 16);
    }
    if (variant != NULL) {
        memset(variant, 0, 16);
    }
    if (file == NULL || main == NULL || variant == NULL
        || !ngx_http_cache_purge_publication_valid(file->path, 32768))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native identity input");
    }
    if (file->vary.len) {
        return ngx_http_cache_purge_publication_variant(file->metadata,
            main, variant, error);
    }
    if (file->path.len < 32
        || (file->path.len > 32
            && file->path.data[file->path.len - 33] != '/'))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "native cache hash is absent");
    }
    memset(digest, 0, sizeof(digest));
    for (i = 0; i < 32; i++) {
        digit = file->path.data[file->path.len - 32 + i];
        if (digit >= '0' && digit <= '9') {
            digit -= '0';
        } else if (digit >= 'a' && digit <= 'f') {
            digit = (unsigned char) (digit - 'a' + 10);
        } else {
            return ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid native cache hash");
        }
        digest[i / 2] = (unsigned char) ((digest[i / 2] << 4) | digit);
    }
    memcpy(main, digest, 16);
    return ngx_http_cache_purge_result(error, 0, "");
}

int
ngx_http_cache_purge_publication_watermark(
    ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_watermark_t *watermark,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_watermark_t  validated;
    int                               rc;

    rc = ngx_http_cache_purge_publication_watermarks(metadata, watermark,
                                                       &validated, error);
    if (rc == 0 && ngx_http_cache_purge_zero(watermark->ledger)) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "file has no full-entity origin");
    }
    return rc;
}

static int
ngx_http_cache_purge_native_read(int fd, void *buffer, size_t length,
    off_t offset, ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char *data;
    ssize_t        n;

    data = buffer;
    while (length) {
        n = pread(fd, data, length, offset);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return ngx_http_cache_purge_io(error, "read native cache key");
        }
        if (n == 0) {
            return ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "truncated native cache key");
        }
        data += n;
        length -= n;
        offset += n;
    }
    return 0;
}

/* Recover only identity; neither the raw key nor the header is persisted. */
static int
ngx_http_cache_purge_native_file(ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_purge_publication_root_t *root,
    const ngx_http_cache_purge_index_file_t *file, unsigned char main[16],
    unsigned char variant[16], ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_native_layout_t      *layout;
    ngx_http_cache_purge_publication_proof_t   proof;
    ngx_http_cache_purge_index_file_t          plain;
    ngx_http_cache_purge_file_marker_t         marker;
    ngx_http_cache_rocksdb_bytes_t             address;
    EVP_MD_CTX                               *md;
    struct stat                               st;
    unsigned char                             buffer[4096], digest[16];
    unsigned char                             filename[16], unused[16];
    unsigned char                             stored_variant[16], computed[16];
    uint64_t                                  offset, version;
    uint32_t                                  short_version, stored_crc, crc;
    uint16_t                                  start;
    unsigned                                  size, bit;
    size_t                                    at, left, count, i;
    int                                       fd, named, lease, rc;

    if (!p->native_layout.size
        || (file->state != NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED
            && file->state != NGX_HTTP_CACHE_PURGE_INDEX_RETIRED)
        || (file->metadata.len >= 3
            && memcmp(file->metadata.data, "NFV", 3) == 0))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "native identity is not ready");
    }
    rc = ngx_http_cache_purge_publication_proof(file->metadata, &proof, error);
    if (rc != 0) {
        return rc;
    }
    plain = *file;
    plain.vary.len = 0;
    rc = ngx_http_cache_purge_publication_native(&plain, filename, unused,
        error);
    if (rc != 0) {
        return rc;
    }
    fd = named = lease = -1;
    md = NULL;
    memset(&address, 0, sizeof(address));
    layout = &p->native_layout;
    rc = ngx_http_cache_purge_publication_address(root, file->path, &address,
        error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_address(address, digest, &offset, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_lease_acquire(p->lease_path,
            p->lease_anchor, offset, &lease, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_verify(root, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_beneath(root->fd, file->path,
            0, 0, &fd, error);
    }
    if (rc != 0) {
        goto done;
    }
    if (fstat(fd, &st) != 0) {
        rc = ngx_http_cache_purge_io(error, "inspect native cache key");
        goto done;
    }
    if (!S_ISREG(st.st_mode) || st.st_uid != geteuid() || st.st_nlink != 1
        || !ngx_http_cache_purge_publication_same(&proof, &st))
    {
        goto mismatch;
    }
    rc = ngx_http_cache_purge_file_marker_read(fd, &marker, error);
    if (rc != 0) {
        goto done;
    }
    if (marker.state != NGX_HTTP_CACHE_PURGE_FILE_READY
        || memcmp(marker.id, file->id, 16)
        || memcmp(marker.address, digest, 16))
    {
        goto mismatch;
    }
    rc = ngx_http_cache_purge_native_read(fd, buffer, layout->size, 0, error);
    if (rc != 0) {
        goto done;
    }
    if (layout->version_size == 4) {
        memcpy(&short_version, buffer + layout->version_offset, 4);
        version = short_version;
    } else {
        memcpy(&version, buffer + layout->version_offset, 8);
    }
    memcpy(&start, buffer + layout->header_start_offset, 2);
    memcpy(&stored_crc, buffer + layout->crc32_offset, 4);
    if (version != layout->version || start < layout->size + 7
        || start > proof.size || file->vary.len > layout->vary_capacity
        || buffer[layout->vary_len_offset] != file->vary.len
        || memcmp(buffer + layout->vary_offset, file->vary.data,
            file->vary.len))
    {
        goto mismatch;
    }
    memcpy(stored_variant, buffer + layout->variant_offset, 16);
    rc = ngx_http_cache_purge_native_read(fd, buffer, 6, layout->size, error);
    if (rc != 0) {
        goto done;
    }
    if (memcmp(buffer, "\nKEY: ", 6)) {
        goto mismatch;
    }
    md = EVP_MD_CTX_new();
    if (md == NULL || EVP_DigestInit_ex(md, EVP_md5(), NULL) != 1) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "native digest setup failed");
        goto done;
    }
    crc = UINT32_MAX;
    at = layout->size + 6;
    left = start - at - 1;
    while (left) {
        count = left < sizeof(buffer) ? left : sizeof(buffer);
        rc = ngx_http_cache_purge_native_read(fd, buffer, count, (off_t) at,
            error);
        if (rc != 0) {
            goto done;
        }
        if (EVP_DigestUpdate(md, buffer, count) != 1) {
            rc = ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_IO, "native digest update failed");
            goto done;
        }
        for (i = 0; i < count; i++) {
            crc ^= buffer[i];
            for (bit = 0; bit < 8; bit++) {
                crc = (crc >> 1) ^ ((crc & 1) ? UINT32_C(0xedb88320) : 0);
            }
        }
        left -= count;
        at += count;
    }
    rc = ngx_http_cache_purge_native_read(fd, buffer, 1, (off_t) at, error);
    if (rc != 0) {
        goto done;
    }
    if (EVP_DigestFinal_ex(md, computed, &size) != 1 || size != 16
        || buffer[0] != '\n' || (crc ^ UINT32_MAX) != stored_crc
        || (memcmp(filename, computed, 16)
            && memcmp(filename, stored_variant, 16)))
    {
        goto mismatch;
    }
    rc = ngx_http_cache_purge_file_marker_read(fd, &marker, error);
    if (rc != 0) {
        goto done;
    }
    if (marker.state != NGX_HTTP_CACHE_PURGE_FILE_READY
        || memcmp(marker.id, file->id, 16)
        || memcmp(marker.address, digest, 16))
    {
        goto mismatch;
    }
    rc = ngx_http_cache_purge_publication_beneath(root->fd, file->path,
        0, 0, &named, error);
    if (rc != 0) {
        goto done;
    }
    if (fstat(fd, &st) != 0) {
        rc = ngx_http_cache_purge_io(error, "recheck native cache descriptor");
        goto done;
    }
    if (st.st_nlink != 1
        || !ngx_http_cache_purge_publication_same(&proof, &st))
    {
        goto mismatch;
    }
    if (fstat(named, &st) != 0) {
        rc = ngx_http_cache_purge_io(error, "recheck native cache name");
        goto done;
    }
    if (st.st_nlink != 1
        || !ngx_http_cache_purge_publication_same(&proof, &st))
    {
        goto mismatch;
    }
    memcpy(main, computed, 16);
    memcpy(variant, stored_variant, 16);
    rc = 0;
    goto done;

mismatch:
    rc = ngx_http_cache_purge_result(error,
        NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "native cache identity differs");

done:
    if (md != NULL) {
        EVP_MD_CTX_free(md);
    }
    if (named >= 0) {
        close(named);
    }
    if (fd >= 0) {
        close(fd);
    }
    if (lease >= 0) {
        close(lease);
    }
    free((void *) address.data);
    return rc;
}

static int
ngx_http_cache_purge_publication_resolve_native(void *data,
    const ngx_http_cache_purge_index_file_t *file, unsigned char main[16],
    unsigned char variant[16], unsigned *state,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publications_t     *p;
    ngx_http_cache_purge_publication_root_t *root;
    int                                      rc;

    p = data;
    *state = NGX_HTTP_CACHE_PURGE_NATIVE_MISSING;
    root = ngx_http_cache_purge_publication_find(p, file->root, file->zone, 1);
    if (root == NULL) {
        return 0;
    }
    if (root->configured.kind != NGX_HTTP_CACHE_PURGE_ROOT_PROXY
        || (file->state == NGX_HTTP_CACHE_PURGE_INDEX_RETIRED
            && file->revision == 2))
    {
        *state = NGX_HTTP_CACHE_PURGE_NATIVE_SKIP;
        return 0;
    }
    rc = ngx_http_cache_purge_publication_native(file, main, variant, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND && file->vary.len) {
        rc = ngx_http_cache_purge_native_file(p, root, file, main, variant,
            error);
    }
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
        || rc == NGX_HTTP_CACHE_ROCKSDB_INVALID
        || rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
        || rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
        || rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT)
    {
        return ngx_http_cache_purge_result(error, 0, "");
    }
    if (rc == 0) {
        *state = NGX_HTTP_CACHE_PURGE_NATIVE_BIND;
    }
    return rc;
}

int
ngx_http_cache_purge_publications_retry_native(
    ngx_http_cache_purge_publications_t *p, ngx_http_cache_rocksdb_t *store,
    size_t budget, unsigned char bookmark[25], size_t *length,
    ngx_http_cache_rocksdb_error_t *error)
{
    int  rc;

    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    return ngx_http_cache_purge_index_retry_native(store, budget,
        ngx_http_cache_purge_publication_resolve_native, p, bookmark, length,
        error);
}

int
ngx_http_cache_purge_publications_migrate_native(
    ngx_http_cache_purge_publications_t *p, ngx_http_cache_rocksdb_t *store,
    size_t budget, int *done, ngx_http_cache_rocksdb_error_t *error)
{
    int  rc;

    if (done != NULL) {
        *done = 0;
    }
    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    return ngx_http_cache_purge_index_migrate_native(store, budget,
        ngx_http_cache_purge_publication_resolve_native, p, done, error);
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
    unsigned char                  main[16], variant[16];
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
    if (target->configured.kind == NGX_HTTP_CACHE_PURGE_ROOT_PROXY) {
        rc = ngx_http_cache_purge_publication_native(&file, main, variant,
            error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_index_prepare_native(store, &file,
                fill->previous, main, variant, &match, error);
        }
    } else {
        rc = ngx_http_cache_purge_index_prepare(store, &file, fill->previous,
            &match, error);
    }
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
ngx_http_cache_purge_publications_finish_checked(
    ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_rocksdb_t *store, const unsigned char id[16],
    uint64_t revision, int anchor, int lease, uint32_t peer_uid,
    ngx_http_cache_purge_publication_guard_pt guard, void *data,
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
        if (p->roots[i].configured.kind != NGX_HTTP_CACHE_PURGE_ROOT_TEMP
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
        if (guard != NULL) {
            rc = guard(data, &file, error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT) {
                goto abandon;
            }
            if (rc != 0) {
                goto done;
            }
        }
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

int
ngx_http_cache_purge_publications_finish(ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_rocksdb_t *store, const unsigned char id[16],
    uint64_t revision, int anchor, int lease, uint32_t peer_uid,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_publications_finish_checked(p, store, id,
        revision, anchor, lease, peer_uid, NULL, NULL, out, error);
}

static int
ngx_http_cache_purge_validation_plan(ngx_http_cache_rocksdb_bytes_t metadata,
    ngx_http_cache_purge_validation_plan_t *plan,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_proof_t  proof;
    size_t                                   base, length, at, i;

    memset(plan, 0, sizeof(*plan));
    if (!ngx_http_cache_purge_publication_valid(metadata, 16384)
        || metadata.len < 64
        || (memcmp(metadata.data, "NFV1", 4)
            && memcmp(metadata.data, "NFV2", 4)
            && memcmp(metadata.data, "NFR1", 4)))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid validation metadata");
    }
    plan->pending = metadata.data[2] == 'V';
    plan->transaction = metadata.data + 4;
    plan->digest = metadata.data + 20;
    plan->revision = ngx_http_cache_purge_publication_number(
        metadata.data + 52, 8);
    at = plan->pending ? 64 : 60;
    if (metadata.len < at + 4 || plan->revision == 0
        || plan->revision > UINT64_MAX - 2
        || ngx_http_cache_purge_zero(plan->transaction))
    {
        goto invalid;
    }
    base = ngx_http_cache_purge_publication_number(metadata.data + at, 4);
    at += 4;
    if (base > metadata.len - at) {
        goto invalid;
    }
    plan->base.data = metadata.data + at;
    plan->base.len = base;
    at += base;
    if (plan->pending) {
        plan->uid = (uint32_t) ngx_http_cache_purge_publication_number(
            metadata.data + 60, 4);
        if (metadata.len - at < 4) {
            goto invalid;
        }
        length = ngx_http_cache_purge_publication_number(metadata.data + at, 4);
        at += 4;
        if (length == 0 || length > 4096 || metadata.len - at < 2 * length) {
            goto invalid;
        }
        plan->before.data = metadata.data + at;
        plan->before.len = length;
        plan->after.data = metadata.data + at + length;
        plan->after.len = length;
        at += 2 * length;
        if (metadata.data[3] == '2') {
            if (at == metadata.len) {
                goto invalid;
            }
            plan->tag_count = metadata.data[at++];
            plan->replace_tags = plan->tag_count != 255;
            if (!plan->replace_tags) {
                plan->tag_count = 0;
            } else if (plan->tag_count > 64) {
                goto invalid;
            }
            for (i = 0; i < plan->tag_count; i++) {
                if (metadata.len - at < 4) {
                    goto invalid;
                }
                length = ngx_http_cache_purge_publication_number(
                    metadata.data + at, 4);
                at += 4;
                if (length == 0 || length > 1024 || length > metadata.len - at)
                {
                    goto invalid;
                }
                plan->tags[i].data = metadata.data + at;
                plan->tags[i].len = length;
                at += length;
            }
        }
    }
    if (at != metadata.len || base < 4
        || memcmp(plan->base.data, "NFP1", 4))
    {
        goto invalid;
    }
    return ngx_http_cache_purge_publication_proof(plan->base, &proof, error);

invalid:
    return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_INVALID,
                                       "malformed validation plan");
}

int
ngx_http_cache_purge_publication_inode(ngx_http_cache_rocksdb_bytes_t metadata,
    uint64_t *inode, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_proof_t  proof;
    ngx_http_cache_purge_validation_plan_t    plan;
    int                                       rc;

    if (inode == NULL || (metadata.len != 0 && metadata.data == NULL)) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid publication inode input");
    }
    *inode = 0;
    if (metadata.len >= 4 && memcmp(metadata.data, "NFV", 3) == 0) {
        rc = ngx_http_cache_purge_validation_plan(metadata, &plan, error);
        if (rc != 0) {
            return rc;
        }
        metadata = plan.base;
    }
    rc = ngx_http_cache_purge_publication_proof(metadata, &proof, error);
    if (rc == 0) {
        *inode = proof.inode;
    }
    return rc;
}

int
ngx_http_cache_purge_publications_remove_checked(
    ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *expected,
    int anchor, int lease, uint32_t uid,
    ngx_http_cache_purge_publication_guard_pt guard, void *data,
    uint32_t *outcome,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_root_t   *root;
    ngx_http_cache_purge_publication_proof_t   proof;
    ngx_http_cache_purge_validation_plan_t     plan;
    ngx_http_cache_purge_index_file_t          file;
    ngx_http_cache_purge_index_match_t         retired;
    ngx_http_cache_purge_file_marker_t         marker;
    ngx_http_cache_rocksdb_bytes_t             address, relative, base;
    struct stat                                st, named;
    unsigned char                              identity[16];
    char                                      *leaf;
    uint64_t                                   offset;
    size_t                                     slash;
    uint32_t                                   result;
    int                                        rc, owned, parent, fd, current;
    int                                        filesystem;

    ngx_http_cache_purge_result(error, 0, "");
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (outcome != NULL) {
        *outcome = 0;
    }
    if (store == NULL || expected == NULL || out == NULL || outcome == NULL
        || expected->revision == 0 || ngx_http_cache_purge_zero(expected->id))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache removal request");
    }
    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_index_get_file(store, expected->id, &file, error);
    if (rc != 0) {
        return rc;
    }
    memset(&address, 0, sizeof(address));
    parent = -1;
    fd = -1;
    current = -1;
    leaf = NULL;
    filesystem = 0;
    result = NGX_HTTP_CACHE_PURGE_MISSING;
    if (file.revision != expected->revision) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "cache removal revision changed");
        goto done;
    }
    rc = ngx_http_cache_purge_index_has_intent(store, file.id, &owned, error);
    if (rc != 0) {
        goto done;
    }
    if (file.state == NGX_HTTP_CACHE_PURGE_INDEX_PREPARED || owned) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "cache publication must finish first");
        goto done;
    }
    base = file.metadata;
    if (base.len >= 4 && memcmp(base.data, "NFV", 3) == 0) {
        rc = ngx_http_cache_purge_validation_plan(base, &plan, error);
        if (rc != 0) {
            goto done;
        }
        base = plan.base;
    }
    rc = ngx_http_cache_purge_publication_proof(base, &proof, error);
    if (rc != 0) {
        goto done;
    }
    root = ngx_http_cache_purge_publication_find(p, file.root, file.zone, 1);
    if (root == NULL) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "unregistered removal root/zone");
        goto done;
    }
    filesystem = 1;
    rc = ngx_http_cache_purge_publication_verify(root, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_address(root, file.path,
                                                       &address, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_address(address, identity, &offset,
                                                error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_lease_adopt(anchor, lease, offset,
                                                   error);
    }
    if (rc != 0) {
        goto done;
    }
    slash = file.path.len;
    while (slash && file.path.data[slash - 1] != '/') {
        slash--;
    }
    leaf = malloc(file.path.len - slash + 1);
    if (leaf == NULL) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "removal filename allocation failed");
        goto done;
    }
    memcpy(leaf, file.path.data + slash, file.path.len - slash);
    leaf[file.path.len - slash] = 0;
    if (slash) {
        relative.data = file.path.data;
        relative.len = slash - 1;
        rc = ngx_http_cache_purge_publication_beneath(root->fd, relative,
                                                       1, 0, &parent, error);
    } else {
        parent = fcntl(root->fd, F_DUPFD_CLOEXEC, 0);
        if (parent < 0) {
            rc = ngx_http_cache_purge_io(error, "duplicate removal directory");
        }
    }
    if (rc != 0) {
        goto done;
    }
    if (parent >= 0) {
        if (fstat(parent, &st) != 0) {
            rc = ngx_http_cache_purge_io(error, "inspect removal directory");
            goto done;
        }
        if (!S_ISDIR(st.st_mode)) {
            rc = ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "removal parent is not a directory");
            goto done;
        }
        fd = openat(parent, leaf,
                    O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
        if (fd < 0 && errno != ENOENT) {
            rc = ngx_http_cache_purge_io(error, "open removal instance");
            goto done;
        }
    }
    if (fd >= 0) {
        if (fstat(fd, &st) != 0) {
            rc = ngx_http_cache_purge_io(error, "inspect removal instance");
            goto done;
        }
        if (!S_ISREG(st.st_mode) || st.st_uid != uid || st.st_nlink != 1) {
            rc = ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "removal file type/owner/link mismatch");
            goto done;
        }
        result = NGX_HTTP_CACHE_PURGE_REPLACED;
        rc = ngx_http_cache_purge_file_marker_read(fd, &marker, error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND
            || rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT)
        {
            rc = ngx_http_cache_purge_result(error, 0, "");
        } else if (rc != 0) {
            goto done;
        } else if (memcmp(marker.id, file.id, 16) == 0
                   && memcmp(marker.address, identity, 16) == 0
                   && proof.device == (uint64_t) st.st_dev
                   && proof.inode == (uint64_t) st.st_ino)
        {
            if (file.state == NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED
                && marker.state != NGX_HTTP_CACHE_PURGE_FILE_READY)
            {
                rc = ngx_http_cache_purge_result(error,
                    NGX_HTTP_CACHE_ROCKSDB_BUSY,
                    "removal marker is unpublished");
                goto done;
            }
            result = NGX_HTTP_CACHE_PURGE_REMOVED;
        }
    }
    filesystem = 0;
    if (guard != NULL) {
        rc = guard(data, &file, error);
        if (rc != 0) {
            goto done;
        }
    }
    rc = ngx_http_cache_purge_publication_apply(store,
        NGX_HTTP_CACHE_PURGE_INDEX_RETIRE, file.id, file.revision,
        &retired, error);
    if (rc != 0) {
        goto done;
    }
    filesystem = 1;
    if (result == NGX_HTTP_CACHE_PURGE_REMOVED) {
        /* Managed writers cannot rename while this lease is held. The native
         * cache manager can only unlink, which is an idempotent missing result.
         */
        rc = ngx_http_cache_purge_publication_verify(root, error);
        if (rc == 0) {
            rc = ngx_http_cache_purge_publication_beneath(root->fd, file.path,
                1, 0, &current, error);
        }
        if (rc != 0) {
            goto done;
        }
        if (current < 0) {
            result = NGX_HTTP_CACHE_PURGE_MISSING;
        } else {
            if (fstat(current, &named) != 0) {
                rc = ngx_http_cache_purge_io(error,
                                               "recheck removal instance");
                goto done;
            }
            if (named.st_dev != st.st_dev || named.st_ino != st.st_ino) {
                result = NGX_HTTP_CACHE_PURGE_REPLACED;
            } else if (unlinkat(parent, leaf, 0) != 0) {
                if (errno != ENOENT) {
                    rc = ngx_http_cache_purge_io(error,
                                                   "unlink cache instance");
                    goto done;
                }
                result = NGX_HTTP_CACHE_PURGE_MISSING;
            }
        }
    }
    rc = ngx_http_cache_purge_publication_sync_parents(root, file.path, error);
    if (rc == 0) {
        *out = retired;
        *outcome = result;
    }

done:
    if (filesystem && rc == NGX_HTTP_CACHE_ROCKSDB_IO) {
        rc = NGX_HTTP_CACHE_PURGE_FILE_ERROR;
        if (error != NULL) {
            error->code = rc;
        }
    }
    if (current >= 0) {
        close(current);
    }
    if (fd >= 0) {
        close(fd);
    }
    if (parent >= 0) {
        close(parent);
    }
    free(leaf);
    free((void *) address.data);
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}

int
ngx_http_cache_purge_publications_remove(ngx_http_cache_purge_publications_t *p,
    ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_index_expected_t *expected,
    int anchor, int lease, uint32_t uid, uint32_t *outcome,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_publications_remove_checked(p, store, expected,
        anchor, lease, uid, NULL, NULL, outcome, out, error);
}

static int
ngx_http_cache_purge_validation_compare(const void *left, const void *right)
{
    const ngx_http_cache_rocksdb_bytes_t  *a, *b;
    size_t                                length;
    int                                   rc;

    a = left;
    b = right;
    length = a->len < b->len ? a->len : b->len;
    rc = length ? memcmp(a->data, b->data, length) : 0;
    return rc ? rc : a->len < b->len ? -1 : a->len != b->len;
}

static int
ngx_http_cache_purge_validation_digest(
    const ngx_http_cache_purge_validation_t *input,
    ngx_http_cache_rocksdb_bytes_t tags[64], size_t *count,
    unsigned char digest[32], ngx_http_cache_rocksdb_error_t *error)
{
    unsigned char  *raw, *p;
    size_t          i, used, length;

    if (input == NULL || input->file.revision == 0
        || input->file.revision > UINT64_MAX - 2
        || ngx_http_cache_purge_zero(input->file.id)
        || ngx_http_cache_purge_zero(input->transaction)
        || !ngx_http_cache_purge_publication_valid(input->before, 4096)
        || !ngx_http_cache_purge_publication_valid(input->after, 4096)
        || input->before.len == 0 || input->before.len != input->after.len
        || *count > 64
        || (input->origin != NULL
            && (ngx_http_cache_purge_zero(input->origin->ledger)
                || input->origin->sequence == UINT64_MAX)))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid validation input");
    }
    for (i = 0; i < *count; i++) {
        if (!ngx_http_cache_purge_publication_valid(tags[i], 1024)
            || tags[i].len == 0)
        {
            return ngx_http_cache_purge_result(error,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid validation TAG");
        }
    }
    qsort(tags, *count, sizeof(tags[0]),
          ngx_http_cache_purge_validation_compare);
    used = 0;
    for (i = 0; i < *count; i++) {
        if (used == 0
            || !ngx_http_cache_purge_publication_equal(tags[used - 1], tags[i]))
        {
            tags[used++] = tags[i];
        }
    }
    *count = used;
    length = 45 + input->before.len * 2;
    if (input->origin != NULL) {
        length += NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE;
    }
    for (i = 0; i < used; i++) {
        length += 4 + tags[i].len;
    }
    raw = malloc(length);
    if (raw == NULL) {
        return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                           "validation allocation failed");
    }
    memcpy(raw, input->file.id, 16);
    memcpy(raw + 16, input->transaction, 16);
    p = ngx_http_cache_purge_publication_encode(raw + 32,
                                                input->file.revision, 8);
    p = ngx_http_cache_purge_publication_encode(p, input->before.len, 4);
    memcpy(p, input->before.data, input->before.len);
    p += input->before.len;
    memcpy(p, input->after.data, input->after.len);
    p += input->after.len;
    *p++ = (unsigned char) used;
    for (i = 0; i < used; i++) {
        p = ngx_http_cache_purge_publication_encode(p, tags[i].len, 4);
        memcpy(p, tags[i].data, tags[i].len);
        p += tags[i].len;
    }
    if (input->origin != NULL) {
        ngx_http_cache_purge_watermark_encode(input->origin, p);
    }
    if (SHA256(raw, length, digest) == NULL) {
        free(raw);
        return ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_IO,
                                           "validation digest failed");
    }
    free(raw);
    return 0;
}

static int
ngx_http_cache_purge_validation_io(int fd, unsigned char *data, size_t length,
    int write, ngx_http_cache_rocksdb_error_t *error)
{
    size_t   at;
    ssize_t  n;

    at = 0;
    while (at < length) {
        n = write ? pwrite(fd, data + at, length - at, (off_t) at) :
                    pread(fd, data + at, length - at, (off_t) at);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            if (n == 0) {
                errno = EIO;
            }
            return ngx_http_cache_purge_io(error, "validation header I/O");
        }
        at += (size_t) n;
    }
    return 0;
}

static void
ngx_http_cache_purge_validation_match(
    const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_purge_index_match_t *out)
{
    memcpy(out->id, file->id, 16);
    out->revision = file->revision;
    out->state = file->state;
}

static int
ngx_http_cache_purge_validation_base(ngx_http_cache_rocksdb_bytes_t base,
    const ngx_http_cache_purge_watermark_t *origin,
    ngx_http_cache_rocksdb_bytes_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_publication_proof_t  proof;
    ngx_http_cache_purge_watermark_t          full, validated;
    ngx_http_cache_rocksdb_bytes_t            original;
    unsigned char                            *raw, *p;
    size_t                                     prefix, length;
    int                                        rc;

    memset(out, 0, sizeof(*out));
    rc = ngx_http_cache_purge_publication_proof(base, &proof, error);
    if (rc != 0) {
        return rc;
    }
    original = proof.metadata;
    rc = ngx_http_cache_purge_metadata_watermarks(original, &full,
                                                    &validated, error);
    if (rc != 0 && rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        return rc;
    }
    if (rc == 0 && memcmp(validated.ledger, origin->ledger, 16) == 0
        && validated.sequence > origin->sequence)
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "validation origin regressed");
    }
    if (original.len >= 4 && memcmp(original.data, "NFW2", 4) == 0) {
        original.data += 32;
        original.len -= 32;
    }
    prefix = base.len - proof.metadata.len - 4;
    length = prefix + 4 + 32 + original.len;
    if (length > 16384) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "validation origin metadata too large");
    }
    raw = malloc(length);
    if (raw == NULL) {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "validation origin allocation failed");
    }
    memcpy(raw, base.data, prefix);
    p = ngx_http_cache_purge_publication_encode(raw + prefix,
                                                  32 + original.len, 4);
    memcpy(p, "NFW2", 4);
    memcpy(p + 4, origin->ledger, 16);
    ngx_http_cache_purge_publication_encode(p + 20, origin->sequence, 8);
    ngx_http_cache_purge_publication_encode(p + 28, original.len, 4);
    if (original.len) {
        memcpy(p + 32, original.data, original.len);
    }
    out->data = raw;
    out->len = length;
    return ngx_http_cache_purge_result(error, 0, "");
}

int
ngx_http_cache_purge_publications_validate_prepare(
    ngx_http_cache_purge_publications_t *p, ngx_http_cache_rocksdb_t *store,
    const ngx_http_cache_purge_validation_t *input, int anchor, int lease,
    int selected_fd, uint32_t uid,
    ngx_http_cache_purge_validation_guard_pt guard, void *guard_data,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_file_t         file, checked, candidate;
    ngx_http_cache_purge_validation_plan_t    plan;
    ngx_http_cache_purge_publication_proof_t  proof;
    ngx_http_cache_purge_publication_root_t  *root;
    ngx_http_cache_purge_file_marker_t        marker;
    ngx_http_cache_rocksdb_bytes_t            address, base, updated;
    ngx_http_cache_rocksdb_bytes_t            metadata, tags[64];
    struct stat                               st;
    unsigned char                             digest[32], identity[16];
    unsigned char                            *raw, *next, *before;
    uint64_t                                  offset;
    size_t                                    count, size, i;
    int                                       rc, owned, flags;

    ngx_http_cache_purge_result(error, 0, "");
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (input == NULL || out == NULL || store == NULL || guard == NULL
        || (input->replace_tags != 0 && input->replace_tags != 1)
        || input->tag_count > 64
        || (input->tag_count && input->tags == NULL))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid validation preparation");
    }
    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    memset(&checked, 0, sizeof(checked));
    memset(&address, 0, sizeof(address));
    memset(&updated, 0, sizeof(updated));
    raw = NULL;
    before = NULL;
    rc = ngx_http_cache_purge_index_get_file(
        store, input->file.id, &file, error);
    if (rc != 0) {
        return rc;
    }
    if (file.state != NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED) {
        goto conflict;
    }
    count = input->replace_tags ? input->tag_count : file.tag_count;
    if (count) {
        memcpy(tags, input->replace_tags ? input->tags : file.tags,
               count * sizeof(tags[0]));
    }
    rc = ngx_http_cache_purge_validation_digest(input, tags, &count,
                                                digest, error);
    if (rc != 0) {
        goto done;
    }
    base = file.metadata;
    memset(&plan, 0, sizeof(plan));
    if (base.len >= 4 && (memcmp(base.data, "NFV", 3) == 0
                         || memcmp(base.data, "NFR1", 4) == 0))
    {
        rc = ngx_http_cache_purge_validation_plan(base, &plan, error);
        if (rc != 0) {
            goto done;
        }
        base = plan.base;
    }
    rc = ngx_http_cache_purge_publication_proof(base, &proof, error);
    if (rc != 0) {
        goto done;
    }
    root = ngx_http_cache_purge_publication_find(p, file.root, file.zone, 1);
    if (root == NULL) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "unregistered validation root");
        goto done;
    }
    rc = ngx_http_cache_purge_publication_verify(root, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_address(root, file.path,
                                                      &address, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_address(
            address, identity, &offset, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_lease_adopt(
            anchor, lease, offset, error);
    }
    if (rc != 0) {
        goto done;
    }
    flags = fcntl(selected_fd, F_GETFL);
    if (flags < 0 || (flags & O_ACCMODE) == O_WRONLY || (flags & O_PATH)) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "validation descriptor is not readable");
        goto done;
    }
    if (fstat(selected_fd, &st) != 0) {
        rc = ngx_http_cache_purge_io(error, "inspect validation descriptor");
        goto done;
    }
    if (!S_ISREG(st.st_mode) || st.st_uid != uid || st.st_nlink != 1
        || (uint64_t) st.st_dev != proof.device
        || (uint64_t) st.st_ino != proof.inode
        || (uint64_t) st.st_size != proof.size)
    {
        goto conflict;
    }
    rc = ngx_http_cache_purge_file_marker_read(selected_fd, &marker, error);
    if (rc != 0) {
        goto done;
    }
    if (marker.state != NGX_HTTP_CACHE_PURGE_FILE_READY
        || memcmp(marker.id, input->file.id, 16)
        || memcmp(marker.address, identity, 16))
    {
        goto conflict;
    }
    if (plan.transaction
        && memcmp(plan.transaction, input->transaction, 16) == 0)
    {
        if (plan.revision != input->file.revision
            || memcmp(plan.digest, digest, 32)
            || file.revision != plan.revision + (plan.pending ? 1 : 2)
            || (plan.pending && plan.uid != uid))
        {
            goto conflict;
        }
        rc = ngx_http_cache_purge_index_has_intent(
            store, file.id, &owned, error);
        if (rc != 0) {
            goto done;
        }
        if (owned != plan.pending) {
            goto conflict;
        }
        if (!owned) {
            rc = ngx_http_cache_purge_publications_inspect(p, store,
                file.root, file.zone, file.path, selected_fd, uid, &checked,
                error);
            if (rc != 0) {
                goto done;
            }
        }
        ngx_http_cache_purge_validation_match(&file, out);
        goto done;
    }
    if (plan.pending || file.revision != input->file.revision) {
        goto conflict;
    }
    if (input->origin == NULL && proof.metadata.len >= 4
        && memcmp(proof.metadata.data, "NFW2", 4) == 0)
    {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "conditional metadata requires a request origin");
        goto done;
    }
    if (input->origin != NULL) {
        rc = ngx_http_cache_purge_validation_base(base, input->origin,
                                                    &updated, error);
        if (rc != 0) {
            goto done;
        }
        base = updated;
    }
    size = 73 + base.len + 2 * input->before.len;
    if (input->replace_tags) {
        for (i = 0; i < count; i++) {
            size += 4 + tags[i].len;
        }
    }
    if (size > 16384 || base.len + 64 > 16384
        || input->before.len > proof.size)
    {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                        "validation metadata exceeds budget");
        goto done;
    }
    rc = ngx_http_cache_purge_publications_inspect(p, store, file.root,
        file.zone, file.path, selected_fd, uid, &checked, error);
    if (rc != 0) {
        goto done;
    }
    if (checked.revision != input->file.revision) {
        goto conflict;
    }
    before = malloc(input->before.len);
    raw = malloc(size);
    if (before == NULL || raw == NULL) {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                           "validation allocation failed");
        goto done;
    }
    rc = ngx_http_cache_purge_validation_io(selected_fd, before,
                                             input->before.len, 0, error);
    if (rc != 0) {
        goto done;
    }
    if (memcmp(before, input->before.data, input->before.len)) {
        goto conflict;
    }
    rc = guard(guard_data, &checked, error);
    if (rc == 0 && input->replace_tags) {
        candidate = checked;
        memcpy(candidate.tags, tags, count * sizeof(tags[0]));
        candidate.tag_count = count;
        rc = guard(guard_data, &candidate, error);
    }
    if (rc != 0) {
        goto done;
    }
    memcpy(raw, "NFV2", 4);
    memcpy(raw + 4, input->transaction, 16);
    memcpy(raw + 20, digest, 32);
    next = ngx_http_cache_purge_publication_encode(raw + 52,
                                                   input->file.revision, 8);
    next = ngx_http_cache_purge_publication_encode(next, uid, 4);
    next = ngx_http_cache_purge_publication_encode(next, base.len, 4);
    memcpy(next, base.data, base.len);
    next += base.len;
    next = ngx_http_cache_purge_publication_encode(next, input->before.len, 4);
    memcpy(next, input->before.data, input->before.len);
    next += input->before.len;
    memcpy(next, input->after.data, input->after.len);
    next += input->after.len;
    *next++ = input->replace_tags ? (unsigned char) count : 255;
    if (input->replace_tags) {
        for (i = 0; i < count; i++) {
            next = ngx_http_cache_purge_publication_encode(next,
                                                          tags[i].len, 4);
            memcpy(next, tags[i].data, tags[i].len);
            next += tags[i].len;
        }
    }
    metadata.data = raw;
    metadata.len = size;
    rc = ngx_http_cache_purge_index_update_pending(store, &input->file,
        metadata, file.tags, file.tag_count, out, error);
    goto done;

conflict:
    rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                    "validation instance or revision changed");
done:
    free(raw);
    free(before);
    free((void *) updated.data);
    free((void *) address.data);
    ngx_http_cache_purge_index_file_free(&checked);
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}

int
ngx_http_cache_purge_publications_validate_finish_checked(
    ngx_http_cache_purge_publications_t *p, ngx_http_cache_rocksdb_t *store,
    const unsigned char id[16], const unsigned char transaction[16],
    uint64_t revision, int anchor, int lease, uint32_t uid,
    ngx_http_cache_purge_validation_guard_pt guard, void *guard_data,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_index_file_t         file, checked, candidate;
    ngx_http_cache_purge_validation_plan_t    plan;
    ngx_http_cache_purge_validation_t         input;
    ngx_http_cache_purge_watermark_t          origin, validated;
    ngx_http_cache_purge_index_expected_t     expected;
    ngx_http_cache_purge_publication_proof_t  proof;
    ngx_http_cache_purge_publication_root_t  *root;
    ngx_http_cache_purge_file_marker_t        marker;
    ngx_http_cache_rocksdb_bytes_t            address, metadata, tags[64];
    struct stat                               st, final;
    unsigned char                             identity[16], digest[32];
    unsigned char                            *prefix, *raw;
    uint64_t                                  offset;
    size_t                                    i, count, size;
    int                                       rc, owned, fd, named;

    ngx_http_cache_purge_result(error, 0, "");
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (id == NULL || store == NULL || out == NULL
        || (transaction == NULL) != (revision == 0))
    {
        return ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid validation finish");
    }
    rc = ngx_http_cache_purge_publication_owner(p, error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_index_get_file(store, id, &file, error);
    if (rc != 0) {
        return rc;
    }
    memset(&checked, 0, sizeof(checked));
    memset(&address, 0, sizeof(address));
    prefix = NULL;
    raw = NULL;
    fd = -1;
    named = -1;
    rc = ngx_http_cache_purge_validation_plan(file.metadata, &plan, error);
    if (rc != 0) {
        goto done;
    }
    if ((transaction && (memcmp(transaction, plan.transaction, 16)
                         || revision != plan.revision))
        || (plan.pending && plan.uid != uid))
    {
        goto conflict;
    }
    if (file.state == NGX_HTTP_CACHE_PURGE_INDEX_RETIRED) {
        ngx_http_cache_purge_validation_match(&file, out);
        goto done;
    }
    if (file.state != NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED
        || file.revision != plan.revision + (plan.pending ? 1 : 2))
    {
        goto conflict;
    }
    rc = ngx_http_cache_purge_index_has_intent(store, id, &owned, error);
    if (rc != 0) {
        goto done;
    }
    if (owned != plan.pending) {
        goto conflict;
    }
    rc = ngx_http_cache_purge_publication_proof(plan.base, &proof, error);
    if (rc != 0) {
        goto done;
    }
    root = ngx_http_cache_purge_publication_find(p, file.root, file.zone, 1);
    if (root == NULL) {
        rc = ngx_http_cache_purge_result(error,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "unregistered validation root");
        goto done;
    }
    rc = ngx_http_cache_purge_publication_verify(root, error);
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_address(root, file.path,
                                                      &address, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_address(
            address, identity, &offset, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_file_lease_adopt(
            anchor, lease, offset, error);
    }
    if (rc == 0) {
        rc = ngx_http_cache_purge_publication_beneath(root->fd, file.path, 1,
                                                      1, &fd, error);
    }
    if (rc != 0) {
        goto done;
    }
    if (fd < 0) {
        goto discarded;
    }
    if (fstat(fd, &st) != 0) {
        rc = ngx_http_cache_purge_io(error, "inspect validation target");
        goto done;
    }
    if (!S_ISREG(st.st_mode) || st.st_uid != uid || st.st_nlink != 1) {
        goto conflict;
    }
    if ((uint64_t) st.st_dev != proof.device
        || (uint64_t) st.st_ino != proof.inode)
    {
        goto discarded;
    }
    rc = ngx_http_cache_purge_file_marker_read(fd, &marker, error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        goto discarded;
    }
    if (rc != 0) {
        goto done;
    }
    if (memcmp(marker.id, id, 16) || memcmp(marker.address, identity, 16)) {
        goto discarded;
    }
    if (marker.state != NGX_HTTP_CACHE_PURGE_FILE_READY
        || (uint64_t) st.st_size != proof.size)
    {
        goto conflict;
    }
    if (!plan.pending) {
        rc = ngx_http_cache_purge_publications_inspect(p, store, file.root,
            file.zone, file.path, fd, uid, &checked, error);
        if (rc == 0) {
            ngx_http_cache_purge_validation_match(&checked, out);
        }
        goto done;
    }
    if (file.metadata.data[3] == '1') {
        /* Legacy plans already discarded their old TAG membership. They
         * cannot prove coverage for a rule accepted during recovery. */
        goto discarded;
    }
    memset(&input, 0, sizeof(input));
    memcpy(input.file.id, id, 16);
    input.file.revision = plan.revision;
    memcpy(input.transaction, plan.transaction, 16);
    input.before = plan.before;
    input.after = plan.after;
    if (proof.metadata.len >= 4
        && memcmp(proof.metadata.data, "NFW2", 4) == 0)
    {
        rc = ngx_http_cache_purge_metadata_watermarks(proof.metadata, &origin,
                                                        &validated, error);
        if (rc != 0) {
            goto done;
        }
        input.origin = &validated;
    }
    count = plan.replace_tags ? plan.tag_count : file.tag_count;
    memcpy(tags, plan.replace_tags ? plan.tags : file.tags,
           count * sizeof(tags[0]));
    rc = ngx_http_cache_purge_validation_digest(&input, tags, &count,
                                                digest, error);
    if (rc != 0) {
        goto done;
    }
    if (memcmp(digest, plan.digest, 32) || plan.before.len > proof.size) {
        goto conflict;
    }
    if (guard != NULL) {
        candidate = file;
        candidate.metadata = plan.base;
        rc = guard(guard_data, &candidate, error);
        if (rc == 0 && plan.replace_tags) {
            memcpy(candidate.tags, tags, count * sizeof(tags[0]));
            candidate.tag_count = count;
            rc = guard(guard_data, &candidate, error);
        }
        if (rc == NGX_HTTP_CACHE_ROCKSDB_CONFLICT) {
            goto discarded;
        }
        if (rc != 0) {
            goto done;
        }
    }
    prefix = malloc(plan.before.len);
    size = 64 + plan.base.len;
    if (size > 16384) {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                           "validation receipt exceeds budget");
        goto done;
    }
    raw = malloc(size);
    if (prefix == NULL || raw == NULL) {
        rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                           "validation allocation failed");
        goto done;
    }
    rc = ngx_http_cache_purge_validation_io(fd, prefix, plan.before.len, 0,
                                             error);
    if (rc != 0) {
        goto done;
    }
    if (memcmp(prefix, plan.before.data, plan.before.len) == 0
        && !ngx_http_cache_purge_publication_same(&proof, &st))
    {
        goto conflict;
    }
    for (i = 0; i < plan.before.len; i++) {
        if (prefix[i] != plan.before.data[i]
            && prefix[i] != plan.after.data[i])
        {
            goto conflict;
        }
    }
    if (memcmp(prefix, plan.after.data, plan.after.len)) {
        memcpy(prefix, plan.after.data, plan.after.len);
        rc = ngx_http_cache_purge_validation_io(fd, prefix, plan.after.len, 1,
                                                 error);
        if (rc != 0) {
            goto done;
        }
    }
    rc = ngx_http_cache_purge_sync_file(fd, 1, error);
    if (rc != 0) {
        goto done;
    }
    rc = ngx_http_cache_purge_publication_beneath(root->fd, file.path, 1, 0,
                                                  &named, error);
    if (rc != 0) {
        goto done;
    }
    if (named < 0) {
        goto discarded;
    }
    if (fstat(fd, &st) != 0 || fstat(named, &final) != 0) {
        rc = ngx_http_cache_purge_io(error, "recheck validated target");
        goto done;
    }
    if (st.st_nlink != 1 || st.st_dev != final.st_dev
        || st.st_ino != final.st_ino)
    {
        goto discarded;
    }
    if ((uint64_t) st.st_size != proof.size || st.st_uid != uid) {
        goto conflict;
    }
    memcpy(raw, "NFR1", 4);
    memcpy(raw + 4, plan.transaction, 16);
    memcpy(raw + 20, plan.digest, 32);
    ngx_http_cache_purge_publication_encode(raw + 52, plan.revision, 8);
    ngx_http_cache_purge_publication_encode(raw + 60, plan.base.len, 4);
    memcpy(raw + 64, plan.base.data, plan.base.len);
    ngx_http_cache_purge_publication_encode(
        raw + 64 + 28, st.st_mtim.tv_sec, 8);
    ngx_http_cache_purge_publication_encode(
        raw + 64 + 36, st.st_mtim.tv_nsec, 4);
    metadata.data = raw;
    metadata.len = size;
    memcpy(expected.id, id, 16);
    expected.revision = file.revision;
    rc = ngx_http_cache_purge_index_publish_tags(store, &expected, metadata,
                                                   tags, count, out, error);
    goto done;

discarded:
    if (!plan.pending) {
        goto conflict;
    }
    rc = ngx_http_cache_purge_publication_apply(store,
        NGX_HTTP_CACHE_PURGE_INDEX_DISCARD, id, file.revision, out, error);
    goto done;
conflict:
    rc = ngx_http_cache_purge_result(error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                                       "validation target changed");
done:
    if (fd >= 0) {
        close(fd);
    }
    if (named >= 0) {
        close(named);
    }
    free(raw);
    free(prefix);
    free((void *) address.data);
    ngx_http_cache_purge_index_file_free(&checked);
    ngx_http_cache_purge_index_file_free(&file);
    return rc;
}

int
ngx_http_cache_purge_publications_validate_finish(
    ngx_http_cache_purge_publications_t *p, ngx_http_cache_rocksdb_t *store,
    const unsigned char id[16], const unsigned char transaction[16],
    uint64_t revision, int anchor, int lease, uint32_t uid,
    ngx_http_cache_purge_index_match_t *out,
    ngx_http_cache_rocksdb_error_t *error)
{
    return ngx_http_cache_purge_publications_validate_finish_checked(p, store,
        id, transaction, revision, anchor, lease, uid, NULL, NULL, out, error);
}
