/* Copyright (C) Hanada */
#include "ngx_cache_purge_files.h"
#include <openssl/sha.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>

namespace {
constexpr char attribute[] = "user.ngx_cache_purge.id";
constexpr uint64_t last_offset = UINT64_C(0x3fffffffffffffff);
int result(ngx_cache_purge_store_error_t *error, int code, const char *message = "") {
    if (error) {
        error->code = code;
        std::snprintf(error->message, sizeof(error->message), "%s", message);
    }
    return code;
}
int io(ngx_cache_purge_store_error_t *error, const char *operation) {
    int saved = errno;
    int code = saved == EMFILE || saved == ENFILE || saved == ENOMEM || saved == ENOLCK
               ? NGX_PURGE_STORE_LIMIT : NGX_PURGE_STORE_IO;
    if (error) {
        error->code = code;
        std::snprintf(error->message, sizeof(error->message), "%s: errno %d", operation, saved);
    }
    return code;
}
bool zero(const unsigned char *id) {
    unsigned value = 0;
    for (size_t i = 0; i < 16; ++i) value |= id[i];
    return value == 0;
}
int sync_file(int fd, int durable, ngx_cache_purge_store_error_t *error) {
    if (durable && fsync(fd) != 0) return io(error, "sync file marker");
    return result(error, NGX_PURGE_STORE_OK);
}
void encode(unsigned char raw[NGX_CACHE_PURGE_FILE_MARKER_SIZE],
            const unsigned char *id, const unsigned char *address, unsigned state) {
    std::memcpy(raw, "NCF1", 4);
    raw[4] = raw[5] = raw[6] = 0; raw[7] = state;
    std::memcpy(raw + 8, id, 16); std::memcpy(raw + 24, address, 16);
}
int lock_request(int fd, uint64_t offset, ngx_cache_purge_store_error_t *error) {
    if (fd < 0 || offset > last_offset
        || offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max()))
        return result(error, NGX_PURGE_STORE_INVALID, "invalid file lease descriptor/offset");
#if defined(F_OFD_SETLK)
    struct flock lock{};
    lock.l_type = F_WRLCK; lock.l_whence = SEEK_SET;
    lock.l_start = static_cast<off_t>(offset); lock.l_len = 1;
    if (fcntl(fd, F_OFD_SETLK, &lock) != 0) {
        if (errno == EAGAIN || errno == EACCES)
            return result(error, NGX_PURGE_STORE_BUSY, "cache path lease is held");
        if (errno == EINVAL || errno == EOPNOTSUPP)
            return result(error, NGX_PURGE_STORE_INVALID, "filesystem/kernel lacks OFD cache leases");
        return io(error, "file lease operation");
    }
    return result(error, NGX_PURGE_STORE_OK);
#else
    return result(error, NGX_PURGE_STORE_INVALID, "build lacks OFD cache leases");
#endif
}
int inspect_locks(int fd, struct stat *st, ngx_cache_purge_store_error_t *error) {
    if (fd < 0) return result(error, NGX_PURGE_STORE_INVALID, "invalid lease file descriptor");
    if (fstat(fd, st) != 0) return io(error, "inspect lease file");
    if (!S_ISREG(st->st_mode) || st->st_nlink != 1 || st->st_size != 0 || (st->st_mode & 0077))
        return result(error, NGX_PURGE_STORE_INVALID, "lease file type, mode or links are invalid");
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0) return io(error, "inspect lease file access");
    if ((flags & O_ACCMODE) != O_RDWR)
        return result(error, NGX_PURGE_STORE_INVALID, "lease file is not read/write");
    return result(error, NGX_PURGE_STORE_OK);
}
}

extern "C" int ngx_cache_purge_file_address(ngx_cache_purge_bytes_t address,
    unsigned char digest[16], uint64_t *offset, ngx_cache_purge_store_error_t *error) {
    if (!address.data || !address.len || address.len > 65536 || address.data[0] != '/'
        || std::memchr(address.data, 0, address.len) || !digest || !offset)
        return result(error, NGX_PURGE_STORE_INVALID, "invalid canonical cache address");
    unsigned char hash[SHA256_DIGEST_LENGTH];
    if (!SHA256(address.data, address.len, hash)) return result(error, NGX_PURGE_STORE_IO, "address digest failed");
    std::memcpy(digest, hash, 16);
    uint64_t value = 0;
    for (size_t i = 16; i < 24; ++i) value = (value << 8) | hash[i];
    *offset = value & last_offset;
    return result(error, NGX_PURGE_STORE_OK);
}

extern "C" int ngx_cache_purge_file_marker_read(int fd, ngx_cache_purge_file_marker_t *marker,
    ngx_cache_purge_store_error_t *error) {
    if (fd < 0 || !marker) return result(error, NGX_PURGE_STORE_INVALID, "invalid file marker input");
    *marker = {};
    unsigned char raw[NGX_CACHE_PURGE_FILE_MARKER_SIZE];
    auto n = fgetxattr(fd, attribute, raw, sizeof(raw));
    if (n < 0) {
        if (errno == ENODATA) return result(error, NGX_PURGE_STORE_NOT_FOUND, "file has no instance marker");
        if (errno == ERANGE) return result(error, NGX_PURGE_STORE_CORRUPT, "file marker is oversized");
        return io(error, "read file marker");
    }
    if (n != sizeof(raw) || std::memcmp(raw, "NCF1", 4)
        || raw[4] || raw[5] || raw[6] || (raw[7] != 1 && raw[7] != 2) || zero(raw + 8))
        return result(error, NGX_PURGE_STORE_CORRUPT, "unknown or malformed file marker");
    marker->state = raw[7];
    std::memcpy(marker->id, raw + 8, 16); std::memcpy(marker->address, raw + 24, 16);
    return result(error, NGX_PURGE_STORE_OK);
}

extern "C" int ngx_cache_purge_file_marker_prepare(int fd, const unsigned char id[16],
    const unsigned char address[16], int durable, ngx_cache_purge_store_error_t *error) {
    if (fd < 0 || !id || !address || zero(id) || (durable != 0 && durable != 1))
        return result(error, NGX_PURGE_STORE_INVALID, "invalid prepared file marker");
    ngx_cache_purge_file_marker_t current;
    int rc = ngx_cache_purge_file_marker_read(fd, &current, error);
    if (rc == NGX_PURGE_STORE_OK) {
        if (current.state != NGX_CACHE_PURGE_FILE_PREPARED || std::memcmp(current.id, id, 16)
            || std::memcmp(current.address, address, 16))
            return result(error, NGX_PURGE_STORE_CONFLICT, "file already has another identity or is readable");
        return sync_file(fd, durable, error);
    }
    if (rc != NGX_PURGE_STORE_NOT_FOUND) return rc;
    unsigned char raw[NGX_CACHE_PURGE_FILE_MARKER_SIZE];
    encode(raw, id, address, NGX_CACHE_PURGE_FILE_PREPARED);
    if (fsetxattr(fd, attribute, raw, sizeof(raw), XATTR_CREATE) != 0) {
        if (errno == EEXIST) return result(error, NGX_PURGE_STORE_CONFLICT, "file identity appeared concurrently");
        return io(error, "prepare file marker");
    }
    return sync_file(fd, durable, error);
}

extern "C" int ngx_cache_purge_file_marker_publish(int fd, const unsigned char id[16],
    const unsigned char address[16], int durable, ngx_cache_purge_store_error_t *error) {
    if (fd < 0 || !id || !address || zero(id) || (durable != 0 && durable != 1))
        return result(error, NGX_PURGE_STORE_INVALID, "invalid published file marker");
    ngx_cache_purge_file_marker_t current;
    int rc = ngx_cache_purge_file_marker_read(fd, &current, error);
    if (rc) return rc;
    if (std::memcmp(current.id, id, 16) || std::memcmp(current.address, address, 16))
        return result(error, NGX_PURGE_STORE_CONFLICT, "file instance/address changed before publication");
    if (current.state == NGX_CACHE_PURGE_FILE_READY) return sync_file(fd, durable, error);
    unsigned char raw[NGX_CACHE_PURGE_FILE_MARKER_SIZE];
    encode(raw, id, address, NGX_CACHE_PURGE_FILE_READY);
    if (fsetxattr(fd, attribute, raw, sizeof(raw), XATTR_REPLACE) != 0) {
        if (errno == ENODATA) return result(error, NGX_PURGE_STORE_CONFLICT, "file identity disappeared");
        return io(error, "publish file marker");
    }
    return sync_file(fd, durable, error);
}

extern "C" int ngx_cache_purge_file_locks_open(const char *path, uint32_t owner,
    int create, int *output, ngx_cache_purge_store_error_t *error) {
    if (output) *output = -1;
    if (!path || path[0] != '/' || !output || (create != 0 && create != 1))
        return result(error, NGX_PURGE_STORE_INVALID, "invalid lease file input");
    bool created = false;
    int fd = -1;
    if (create) {
        fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
        if (fd >= 0) created = true;
        else if (errno != EEXIST) return io(error, "create lease file");
    }
    if (fd < 0) fd = open(path, O_RDWR | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) return io(error, "open lease file");
    if (created && (fchown(fd, static_cast<uid_t>(owner), static_cast<gid_t>(-1)) != 0 || fchmod(fd, 0600) != 0)) {
        int rc = io(error, "set lease file ownership"); close(fd); return rc;
    }
    struct stat st{};
    if (fstat(fd, &st) != 0) { int rc = io(error, "inspect lease file"); close(fd); return rc; }
    if (!S_ISREG(st.st_mode) || st.st_nlink != 1 || st.st_size != 0 || st.st_uid != owner || (st.st_mode & 0077)) {
        close(fd); return result(error, NGX_PURGE_STORE_INVALID, "lease file type, owner, mode or links are invalid");
    }
    *output = fd;
    return result(error, NGX_PURGE_STORE_OK);
}

extern "C" int ngx_cache_purge_file_lease_acquire(const char *path, int anchor_fd,
    uint64_t offset, int *output, ngx_cache_purge_store_error_t *error) {
    if (output) *output = -1;
    if (!output || offset > last_offset)
        return result(error, NGX_PURGE_STORE_INVALID, "invalid lease output/offset");
    struct stat anchor{};
    int rc = inspect_locks(anchor_fd, &anchor, error);
    if (rc) return rc;
    int fd;
    rc = ngx_cache_purge_file_locks_open(path, anchor.st_uid, 0, &fd, error);
    if (rc) return rc;
    rc = ngx_cache_purge_file_lease_adopt(anchor_fd, fd, offset, error);
    if (rc) { close(fd); return rc; }
    *output = fd;
    return result(error, NGX_PURGE_STORE_OK);
}

extern "C" int ngx_cache_purge_file_lease_adopt(int anchor_fd, int fd, uint64_t offset,
    ngx_cache_purge_store_error_t *error) {
    if (offset > last_offset)
        return result(error, NGX_PURGE_STORE_INVALID, "invalid lease offset");
    struct stat anchor{}, received{};
    int rc = inspect_locks(anchor_fd, &anchor, error);
    if (rc) return rc;
    rc = inspect_locks(fd, &received, error);
    if (rc) return rc;
    if (anchor.st_dev != received.st_dev || anchor.st_ino != received.st_ino
        || anchor.st_uid != received.st_uid)
        return result(error, NGX_PURGE_STORE_OWNER, "lease descriptor names another lock file");
    int flags = fcntl(fd, F_GETFD);
    if (flags < 0 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0)
        return io(error, "set lease close-on-exec");
    return lock_request(fd, offset, error);
}

extern "C" int ngx_cache_purge_file_lease_release(int *fd, ngx_cache_purge_store_error_t *error) {
    if (!fd || *fd < 0)
        return result(error, NGX_PURGE_STORE_INVALID, "invalid lease reference");
    int value = *fd;
    *fd = -1;
    /* On Linux close consumes the descriptor even on EINTR; never retry it. */
    if (close(value) != 0) return io(error, "close lease reference");
    return result(error, NGX_PURGE_STORE_OK);
}
