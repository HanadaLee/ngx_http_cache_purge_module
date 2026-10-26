/* Copyright (C) Hanada */
#ifndef _NGX_HTTP_CACHE_PURGE_FILES_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_FILES_H_INCLUDED_

#include "rocksdb/ngx_http_cache_rocksdb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_HTTP_CACHE_PURGE_FILES_ABI 2
#define NGX_HTTP_CACHE_PURGE_FILE_PREPARED 1
#define NGX_HTTP_CACHE_PURGE_FILE_READY 2
#define NGX_HTTP_CACHE_PURGE_FILE_MARKER_SIZE 40

typedef struct {
    unsigned char  id[16];
    unsigned char  address[16];
    uint32_t       state;
} ngx_http_cache_purge_file_marker_t;

/* The address is a canonical absolute cache filename, not a zone name or key.
 * Lock collisions only serialize unrelated files; marker matching compares
 * the independent 128-bit address digest as well as the full instance ID. */
int ngx_http_cache_purge_file_address(ngx_http_cache_rocksdb_bytes_t address,
                                      unsigned char                  digest[16],
                                      uint64_t *lock_offset,
                                      ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_file_marker_read(
    int fd, ngx_http_cache_purge_file_marker_t *marker,
    ngx_http_cache_rocksdb_error_t *error);
/* Caller must hold the path lease. These descriptor operations may block and
 * belong on a bounded filesystem executor, never under an nginx SHM mutex.
 * durable=1 fsyncs the file; the caller separately syncs renamed directories.
 * Readiness must be written only after durable index commit_pending(). */
int ngx_http_cache_purge_file_marker_prepare(
    int fd, const unsigned char id[16], const unsigned char address[16],
    int durable, ngx_http_cache_rocksdb_error_t *error);
int ngx_http_cache_purge_file_marker_publish(
    int fd, const unsigned char id[16], const unsigned char address[16],
    int durable, ngx_http_cache_rocksdb_error_t *error);

/* The lock file is persistent and must never be replaced/unlinked while any
 * worker can hold it. Only a newly created file may be chowned. The caller
 * validates the configured parent directory before enabling this resource. */
int ngx_http_cache_purge_file_locks_open(const char *path, uint32_t owner_uid,
                                         int create, int *fd,
                                         ngx_http_cache_rocksdb_error_t *error);
/* Linux open-file-description leases. Each acquisition opens an independent
 * description, so separate writers conflict even in the same process.
 * anchor_fd is the validated persistent lock file opened at initialization;
 * path must still name that inode. No process-lock fallback is safe here.
 * Opening/validating a lease belongs on the filesystem executor. Callers must
 * bound live transactions/descriptors, not retain one lease per cached file. */
int
ngx_http_cache_purge_file_lease_acquire(const char *path, int anchor_fd,
                                        uint64_t offset, int *lease_fd,
                                        ngx_http_cache_rocksdb_error_t *error);
/* Adopt an owned descriptor received with SCM_RIGHTS or duplicated in-process.
 * This checks the anchor inode/access mode and acquires the requested OFD lock
 * if necessary; it does NOT attest which PID previously held it. Peer UID,
 * operation identity and file marker still require independent validation.
 * Success retains the caller's fd and sets CLOEXEC; failure does not close it.
 * */
int
ngx_http_cache_purge_file_lease_adopt(int anchor_fd, int lease_fd,
                                      uint64_t                        offset,
                                      ngx_http_cache_rocksdb_error_t *error);
/* Consume one descriptor reference. Never issue F_UNLCK: other references may
 * still be doing filesystem work. The kernel releases the lease only when
 * the last reference closes, including references queued in a Unix socket.
 * After fork inherited references must be closed before unrelated work. */
int
ngx_http_cache_purge_file_lease_release(int *lease_fd,
                                        ngx_http_cache_rocksdb_error_t *error);

#ifdef __cplusplus
}
#endif
#endif /* _NGX_HTTP_CACHE_PURGE_FILES_H_INCLUDED_ */
