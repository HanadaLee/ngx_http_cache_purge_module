/* Copyright (C) Hanada */
#ifndef NGX_CACHE_PURGE_FILES_H
#define NGX_CACHE_PURGE_FILES_H

#include "ngx_cache_purge_store.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NGX_CACHE_PURGE_FILES_ABI 1
#define NGX_CACHE_PURGE_FILE_PREPARED 1
#define NGX_CACHE_PURGE_FILE_READY 2
#define NGX_CACHE_PURGE_FILE_MARKER_SIZE 40

typedef struct {
    unsigned char id[16];
    unsigned char address[16];
    uint32_t state;
} ngx_cache_purge_file_marker_t;

/* The address is a canonical absolute cache filename, not a zone name or key.
 * Lock collisions only serialize unrelated files; marker matching compares
 * the independent 128-bit address digest as well as the full instance ID. */
int ngx_cache_purge_file_address(ngx_cache_purge_bytes_t address,
    unsigned char digest[16], uint64_t *lock_offset,
    ngx_cache_purge_store_error_t *error);
int ngx_cache_purge_file_marker_read(int fd, ngx_cache_purge_file_marker_t *marker,
    ngx_cache_purge_store_error_t *error);
/* Caller must hold the path lease. These descriptor operations may block and
 * belong on a bounded filesystem executor, never under an nginx SHM mutex.
 * durable=1 fsyncs the file; the caller separately syncs renamed directories.
 * Readiness must be written only after durable index commit_pending(). */
int ngx_cache_purge_file_marker_prepare(int fd, const unsigned char id[16],
    const unsigned char address[16], int durable,
    ngx_cache_purge_store_error_t *error);
int ngx_cache_purge_file_marker_publish(int fd, const unsigned char id[16],
    const unsigned char address[16], int durable,
    ngx_cache_purge_store_error_t *error);

/* The lock file is persistent and must never be replaced/unlinked while any
 * worker can hold it. Only a newly created file may be chowned. The caller
 * validates the configured parent directory before enabling this resource. */
int ngx_cache_purge_file_locks_open(const char *path, uint32_t owner_uid,
    int create, int *fd, ngx_cache_purge_store_error_t *error);
/* POSIX process locks: keep exactly one lock-file descriptor per process.
 * Closing any descriptor for that inode releases all of that process's locks.
 * A worker must also reject duplicate active offsets in its bounded local
 * lease set; acquiring a second lease in the same process is not exclusive. */
int ngx_cache_purge_file_lock(int fd, uint64_t offset,
    ngx_cache_purge_store_error_t *error);
int ngx_cache_purge_file_unlock(int fd, uint64_t offset,
    ngx_cache_purge_store_error_t *error);
/* Called by the agent, a different process, to verify the RPC peer owns the
 * requested lock. expected_pid comes from SO_PEERCRED, not the request body. */
int ngx_cache_purge_file_lock_owner(int fd, uint64_t offset,
    uint32_t expected_pid, ngx_cache_purge_store_error_t *error);

#ifdef __cplusplus
}
#endif
#endif
