/* Copyright (C) Hanada */
#ifndef _NGX_HTTP_CACHE_PURGE_TRANSPORT_H_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_TRANSPORT_H_INCLUDED_
#include <stddef.h>
#include <sys/types.h>
#ifdef __cplusplus
extern "C" {
#endif
#define NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS 2
/* Nonblocking Unix transport. Rights are attached only to one first frame byte.
 * A successful receive transfers ownership of count descriptors to the caller.
 * Invalid/truncated ancillary data closes all received rights and sets EPROTO.
 * Sending borrows descriptors; callers must not resend rights after n > 0. */
ssize_t ngx_http_cache_purge_transport_recv(
    int socket, void *data, size_t size, int allow_rights,
    int fds[NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS], size_t *count);
ssize_t ngx_http_cache_purge_transport_send(int socket, const void *data,
                                            size_t size, const int *fds,
                                            size_t count);
#ifdef __cplusplus
}
#endif
#endif /* _NGX_HTTP_CACHE_PURGE_TRANSPORT_H_INCLUDED_ */
