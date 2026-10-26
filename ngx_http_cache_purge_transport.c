/* Copyright (C) Hanada */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "ngx_http_cache_purge_transport.h"
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

ssize_t
ngx_http_cache_purge_transport_recv(int socket, void *data, size_t size,
                                    int allow_rights,
                                    int fds[NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS],
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
