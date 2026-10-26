/* Copyright (C) Hanada */
#include "ngx_cache_purge_client.h"
#include "ngx_cache_purge_transport.h"
#include <sys/un.h>

#define NGX_PURGE_CLIENT_IO_BUDGET  65536

struct ngx_cache_purge_call_s {
    ngx_queue_t queue;
    ngx_cache_purge_client_t *client;
    ngx_event_t deadline;
    ngx_cache_purge_client_done_pt done;
    void *data;
    u_char *packet;
    size_t length, sent;
    uint64_t id;
    uint16_t opcode;
    int fds[2];
    size_t fd_count;
    unsigned completing:1;
};

struct ngx_cache_purge_client_s {
    ngx_cache_purge_client_options_t options;
    ngx_cache_purge_client_stats_t stats;
    ngx_log_t *log;
    struct sockaddr_un address;
    socklen_t address_len;
    ngx_str_t name;
    ngx_peer_connection_t peer;
    ngx_connection_t *connection;
    ngx_queue_t queue;
    ngx_cache_purge_call_t *current;
    ngx_event_t pump, idle;
    u_char header[NGX_CACHE_PURGE_FRAME_SIZE];
    size_t header_used, received;
    ngx_str_t response;
    uint32_t result;
    int lease_fd;
    uint64_t next_id;
    unsigned connecting:1;
    unsigned stopped:1;
};

static void ngx_cache_purge_client_pump(ngx_event_t *ev);
static void ngx_cache_purge_client_read(ngx_event_t *ev);
static void ngx_cache_purge_client_write(ngx_event_t *ev);
static void ngx_cache_purge_client_deadline(ngx_event_t *ev);
static void ngx_cache_purge_client_idle(ngx_event_t *ev);
static void ngx_cache_purge_client_close(ngx_cache_purge_client_t *client);
static void ngx_cache_purge_client_finish(ngx_cache_purge_call_t *call,
    ngx_cache_purge_client_result_t result, ngx_uint_t notify);

static void
ngx_cache_purge_client_put(u_char *p, uint64_t value, ngx_uint_t width)
{
    while (width) {
        p[--width] = (u_char) value;
        value >>= 8;
    }
}

static uint64_t
ngx_cache_purge_client_get(const u_char *p, ngx_uint_t width)
{
    uint64_t value = 0;
    while (width--) {
        value = (value << 8) | *p++;
    }
    return value;
}

static void
ngx_cache_purge_client_peak(ngx_cache_purge_client_t *client)
{
    size_t bytes = client->stats.request_bytes + client->stats.response_bytes;
    if (bytes > client->stats.peak_bytes) {
        client->stats.peak_bytes = bytes;
    }
}

void
ngx_cache_purge_client_options(ngx_cache_purge_client_options_t *options)
{
    ngx_memzero(options, sizeof(*options));
    options->max_pending = 64;
    options->request_bytes = 512 * 1024 + NGX_CACHE_PURGE_FRAME_SIZE;
    options->max_payload = 512 * 1024;
    options->idle_timeout = 1000;
    options->server_uid = geteuid();
}

ngx_cache_purge_client_t *
ngx_cache_purge_client_create(ngx_pool_t *pool, ngx_log_t *log,
    const ngx_str_t *path, const ngx_cache_purge_client_options_t *options)
{
    ngx_cache_purge_client_t *client;

    if (pool == NULL || log == NULL || path == NULL || path->data == NULL
        || path->len == 0 || path->data[0] != '/'
        || path->len >= sizeof(client->address.sun_path)
        || ngx_strlchr(path->data, path->data + path->len, '\0') != NULL
        || options == NULL || options->max_pending == 0
        || options->max_pending > 1024 || options->max_payload == 0
        || options->max_payload > 1024 * 1024
        || options->request_bytes < options->max_payload + NGX_CACHE_PURGE_FRAME_SIZE
        || options->request_bytes > 16 * 1024 * 1024
        || options->idle_timeout == 0 || options->idle_timeout > 30000)
    {
        return NULL;
    }
    client = ngx_pcalloc(pool, sizeof(*client));
    if (client == NULL) {
        return NULL;
    }
    client->options = *options;
    client->lease_fd = -1;
    client->log = log;
    client->address.sun_family = AF_UNIX;
    ngx_memcpy(client->address.sun_path, path->data, path->len);
    client->address_len = (socklen_t) (offsetof(struct sockaddr_un, sun_path) + path->len + 1);
    client->name.data = (u_char *) client->address.sun_path;
    client->name.len = path->len;
    ngx_queue_init(&client->queue);
    client->pump.data = client;
    client->pump.handler = ngx_cache_purge_client_pump;
    client->pump.log = log;
    client->idle.data = client;
    client->idle.handler = ngx_cache_purge_client_idle;
    client->idle.log = log;
    client->idle.cancelable = 1;
    return client;
}

static ngx_int_t
ngx_cache_purge_client_submit_internal(ngx_cache_purge_client_t *client, uint16_t opcode,
    const ngx_str_t *payload, ngx_msec_t timeout,
    ngx_cache_purge_client_done_pt done, void *data, ngx_cache_purge_call_t **out,
    const int *fds, size_t fd_count)
{
    ngx_cache_purge_call_t *call;
    size_t length, i;

    if (out != NULL) {
        *out = NULL;
    }
    if (client == NULL || client->stopped || out == NULL || done == NULL
        || opcode == 0 || payload == NULL || (payload->len && payload->data == NULL)
        || timeout == 0 || timeout > 300000
        || fd_count != (opcode == NGX_PURGE_OP_PREPARE_FILE ? 2U : opcode == NGX_PURGE_OP_FINISH_FILE ? 1U : 0U)
        || (fd_count && fds == NULL))
    {
        return NGX_ERROR;
    }
    if (payload->len > client->options.max_payload) {
        client->stats.rejected++;
        return NGX_BUSY;
    }
    length = NGX_CACHE_PURGE_FRAME_SIZE + payload->len;
    if (client->stats.pending >= client->options.max_pending
        || length > client->options.request_bytes - client->stats.request_bytes)
    {
        client->stats.rejected++;
        return NGX_BUSY;
    }
    call = ngx_calloc(sizeof(*call), client->log);
    if (call == NULL) {
        return NGX_ERROR;
    }
    call->packet = ngx_alloc(length, client->log);
    if (call->packet == NULL) {
        ngx_free(call);
        return NGX_ERROR;
    }
    for (i = 0; i < fd_count; i++) {
        call->fds[i] = fcntl(fds[i], F_DUPFD_CLOEXEC, 0);
        if (call->fds[i] < 0) {
            while (i) { close(call->fds[--i]); }
            ngx_free(call->packet); ngx_free(call);
            return NGX_ERROR;
        }
    }
    call->fd_count = fd_count;
    call->client = client;
    call->opcode = opcode;
    call->id = ++client->next_id;
    call->done = done;
    call->data = data;
    call->length = length;
    ngx_memcpy(call->packet, "NCP1", 4);
    ngx_cache_purge_client_put(call->packet + 4, NGX_CACHE_PURGE_PROTOCOL_VERSION, 2);
    ngx_cache_purge_client_put(call->packet + 6, opcode, 2);
    ngx_cache_purge_client_put(call->packet + 8, call->id, 8);
    ngx_cache_purge_client_put(call->packet + 16, payload->len, 4);
    ngx_memzero(call->packet + 20, 4);
    if (payload->len) {
        ngx_memcpy(call->packet + NGX_CACHE_PURGE_FRAME_SIZE, payload->data, payload->len);
    }
    call->deadline.data = call;
    call->deadline.handler = ngx_cache_purge_client_deadline;
    call->deadline.log = client->log;
    ngx_queue_insert_tail(&client->queue, &call->queue);
    client->stats.accepted++;
    client->stats.pending++;
    client->stats.request_bytes += length;
    ngx_cache_purge_client_peak(client);
    ngx_add_timer(&call->deadline, timeout);
    *out = call;
    ngx_post_event(&client->pump, &ngx_posted_events);
    return NGX_OK;
}

ngx_int_t
ngx_cache_purge_client_submit(ngx_cache_purge_client_t *client, uint16_t opcode,
    const ngx_str_t *payload, ngx_msec_t timeout,
    ngx_cache_purge_client_done_pt done, void *data, ngx_cache_purge_call_t **out)
{
    return ngx_cache_purge_client_submit_internal(client, opcode, payload, timeout, done, data, out, NULL, 0);
}

ngx_int_t
ngx_cache_purge_client_submit_files(ngx_cache_purge_client_t *client,
    const ngx_str_t *payload, const int fds[2], ngx_msec_t timeout,
    ngx_cache_purge_client_done_pt done, void *data, ngx_cache_purge_call_t **out)
{
    return ngx_cache_purge_client_submit_internal(client, NGX_PURGE_OP_PREPARE_FILE, payload, timeout,
                                                 done, data, out, fds, 2);
}

ngx_int_t
ngx_cache_purge_client_finish_file(ngx_cache_purge_client_t *client,
    const ngx_str_t *payload, int lease_fd, ngx_msec_t timeout,
    ngx_cache_purge_client_done_pt done, void *data, ngx_cache_purge_call_t **out)
{
    return ngx_cache_purge_client_submit_internal(client, NGX_PURGE_OP_FINISH_FILE, payload, timeout,
                                                 done, data, out, &lease_fd, 1);
}

static ngx_int_t
ngx_cache_purge_client_connected(ngx_cache_purge_client_t *client)
{
    int error;
    socklen_t length;
    struct ucred peer;

    length = sizeof(error);
    if (getsockopt(client->connection->fd, SOL_SOCKET, SO_ERROR, &error, &length) == -1
        || error != 0)
    {
        return NGX_PURGE_CLIENT_IO;
    }
    length = sizeof(peer);
    if (getsockopt(client->connection->fd, SOL_SOCKET, SO_PEERCRED, &peer, &length) == -1
        || length != sizeof(peer) || peer.uid != client->options.server_uid)
    {
        return NGX_PURGE_CLIENT_OWNER;
    }
    client->connecting = 0;
    return NGX_PURGE_CLIENT_OK;
}

static void
ngx_cache_purge_client_pump(ngx_event_t *ev)
{
    ngx_cache_purge_client_t *client = ev->data;
    ngx_connection_t *c;
    ngx_int_t rc;

    if (client->stopped || client->current || ngx_queue_empty(&client->queue)) {
        return;
    }
    if (client->idle.timer_set) {
        ngx_del_timer(&client->idle);
    }
    c = client->connection;
    if (c && (c->close || c->error || c->read->eof)) {
        ngx_cache_purge_client_close(client);
        c = NULL;
    }
    client->current = ngx_queue_data(ngx_queue_head(&client->queue), ngx_cache_purge_call_t, queue);
    if (c == NULL) {
        ngx_memzero(&client->peer, sizeof(client->peer));
        client->peer.sockaddr = (struct sockaddr *) &client->address;
        client->peer.socklen = client->address_len;
        client->peer.name = &client->name;
        client->peer.get = ngx_event_get_peer;
        client->peer.log = client->log;
        client->peer.log_error = NGX_ERROR_ERR;
        client->peer.type = SOCK_STREAM;
        rc = ngx_event_connect_peer(&client->peer);
        client->connection = c = client->peer.connection;
        if (rc == NGX_ERROR || rc == NGX_DECLINED || rc == NGX_BUSY) {
            ngx_cache_purge_client_finish(client->current, NGX_PURGE_CLIENT_IO, 1);
            return;
        }
        client->stats.connections++;
        c->data = client;
        c->read->handler = ngx_cache_purge_client_read;
        c->write->handler = ngx_cache_purge_client_write;
        client->connecting = 1;
        if (rc == NGX_AGAIN) {
            return;
        }
    }
    ngx_reusable_connection(c, 0);
    c->idle = 0;
    ngx_cache_purge_client_write(c->write);
}

static void
ngx_cache_purge_client_write(ngx_event_t *ev)
{
    ngx_connection_t *c = ev->data;
    ngx_cache_purge_client_t *client = c->data;
    ngx_cache_purge_call_t *call = client->current;
    size_t budget = NGX_PURGE_CLIENT_IO_BUDGET, amount;
    ssize_t n;
    ngx_int_t rc;

    if (call == NULL) {
        return;
    }
    if (client->connecting) {
        rc = ngx_cache_purge_client_connected(client);
        if (rc != NGX_PURGE_CLIENT_OK) {
            ngx_cache_purge_client_finish(call, rc, 1);
            return;
        }
    }
    while (budget && call->sent < call->length) {
        amount = ngx_min(budget, call->length - call->sent);
        if (call->fd_count) {
            n = ngx_cache_purge_transport_send(c->fd, call->packet + call->sent, 1, call->fds, call->fd_count);
            if (n < 0 && errno == EINTR) { continue; }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { c->write->ready = 0; n = NGX_AGAIN; }
            if (n > 0) {
                while (call->fd_count) { close(call->fds[--call->fd_count]); }
            }
        } else {
            n = c->send(c, call->packet + call->sent, amount);
        }
        if (n == NGX_AGAIN) {
            if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_IO, 1);
            }
            return;
        }
        if (n <= 0) {
            ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_IO, 1);
            return;
        }
        call->sent += n;
        budget -= n;
    }
    if (call->sent != call->length) {
        ngx_post_event(c->write, &ngx_posted_next_events);
        return;
    }
    if (c->write->active && ngx_del_event(c->write, NGX_WRITE_EVENT, 0) != NGX_OK) {
        ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_IO, 1);
        return;
    }
    if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
        ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_IO, 1);
        return;
    }
    ngx_cache_purge_client_read(c->read);
}

static void
ngx_cache_purge_client_read(ngx_event_t *ev)
{
    ngx_connection_t *c = ev->data;
    ngx_cache_purge_client_t *client = c->data;
    ngx_cache_purge_call_t *call = client->current;
    u_char byte, *destination;
    size_t budget = NGX_PURGE_CLIENT_IO_BUDGET, remaining;
    ssize_t n;
    int rights[NGX_CACHE_PURGE_TRANSPORT_FDS], saved;
    size_t count, i;

    if (call == NULL) {
        if (c->close || c->recv(c, &byte, 1) != NGX_AGAIN) {
            ngx_cache_purge_client_close(client);
        } else if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
            ngx_cache_purge_client_close(client);
        }
        return;
    }
    if (client->connecting || call->sent != call->length) {
        ngx_cache_purge_client_write(c->write);
        return;
    }
    while (budget) {
        if (client->header_used < sizeof(client->header)) {
            destination = client->header + client->header_used;
            remaining = sizeof(client->header) - client->header_used;
        } else {
            destination = client->response.data + client->received;
            remaining = client->response.len - client->received;
        }
        /* Only lease replies need a one-byte boundary for ancillary rights. */
        if (client->header_used == 0 && call->opcode == NGX_PURGE_OP_ACQUIRE_FILE_LEASE) {
            remaining = 1;
        }
        n = ngx_cache_purge_transport_recv(c->fd, destination, ngx_min(budget, remaining),
                         client->header_used == 0 && call->opcode == NGX_PURGE_OP_ACQUIRE_FILE_LEASE,
                         rights, &count);
        saved = n < 0 ? errno : 0;
        if (n < 0 && saved == EINTR) { continue; }
        if (n < 0 && (saved == EAGAIN || saved == EWOULDBLOCK)) {
            c->read->ready = 0;
            if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_IO, 1);
            }
            return;
        }
        if (n <= 0) {
            ngx_cache_purge_client_finish(call, saved == EPROTO ? NGX_PURGE_CLIENT_PROTOCOL : NGX_PURGE_CLIENT_IO, 1);
            return;
        }
        if (count) {
            if (count != 1 || client->lease_fd >= 0 || call->opcode != NGX_PURGE_OP_ACQUIRE_FILE_LEASE) {
                for (i = 0; i < count; i++) { close(rights[i]); }
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_PROTOCOL, 1);
                return;
            }
            client->lease_fd = rights[0];
        }
        budget -= n;
        if (client->header_used < sizeof(client->header)) {
            client->header_used += n;
            if (client->header_used < sizeof(client->header)) {
                continue;
            }
            if (ngx_memcmp(client->header, "NCP1", 4)
                || ngx_cache_purge_client_get(client->header + 4, 2) != NGX_CACHE_PURGE_PROTOCOL_VERSION
                || ngx_cache_purge_client_get(client->header + 6, 2) != call->opcode
                || ngx_cache_purge_client_get(client->header + 8, 8) != call->id)
            {
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_PROTOCOL, 1);
                return;
            }
            client->response.len = (size_t) ngx_cache_purge_client_get(client->header + 16, 4);
            client->result = (uint32_t) ngx_cache_purge_client_get(client->header + 20, 4);
            if ((client->lease_fd >= 0) != (call->opcode == NGX_PURGE_OP_ACQUIRE_FILE_LEASE && client->result == 0)) {
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_PROTOCOL, 1);
                return;
            }
            if (client->response.len > client->options.max_payload) {
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_LIMIT, 1);
                return;
            }
            if (client->response.len == 0) {
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_OK, 1);
                return;
            }
            client->response.data = ngx_alloc(client->response.len, client->log);
            if (client->response.data == NULL) {
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_LIMIT, 1);
                return;
            }
            client->stats.response_bytes = client->response.len;
            ngx_cache_purge_client_peak(client);
        } else {
            client->received += n;
            if (client->received == client->response.len) {
                ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_OK, 1);
                return;
            }
        }
    }
    ngx_post_event(c->read, &ngx_posted_next_events);
}

static void
ngx_cache_purge_client_close(ngx_cache_purge_client_t *client)
{
    if (client->idle.timer_set) {
        ngx_del_timer(&client->idle);
    }
    if (client->connection) {
        ngx_close_connection(client->connection);
        client->connection = NULL;
    }
    client->connecting = 0;
}

static void
ngx_cache_purge_client_finish(ngx_cache_purge_call_t *call,
    ngx_cache_purge_client_result_t result, ngx_uint_t notify)
{
    ngx_cache_purge_client_t *client = call->client;
    ngx_cache_purge_reply_t reply;
    u_char *response = NULL;
    int lease_fd = -1;

    call->completing = 1;
    ngx_memzero(&reply, sizeof(reply));
    reply.lease_fd = -1;
    reply.transport = result;
    reply.may_have_executed = call->sent != 0;
    if (client->current == call) {
        if (result != NGX_PURGE_CLIENT_OK) {
            ngx_cache_purge_client_close(client);
        } else {
            reply.result = client->result;
            reply.payload = client->response;
            reply.lease_fd = client->lease_fd;
        }
        lease_fd = client->lease_fd;
        client->lease_fd = -1;
        response = client->response.data;
        ngx_str_null(&client->response);
        client->current = NULL;
        client->received = client->header_used = 0;
        client->result = 0;
    }
    if (call->deadline.timer_set) {
        ngx_del_timer(&call->deadline);
    }
    ngx_queue_remove(&call->queue);
    while (call->fd_count) { close(call->fds[--call->fd_count]); }
    client->stats.pending--;
    client->stats.request_bytes -= call->length;
    ngx_free(call->packet);
    if (notify) {
        client->stats.completed++;
        call->done(call, call->data, &reply);
    } else {
        client->stats.cancelled++;
    }
    if (response) {
        ngx_free(response);
        client->stats.response_bytes = 0;
    }
    if (lease_fd >= 0) { close(lease_fd); }
    ngx_free(call);
    if (!client->stopped) {
        if (!ngx_queue_empty(&client->queue)) {
            ngx_post_event(&client->pump, &ngx_posted_events);
        } else if (client->connection) {
            client->connection->idle = 1;
            ngx_reusable_connection(client->connection, 1);
            ngx_add_timer(&client->idle, client->options.idle_timeout);
        }
    }
}

static void
ngx_cache_purge_client_deadline(ngx_event_t *ev)
{
    ngx_cache_purge_client_finish(ev->data, NGX_PURGE_CLIENT_TIMEOUT, 1);
}

static void
ngx_cache_purge_client_idle(ngx_event_t *ev)
{
    ngx_cache_purge_client_close(ev->data);
}

void
ngx_cache_purge_client_cancel(ngx_cache_purge_call_t *call)
{
    if (call && !call->completing) {
        ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_IO, 0);
    }
}

void
ngx_cache_purge_client_stop(ngx_cache_purge_client_t *client)
{
    ngx_cache_purge_call_t *call;
    if (client == NULL || client->stopped) {
        return;
    }
    client->stopped = 1;
    if (client->pump.posted) {
        ngx_delete_posted_event(&client->pump);
    }
    ngx_cache_purge_client_close(client);
    while (!ngx_queue_empty(&client->queue)) {
        call = ngx_queue_data(ngx_queue_head(&client->queue), ngx_cache_purge_call_t, queue);
        ngx_cache_purge_client_finish(call, NGX_PURGE_CLIENT_STOPPED, 1);
    }
}

void
ngx_cache_purge_client_stats(ngx_cache_purge_client_t *client,
    ngx_cache_purge_client_stats_t *stats)
{
    *stats = client->stats;
}
