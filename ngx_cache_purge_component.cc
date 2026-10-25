/* Copyright (C) Hanada */
#include "ngx_cache_purge_component.h"
#include "ngx_cache_purge_protocol.hpp"
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <memory>
#include <thread>

using namespace ngx_cache_purge;
namespace wire = ngx_cache_purge::protocol;
using Clock = std::chrono::steady_clock;

namespace {
int failure(ngx_cache_purge_store_error_t *error, int code, const char *message) noexcept {
    if (error) { error->code = code; std::snprintf(error->message, sizeof(error->message), "%s", message); }
    return code;
}
template<class Function> int boundary(ngx_cache_purge_store_error_t *error, Function function) noexcept {
    if (error) { error->code = 0; error->message[0] = 0; }
    try { return function(); }
    catch (const IndexError &e) { return failure(error, e.code, e.what()); }
    catch (const std::bad_alloc &) { return failure(error, NGX_PURGE_STORE_LIMIT, "component allocation failed"); }
    catch (const std::exception &e) { return failure(error, NGX_PURGE_STORE_IO, e.what()); }
    catch (...) { return failure(error, NGX_PURGE_STORE_IO, "unknown component error"); }
}
[[noreturn]] void system_error(const char *message) {
    int saved = errno;
    throw IndexError(NGX_PURGE_STORE_IO, std::string(message) + ": " + std::strerror(saved));
}
void checked(int rc, const ngx_cache_purge_store_error_t &error) {
    if (rc) throw IndexError(rc, error.message);
}
void nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 || fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
        system_error("component descriptor flags");
}
struct Descriptor {
    int value = -1;
    ~Descriptor() { reset(); }
    void reset() { if (value >= 0) close(value); value = -1; }
};
struct Peer {
    Descriptor fd;
    std::array<char, NGX_CACHE_PURGE_FRAME_SIZE> header{};
    size_t header_used = 0, input_used = 0, output_used = 0;
    std::string input, output;
    wire::Frame request{};
    bool parsed = false;
    Clock::time_point deadline;
};
} // namespace

struct ngx_cache_purge_component_s {
    std::string path, socket_path;
    ngx_cache_purge_component_options_t options{};
    pid_t pid = getpid();
    std::thread::id creator = std::this_thread::get_id();
    std::thread executor;
    Descriptor wake_read, wake_write, listener;
    bool socket_owned = false;
    dev_t socket_device = 0;
    ino_t socket_inode = 0;
    std::atomic<bool> stopping{false};
    std::atomic<uint32_t> state{NGX_PURGE_COMPONENT_STARTING}, error_code{0};
    std::atomic<uint64_t> connections{0}, requests{0}, rejected{0}, buffered{0}, peak{0};
    ngx_cache_purge_store_error_t terminal_error{};
    std::vector<std::unique_ptr<Peer>> peers;
    size_t next_peer = 0;

    bool owner() const { return pid == getpid() && creator == std::this_thread::get_id(); }
    void wake() noexcept {
        const char byte = 1;
        ssize_t rc;
        do { rc = write(wake_write.value, &byte, 1); } while (rc < 0 && errno == EINTR);
        // EAGAIN means a wakeup is already pending; stopping is independently atomic.
    }
    void clear_wake() noexcept {
        char data[64];
        while (read(wake_read.value, data, sizeof(data)) > 0) {}
    }
    void fail(int code, const char *message) noexcept {
        if (!error_code.load()) {
            failure(&terminal_error, code, message);
            error_code.store(code, std::memory_order_release);
        }
    }
    void resize(std::string &value, size_t size) {
        auto current = buffered.load();
        auto old_size = value.size();
        if (size > value.size() && size - value.size() > options.buffer_bytes - current)
            throw IndexError(NGX_PURGE_STORE_LIMIT, "component input/output budget");
        value.resize(size);
        auto used = current - old_size + size;
        buffered.store(used);
        if (used > peak.load()) peak.store(used);
    }
    void release(std::string &value) {
        buffered.fetch_sub(value.size()); std::string().swap(value);
    }
    void drop(size_t i) {
        if (!peers[i]) return;
        release(peers[i]->input); release(peers[i]->output);
        peers[i].reset(); connections.fetch_sub(1);
    }
    void bind_socket() {
        struct stat st;
        if (lstat(socket_path.c_str(), &st) == 0) {
            if (!S_ISSOCK(st.st_mode) || st.st_uid != geteuid())
                throw IndexError(NGX_PURGE_STORE_INVALID, "component socket path is not an owned socket");
            if (unlink(socket_path.c_str()) < 0) system_error("remove stale component socket");
        } else if (errno != ENOENT) system_error("inspect component socket");
        listener.value = socket(AF_UNIX, SOCK_STREAM, 0);
        if (listener.value < 0) system_error("component socket");
        nonblocking(listener.value);
        sockaddr_un address{}; address.sun_family = AF_UNIX;
        std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size() + 1);
        if (bind(listener.value, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0)
            system_error("bind component socket");
        if (lstat(socket_path.c_str(), &st) < 0) system_error("inspect bound component socket");
        socket_owned = true; socket_device = st.st_dev; socket_inode = st.st_ino;
        if (chmod(socket_path.c_str(), 0600) < 0 || listen(listener.value, options.max_connections) < 0)
            system_error("listen component socket");
    }
    void remove_socket() noexcept {
        listener.reset();
        if (!socket_owned) return;
        struct stat st;
        if (lstat(socket_path.c_str(), &st) == 0 && S_ISSOCK(st.st_mode)
            && st.st_dev == socket_device && st.st_ino == socket_inode)
            unlink(socket_path.c_str());
        socket_owned = false;
    }
    void accept_peers() {
        for (unsigned accepted = 0; accepted != 4; ++accepted) {
            Descriptor client; client.value = accept(listener.value, nullptr, nullptr);
            if (client.value < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) { ++rejected; return; }
                system_error("accept component peer");
            }
            auto slot = std::find_if(peers.begin(), peers.end(), [](const auto &p) { return !p; });
            struct ucred credentials; socklen_t length = sizeof(credentials);
            if (slot == peers.end() || getsockopt(client.value, SOL_SOCKET, SO_PEERCRED, &credentials, &length) != 0
                || length != sizeof(credentials) || credentials.uid != geteuid()) { ++rejected; continue; }
            nonblocking(client.value);
            auto peer = std::make_unique<Peer>(); peer->fd.value = client.value; client.value = -1;
            peer->deadline = Clock::now() + std::chrono::milliseconds(options.io_timeout_ms);
            *slot = std::move(peer); ++connections;
        }
    }
    bool read_peer(Peer &p) {
        size_t budget = 65536;
        while (budget) {
            char *destination;
            size_t remaining;
            if (!p.parsed) { destination = p.header.data() + p.header_used; remaining = p.header.size() - p.header_used; }
            else { destination = p.input.data() + p.input_used; remaining = p.input.size() - p.input_used; }
            if (!remaining) return true;
            ssize_t n = recv(p.fd.value, destination, std::min(budget, remaining), 0);
            if (n < 0) {
                if (errno == EINTR) continue;
                return errno == EAGAIN || errno == EWOULDBLOCK;
            }
            if (!n) return false;
            budget -= static_cast<size_t>(n);
            if (p.parsed) { p.input_used += n; continue; }
            p.header_used += n;
            if (p.header_used == p.header.size()) {
                p.request = wire::frame({p.header.data(), p.header.size()});
                if (p.request.result || p.request.size > options.max_payload_bytes)
                    throw IndexError(NGX_PURGE_STORE_LIMIT, "invalid component request size/result");
                resize(p.input, p.request.size); p.parsed = true;
            }
        }
        return true;
    }
    bool write_peer(Peer &p) {
        const auto remaining = p.output.size() - p.output_used;
        auto n = send(p.fd.value, p.output.data() + p.output_used, std::min<size_t>(65536, remaining), MSG_NOSIGNAL);
        if (n < 0) return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
        if (!n) return false;
        p.output_used += n;
        if (p.output_used == p.output.size()) {
            release(p.output); p.output_used = p.header_used = p.input_used = 0; p.parsed = false;
            p.deadline = Clock::now() + std::chrono::milliseconds(options.io_timeout_ms);
        }
        return true;
    }
    std::string dispatch(Index &index, Operations &operations, ngx_cache_purge_store_t *store, const Peer &p) {
        wire::Decoder d{p.input}; wire::Encoder e;
        switch (p.request.opcode) {
        case NGX_PURGE_OP_PING: {
            d.end(); ngx_cache_purge_store_stats_t stats; ngx_cache_purge_store_error_t error;
            checked(ngx_cache_purge_store_stats(store, &stats, &error), error);
            e.integer(pid, 8); e.integer(stats.writes, 8); e.integer(stats.cache_usage, 8);
            e.integer(stats.write_buffer_usage, 8); e.integer(connections.load(), 8);
            e.integer(requests.load(), 8); e.integer(rejected.load(), 8);
            e.integer(buffered.load(), 8); e.integer(peak.load(), 8); break;
        }
        case NGX_PURGE_OP_GET: {
            auto id = d.id(); d.end(); auto f = index.get(id);
            e.byte(static_cast<unsigned>(f.state)); e.integer(f.revision, 8); wire::file(e, f); break;
        }
        case NGX_PURGE_OP_PREPARE: {
            auto count = d.integer(1);
            if (!count || count > 32) throw IndexError(NGX_PURGE_STORE_LIMIT, "IPC prepare batch count");
            std::vector<Fill> fills; fills.reserve(count);
            while (count--) { auto f = wire::file(d); fills.push_back({std::move(f), d.id()}); }
            d.end(); auto result = index.prepare_batch(fills); e.byte(result.size());
            for (auto &m : result) wire::match(e, m);
            break;
        }
        case NGX_PURGE_OP_COMMIT:
        case NGX_PURGE_OP_RETIRE:
        case NGX_PURGE_OP_COLLECT: {
            auto files = wire::expected(d); d.end();
            if (p.request.opcode == NGX_PURGE_OP_COLLECT) { index.collect_files(files); break; }
            auto result = p.request.opcode == NGX_PURGE_OP_COMMIT ? index.commit_batch(files) : index.retire_batch(files);
            e.byte(result.size()); for (auto &m : result) wire::match(e, m);
            break;
        }
        case NGX_PURGE_OP_ABORT: {
            auto id = d.id(); auto revision = d.integer(8); d.end(); wire::match(e, index.abort_prepare(id, revision)); break;
        }
        case NGX_PURGE_OP_QUERY:
        case NGX_PURGE_OP_PENDING:
        case NGX_PURGE_OP_RETIRED: {
            Selector selector;
            if (p.request.opcode == NGX_PURGE_OP_QUERY) selector = wire::selector(d);
            auto bookmark = d.string(33024); auto limit = d.integer(1); d.end();
            if (!limit || limit > 32) throw IndexError(NGX_PURGE_STORE_LIMIT, "IPC query page limit");
            auto query = p.request.opcode == NGX_PURGE_OP_QUERY ? index.query(selector, bookmark)
                : p.request.opcode == NGX_PURGE_OP_PENDING ? index.pending_fills(bookmark) : index.retired_files(bookmark);
            std::vector<Match> result; bool done = false; Match match;
            try {
                while (result.size() < limit) {
                    if (!query->next(match)) { done = true; break; }
                    result.push_back(match); bookmark = query->bookmark();
                }
            } catch (const IndexError &error) {
                if (error.code != NGX_PURGE_STORE_EXPIRED || result.empty()) throw;
            }
            e.byte(result.size()); e.byte(done); e.string(bookmark);
            for (auto &m : result) wire::match(e, m);
            break;
        }
        case NGX_PURGE_OP_COLLECT_KEYS: {
            auto limit = d.integer(1); d.end(); e.integer(index.collect_keys(limit), 4); break;
        }
        case NGX_PURGE_OP_ADD_URL: {
            auto key = d.string(), url = d.string(); d.end(); index.add_url(key, url); break;
        }
        case NGX_PURGE_OP_UPDATE: {
            auto id = d.id(); auto revision = d.integer(8); auto metadata = d.string(16384); auto tags = wire::tags(d);
            d.end(); wire::match(e, index.update_metadata(id, revision, metadata, tags)); break;
        }
        case NGX_PURGE_OP_ACCEPT_OPERATION: {
            auto input = wire::operation_input(d); d.end();
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            if (now <= 0) throw IndexError(NGX_PURGE_STORE_IO, "invalid component wall clock");
            wire::operation(e, operations.accept(input, static_cast<uint64_t>(now))); break;
        }
        case NGX_PURGE_OP_GET_OPERATION: {
            auto id = d.string(128); d.end(); wire::operation(e, operations.get(id)); break;
        }
        default: throw IndexError(NGX_PURGE_STORE_INVALID, "unknown component operation");
        }
        return e.data;
    }
    void execute(Index &index, Operations &operations, ngx_cache_purge_store_t *store, Peer &p) {
        std::string response; uint32_t code = 0; ++requests;
        try { response = dispatch(index, operations, store, p); }
        catch (const IndexError &error) {
            if (error.code == NGX_PURGE_STORE_CORRUPT || error.code == NGX_PURGE_STORE_SCHEMA || error.code == NGX_PURGE_STORE_IO) throw;
            wire::Encoder e; e.string(error.what()); response = std::move(e.data); code = error.code;
        }
        release(p.input);
        if (response.size() > options.max_payload_bytes) throw IndexError(NGX_PURGE_STORE_LIMIT, "IPC response size limit");
        auto packet = wire::frame(p.request.opcode, p.request.id, response, code);
        resize(p.output, packet.size()); p.output.swap(packet);
        p.deadline = Clock::now() + std::chrono::milliseconds(options.io_timeout_ms);
    }
    void serve(ngx_cache_purge_store_t *store) {
        Index index(store); Operations operations(store); bind_socket();
        peers.resize(options.max_connections); state.store(NGX_PURGE_COMPONENT_OPEN);
        while (!stopping.load()) {
            std::array<pollfd, 66> events{};
            events[0] = {wake_read.value, POLLIN, 0}; events[1] = {listener.value, POLLIN, 0};
            bool ready = false;
            for (size_t i = 0; i < peers.size(); ++i) {
                events[i + 2].fd = -1;
                if (!peers[i]) continue;
                auto &p = *peers[i];
                if (Clock::now() >= p.deadline) { ++rejected; drop(i); continue; }
                bool complete = p.parsed && p.input_used == p.input.size();
                events[i + 2] = {p.fd.value, static_cast<short>(!p.output.empty() ? POLLOUT : complete ? 0 : POLLIN), 0};
                ready |= complete && p.output.empty();
            }
            int rc = poll(events.data(), peers.size() + 2, ready ? 0 : 50);
            if (rc < 0) { if (errno == EINTR) continue; system_error("component poll"); }
            if (events[0].revents) clear_wake();
            if (stopping.load()) break;
            if (events[1].revents & POLLIN) accept_peers();
            bool executed = false;
            size_t start = next_peer;
            for (size_t step = 0; step < peers.size(); ++step) {
                size_t i = (start + step) % peers.size();
                if (!peers[i]) continue;
                auto &p = *peers[i]; short event = events[i + 2].revents;
                if (event & (POLLERR | POLLHUP | POLLNVAL)) { drop(i); continue; }
                try {
                    if (event & POLLOUT) { if (!write_peer(p)) drop(i); continue; }
                    if (event & POLLIN) { if (!read_peer(p)) { drop(i); continue; } }
                    if (!executed && p.output.empty() && p.parsed && p.input_used == p.input.size()) {
                        execute(index, operations, store, p); executed = true; next_peer = (i + 1) % peers.size();
                    }
                } catch (const IndexError &e) {
                    if (e.code != NGX_PURGE_STORE_INVALID && e.code != NGX_PURGE_STORE_LIMIT) throw;
                    ++rejected; drop(i);
                }
            }
            // Page cursors are scoped to dispatch, so idle peers retain no DB
            // snapshots. Do not collect RocksDB property statistics per poll.
        }
    }
    void run() noexcept {
        ngx_cache_purge_store_t *store = nullptr;
        int rc = boundary(&terminal_error, [&]() -> int {
            ngx_cache_purge_store_error_t error;
            while (!stopping.load()) {
                int result = ngx_cache_purge_store_open(path.c_str(), &options.store, &store, &error);
                if (!result) break;
                if (result != NGX_PURGE_STORE_BUSY) checked(result, error);
                state.store(NGX_PURGE_COMPONENT_STANDBY);
                pollfd p{wake_read.value, POLLIN, 0};
                if (poll(&p, 1, options.owner_retry_ms) < 0 && errno != EINTR) system_error("component standby poll");
                if (p.revents) clear_wake();
            }
            if (store && !stopping.load()) serve(store);
            return 0;
        });
        if (rc) error_code.store(rc, std::memory_order_release);
        state.store(NGX_PURGE_COMPONENT_STOPPING);
        for (size_t i = 0; i < peers.size(); ++i) drop(i);
        remove_socket(); // Still own the DB lock when removing the rendezvous path.
        if (store) {
            ngx_cache_purge_store_error_t error;
            int closed = ngx_cache_purge_store_close(&store, &error);
            if (closed) fail(closed, error.message);
        }
        state.store(error_code.load() ? NGX_PURGE_COMPONENT_FAILED : NGX_PURGE_COMPONENT_STOPPED,
                    std::memory_order_release);
    }
};

extern "C" void ngx_cache_purge_component_options(ngx_cache_purge_component_options_t *o) {
    if (!o) return;
    *o = {}; o->abi = NGX_CACHE_PURGE_COMPONENT_ABI; ngx_cache_purge_store_options(&o->store);
    o->max_connections = 32; o->max_payload_bytes = 512 * 1024; o->buffer_bytes = 4 * 1024 * 1024;
    o->io_timeout_ms = 30000; o->owner_retry_ms = 50;
}
extern "C" int ngx_cache_purge_component_start(const char *path,
    const ngx_cache_purge_component_options_t *options,
    ngx_cache_purge_component_t **output, ngx_cache_purge_store_error_t *error) {
    if (output) *output = nullptr;
    return boundary(error, [&]() -> int {
        if (!path || !options || !output || options->abi != NGX_CACHE_PURGE_COMPONENT_ABI
            || !options->max_connections || options->max_connections > 64
            || options->max_payload_bytes < 1024 || options->max_payload_bytes > 1024 * 1024
            || options->buffer_bytes < 2 * options->max_payload_bytes + NGX_CACHE_PURGE_FRAME_SIZE
            || options->buffer_bytes > 64 * 1024 * 1024 || !options->io_timeout_ms || options->io_timeout_ms > 300000
            || !options->owner_retry_ms || options->owner_retry_ms > 1000)
            return failure(error, NGX_PURGE_STORE_INVALID, "invalid component options");
        auto component = std::make_unique<ngx_cache_purge_component_s>();
        component->path = path; component->socket_path = component->path + "/component.sock";
        if (component->path.empty() || component->path[0] != '/' || component->socket_path.size() >= sizeof(sockaddr_un::sun_path))
            return failure(error, NGX_PURGE_STORE_INVALID, "component requires a short absolute Unix socket path");
        component->options = *options;
        int descriptors[2]; if (pipe(descriptors) < 0) system_error("component wake pipe");
        component->wake_read.value = descriptors[0]; component->wake_write.value = descriptors[1];
        nonblocking(descriptors[0]); nonblocking(descriptors[1]);
        sigset_t blocked, previous;
        sigfillset(&blocked);
        for (int signal : {SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT}) sigdelset(&blocked, signal);
        if (pthread_sigmask(SIG_BLOCK, &blocked, &previous) != 0)
            return failure(error, NGX_PURGE_STORE_IO, "cannot block component control signals");
        try { component->executor = std::thread([value = component.get()] { value->run(); }); }
        catch (...) { pthread_sigmask(SIG_SETMASK, &previous, nullptr); throw; }
        if (pthread_sigmask(SIG_SETMASK, &previous, nullptr) != 0) {
            component->stopping.store(true); component->wake(); component->executor.join();
            return failure(error, NGX_PURGE_STORE_IO, "cannot restore manager signal mask");
        }
        *output = component.release(); return 0;
    });
}
extern "C" int ngx_cache_purge_component_status(ngx_cache_purge_component_t *c,
    ngx_cache_purge_component_status_t *status, ngx_cache_purge_store_error_t *error) {
    return boundary(error, [&]() -> int {
        if (!c || !status) return failure(error, NGX_PURGE_STORE_INVALID, "null component status");
        if (!c->owner()) return failure(error, NGX_PURGE_STORE_OWNER, "component creator mismatch");
        *status = {c->state.load(), c->error_code.load(), c->connections.load(), c->requests.load(),
                   c->rejected.load(), c->buffered.load(), c->peak.load()};
        return 0;
    });
}
extern "C" int ngx_cache_purge_component_stop(ngx_cache_purge_component_t **output,
    ngx_cache_purge_store_error_t *error) {
    return boundary(error, [&]() -> int {
        if (!output || !*output) return failure(error, NGX_PURGE_STORE_INVALID, "null component stop");
        if (!(*output)->owner()) return failure(error, NGX_PURGE_STORE_OWNER, "component creator mismatch");
        auto *component = *output;
        component->stopping.store(true); component->wake(); component->executor.join();
        int code = component->error_code.load();
        if (code && error) *error = component->terminal_error;
        delete component; *output = nullptr; return code;
    });
}
