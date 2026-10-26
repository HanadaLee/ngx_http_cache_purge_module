/* Copyright (C) Hanada */
#include "ngx_http_cache_purge_internal.h"
#include "ngx_http_cache_purge_backend.hpp"
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

using namespace ngx_http_cache_purge;
namespace wire = ngx_http_cache_purge::protocol;
using Clock = std::chrono::steady_clock;

namespace {
int failure(ngx_http_cache_rocksdb_error_t *error, int code, const char *message) noexcept {
    if (error) { error->code = code; std::snprintf(error->message, sizeof(error->message), "%s", message); }
    return code;
}
template<class Function> int boundary(ngx_http_cache_rocksdb_error_t *error, Function function) noexcept {
    if (error) { error->code = 0; error->message[0] = 0; }
    try { return function(); }
    catch (const IndexError &e) { return failure(error, e.code, e.what()); }
    catch (const std::bad_alloc &) { return failure(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "component allocation failed"); }
    catch (const std::exception &e) { return failure(error, NGX_HTTP_CACHE_ROCKSDB_IO, e.what()); }
    catch (...) { return failure(error, NGX_HTTP_CACHE_ROCKSDB_IO, "unknown component error"); }
}
[[noreturn]] void system_error(const char *message) {
    int saved = errno;
    throw IndexError(NGX_HTTP_CACHE_ROCKSDB_IO, std::string(message) + ": " + std::strerror(saved));
}
void checked(int rc, const ngx_http_cache_rocksdb_error_t &error) {
    if (rc) throw IndexError(rc, error.message);
}
void logical_record(Index &index, const FileId &id, bool missing = false) {
    try {
        if (index.get(id).metadata.compare(0, 4, "NFP1") == 0)
            throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "physical file publication requires filesystem operations");
    } catch (const IndexError &error) {
        if (!missing || error.code != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) throw;
    }
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
    Descriptor output_lease;
    Descriptor input_rights[2];
    size_t rights_count = 0;
    uid_t uid = 0;
    std::array<char, NGX_HTTP_CACHE_PURGE_FRAME_SIZE> header{};
    size_t header_used = 0, input_used = 0, output_used = 0;
    std::string input, output;
    wire::Frame request{};
    bool parsed = false;
    Clock::time_point deadline;
};
} // namespace

struct ngx_http_cache_purge_component_s {
    std::string path, socket_path;
    ngx_http_cache_purge_component_options_t options{};
    pid_t pid = getpid();
    std::thread::id creator = std::this_thread::get_id();
    std::thread executor;
    Descriptor wake_read, wake_write, listener, lease_anchor;
    bool socket_owned = false;
    dev_t socket_device = 0;
    ino_t socket_inode = 0;
    std::atomic<bool> stopping{false};
    std::atomic<uint32_t> state{NGX_HTTP_CACHE_PURGE_COMPONENT_STARTING}, error_code{0};
    std::atomic<uint64_t> connections{0}, requests{0}, rejected{0}, buffered{0}, peak{0};
    std::atomic<uint32_t> control_state{0};
    std::atomic<uint64_t> control_publication{0};
    ngx_http_cache_rocksdb_error_t terminal_error{};
    std::vector<std::unique_ptr<Peer>> peers;
    std::vector<RootSpec> roots;
    std::unique_ptr<Publications> publications;
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
            throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "component input/output budget");
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
            if (!S_ISSOCK(st.st_mode)
                || (st.st_uid != geteuid() && st.st_uid != options.client_uid))
                throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "component socket path is not an owned socket");
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
        if (options.client_uid != geteuid()
            && lchown(socket_path.c_str(), static_cast<uid_t>(options.client_uid), static_cast<gid_t>(-1)) < 0)
            system_error("set component socket client owner");
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
                || length != sizeof(credentials)
                || (credentials.uid != geteuid() && credentials.uid != options.client_uid)) { ++rejected; continue; }
            nonblocking(client.value);
            auto peer = std::make_unique<Peer>(); peer->fd.value = client.value; client.value = -1;
            peer->uid = credentials.uid;
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
            int rights[NGX_HTTP_CACHE_PURGE_TRANSPORT_FDS]; size_t count;
            bool first = !p.parsed && p.header_used == 0;
            ssize_t n = ngx_http_cache_purge_transport_recv(p.fd.value, destination,
                              first ? 1 : std::min(budget, remaining), first, rights, &count);
            if (n < 0) {
                if (errno == EINTR) continue;
                return errno == EAGAIN || errno == EWOULDBLOCK;
            }
            if (!n) return false;
            if (count) {
                for (size_t i = 0; i < count; i++) p.input_rights[i].value = rights[i];
                p.rights_count = count;
            }
            budget -= static_cast<size_t>(n);
            if (p.parsed) { p.input_used += n; continue; }
            p.header_used += n;
            if (p.header_used == p.header.size()) {
                p.request = wire::frame({p.header.data(), p.header.size()});
                if (p.request.result || p.request.size > options.max_payload_bytes)
                    throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "invalid component request size/result");
                if (p.rights_count != (p.request.opcode == NGX_HTTP_CACHE_PURGE_OP_PREPARE_FILE ? 2U
                                      : p.request.opcode == NGX_HTTP_CACHE_PURGE_OP_FINISH_FILE ? 1U : 0U))
                    throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "unexpected component request descriptors");
                resize(p.input, p.request.size); p.parsed = true;
            }
        }
        return true;
    }
    bool write_peer(Peer &p) {
        const auto remaining = p.output.size() - p.output_used;
        bool attach = p.output_lease.value >= 0;
        auto n = ngx_http_cache_purge_transport_send(p.fd.value, p.output.data() + p.output_used,
                         attach ? 1 : std::min<size_t>(65536, remaining),
                         &p.output_lease.value, attach ? 1 : 0);
        if (n < 0) return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
        if (!n) return false;
        if (attach) p.output_lease.reset(); // The socket queue now owns the transferred reference.
        p.output_used += n;
        if (p.output_used == p.output.size()) {
            release(p.output); p.output_used = p.header_used = p.input_used = 0; p.parsed = false;
            p.deadline = Clock::now() + std::chrono::milliseconds(options.io_timeout_ms);
        }
        return true;
    }
    std::string dispatch(Index &index, Operations &operations, Coordinator *coordinator, ngx_http_cache_rocksdb_t *store, Peer &p) {
        wire::Decoder d{p.input}; wire::Encoder e;
        switch (p.request.opcode) {
        case NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_PUBLICATION: {
            auto root = d.string(); auto zone = d.string(); auto name = d.string(); d.end();
            auto target = publications->target(index, root, zone, name);
            unsigned char digest[16]; uint64_t offset;
            ngx_http_cache_rocksdb_error_t error;
            checked(ngx_http_cache_purge_file_address({reinterpret_cast<const unsigned char *>(target.address.data()),
                                                  target.address.size()}, digest, &offset, &error), error);
            auto lock_path = path + "/file.leases";
            if (lease_anchor.value < 0)
                checked(ngx_http_cache_purge_file_locks_open(lock_path.c_str(), geteuid(), 1, &lease_anchor.value, &error), error);
            checked(ngx_http_cache_purge_file_lease_acquire(lock_path.c_str(), lease_anchor.value, offset,
                                                     &p.output_lease.value, &error), error);
            e.string(target.root); e.id(target.previous);
            e.data.append(reinterpret_cast<const char *>(digest), sizeof(digest)); e.integer(offset, 8);
            break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_FINISH_FILE: {
            auto id = d.id(); auto revision = d.integer(8); d.end();
            auto lock_path = path + "/file.leases";
            ngx_http_cache_rocksdb_error_t error;
            if (lease_anchor.value < 0)
                checked(ngx_http_cache_purge_file_locks_open(lock_path.c_str(), geteuid(), 1, &lease_anchor.value, &error), error);
            wire::match(e, publications->finish(index, id, revision, lease_anchor.value, p.input_rights[0].value, p.uid));
            break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_PREPARE_FILE: {
            Fill fill{wire::file(d), d.id()}; auto temporary = d.string(); d.end();
            auto lock_path = path + "/file.leases";
            ngx_http_cache_rocksdb_error_t error;
            if (lease_anchor.value < 0)
                checked(ngx_http_cache_purge_file_locks_open(lock_path.c_str(), geteuid(), 1, &lease_anchor.value, &error), error);
            auto match = publications->prepare(index, std::move(fill), temporary, lease_anchor.value,
                                               p.input_rights[0].value, p.input_rights[1].value, p.uid);
            wire::match(e, match); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_ACQUIRE_FILE_LEASE: {
            auto address = d.string(65536); d.end();
            unsigned char digest[16]; uint64_t offset;
            ngx_http_cache_rocksdb_error_t error;
            checked(ngx_http_cache_purge_file_address({reinterpret_cast<const unsigned char *>(address.data()), address.size()},
                                                digest, &offset, &error), error);
            auto lock_path = path + "/file.leases";
            if (lease_anchor.value < 0)
                checked(ngx_http_cache_purge_file_locks_open(lock_path.c_str(), geteuid(), 1, &lease_anchor.value, &error), error);
            checked(ngx_http_cache_purge_file_lease_acquire(lock_path.c_str(), lease_anchor.value, offset,
                                                     &p.output_lease.value, &error), error);
            e.data.append(reinterpret_cast<const char *>(digest), sizeof(digest)); e.integer(offset, 8);
            break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_PING: {
            d.end(); ngx_http_cache_rocksdb_stats_t stats; ngx_http_cache_rocksdb_error_t error;
            checked(ngx_http_cache_rocksdb_stats(store, &stats, &error), error);
            e.integer(pid, 8); e.integer(stats.writes, 8); e.integer(stats.cache_usage, 8);
            e.integer(stats.write_buffer_usage, 8); e.integer(connections.load(), 8);
            e.integer(requests.load(), 8); e.integer(rejected.load(), 8);
            e.integer(buffered.load(), 8); e.integer(peak.load(), 8); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_GET: {
            auto id = d.id(); d.end(); auto f = index.get(id);
            e.byte(static_cast<unsigned>(f.state)); e.integer(f.revision, 8); wire::file(e, f); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_PREPARE: {
            auto count = d.integer(1);
            if (!count || count > 32) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC prepare batch count");
            std::vector<Fill> fills; fills.reserve(count);
            while (count--) {
                auto f = wire::file(d);
                if (f.metadata.compare(0, 4, "NFP1") == 0)
                    throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "reserved physical publication metadata");
                fills.push_back({std::move(f), d.id()});
            }
            d.end(); auto result = index.prepare_batch(fills); e.byte(result.size());
            for (auto &m : result) wire::match(e, m);
            break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_COMMIT:
        case NGX_HTTP_CACHE_PURGE_OP_RETIRE:
        case NGX_HTTP_CACHE_PURGE_OP_COLLECT: {
            auto files = wire::expected(d); d.end();
            for (const auto &file : files) logical_record(index, file.id, p.request.opcode == NGX_HTTP_CACHE_PURGE_OP_COLLECT);
            if (p.request.opcode == NGX_HTTP_CACHE_PURGE_OP_COLLECT) { index.collect_files(files); break; }
            auto result = p.request.opcode == NGX_HTTP_CACHE_PURGE_OP_COMMIT ? index.commit_batch(files) : index.retire_batch(files);
            e.byte(result.size()); for (auto &m : result) wire::match(e, m);
            break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_ABORT: {
            auto id = d.id(); auto revision = d.integer(8); d.end(); logical_record(index, id);
            wire::match(e, index.abort_prepare(id, revision)); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_QUERY:
        case NGX_HTTP_CACHE_PURGE_OP_PENDING:
        case NGX_HTTP_CACHE_PURGE_OP_RETIRED: {
            Selector selector;
            if (p.request.opcode == NGX_HTTP_CACHE_PURGE_OP_QUERY) selector = wire::selector(d);
            auto bookmark = d.string(33024); auto limit = d.integer(1); d.end();
            if (!limit || limit > 32) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC query page limit");
            auto query = p.request.opcode == NGX_HTTP_CACHE_PURGE_OP_QUERY ? index.query(selector, bookmark)
                : p.request.opcode == NGX_HTTP_CACHE_PURGE_OP_PENDING ? index.pending_fills(bookmark) : index.retired_files(bookmark);
            std::vector<Match> result; bool done = false; Match match;
            try {
                while (result.size() < limit) {
                    if (!query->next(match)) { done = true; break; }
                    result.push_back(match); bookmark = query->bookmark();
                }
            } catch (const IndexError &error) {
                if (error.code != NGX_HTTP_CACHE_ROCKSDB_EXPIRED || result.empty()) throw;
            }
            e.byte(result.size()); e.byte(done); e.string(bookmark);
            for (auto &m : result) wire::match(e, m);
            break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_COLLECT_KEYS: {
            auto limit = d.integer(1); d.end(); e.integer(index.collect_keys(limit), 4); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_ADD_URL: {
            auto key = d.string(), url = d.string(); d.end(); index.add_url(key, url); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_UPDATE: {
            auto id = d.id(); auto revision = d.integer(8); auto metadata = d.string(16384); auto tags = wire::tags(d);
            d.end(); logical_record(index, id);
            if (metadata.compare(0, 4, "NFP1") == 0)
                throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "reserved physical publication metadata");
            wire::match(e, index.update_metadata(id, revision, metadata, tags)); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_ACCEPT_OPERATION: {
            auto input = wire::operation_input(d); d.end();
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            if (now <= 0) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_IO, "invalid component wall clock");
            wire::operation(e, coordinator ? coordinator->accept(input, static_cast<uint64_t>(now), options.propagation_ms)
                                           : operations.accept(input, static_cast<uint64_t>(now), options.propagation_ms)); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_GET_OPERATION: {
            auto id = d.string(128); d.end(); wire::operation(e, operations.get(id)); break;
        }
        case NGX_HTTP_CACHE_PURGE_OP_GET_CONTROL_STATUS: {
            d.end(); e.integer(coordinator ? static_cast<unsigned>(coordinator->state()) : 0, 4);
            e.integer(coordinator ? coordinator->publication() : 0, 8); break;
        }
        default: throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "unknown component operation");
        }
        return e.data;
    }
    void execute(Index &index, Operations &operations, Coordinator *coordinator, ngx_http_cache_rocksdb_t *store, Peer &p) {
        std::string response; uint32_t code = 0; ++requests;
        try { response = dispatch(index, operations, coordinator, store, p); }
        catch (const IndexError &error) {
            p.output_lease.reset();
            if (error.code == NGX_HTTP_CACHE_ROCKSDB_CORRUPT || error.code == NGX_HTTP_CACHE_ROCKSDB_SCHEMA || error.code == NGX_HTTP_CACHE_ROCKSDB_IO) throw;
            wire::Encoder e; e.string(error.what()); response = std::move(e.data); code = error.code;
        }
        release(p.input);
        for (auto &right : p.input_rights) right.reset();
        p.rights_count = 0;
        if (response.size() > options.max_payload_bytes) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC response size limit");
        auto packet = wire::frame(p.request.opcode, p.request.id, response, code);
        resize(p.output, packet.size()); p.output.swap(packet);
        p.deadline = Clock::now() + std::chrono::milliseconds(options.io_timeout_ms);
    }
    void serve(ngx_http_cache_rocksdb_t *store) {
        publications = std::make_unique<Publications>(roots, path);
        Index index(store); Operations operations(store, {options.max_live_operations, options.live_selector_bytes});
        std::unique_ptr<Coordinator> coordinator;
        if (options.control_memory) coordinator = std::make_unique<Coordinator>(operations, options.control_memory, options.control_bytes);
        auto expire_at = Clock::now() + std::chrono::seconds(1);
        bind_socket();
        peers.resize(options.max_connections); state.store(NGX_HTTP_CACHE_PURGE_COMPONENT_OPEN);
        while (!stopping.load()) {
            bool control_progress = false;
            if (coordinator) {
                if (coordinator->state() != Coordination::ready) control_progress = coordinator->step();
                else if (Clock::now() >= expire_at) {
                    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
                    if (now <= 0) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_IO, "invalid component expiration clock");
                    if (operations.expire_propagation(static_cast<uint64_t>(now), 32)) coordinator->refresh();
                    expire_at = Clock::now() + std::chrono::seconds(1);
                }
                control_state.store(static_cast<unsigned>(coordinator->state()));
                control_publication.store(coordinator->publication());
            }
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
            bool control_pending = coordinator && coordinator->state() != Coordination::ready;
            int rc = poll(events.data(), peers.size() + 2, ready || (control_pending && control_progress) ? 0 : 50);
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
                        execute(index, operations, coordinator.get(), store, p); executed = true; next_peer = (i + 1) % peers.size();
                    }
                } catch (const IndexError &e) {
                    if (e.code != NGX_HTTP_CACHE_ROCKSDB_INVALID && e.code != NGX_HTTP_CACHE_ROCKSDB_LIMIT) throw;
                    ++rejected; drop(i);
                }
            }
            // Page cursors are scoped to dispatch, so idle peers retain no DB
            // snapshots. Do not collect RocksDB property statistics per poll.
        }
    }
    void run() noexcept {
        ngx_http_cache_rocksdb_t *store = nullptr;
        int rc = boundary(&terminal_error, [&]() -> int {
            ngx_http_cache_rocksdb_error_t error;
            while (!stopping.load()) {
                int result = ngx_http_cache_rocksdb_open(path.c_str(), &options.store, &store, &error);
                if (!result) break;
                if (result != NGX_HTTP_CACHE_ROCKSDB_BUSY) checked(result, error);
                state.store(NGX_HTTP_CACHE_PURGE_COMPONENT_STANDBY);
                pollfd p{wake_read.value, POLLIN, 0};
                if (poll(&p, 1, options.owner_retry_ms) < 0 && errno != EINTR) system_error("component standby poll");
                if (p.revents) clear_wake();
            }
            if (store && !stopping.load()) serve(store);
            return 0;
        });
        if (rc) error_code.store(rc, std::memory_order_release);
        state.store(NGX_HTTP_CACHE_PURGE_COMPONENT_STOPPING);
        for (size_t i = 0; i < peers.size(); ++i) drop(i);
        remove_socket(); // Still own the DB lock when removing the rendezvous path.
        lease_anchor.reset();
        publications.reset();
        if (store) {
            ngx_http_cache_rocksdb_error_t error;
            int closed = ngx_http_cache_rocksdb_close(&store, &error);
            if (closed) fail(closed, error.message);
        }
        state.store(error_code.load() ? NGX_HTTP_CACHE_PURGE_COMPONENT_FAILED : NGX_HTTP_CACHE_PURGE_COMPONENT_STOPPED,
                    std::memory_order_release);
    }
};

extern "C" void ngx_http_cache_purge_component_options(ngx_http_cache_purge_component_options_t *o) {
    if (!o) return;
    *o = {}; o->abi = NGX_HTTP_CACHE_PURGE_COMPONENT_ABI; ngx_http_cache_rocksdb_options(&o->store);
    o->max_connections = 32; o->max_payload_bytes = 512 * 1024; o->buffer_bytes = 4 * 1024 * 1024;
    o->io_timeout_ms = 30000; o->owner_retry_ms = 50;
    o->client_uid = UINT32_MAX;
    o->propagation_ms = 300000; o->max_live_operations = 4096; o->live_selector_bytes = 4 * 1024 * 1024;
}
extern "C" uint32_t ngx_http_cache_purge_component_abi_5(void) { return NGX_HTTP_CACHE_PURGE_COMPONENT_ABI; }
extern "C" int ngx_http_cache_purge_component_start(const char *path,
    const ngx_http_cache_purge_component_options_t *options,
    ngx_http_cache_purge_component_t **output, ngx_http_cache_rocksdb_error_t *error) {
    if (output) *output = nullptr;
    return boundary(error, [&]() -> int {
        if (!path || !options || !output || options->abi != NGX_HTTP_CACHE_PURGE_COMPONENT_ABI
            || !options->max_connections || options->max_connections > 64
            || options->max_payload_bytes < 1024 || options->max_payload_bytes > 1024 * 1024
            || options->buffer_bytes < 2 * options->max_payload_bytes + NGX_HTTP_CACHE_PURGE_FRAME_SIZE
            || options->buffer_bytes > 64 * 1024 * 1024 || !options->io_timeout_ms || options->io_timeout_ms > 300000
            || !options->owner_retry_ms || options->owner_retry_ms > 1000
            || !options->max_live_operations || options->max_live_operations > 65536
            || options->live_selector_bytes < 1024 || options->live_selector_bytes > 64 * 1024 * 1024
            || (options->control_memory == nullptr) != (options->control_bytes == 0)
            || options->root_count > 256 || (options->root_count && !options->roots))
            return failure(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid component options");
        auto component = std::make_unique<ngx_http_cache_purge_component_s>();
        component->path = path; component->socket_path = component->path + "/component.sock";
        if (component->path.empty() || component->path[0] != '/' || component->socket_path.size() >= sizeof(sockaddr_un::sun_path))
            return failure(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "component requires a short absolute Unix socket path");
        component->options = *options;
        size_t root_bytes = 0;
        for (size_t i = 0; i < options->root_count; i++) {
            const auto &root = options->roots[i];
            if (!root.path.data || root.path.len < 2 || root.path.len > 4096 || root.zone.len > 128
                || (root.zone.len && !root.zone.data) || root.path.data[0] != '/'
                || std::memchr(root.path.data, 0, root.path.len)
                || (root.zone.len && std::memchr(root.zone.data, 0, root.zone.len))
                || (root.kind != NGX_HTTP_CACHE_PURGE_ROOT_CACHE && root.kind != NGX_HTTP_CACHE_PURGE_ROOT_TEMP)
                || (root.kind == NGX_HTTP_CACHE_PURGE_ROOT_CACHE && !root.zone.len))
                return failure(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid publication root");
            root_bytes += root.path.len + root.zone.len;
            if (root_bytes > 65536) return failure(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "publication roots exceed budget");
            component->roots.push_back({std::string(reinterpret_cast<const char *>(root.path.data), root.path.len),
                root.zone.len ? std::string(reinterpret_cast<const char *>(root.zone.data), root.zone.len) : "", root.kind});
        }
        component->options.roots = nullptr; // No caller-owned pointers cross the executor boundary.
        if (component->options.client_uid == UINT32_MAX)
            component->options.client_uid = static_cast<uint32_t>(geteuid());
        if (component->options.client_uid != geteuid() && geteuid() != 0)
            return failure(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "component client UID requires root creator");
        int descriptors[2]; if (pipe(descriptors) < 0) system_error("component wake pipe");
        component->wake_read.value = descriptors[0]; component->wake_write.value = descriptors[1];
        nonblocking(descriptors[0]); nonblocking(descriptors[1]);
        sigset_t blocked, previous;
        sigfillset(&blocked);
        for (int signal : {SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT}) sigdelset(&blocked, signal);
        if (pthread_sigmask(SIG_BLOCK, &blocked, &previous) != 0)
            return failure(error, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot block component control signals");
        try { component->executor = std::thread([value = component.get()] { value->run(); }); }
        catch (...) { pthread_sigmask(SIG_SETMASK, &previous, nullptr); throw; }
        if (pthread_sigmask(SIG_SETMASK, &previous, nullptr) != 0) {
            component->stopping.store(true); component->wake(); component->executor.join();
            return failure(error, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot restore manager signal mask");
        }
        *output = component.release(); return 0;
    });
}
extern "C" int ngx_http_cache_purge_component_status(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_purge_component_status_t *status, ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        if (!c || !status) return failure(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null component status");
        if (!c->owner()) return failure(error, NGX_HTTP_CACHE_ROCKSDB_OWNER, "component creator mismatch");
        *status = {c->state.load(), c->error_code.load(), c->connections.load(), c->requests.load(),
                   c->rejected.load(), c->buffered.load(), c->peak.load(), c->control_state.load(), c->control_publication.load()};
        return 0;
    });
}
extern "C" int ngx_http_cache_purge_component_stop(ngx_http_cache_purge_component_t **output,
    ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        if (!output || !*output) return failure(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null component stop");
        if (!(*output)->owner()) return failure(error, NGX_HTTP_CACHE_ROCKSDB_OWNER, "component creator mismatch");
        auto *component = *output;
        component->stopping.store(true); component->wake(); component->executor.join();
        int code = component->error_code.load();
        if (code && error) *error = component->terminal_error;
        delete component; *output = nullptr; return code;
    });
}

extern "C" int ngx_http_cache_purge_component_request_stop(ngx_http_cache_purge_component_t *c,
    ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        if (!c) return failure(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null component stop request");
        if (!c->owner()) return failure(error, NGX_HTTP_CACHE_ROCKSDB_OWNER, "component creator mismatch");
        c->stopping.store(true); c->wake(); return 0;
    });
}
