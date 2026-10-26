#include "ngx_http_cache_purge_control.hpp"
#include <sys/mman.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <random>
#include <thread>

using namespace ngx_http_cache_purge;
static unsigned assertions;
static void check(bool value, const char *message) {
    if (!value) throw std::runtime_error(message);
    ++assertions;
}
static ngx_http_cache_rocksdb_error_t error;
static void expect(int rc, int wanted, const char *message) {
    if (rc != wanted) throw std::runtime_error(std::string(message) + ": " + error.message + " (" + std::to_string(rc) + ")");
    ++assertions;
}
template<class Function> static void fails(int code, Function function) {
    try { function(); } catch (const IndexError &e) { check(e.code == code, e.what()); return; }
    throw std::runtime_error("expected publication failure");
}
static ngx_http_cache_rocksdb_bytes_t bytes(const std::string &s) { return {reinterpret_cast<const unsigned char *>(s.data()), s.size()}; }
static Operation operation(unsigned n, SelectorKind kind, std::string value, std::string scope = "") {
    Operation op; op.input.id = "op-" + std::to_string(n); op.sequence = n; op.revision = 2;
    op.input.mode = n % 2 ? PurgeMode::invalidate : PurgeMode::remove;
    op.input.selector = {kind, std::move(value), std::move(scope)}; op.input.issued_at_ms = 1000 + n;
    op.stage = OperationStage::active; op.effective_at_ms = 2000; op.propagation_until_ms = 302000; op.propagate = true;
    return op;
}
struct Context {
    std::string key, url, host = "business.test", ns = "team";
    std::vector<std::string> tags;
    std::vector<ngx_http_cache_rocksdb_bytes_t> tag_bytes;
    ngx_http_cache_purge_control_context_t raw{};
    ngx_http_cache_purge_control_context_t *get(bool proxy = true, uint64_t now = 5000) {
        tag_bytes.clear(); for (const auto &tag : tags) tag_bytes.push_back(bytes(tag));
        raw = {bytes(key), bytes(url), bytes(host), bytes(ns), tag_bytes.data(), tag_bytes.size(), now, proxy}; return &raw;
    }
};
using Matches = std::map<uint64_t, uint32_t>;
static int collect(void *data, const ngx_http_cache_purge_control_match_t *m) {
    auto &found = *static_cast<Matches *>(data);
    check(found.emplace(m->sequence, m->flags).second, "operation emitted once");
    check(m->operation_id.len && m->mode == (m->sequence % 2 ? 1U : 2U), "copied identity/mode retained");
    return 0;
}
static int ignore(void *, const ngx_http_cache_purge_control_match_t *) { return 0; }
static Matches lookup(ngx_http_cache_purge_control_reader_t *reader, Context &context, bool proxy = true, uint64_t now = 5000) {
    Matches found; uint64_t version = 0;
    expect(ngx_http_cache_purge_control_match(reader, context.get(proxy, now), 65536, 8 * 1024 * 1024, collect, &found, &version, &error), 0, "control lookup");
    check(version != 0, "lookup returns publication"); return found;
}
static Matches reference(const std::vector<Operation> &ops, const Context &c, bool proxy, uint64_t now) {
    Matches result;
    for (const auto &op : ops) {
        const auto &s = op.input.selector; bool match = false;
        switch (s.kind) {
        case SelectorKind::key: match = c.key == s.value; break;
        case SelectorKind::url: match = c.url == s.value; break;
        case SelectorKind::key_prefix: match = c.key.compare(0, s.value.size(), s.value) == 0; break;
        case SelectorKind::url_prefix: match = c.url.compare(0, s.value.size(), s.value) == 0; break;
        case SelectorKind::host: match = c.host == s.value; break;
        case SelectorKind::namespace_tag:
        case SelectorKind::host_tag:
            match = (s.kind == SelectorKind::namespace_tag ? c.ns : c.host) == s.scope
                && std::find(c.tags.begin(), c.tags.end(), s.value) != c.tags.end(); break;
        }
        if (!match) continue;
        uint32_t flags = op.stage == OperationStage::materialized ? 0 : NGX_HTTP_CACHE_PURGE_CONTROL_FENCE;
        if (op.stage == OperationStage::preparing) flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PREPARING;
        if (op.propagate && proxy && now < op.propagation_until_ms) flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
        if (flags) result[op.sequence] = flags;
    }
    return result;
}
static void notify(int fd) { char byte = 1; check(write(fd, &byte, 1) == 1, "pipe notify"); }
static void wait(int fd) { char byte; check(read(fd, &byte, 1) == 1, "pipe wait"); }
struct Probe { uint64_t count = 0, base = 0; };
static int probe(void *data, const ngx_http_cache_purge_control_match_t *m) {
    auto &p = *static_cast<Probe *>(data);
    uint64_t base = m->sequence > 1000 ? 1000 : 0;
    if ((p.count && base != p.base) || m->sequence <= base || m->sequence > base + 100) return NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
    p.base = base; ++p.count; return 0;
}

int main() {
    try {
        size_t size = ngx_http_cache_purge_control_size(2 * 1024 * 1024, 32);
        void *memory = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        check(memory != MAP_FAILED, "allocate shared control memory");
        expect(ngx_http_cache_purge_control_init(memory, size, 2 * 1024 * 1024, 32, &error), 0, "initialize control table");
        ngx_http_cache_purge_control_writer_t *writer = nullptr, *contender = nullptr;
        ngx_http_cache_purge_control_reader_t *reader = nullptr;
        expect(ngx_http_cache_purge_control_writer_open(memory, size, &writer, &error), 0, "claim publisher");
        expect(ngx_http_cache_purge_control_writer_open(memory, size, &contender, &error), NGX_HTTP_CACHE_ROCKSDB_BUSY, "second publisher rejected");
        expect(ngx_http_cache_purge_control_reader_open(memory, size, &reader, &error), 0, "register reader");
        Context context;
        const char binary_key[] = "team\x1fhttps\x1fhost\x1f/a\\b\0z";
        context.key.assign(binary_key, sizeof(binary_key) - 1);
        context.url = "https://cache.test/a/b"; context.tags = {"photo", "photo", "other"};
        uint64_t version = 0;
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 10, 4096, ignore, nullptr, &version, &error), NGX_HTTP_CACHE_ROCKSDB_BUSY, "unrecovered gate rejects reads");
        std::vector<Operation> ops{
            operation(1, SelectorKind::key, context.key), operation(2, SelectorKind::url, context.url),
            operation(3, SelectorKind::key_prefix, context.key.substr(0, 12)), operation(4, SelectorKind::url_prefix, "https://cache.test/a/"),
            operation(5, SelectorKind::host, context.host), operation(6, SelectorKind::namespace_tag, "photo", context.ns),
            operation(7, SelectorKind::host_tag, "photo", context.host), operation(8, SelectorKind::namespace_tag, "photo", "other"),
            operation(9, SelectorKind::url_prefix, "https://cache.test/ax"), operation(10, SelectorKind::key_prefix, "")};
        ops[1].stage = OperationStage::materialized;
        ops[4].stage = OperationStage::preparing; ops[4].propagate = false;
        version = publish_controls(writer, ops);
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0, "open recovered image");
        check(lookup(reader, context) == reference(ops, context, true, 5000), "all selector families and duplicate TAG input");
        check(lookup(reader, context, false) == reference(ops, context, false, 5000), "non-proxy requests never propagate");
        check(lookup(reader, context, true, 302000) == reference(ops, context, true, 302000), "window expiry does not clear FENCE");
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 1, 4096, ignore, nullptr, &version, &error), NGX_HTTP_CACHE_ROCKSDB_LIMIT, "match count overflow fails closed");
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 64, 1, ignore, nullptr, &version, &error), NGX_HTTP_CACHE_ROCKSDB_LIMIT, "match byte overflow fails closed");
        fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, [&] { publish_controls(writer, ops); });
        int wrong_thread = 0;
        std::thread foreign([&] { uint64_t v; ngx_http_cache_rocksdb_error_t e;
            wrong_thread = ngx_http_cache_purge_control_match(reader, context.get(), 64, 4096, ignore, nullptr, &v, &e); });
        foreign.join(); check(wrong_thread == NGX_HTTP_CACHE_ROCKSDB_OWNER, "reader cannot migrate threads");
        auto staged = prepare_controls(writer, ops);
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0, "prepared image transition");
        publish_controls(writer, {});
        fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, [&] { publish_controls(writer, staged); });
        publish_controls(writer, ops); expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0, "restore current prepared image");
        std::vector<Operation> too_large;
        for (unsigned n = 1; n <= 70; ++n) too_large.push_back(operation(n, SelectorKind::key_prefix, std::string(32768, 'x')));
        fails(NGX_HTTP_CACHE_ROCKSDB_LIMIT, [&] { prepare_controls(writer, too_large); });
        check(lookup(reader, context) == reference(ops, context, true, 5000), "failed preparation preserves live image and gate");
        std::vector<ngx_http_cache_purge_control_reader_t *> registered;
        for (;;) {
            ngx_http_cache_purge_control_reader_t *extra = nullptr;
            int rc = ngx_http_cache_purge_control_reader_open(memory, size, &extra, &error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT) break;
            expect(rc, 0, "bounded reader registration"); registered.push_back(extra);
            check(registered.size() < 32, "registration cannot exceed shared slots");
        }
        check(registered.size() == 31, "reader slot limit is enforced");
        for (auto *extra : registered) expect(ngx_http_cache_purge_control_reader_close(&extra, &error), 0, "release extra slot");

        // Differential checks against a deliberately simple, independent scan.
        std::mt19937 random(20261026); ops.clear();
        for (unsigned n = 1; n <= 700; ++n) {
            auto kind = static_cast<SelectorKind>(random() % 7); auto value = "/a/" + std::to_string(random() % 30);
            if (kind == SelectorKind::host) value = "host-" + std::to_string(random() % 5);
            if (kind == SelectorKind::key_prefix || kind == SelectorKind::url_prefix) value.resize(random() % (value.size() + 1));
            std::string scope;
            if (kind == SelectorKind::namespace_tag || kind == SelectorKind::host_tag) { value = "tag-" + std::to_string(random() % 5); scope = "scope-" + std::to_string(random() % 3); }
            auto op = operation(n, kind, value, scope);
            if (n % 11 == 0) op.stage = OperationStage::materialized;
            if (n % 7 == 0 && op.stage != OperationStage::materialized) op.propagate = false;
            ops.push_back(op);
        }
        ops.push_back(operation(701, SelectorKind::key_prefix, std::string("\xff\0", 2) + std::string(6000, 'x')));
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0, "close before rebuild");
        publish_controls(writer, ops); expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0, "open rebuilt image");
        for (unsigned n = 0; n != 300; ++n) {
            Context c; c.key = "/a/" + std::to_string(random() % 35); c.url = "/a/" + std::to_string(random() % 35);
            c.host = random() % 2 ? "host-" + std::to_string(random() % 7) : "scope-" + std::to_string(random() % 3);
            c.ns = "scope-" + std::to_string(random() % 4); c.tags = {"tag-0", "tag-2", "tag-2"};
            bool proxy = random() % 2; uint64_t now = random() % 2 ? 5000 : 400000;
            check(lookup(reader, c, proxy, now) == reference(ops, c, proxy, now), "radix/hash results match independent selector semantics");
        }
        Context long_key; long_key.key = std::string("\xff\0", 2) + std::string(6000, 'x') + "tail";
        check(lookup(reader, long_key) == reference(ops, long_key, true, 5000), "full binary long prefix retained");

        std::vector<Operation> binary_prefixes;
        for (unsigned n = 0; n != 256; ++n) binary_prefixes.push_back(operation(n + 2000, SelectorKind::key_prefix, std::string(1, static_cast<char>(n))));
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0, "binary prefix image");
        publish_controls(writer, binary_prefixes); expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0, "open binary prefix image");
        for (unsigned n = 0; n != 256; ++n) {
            Context c; c.key = std::string(1, static_cast<char>(n)) + "suffix";
            check(lookup(reader, c) == reference(binary_prefixes, c, true, 5000), "all byte values use exact prefix edges");
        }

        // A paused live reader prevents both activation and bank overwrite.
        int ready[2], command[2]; check(pipe(ready) == 0 && pipe(command) == 0, "control pipes");
        uint64_t pinned_version;
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 65536, 8 * 1024 * 1024, ignore, nullptr, &pinned_version, &error), 0, "get admission version");
        auto child = fork(); check(child >= 0, "fork pinned reader");
        if (!child) {
            ngx_http_cache_purge_control_reader_t *local = nullptr;
            expect(ngx_http_cache_purge_control_reader_open(memory, size, &local, &error), 0, "child reader");
            expect(ngx_http_cache_purge_control_guard_begin(local, pinned_version, &error), 0, "child guard");
            notify(ready[1]); wait(command[0]);
            expect(ngx_http_cache_purge_control_guard_end(local, &error), 0, "release child guard");
            expect(ngx_http_cache_purge_control_reader_close(&local, &error), 0, "close child reader"); _exit(0);
        }
        wait(ready[0]);
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0, "gate closes before new publication");
        publish_controls(writer, {operation(900, SelectorKind::host, "new.test")});
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), NGX_HTTP_CACHE_ROCKSDB_BUSY, "old synchronous admission must drain");
        fails(NGX_HTTP_CACHE_ROCKSDB_BUSY, [&] { publish_controls(writer, {}); });
        notify(command[1]); int status; check(waitpid(child, &status, 0) == child && status == 0, "child releases guard");
        version = publish_controls(writer, {}); expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0, "old bank reused after drain");
        check(lookup(reader, context).empty(), "empty control image");

        child = fork(); check(child >= 0, "fork doomed reader");
        if (!child) {
            ngx_http_cache_purge_control_reader_t *local = nullptr;
            expect(ngx_http_cache_purge_control_reader_open(memory, size, &local, &error), 0, "doomed reader");
            expect(ngx_http_cache_purge_control_guard_begin(local, version, &error), 0, "doomed guard");
            notify(ready[1]); for (;;) pause();
        }
        wait(ready[0]); check(kill(child, SIGKILL) == 0 && waitpid(child, &status, 0) == child, "kill pinned reader");
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0, "close after reader death");
        publish_controls(writer, {operation(901, SelectorKind::host, context.host)});
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0, "dead reader pin reclaimed without timeout eviction");

        expect(ngx_http_cache_purge_control_writer_close(&writer, &error), 0, "release publisher");
        child = fork(); check(child >= 0, "fork publisher");
        if (!child) {
            ngx_http_cache_purge_control_writer_t *local = nullptr;
            expect(ngx_http_cache_purge_control_writer_open(memory, size, &local, &error), 0, "child publisher");
            publish_controls(local, {operation(902, SelectorKind::host, context.host)});
            expect(ngx_http_cache_purge_control_gate_open(local, &error), 0, "child opens gate"); notify(ready[1]); for (;;) pause();
        }
        wait(ready[0]); check(lookup(reader, context).count(902), "parent sees child publication");
        expect(ngx_http_cache_purge_control_writer_open(memory, size, &writer, &error), NGX_HTTP_CACHE_ROCKSDB_BUSY, "live publisher cannot be stolen");
        check(kill(child, SIGKILL) == 0 && waitpid(child, &status, 0) == child, "kill publisher");
        expect(ngx_http_cache_purge_control_writer_open(memory, size, &writer, &error), 0, "reclaim dead publisher");
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), NGX_HTTP_CACHE_ROCKSDB_BUSY, "new owner cannot bypass recovery publication");
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 10, 4096, ignore, nullptr, &version, &error), NGX_HTTP_CACHE_ROCKSDB_BUSY, "takeover requires recovery gate");
        publish_controls(writer, {}); expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0, "recovered publisher");

        // Readers race repeated immutable publications. Every successful read
        // must contain exactly one complete image, never a mixture of versions.
        auto *counters = static_cast<uint64_t *>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE,
                                                     MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        check(counters != MAP_FAILED, "stress counters");
        std::vector<pid_t> children;
        for (unsigned i = 0; i != 4; ++i) {
            auto worker = fork(); check(worker >= 0, "fork stress reader");
            if (!worker) {
                ngx_http_cache_purge_control_reader_t *local = nullptr; int rc;
                do { rc = ngx_http_cache_purge_control_reader_open(memory, size, &local, &error); if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) sched_yield(); } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
                if (rc) _exit(3);
                auto *request = context.get();
                while (!__atomic_load_n(counters, __ATOMIC_SEQ_CST)) {
                    Probe result; uint64_t publication;
                    rc = ngx_http_cache_purge_control_match(local, request, 128, 65536, probe, &result, &publication, &error);
                    if (rc == 0) {
                        if (result.count && result.count != 100) _exit(4);
                        __atomic_add_fetch(counters + 1, 1, __ATOMIC_SEQ_CST);
                        if (result.count) __atomic_add_fetch(counters + 2, 1, __ATOMIC_SEQ_CST);
                    } else if (rc != NGX_HTTP_CACHE_ROCKSDB_BUSY) _exit(5);
                }
                do { rc = ngx_http_cache_purge_control_reader_close(&local, &error); if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) sched_yield(); } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
                _exit(rc ? 6 : 0);
            }
            children.push_back(worker);
        }
        for (unsigned pass = 0; pass != 200; ++pass) {
            std::vector<Operation> image;
            for (unsigned n = 1; n <= 100; ++n) image.push_back(operation(n + (pass % 2 ? 1000 : 0), SelectorKind::host, context.host));
            expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0, "stress gate close");
            for (;;) {
                try { publish_controls(writer, image); break; }
                catch (const IndexError &e) { if (e.code != NGX_HTTP_CACHE_ROCKSDB_BUSY) throw; sched_yield(); }
            }
            int rc;
            do { rc = ngx_http_cache_purge_control_gate_open(writer, &error); if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) sched_yield(); } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
            expect(rc, 0, "stress gate open");
        }
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (__atomic_load_n(counters + 2, __ATOMIC_SEQ_CST) < 1000 && std::chrono::steady_clock::now() < deadline) usleep(1000);
        check(__atomic_load_n(counters + 2, __ATOMIC_SEQ_CST) >= 1000, "stress readers see completed images");
        __atomic_store_n(counters, uint64_t(1), __ATOMIC_SEQ_CST);
        for (auto worker : children) check(waitpid(worker, &status, 0) == worker && status == 0, "concurrent reader stayed consistent");
        munmap(counters, 4096);
        expect(ngx_http_cache_purge_control_reader_close(&reader, &error), 0, "close reader");
        expect(ngx_http_cache_purge_control_writer_close(&writer, &error), 0, "close writer");
        close(ready[0]); close(ready[1]); close(command[0]); close(command[1]); munmap(memory, size);
        std::cout << "PASS " << assertions << " control assertions" << std::endl;
        return 0;
    } catch (const std::exception &e) { std::cerr << "FAIL " << e.what() << std::endl; _exit(1); }
}
