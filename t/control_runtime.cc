#include "ngx_http_cache_purge_test.hpp"
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
static void
check(bool value, const char *message)
{
    if (!value) {
        throw std::runtime_error(message);
    }
    ++assertions;
}
static ngx_http_cache_rocksdb_error_t error;
static void
expect(int rc, int wanted, const char *message)
{
    if (rc != wanted) {
        throw std::runtime_error(std::string(message) + ": " + error.message
                                 + " (" + std::to_string(rc) + ")");
    }
    ++assertions;
}
template <class Function>
static void
fails(int code, Function function)
{
    try {
        function();
    } catch (const IndexError &e) {
        check(e.code == code, e.what());
        return;
    }
    throw std::runtime_error("expected publication failure");
}
static ngx_http_cache_rocksdb_bytes_t
bytes(const std::string &s)
{
    return { reinterpret_cast<const unsigned char *>(s.data()), s.size() };
}
static Operation
operation(
    unsigned n, SelectorKind kind, std::string value, std::string scope = "")
{
    Operation op;
    op.input.id = "op-" + std::to_string(n);
    op.sequence = n;
    op.revision = 2;
    op.input.mode = n % 2 ? PurgeMode::invalidate : PurgeMode::remove;
    op.input.selector = { kind, std::move(value), std::move(scope) };
    op.input.issued_at_ms = 1000 + n;
    op.stage = OperationStage::active;
    op.effective_at_ms = 2000;
    op.propagation_until_ms = 302000;
    op.propagate = true;
    return op;
}
struct Context {
    std::string key, url, host = "business.test", ns = "team";
    std::vector<std::string> tags;
    std::vector<ngx_http_cache_rocksdb_bytes_t> tag_bytes;
    ngx_http_cache_purge_control_context_t raw{};
    ngx_http_cache_purge_control_context_t *
    get(bool proxy = true, uint64_t now = 5000)
    {
        tag_bytes.clear();
        for (const auto &tag : tags) {
            tag_bytes.push_back(bytes(tag));
        }
        raw = { bytes(key), bytes(url), bytes(host), bytes(ns),
            tag_bytes.data(), tag_bytes.size(), now, proxy };
        return &raw;
    }
};
using Matches = std::map<uint64_t, uint32_t>;
static int
collect(void *data, const ngx_http_cache_purge_control_match_t *m)
{
    auto &found = *static_cast<Matches *>(data);
    check(
        found.emplace(m->sequence, m->flags).second, "operation emitted once");
    check(m->operation_id.len && m->mode == (m->sequence % 2 ? 1U : 2U),
        "copied identity/mode retained");
    return 0;
}
static int
ignore(void *, const ngx_http_cache_purge_control_match_t *)
{
    return 0;
}
static Matches
lookup(ngx_http_cache_purge_control_reader_t *reader, Context &context,
    bool proxy = true, uint64_t now = 5000)
{
    Matches found;
    uint64_t version = 0;
    expect(ngx_http_cache_purge_control_match(reader, context.get(proxy, now),
               65536, 8 * 1024 * 1024, collect, &found, &version, &error),
        0, "control lookup");
    check(version != 0, "lookup returns publication");
    return found;
}
static Matches
reference(const std::vector<Operation> &ops, const Context &c, bool proxy,
    uint64_t now)
{
    Matches result;
    for (const auto &op : ops) {
        const auto &s = op.input.selector;
        bool match = false;
        switch (s.kind) {
        case SelectorKind::key:
            match = c.key == s.value;
            break;
        case SelectorKind::url:
            match = c.url == s.value;
            break;
        case SelectorKind::key_prefix:
            match = c.key.compare(0, s.value.size(), s.value) == 0;
            break;
        case SelectorKind::url_prefix:
            match = c.url.compare(0, s.value.size(), s.value) == 0;
            break;
        case SelectorKind::host:
            match = c.host == s.value;
            break;
        case SelectorKind::namespace_tag:
        case SelectorKind::host_tag:
            match = (s.kind == SelectorKind::namespace_tag ? c.ns : c.host)
                        == s.scope
                    && std::find(c.tags.begin(), c.tags.end(), s.value)
                           != c.tags.end();
            break;
        case SelectorKind::namespace_tags:
        case SelectorKind::host_tags:
            if ((s.kind == SelectorKind::namespace_tags ? c.ns : c.host)
                == s.scope)
            {
                size_t at = 1;
                while (at < s.value.size()) {
                    size_t size = static_cast<unsigned char>(s.value[at]) * 256
                                  + static_cast<unsigned char>(s.value[at + 1]);
                    at += 2;
                    match |= std::find(c.tags.begin(), c.tags.end(),
                                 s.value.substr(at, size))
                             != c.tags.end();
                    at += size;
                }
            }
            break;
        }
        if (!match) {
            continue;
        }
        uint32_t flags = op.stage == OperationStage::materialized ?
                             0 :
                             NGX_HTTP_CACHE_PURGE_CONTROL_FENCE;
        if (op.stage == OperationStage::preparing) {
            flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PREPARING;
        }
        if (op.propagate && proxy && now < op.propagation_until_ms) {
            flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
        }
        if (flags) {
            result[op.sequence] = flags;
        }
    }
    return result;
}
static void
notify(int fd)
{
    char byte = 1;
    check(write(fd, &byte, 1) == 1, "pipe notify");
}

static Matches
propagation_reference(const std::vector<Operation> &ops, const Context &c,
    bool known, bool proxy, uint64_t now)
{
    auto result = reference(ops, c, proxy, now);
    for (auto it = result.begin(); it != result.end();) {
        if (proxy
            && (it->second
                & (NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE
                    | NGX_HTTP_CACHE_PURGE_CONTROL_PREPARING)))
        {
            it->second = NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
            ++it;
        } else {
            it = result.erase(it);
        }
    }
    if (known || !proxy) {
        return result;
    }
    for (const auto &op : ops) {
        const auto &s = op.input.selector;
        bool scoped = ((s.kind == SelectorKind::namespace_tag
                           || s.kind == SelectorKind::namespace_tags)
                          && s.scope == c.ns)
                      || ((s.kind == SelectorKind::host_tag
                              || s.kind == SelectorKind::host_tags)
                          && s.scope == c.host);
        if (scoped
            && (op.stage == OperationStage::preparing
                || (op.propagate && now < op.propagation_until_ms)))
        {
            result[op.sequence] = NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE
                                  | NGX_HTTP_CACHE_PURGE_CONTROL_TAG_CANDIDATE;
        }
    }
    return result;
}

static void
propagation_needed_tests()
{
    char path[] = "/tmp/ncp-propagation-needed-XXXXXX";
    check(mkdtemp(path) != nullptr, "idle propagation ledger directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    expect(ngx_http_cache_rocksdb_open(path, &options, &store, &error), 0,
        "idle propagation ledger");
    size_t size = ngx_http_cache_purge_control_size(1024 * 1024, 8);
    void *shared = mmap(nullptr, size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    check(shared != MAP_FAILED, "idle propagation shared region");
    expect(
        ngx_http_cache_purge_control_init(shared, size, 1024 * 1024, 8, &error),
        0, "initialize idle propagation controls");
    ngx_http_cache_purge_control_reader_t *reader = nullptr;
    expect(
        ngx_http_cache_purge_control_reader_open(shared, size, &reader, &error),
        0, "idle propagation reader");
    {
        Index index(store);
        Operations ledger(store);
        Coordinator coordinator(ledger, shared, size);
        auto ready = [&] {
            for (unsigned i = 0; i < 1000; i++) {
                if (coordinator.state() == Coordination::ready) {
                    return;
                }
                check(coordinator.step(), "idle propagation recovery");
            }
            throw std::runtime_error("idle propagation recovery deadline");
        };
        auto draining = [&] {
            for (unsigned i = 0; i < 1000; i++) {
                if (coordinator.state() == Coordination::draining) {
                    return;
                }
                check(coordinator.step(), "idle propagation publication");
            }
            throw std::runtime_error("idle propagation publication deadline");
        };
        auto query = [&](uint64_t now, int expected, uint64_t sequence,
                         int wanted = 0) {
            ngx_http_cache_purge_watermark_t watermark{};
            uint64_t publication = 123;
            int needed = -1;
            expect(ngx_http_cache_purge_control_propagation_needed(
                       reader, now, &needed, &watermark, &publication, &error),
                wanted, "global propagation summary");
            check(needed == expected, "global propagation requirement");
            if (wanted == 0) {
                check(publication == coordinator.publication()
                          && watermark.sequence == sequence
                          && std::memcmp(
                                 watermark.ledger, ledger.native()->ledger, 16)
                                 == 0,
                    "idle proof and watermark share a publication");
            } else {
                check(publication == 0 && watermark.sequence == 0,
                    "failed summary cannot publish idle proof");
            }
        };
        query(2000, 1, 0, NGX_HTTP_CACHE_ROCKSDB_BUSY);
        ready();
        query(2000, 0, 0);
        query(0, 1, 0, NGX_HTTP_CACHE_ROCKSDB_INVALID);
        auto input =
            operation(1, SelectorKind::namespace_tag, "news", "one").input;
        auto op = coordinator.accept(input, 2000, 120000);
        ready();
        query(UINT64_MAX, 1, 1);
        op = ledger.activate(input.id, op.revision, 2000);
        coordinator.refresh();
        draining();
        query(2000, 1, 0, NGX_HTTP_CACHE_ROCKSDB_BUSY);
        ready();
        query(2000, 1, 1);
        query(121999, 1, 1);
        query(122000, 0, 1);
        query(123000, 0, 1);
        query(121999, 1, 1);
        Context c;
        c.ns = "one";
        c.tags = { "news" };
        check(!lookup(reader, c, true, 123000).empty(),
            "outgoing idle proof does not clear a local FENCE");
        check(ledger.expire_propagation(123000) == 1,
            "persist expiry without materializing the FENCE");
        coordinator.refresh();
        ready();
        query(2000, 0, 1);
        auto second = operation(2, SelectorKind::host, "different.test").input;
        auto next = coordinator.accept(second, 124000, 120000);
        ready();
        query(UINT64_MAX, 1, 2);
        next = ledger.activate(second.id, next.revision, 125000);
        coordinator.refresh();
        ready();
        query(245000, 0, 2);
        query(244999, 1, 2);
        for (unsigned i = 0; i < 100; i++) {
            query(i % 2 ? 245000 : 244999, i % 2 ? 0 : 1, 2);
        }
        coordinator.refresh();
        draining();
        query(245000, 1, 0, NGX_HTTP_CACHE_ROCKSDB_BUSY);
        ready();
        query(245000, 0, 2);
    }
    expect(ngx_http_cache_purge_control_reader_close(&reader, &error), 0,
        "close idle propagation reader");
    expect(ngx_http_cache_rocksdb_close(&store, &error), 0,
        "close idle propagation ledger");
    munmap(shared, size);
}

static void
propagation_tests()
{
    char path[] = "/tmp/ncp-propagation-control-XXXXXX";
    check(mkdtemp(path) != nullptr, "propagation ledger directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    expect(ngx_http_cache_rocksdb_open(path, &options, &store, &error), 0,
        "propagation ledger");
    size_t size = ngx_http_cache_purge_control_size(2 * 1024 * 1024, 8);
    void *shared = mmap(nullptr, size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    check(shared != MAP_FAILED, "propagation shared region");
    expect(ngx_http_cache_purge_control_init(
               shared, size, 2 * 1024 * 1024, 8, &error),
        0, "initialize propagation controls");
    ngx_http_cache_purge_control_reader_t *reader = nullptr;
    expect(
        ngx_http_cache_purge_control_reader_open(shared, size, &reader, &error),
        0, "propagation reader");
    {
        Index index(store);
        Operations ledger(store);
        std::vector<Operation> ops;
        std::mt19937 random(20261027);
        for (unsigned n = 1; n <= 420; n++) {
            auto kind = static_cast<SelectorKind>(n % 9);
            auto value = "/a/" + std::to_string(random() % 10);
            std::string scope;
            if (kind == SelectorKind::key_prefix
                || kind == SelectorKind::url_prefix)
            {
                value.resize(random() % (value.size() + 1));
            }
            if (kind >= SelectorKind::namespace_tag) {
                scope = "scope-" + std::to_string(random() % 6);
                if (n % 13 == 0) {
                    scope = std::string("scope-0\0tail", 12);
                }
                value = "tag-" + std::to_string(random() % 10);
                if (kind >= SelectorKind::namespace_tags) {
                    std::vector<std::string> tags{ value };
                    for (unsigned j = 1; j < 64; j++) {
                        tags.push_back("zz-" + std::to_string(j + 100));
                    }
                    value = tag_group(tags);
                }
            }
            auto input = operation(n, kind, value, scope).input;
            input.binding = "propagation-test";
            auto op = ledger.accept(input, 2000);
            if (n % 11) {
                op = ledger.activate(input.id, op.revision, 2000);
                if (n % 17 == 0) {
                    OperationCheckpoint point;
                    point.id = 1;
                    point.scan_done = true;
                    op = ledger.checkpoint(input.id, op.revision, point);
                    op = ledger.materialize(input.id, op.revision, op.sequence);
                }
            }
            ops.push_back(std::move(op));
        }
        Coordinator coordinator(ledger, shared, size);
        auto ready = [&] {
            for (unsigned n = 0; n < 1000; n++) {
                if (coordinator.state() == Coordination::ready) {
                    return;
                }
                check(coordinator.step(), "propagation coordinator progress");
            }
            throw std::runtime_error("propagation coordinator did not recover");
        };
        Context c;
        ngx_http_cache_purge_watermark_t watermark{};
        uint64_t publication = 0;
        auto query = [&](bool known, bool proxy, uint64_t now,
                         size_t count = 65536, size_t bytes = 8 * 1024 * 1024) {
            Matches matches;
            int rc = ngx_http_cache_purge_control_propagation(reader,
                c.get(proxy, now), known, count, bytes, collect, &matches,
                &watermark, &publication, &error);
            return std::make_pair(rc, matches);
        };
        expect(query(false, true, 5000).first, NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "unrecovered propagation gate");
        ready();
        for (unsigned n = 0; n < 300; n++) {
            c.key = "/a/" + std::to_string(random() % 12);
            c.url = "/a/" + std::to_string(random() % 12);
            c.host = "scope-" + std::to_string(random() % 8);
            c.ns = "scope-" + std::to_string(random() % 8);
            if (n % 9 == 0) {
                c.ns = std::string("scope-0\0tail", 12);
            }
            if (n % 7 == 0) {
                c.host = c.ns;
            }
            bool known = random() % 2;
            bool proxy = random() % 2;
            uint64_t now = random() % 3 ? 5000 : 302000;
            c.tags = known ? std::vector<std::string>{ "tag-0", "tag-2",
                "tag-2", "zz-101" } :
                             std::vector<std::string>{};
            const auto result = query(known, proxy, now);
            expect(result.first, 0, "propagation snapshot");
            check(result.second
                      == propagation_reference(ops, c, known, proxy, now),
                "propagation lookup matches independent scoped scan");
            check(publication == coordinator.publication()
                      && watermark.sequence == ops.size()
                      && std::memcmp(
                             watermark.ledger, ledger.native()->ledger, 16)
                             == 0,
                "requirements and ledger are from one publication");
        }
        c = Context{};
        c.ns = "scope-0";
        c.host = "scope-0";
        auto unknown = query(false, true, 5000);
        auto empty = query(true, true, 5000);
        expect(unknown.first, 0, "unknown TAGs snapshot");
        expect(empty.first, 0, "known empty TAGs snapshot");
        check(unknown.second.size() > empty.second.size(),
            "unknown TAGs differ from confirmed empty TAGs");
        check(lookup(reader, c) == reference(ops, c, true, 5000),
            "propagation candidates do not change local admission matching");
        expect(query(false, true, 302000).first, 0, "expired propagation");
        const auto expired = query(false, true, 302000).second;
        check(expired == propagation_reference(ops, c, false, true, 302000)
                  && !lookup(reader, c, true, 302000).empty(),
            "window end suppresses active propagation without clearing fences");
        check(!expired.empty(), "published preparing fences still propagate");
        for (const auto &match : expired) {
            check(ops[match.first - 1].stage == OperationStage::preparing,
                "only pending first activations remain after active windows");
        }
        for (auto limits :
            { std::make_pair(1U, 65536U), std::make_pair(64U, 1U) })
        {
            expect(query(false, true, 5000, limits.first, limits.second).first,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "candidate overflow cannot return a successful partial set");
            check(publication == 0 && watermark.sequence == 0
                      && watermark.ledger[0] == 0,
                "failed propagation has no publication proof");
        }
        c.tags = { "tag-0" };
        expect(query(false, true, 5000).first, NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "unknown TAG mode rejects a partial TAG list");
        c.tags.clear();
        expect(ngx_http_cache_purge_control_propagation(reader, c.get(), 2, 64,
                   65536, ignore, nullptr, &watermark, &publication, &error),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid TAG knowledge flag");
        auto refresh =
            [](void *data,
                const ngx_http_cache_purge_control_match_t *) -> int {
            static_cast<Coordinator *>(data)->refresh();
            return NGX_HTTP_CACHE_ROCKSDB_BUSY;
        };
        expect(ngx_http_cache_purge_control_propagation(reader, c.get(), 0,
                   65536, 8 * 1024 * 1024, refresh, &coordinator, &watermark,
                   &publication, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "interrupted visitor releases image");
        check(publication == 0 && watermark.sequence == 0,
            "interrupted snapshot publishes no proof");
        ready();
        check(query(false, true, 5000).second == unknown.second,
            "reader remains usable after interrupted propagation");
        auto change = [](void *data,
                          const ngx_http_cache_purge_control_match_t *) {
            auto &state = *static_cast<std::pair<Coordinator *, bool> *>(data);
            if (!state.second) {
                state.first->refresh();
                for (unsigned n = 0;
                    state.first->state() != Coordination::draining; n++)
                {
                    check(n < 1000 && state.first->step(),
                        "publish while propagation holds the prior image");
                }
                state.second = true;
            }
            return 0;
        };
        auto changed = std::make_pair(&coordinator, false);
        expect(ngx_http_cache_purge_control_propagation(reader, c.get(), 0,
                   65536, 8 * 1024 * 1024, change, &changed, &watermark,
                   &publication, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "gate change invalidates a completed visitor result");
        check(publication == 0 && watermark.sequence == 0,
            "stale publication never becomes a successful snapshot");
        ready();
    }
    expect(ngx_http_cache_purge_control_reader_close(&reader, &error), 0,
        "close propagation reader");
    expect(ngx_http_cache_rocksdb_close(&store, &error), 0,
        "close propagation ledger");
    munmap(shared, size);
}
static void
wait(int fd)
{
    char byte;
    check(read(fd, &byte, 1) == 1, "pipe wait");
}
struct Probe {
    uint64_t count = 0, base = 0;
};
static int
probe(void *data, const ngx_http_cache_purge_control_match_t *m)
{
    auto &p = *static_cast<Probe *>(data);
    uint64_t base = m->sequence > 1000 ? 1000 : 0;
    if ((p.count && base != p.base) || m->sequence <= base
        || m->sequence > base + 100)
    {
        return NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
    }
    p.base = base;
    ++p.count;
    return 0;
}

int
main()
{
    try {
        propagation_needed_tests();
        propagation_tests();
        size_t size = ngx_http_cache_purge_control_size(2 * 1024 * 1024, 32);
        void *memory = mmap(nullptr, size, PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        check(memory != MAP_FAILED, "allocate shared control memory");
        expect(ngx_http_cache_purge_control_init(
                   memory, size, 2 * 1024 * 1024, 32, &error),
            0, "initialize control table");
        ngx_http_cache_purge_control_writer_t *writer = nullptr,
                                              *contender = nullptr;
        ngx_http_cache_purge_control_reader_t *reader = nullptr;
        expect(ngx_http_cache_purge_control_writer_open(
                   memory, size, &writer, &error),
            0, "claim publisher");
        expect(ngx_http_cache_purge_control_writer_open(
                   memory, size, &contender, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "second publisher rejected");
        expect(ngx_http_cache_purge_control_reader_open(
                   memory, size, &reader, &error),
            0, "register reader");
        Context context;
        const char binary_key[] = "team\x1fhttps\x1fhost\x1f/a\\b\0z";
        context.key.assign(binary_key, sizeof(binary_key) - 1);
        context.url = "https://cache.test/a/b";
        context.tags = { "photo", "photo", "other" };
        uint64_t version = 0;
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 10,
                   4096, ignore, nullptr, &version, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "unrecovered gate rejects reads");
        std::vector<Operation> ops{ operation(
                                        1, SelectorKind::key, context.key),
            operation(2, SelectorKind::url, context.url),
            operation(3, SelectorKind::key_prefix, context.key.substr(0, 12)),
            operation(4, SelectorKind::url_prefix, "https://cache.test/a/"),
            operation(5, SelectorKind::host, context.host),
            operation(6, SelectorKind::namespace_tag, "photo", context.ns),
            operation(7, SelectorKind::host_tag, "photo", context.host),
            operation(8, SelectorKind::namespace_tag, "photo", "other"),
            operation(9, SelectorKind::url_prefix, "https://cache.test/ax"),
            operation(10, SelectorKind::key_prefix, ""),
            operation(11, SelectorKind::namespace_tags,
                tag_group({ "other", "photo" }), context.ns),
            operation(12, SelectorKind::host_tags,
                tag_group({ "other", "photo" }), context.host),
            operation(13, SelectorKind::namespace_tags,
                tag_group({ "other", "photo" }), "wrong-namespace") };
        ops[1].stage = OperationStage::materialized;
        ops[4].stage = OperationStage::preparing;
        ops[4].propagate = false;
        version = publish_controls(writer, ops);
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "open recovered image");
        ngx_http_cache_purge_watermark_t unstamped{};
        expect(ngx_http_cache_purge_control_propagation(reader, context.get(),
                   1, 64, 65536, ignore, nullptr, &unstamped, &version, &error),
            NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
            "unstamped control image cannot supply propagation identity");
        check(lookup(reader, context) == reference(ops, context, true, 5000),
            "all selector families and duplicate TAG input");
        check(lookup(reader, context, false)
                  == reference(ops, context, false, 5000),
            "non-proxy requests never propagate");
        check(lookup(reader, context, true, 302000)
                  == reference(ops, context, true, 302000),
            "window expiry does not clear FENCE");
        std::vector<std::string> members;
        for (unsigned n = 100; n < 164; n++) {
            members.push_back("tag-" + std::to_string(n));
        }
        const auto grouped = operation(
            101, SelectorKind::namespace_tags, tag_group(members), context.ns);
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0,
            "close before group budget test");
        publish_controls(writer, { grouped });
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "open group budget test");
        Context many = context;
        many.tags = members;
        Matches emitted;
        expect(ngx_http_cache_purge_control_match(reader, many.get(), 1, 65536,
                   collect, &emitted, &version, &error),
            0, "64 matched members consume exactly one operation budget");
        check(emitted.size() == 1, "group callback emitted once");
        auto complete = [](void *data,
                            const ngx_http_cache_purge_control_match_t *match) {
            auto &op = *static_cast<const Operation *>(data);
            check(match->kind == NGX_HTTP_CACHE_PURGE_NAMESPACE_TAGS
                      && std::string(
                             reinterpret_cast<const char *>(match->value.data),
                             match->value.len)
                             == op.input.selector.value,
                "group callback retains the complete canonical operation");
            return 0;
        };
        expect(
            ngx_http_cache_purge_control_match(reader, many.get(), 1, 65536,
                complete, const_cast<Operation *>(&grouped), &version, &error),
            0, "complete group callback");
        for (const auto &bad : { tag_group({ "b", "a" }),
                 tag_group({ "a", "a" }), tag_group({ "a" }) })
        {
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID, [&] {
                prepare_controls(
                    writer, { operation(102, SelectorKind::namespace_tags, bad,
                                context.ns) });
            });
        }
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0,
            "restore selector image after group checks");
        publish_controls(writer, ops);
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "open restored selector image");
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 1,
                   4096, ignore, nullptr, &version, &error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "match count overflow fails closed");
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 64, 1,
                   ignore, nullptr, &version, &error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "match byte overflow fails closed");
        fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            [&] { publish_controls(writer, ops); });
        int wrong_thread = 0;
        std::thread foreign([&] {
            uint64_t v;
            ngx_http_cache_rocksdb_error_t e;
            wrong_thread = ngx_http_cache_purge_control_match(
                reader, context.get(), 64, 4096, ignore, nullptr, &v, &e);
        });
        foreign.join();
        check(wrong_thread == NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "reader cannot migrate threads");
        auto staged = prepare_controls(writer, ops);
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0,
            "prepared image transition");
        publish_controls(writer, {});
        fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            [&] { publish_controls(writer, staged); });
        publish_controls(writer, ops);
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "restore current prepared image");
        std::vector<Operation> too_large;
        for (unsigned n = 1; n <= 70; ++n) {
            too_large.push_back(operation(
                n, SelectorKind::key_prefix, std::string(32768, 'x')));
        }
        fails(NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            [&] { prepare_controls(writer, too_large); });
        check(lookup(reader, context) == reference(ops, context, true, 5000),
            "failed preparation preserves live image and gate");
        std::vector<ngx_http_cache_purge_control_reader_t *> registered;
        for (;;) {
            ngx_http_cache_purge_control_reader_t *extra = nullptr;
            int rc = ngx_http_cache_purge_control_reader_open(
                memory, size, &extra, &error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_LIMIT) {
                break;
            }
            expect(rc, 0, "bounded reader registration");
            registered.push_back(extra);
            check(registered.size() < 32,
                "registration cannot exceed shared slots");
        }
        check(registered.size() == 31, "reader slot limit is enforced");
        for (auto *extra : registered) {
            expect(ngx_http_cache_purge_control_reader_close(&extra, &error), 0,
                "release extra slot");
        }

        /*
         * Differential checks against a deliberately simple, independent scan.
         */
        std::mt19937 random(20261026);
        ops.clear();
        for (unsigned n = 1; n <= 700; ++n) {
            auto kind = static_cast<SelectorKind>(random() % 9);
            auto value = "/a/" + std::to_string(random() % 30);
            if (kind == SelectorKind::host) {
                value = "host-" + std::to_string(random() % 5);
            }
            if (kind == SelectorKind::key_prefix
                || kind == SelectorKind::url_prefix)
            {
                value.resize(random() % (value.size() + 1));
            }
            std::string scope;
            if (kind == SelectorKind::namespace_tag
                || kind == SelectorKind::host_tag
                || kind == SelectorKind::namespace_tags
                || kind == SelectorKind::host_tags)
            {
                value = "tag-" + std::to_string(random() % 5);
                scope = "scope-" + std::to_string(random() % 3);
                if (kind == SelectorKind::namespace_tags
                    || kind == SelectorKind::host_tags)
                {
                    value = tag_group({ value, "zz-other" });
                }
            }
            auto op = operation(n, kind, value, scope);
            if (n % 11 == 0) {
                op.stage = OperationStage::materialized;
            }
            if (n % 7 == 0 && op.stage != OperationStage::materialized) {
                op.propagate = false;
            }
            ops.push_back(op);
        }
        ops.push_back(operation(701, SelectorKind::key_prefix,
            std::string("\xff\0", 2) + std::string(6000, 'x')));
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0,
            "close before rebuild");
        publish_controls(writer, ops);
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "open rebuilt image");
        for (unsigned n = 0; n != 300; ++n) {
            Context c;
            c.key = "/a/" + std::to_string(random() % 35);
            c.url = "/a/" + std::to_string(random() % 35);
            c.host = random() % 2 ? "host-" + std::to_string(random() % 7) :
                                    "scope-" + std::to_string(random() % 3);
            c.ns = "scope-" + std::to_string(random() % 4);
            c.tags = { "tag-0", "tag-2", "tag-2" };
            bool proxy = random() % 2;
            uint64_t now = random() % 2 ? 5000 : 400000;
            check(
                lookup(reader, c, proxy, now) == reference(ops, c, proxy, now),
                "radix/hash results match independent selector semantics");
        }
        Context long_key;
        long_key.key =
            std::string("\xff\0", 2) + std::string(6000, 'x') + "tail";
        check(lookup(reader, long_key) == reference(ops, long_key, true, 5000),
            "full binary long prefix retained");

        std::vector<Operation> binary_prefixes;
        for (unsigned n = 0; n != 256; ++n) {
            binary_prefixes.push_back(
                operation(n + 2000, SelectorKind::key_prefix,
                    std::string(1, static_cast<char>(n))));
        }
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0,
            "binary prefix image");
        publish_controls(writer, binary_prefixes);
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "open binary prefix image");
        for (unsigned n = 0; n != 256; ++n) {
            Context c;
            c.key = std::string(1, static_cast<char>(n)) + "suffix";
            check(
                lookup(reader, c) == reference(binary_prefixes, c, true, 5000),
                "all byte values use exact prefix edges");
        }

        /* A paused live reader prevents both activation and bank overwrite. */
        int ready[2], command[2];
        check(pipe(ready) == 0 && pipe(command) == 0, "control pipes");
        uint64_t pinned_version;
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 65536,
                   8 * 1024 * 1024, ignore, nullptr, &pinned_version, &error),
            0, "get admission version");
        auto child = fork();
        check(child >= 0, "fork pinned reader");
        if (!child) {
            ngx_http_cache_purge_control_reader_t *local = nullptr;
            expect(ngx_http_cache_purge_control_reader_open(
                       memory, size, &local, &error),
                0, "child reader");
            expect(ngx_http_cache_purge_control_guard_begin(
                       local, pinned_version, &error),
                0, "child guard");
            notify(ready[1]);
            wait(command[0]);
            expect(ngx_http_cache_purge_control_guard_end(local, &error), 0,
                "release child guard");
            expect(ngx_http_cache_purge_control_reader_close(&local, &error), 0,
                "close child reader");
            _exit(0);
        }
        wait(ready[0]);
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0,
            "gate closes before new publication");
        publish_controls(
            writer, { operation(900, SelectorKind::host, "new.test") });
        expect(ngx_http_cache_purge_control_gate_open(writer, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "old synchronous admission must drain");
        fails(
            NGX_HTTP_CACHE_ROCKSDB_BUSY, [&] { publish_controls(writer, {}); });
        notify(command[1]);
        int status;
        check(waitpid(child, &status, 0) == child && status == 0,
            "child releases guard");
        version = publish_controls(writer, {});
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "old bank reused after drain");
        check(lookup(reader, context).empty(), "empty control image");

        child = fork();
        check(child >= 0, "fork doomed reader");
        if (!child) {
            ngx_http_cache_purge_control_reader_t *local = nullptr;
            expect(ngx_http_cache_purge_control_reader_open(
                       memory, size, &local, &error),
                0, "doomed reader");
            expect(ngx_http_cache_purge_control_guard_begin(
                       local, version, &error),
                0, "doomed guard");
            notify(ready[1]);
            for (;;) {
                pause();
            }
        }
        wait(ready[0]);
        check(kill(child, SIGKILL) == 0 && waitpid(child, &status, 0) == child,
            "kill pinned reader");
        expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0,
            "close after reader death");
        publish_controls(
            writer, { operation(901, SelectorKind::host, context.host) });
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "dead reader pin reclaimed without timeout eviction");

        expect(ngx_http_cache_purge_control_writer_close(&writer, &error), 0,
            "release publisher");
        child = fork();
        check(child >= 0, "fork publisher");
        if (!child) {
            ngx_http_cache_purge_control_writer_t *local = nullptr;
            expect(ngx_http_cache_purge_control_writer_open(
                       memory, size, &local, &error),
                0, "child publisher");
            publish_controls(
                local, { operation(902, SelectorKind::host, context.host) });
            expect(ngx_http_cache_purge_control_gate_open(local, &error), 0,
                "child opens gate");
            notify(ready[1]);
            for (;;) {
                pause();
            }
        }
        wait(ready[0]);
        check(lookup(reader, context).count(902),
            "parent sees child publication");
        expect(ngx_http_cache_purge_control_writer_open(
                   memory, size, &writer, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "live publisher cannot be stolen");
        check(kill(child, SIGKILL) == 0 && waitpid(child, &status, 0) == child,
            "kill publisher");
        expect(ngx_http_cache_purge_control_writer_open(
                   memory, size, &writer, &error),
            0, "reclaim dead publisher");
        expect(ngx_http_cache_purge_control_gate_open(writer, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "new owner cannot bypass recovery publication");
        expect(ngx_http_cache_purge_control_match(reader, context.get(), 10,
                   4096, ignore, nullptr, &version, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "takeover requires recovery gate");
        publish_controls(writer, {});
        expect(ngx_http_cache_purge_control_gate_open(writer, &error), 0,
            "recovered publisher");

        /*
         * Readers race repeated immutable publications. Every successful read
         */
        /*
         * must contain exactly one complete image, never a mixture of
         * versions.
         */
        auto *counters = static_cast<uint64_t *>(mmap(nullptr, 4096,
            PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        check(counters != MAP_FAILED, "stress counters");
        std::vector<pid_t> children;
        for (unsigned i = 0; i != 4; ++i) {
            auto worker = fork();
            check(worker >= 0, "fork stress reader");
            if (!worker) {
                ngx_http_cache_purge_control_reader_t *local = nullptr;
                int rc;
                do {
                    rc = ngx_http_cache_purge_control_reader_open(
                        memory, size, &local, &error);
                    if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
                        sched_yield();
                    }
                } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
                if (rc) {
                    _exit(3);
                }
                auto *request = context.get();
                while (!__atomic_load_n(counters, __ATOMIC_SEQ_CST)) {
                    Probe result;
                    uint64_t publication;
                    rc = ngx_http_cache_purge_control_match(local, request, 128,
                        65536, probe, &result, &publication, &error);
                    if (rc == 0) {
                        if (result.count && result.count != 100) {
                            _exit(4);
                        }
                        __atomic_add_fetch(counters + 1, 1, __ATOMIC_SEQ_CST);
                        if (result.count) {
                            __atomic_add_fetch(
                                counters + 2, 1, __ATOMIC_SEQ_CST);
                        }
                    } else if (rc != NGX_HTTP_CACHE_ROCKSDB_BUSY) {
                        _exit(5);
                    }
                }
                do {
                    rc = ngx_http_cache_purge_control_reader_close(
                        &local, &error);
                    if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
                        sched_yield();
                    }
                } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
                _exit(rc ? 6 : 0);
            }
            children.push_back(worker);
        }
        for (unsigned pass = 0; pass != 200; ++pass) {
            std::vector<Operation> image;
            for (unsigned n = 1; n <= 100; ++n) {
                image.push_back(operation(n + (pass % 2 ? 1000 : 0),
                    SelectorKind::host, context.host));
            }
            expect(ngx_http_cache_purge_control_gate_close(writer, &error), 0,
                "stress gate close");
            for (;;) {
                try {
                    publish_controls(writer, image);
                    break;
                } catch (const IndexError &e) {
                    if (e.code != NGX_HTTP_CACHE_ROCKSDB_BUSY) {
                        throw;
                    }
                    sched_yield();
                }
            }
            int rc;
            do {
                rc = ngx_http_cache_purge_control_gate_open(writer, &error);
                if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
                    sched_yield();
                }
            } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
            expect(rc, 0, "stress gate open");
        }
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (__atomic_load_n(counters + 2, __ATOMIC_SEQ_CST) < 1000
               && std::chrono::steady_clock::now() < deadline)
        {
            usleep(1000);
        }
        check(__atomic_load_n(counters + 2, __ATOMIC_SEQ_CST) >= 1000,
            "stress readers see completed images");
        __atomic_store_n(counters, uint64_t(1), __ATOMIC_SEQ_CST);
        for (auto worker : children) {
            check(waitpid(worker, &status, 0) == worker && status == 0,
                "concurrent reader stayed consistent");
        }
        munmap(counters, 4096);
        expect(ngx_http_cache_purge_control_reader_close(&reader, &error), 0,
            "close reader");
        expect(ngx_http_cache_purge_control_writer_close(&writer, &error), 0,
            "close writer");
        close(ready[0]);
        close(ready[1]);
        close(command[0]);
        close(command[1]);
        munmap(memory, size);
        std::cout << "PASS " << assertions << " control assertions"
                  << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "FAIL " << e.what() << std::endl;
        _exit(1);
    }
}
