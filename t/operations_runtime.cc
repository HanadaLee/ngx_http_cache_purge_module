#include "ngx_http_cache_purge_test.hpp"
#include <unistd.h>
#include <iostream>
#include <algorithm>

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
template <class Function>
void
fails(int code, Function function)
{
    try {
        function();
    } catch (const IndexError &error) {
        check(error.code == code, error.what());
        return;
    }
    throw std::runtime_error("expected operation error");
}
static OperationInput
input(std::string id, PurgeMode mode = PurgeMode::invalidate)
{
    return { std::move(id), { SelectorKind::namespace_tag, "photo", "team" },
        mode, 1000, "config-v1" };
}
static ngx_http_cache_rocksdb_t *
open(const char *path)
{
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error;
    check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0,
        error.message);
    return store;
}
static void
close(ngx_http_cache_rocksdb_t *&store)
{
    ngx_http_cache_rocksdb_error_t error;
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
}
static OperationCheckpoint
point(uint64_t id, bool done, uint64_t pending = 0, uint64_t failed = 0)
{
    return { id, "opaque-index-bookmark", done, 1200, 1, pending, failed,
        failed ? "file busy" : "" };
}
static uint64_t
writes(ngx_http_cache_rocksdb_t *store)
{
    ngx_http_cache_rocksdb_stats_t stats;
    ngx_http_cache_rocksdb_error_t error;
    check(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0,
        "ledger write stats");
    return stats.writes;
}
static ngx_http_cache_rocksdb_bytes_t
bytes(const std::string &s)
{
    return { reinterpret_cast<const unsigned char *>(s.data()), s.size() };
}
static void
raw_write(ngx_http_cache_rocksdb_t *store, const std::string &key,
    const std::string &value, bool remove = false)
{
    ngx_http_cache_rocksdb_mutation_t mutation{ NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        remove ? NGX_HTTP_CACHE_ROCKSDB_DELETE : NGX_HTTP_CACHE_ROCKSDB_PUT,
        bytes(key), bytes(value) };
    ngx_http_cache_rocksdb_error_t error;
    check(ngx_http_cache_rocksdb_write(store, &mutation, 1, nullptr, 0, &error)
              == 0,
        "raw corruption fixture");
}

int
main(int argc, char **argv)
{
    try {
        if (argc == 4 && std::string(argv[1]) == "--retire-fixture") {
            /*
             * Offline fixture only: no claim of physical work or native
             * quiescence.
             */
            auto *store = open(argv[2]);
            Index index(store);
            Operations ops(store);
            auto op = ops.get(argv[3]);
            op = ops.activate(argv[3], op.revision, 2000);
            op = ops.checkpoint(argv[3], op.revision, point(1, true));
            ops.materialize(argv[3], op.revision, op.sequence);
            check(ops.usage().retired_sequence == op.sequence,
                "durable retirement floor");
            std::cout << "PASS offline retirement fixture" << std::endl;
            _exit(0);
        }
        if (argc == 3) {
            auto *store = open(argv[2]);
            Index index(store);
            Operations ops(store);
            const std::string mode = argv[1];
            if (mode == "--crash-accept") {
                ops.accept(input("pending"), 1000);
                auto active = ops.accept(input("active"), 1000);
                active = ops.activate("active", active.revision, 1100);
                ops.checkpoint(
                    "active", active.revision, point(1, false, 2, 1));
                std::cout << "PASS durable accepted/active checkpoint"
                          << std::endl;
                _exit(0);
            }
            if (mode != "--recover") {
                return 2;
            }
            auto pending = ops.get("pending"), active = ops.get("active");
            check(pending.stage == OperationStage::preparing
                      && !pending.effective_at_ms,
                "unpublished acceptance survives crash");
            check(active.stage == OperationStage::active && active.pending == 2
                      && active.failed == 1
                      && active.bookmark == "opaque-index-bookmark",
                "active checkpoint survives crash");
            check(
                ops.live().items.size() == 2 && ops.due(1200).items.size() == 1,
                "recovery uses control and due ranges");
            check(ops.accept(input("active"), 999999, 1).propagation_until_ms
                      == 301100,
                "retry does not restart propagation");
            check(ops.expire_propagation(400000) == 1
                      && ops.live().items.size() == 2,
                "expiry retains unfinished fences after restart");
            close(store);
            std::cout << "PASS " << assertions << " recovery assertions"
                      << std::endl;
            return 0;
        }
        char path[] = "/tmp/cache-purge-operations-XXXXXX";
        check(mkdtemp(path) != nullptr, "test directory");
        auto *store = open(path);
        FileId ledger{};
        {
            Index index(store);
            Operations ops(store, { 2, 4096 });
            std::copy_n(ops.native()->ledger, ledger.size(), ledger.begin());
            check(ledger != FileId{}, "ledger identity is nonzero");
            auto initialized = writes(store);
            ngx_http_cache_rocksdb_error_t origin_error;
            ngx_http_cache_purge_watermark_t origin{};
            std::copy(ledger.begin(), ledger.end(), origin.ledger);
            check(ops.usage().retired_sequence == 0
                      && ngx_http_cache_purge_operations_origin(
                             ops.native(), nullptr, &origin_error)
                             == 0,
                "new ledger has no retired boundary");
            Operations reopened(store, { 2, 4096 });
            check(std::equal(
                      ledger.begin(), ledger.end(), reopened.native()->ledger)
                      && writes(store) == initialized,
                "reopening the ledger retains identity without writes");
            auto invalid = input("bad id");
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID,
                [&] { ops.accept(invalid, 1000); });
            invalid = input("bad");
            invalid.selector.scope.clear();
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID,
                [&] { ops.accept(invalid, 1000); });
            auto a = ops.accept(input("a"), 1000);
            check(a.revision == 1 && a.sequence == 1 && !a.effective_at_ms
                      && !a.propagate && a.stage == OperationStage::preparing,
                "durable acceptance is not effective");
            auto before = writes(store);
            check(ops.accept(input("a"), 9999, 0).revision == 1
                      && writes(store) == before,
                "same ID is a read-only replay");
            auto changed = input("a");
            changed.selector.scope = "other";
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { ops.accept(changed, 1000); });
            changed = input("a", PurgeMode::remove);
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { ops.accept(changed, 1000); });
            auto b = ops.accept(input("b", PurgeMode::remove), 1001, 100);
            check(b.sequence == 2 && ops.live().items.size() == 2,
                "overlapping Delete/Invalidate remain separate");
            check(
                ops.usage().live_records == 2 && ops.usage().last_sequence == 2,
                "persistent resident usage watermark");
            fails(NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                [&] { ops.accept(input("c"), 1002); });
            check(ops.due(2000).items.empty(),
                "unpublished operations cannot be scheduled");
            auto first = ops.live("", 1), second = ops.live(first.after, 1),
                 end = ops.live(second.after, 1);
            check(first.items[0].input.id == "a"
                      && second.items[0].input.id == "b" && end.done,
                "paged recovery order");
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID, [&] { ops.live("Qbad", 1); });
            a = ops.activate("a", 1, 1100);
            check(a.propagate && a.propagation_until_ms == 301100
                      && a.stage == OperationStage::active,
                "activation freezes sending window");
            check(ops.activate("a", a.revision, 9999).propagation_until_ms
                      == 301100,
                "activation retry does not extend window");
            check(
                ops.due(1099).items.empty() && ops.due(1100).items.size() == 1,
                "due time scheduling");
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { ops.materialize("a", a.revision, a.sequence); });
            auto p = point(1, true, 1, 1);
            a = ops.checkpoint("a", a.revision, p);
            check(a.stage == OperationStage::draining && a.visits == 1,
                "EOF enters drain state");
            before = writes(store);
            check(ops.checkpoint("a", 2, p).visits == 1
                      && writes(store) == before,
                "checkpoint reply loss does not double count");
            auto other = p;
            other.failed = 0;
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { ops.checkpoint("a", a.revision, other); });
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { ops.materialize("a", a.revision, a.sequence); });
            check(ops.expire_propagation(301099) == 0
                      && ops.expire_propagation(301100) == 1,
                "local propagation expiration");
            a = ops.get("a");
            check(!a.propagate && ops.live().items.size() == 2
                      && !ops.due(400000).items.empty(),
                "propagation timeout never clears FENCE or jobs");
            p = point(2, true);
            a = ops.checkpoint("a", a.revision, p);
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { ops.materialize("a", a.revision, 0); });
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID,
                [&] { ops.materialize("a", a.revision, 3); });
            check(ops.usage().retired_sequence == 0,
                "failed materialization cannot advance the floor");
            before = writes(store);
            a = ops.materialize("a", a.revision, a.sequence);
            check(writes(store) == before + 1
                      && ops.usage().retired_sequence == 1,
                "retirement floor and FENCE removal share one durable batch");
            check(ngx_http_cache_purge_operations_origin(
                      ops.native(), &origin, &origin_error)
                          == NGX_HTTP_CACHE_ROCKSDB_CONFLICT
                      && ngx_http_cache_purge_operations_origin(
                             ops.native(), nullptr, &origin_error)
                             == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "retired floor rejects abandoned old and unknown origins");
            origin.sequence = 1;
            check(ngx_http_cache_purge_operations_origin(
                      ops.native(), &origin, &origin_error)
                      == 0,
                "origin at the retirement boundary is writable");
            origin.sequence = 3;
            check(ngx_http_cache_purge_operations_origin(
                      ops.native(), &origin, &origin_error)
                      == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "future origin cannot bypass the retirement floor");
            check(ops.live().items.size() == 1 && ops.due(400000).items.empty(),
                "verified drain releases fence and schedule");
            check(ops.accept(input("a"), 900000).stage
                      == OperationStage::materialized,
                "completed ID remains a receipt");
            check(
                ops.checkpoint("a", 1, p).stage == OperationStage::materialized,
                "late identical checkpoint cannot resurrect task");
            auto c = ops.accept(input("c"), 1100, 0);
            check(c.sequence == 3, "released capacity can accept new work");
            b = ops.activate("b", 1, 2000);
            b = ops.checkpoint("b", b.revision, point(1, true));
            b = ops.materialize("b", b.revision, b.sequence);
            check(b.propagate && ops.live().items.size() == 2,
                "materialized operation retains PROPAGATE independently");
            check(ops.expire_propagation(2100) == 1
                      && ops.live().items.size() == 1,
                "both duties ended releases resident record");
            c = ops.activate("c", c.revision, 3000);
            check(!c.propagate && c.propagation_until_ms == 3000,
                "zero propagation policy creates no sending obligation");
            auto d = ops.accept(input("d"), 3001);
            (void) d;
            Operations lowered(store, { 1, 1024 });
            check(lowered.live().items.size() == 2,
                "lowered capacity retains existing controls");
            fails(NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                [&] { lowered.accept(input("e"), 3002); });
        }
        close(store);
        store = open(path);
        {
            Index index(store);
            Operations ops(store, { 64, 1024 * 1024 });
            check(
                std::equal(ledger.begin(), ledger.end(), ops.native()->ledger),
                "ledger identity survives database reopen");
            check(ops.usage().retired_sequence == 2,
                "retirement floor survives database reopen");
            check(ops.get("c").stage == OperationStage::active
                      && ops.get("d").stage == OperationStage::preparing,
                "ledger stages survive reopen");
            auto c = ops.get("c");
            c = ops.checkpoint("c", c.revision, point(1, true));
            ops.materialize("c", c.revision, c.sequence);
            for (unsigned n = 0; n != 32; ++n) {
                auto id = "batch-" + std::to_string(n);
                auto op = ops.accept(input(id), 4000, 100);
                op = ops.activate(id, op.revision, 4000);
                op = ops.checkpoint(id, op.revision, point(1, true));
                ops.materialize(id, op.revision, op.sequence);
            }
            auto before = writes(store);
            check(ops.expire_propagation(4100, 32) == 32
                      && writes(store) == before + 1,
                "32 propagation expirations share one durable batch");
            check(ops.live().items.size() == 1
                      && ops.live().items[0].input.id == "d",
                "batch expiry preserves unpublished fence");
            fails(
                NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, [&] { ops.get("missing"); });
            auto d = ops.get("d");
            std::string link = "L";
            auto floor = ops.usage().retired_sequence;
            d = ops.activate("d", d.revision, 5000);
            d = ops.checkpoint("d", d.revision, point(1, true));
            ops.materialize("d", d.revision, d.sequence);
            check(ops.usage().retired_sequence == floor,
                "out-of-order materialization never regresses floor");
            for (int shift = 56; shift >= 0; shift -= 8) {
                link += static_cast<char>(d.sequence >> shift);
            }
            raw_write(store, link, "", true);
            fails(NGX_HTTP_CACHE_ROCKSDB_CORRUPT, [&] { ops.get("d"); });
            fails(NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                [&] { ops.accept(input("d"), 9999); });
            raw_write(store, link, "d");
            raw_write(store, "M:operations", "future");
            fails(NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
                [&] { Operations unsupported(store); });
            raw_write(store, "M:operations", "1");
            raw_write(store, "M:op-ledger", "broken");
            fails(NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                [&] { Operations corrupted(store); });
            raw_write(store, "M:op-ledger",
                std::string(
                    reinterpret_cast<char *>(ledger.data()), ledger.size()));
            check(ops.get("d").sequence == d.sequence,
                "schema rejection preserves existing operation");
            for (unsigned n = 0; n != 10; ++n) {
                auto large = input("large-" + std::to_string(n));
                large.selector = { SelectorKind::key_prefix,
                    std::string(32768, static_cast<char>('a' + n)), "" };
                ops.accept(large, 5000);
            }
            auto p = ops.live();
            check(p.items.size() < 11 && !p.done,
                "live recovery page enforces byte budget as well as count");
            auto count = p.items.size();
            while (!p.done) {
                p = ops.live(p.after);
                count += p.items.size();
            }
            check(count == ops.usage().live_records && count == 11,
                "paged recovery matches persisted live count");
            auto last = ops.usage().last_sequence;
            raw_write(store, "M:op-retired", "broken");
            fails(NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                [&] { Operations broken(store); });
            raw_write(store, "M:op-retired", std::string(8, '\xff'));
            fails(NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                [&] { Operations future(store); });
            raw_write(store, "M:op-retired", "", true);
            before = writes(store);
            Operations upgraded(store);
            check(upgraded.usage().retired_sequence == last
                      && writes(store) == before + 1,
                "old ledger upgrade establishes a conservative boundary "
                "without index rebuild");
            before = writes(store);
            Operations again(store);
            check(again.usage().retired_sequence == last
                      && writes(store) == before,
                "normal reopen preserves the retired floor without writes");
        }
        close(store);
        std::cout << "PASS " << assertions << " operation assertions; database "
                  << path << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "FAIL " << e.what() << std::endl;
        _exit(1);
    }
}
