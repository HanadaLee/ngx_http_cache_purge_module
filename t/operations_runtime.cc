#include "ngx_http_cache_purge_test.hpp"
#include <unistd.h>
#include <iostream>
#include <algorithm>
#include <filesystem>

using namespace ngx_http_cache_purge;
extern "C" unsigned ngx_http_cache_purge_test_sync_count(void);
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
static ngx_http_cache_rocksdb_bytes_t
bytes(const char *s)
{
    return { reinterpret_cast<const unsigned char *>(s), std::strlen(s) };
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

static Operation
ngx_http_cache_purge_test_checkpoint(
    Operations &ops, const Operation &op, const OperationCheckpoint &point)
{
    ngx_http_cache_purge_checkpoint_t raw{ point.id, bytes(point.bookmark),
        point.scan_done, point.next_run_ms, point.visits_delta, point.pending,
        point.failed, bytes(point.last_error) };
    ngx_http_cache_purge_operation_t output{};
    ngx_http_cache_rocksdb_error_t error{};
    int rc = ngx_http_cache_purge_operations_checkpoint_deferred(
        ops.native(), bytes(op.input.id), op.revision, &raw, &output, &error);
    auto result =
        rc == 0 ? ngx_http_cache_purge_cpp_operation(output) : Operation{};
    ngx_http_cache_purge_operation_free(&output);
    check(rc == 0, error.message);
    return result;
}

static void
ngx_http_cache_purge_test_checkpoint_sync()
{
    char path[] = "/tmp/cache-purge-checkpoint-sync-XXXXXX";
    check(mkdtemp(path) != nullptr, "checkpoint sync directory");
    auto *store = open(path);
    {
        Index index(store);
        Operations ops(store);
        auto before = ngx_http_cache_purge_test_sync_count();
        auto op = ops.accept(input("sync"), 1000);
        check(ngx_http_cache_purge_test_sync_count() > before,
            "acceptance synchronizes immediately");
        before = ngx_http_cache_purge_test_sync_count();
        op = ops.activate("sync", op.revision, 1100);
        check(ngx_http_cache_purge_test_sync_count() > before,
            "activation synchronizes immediately");
        before = ngx_http_cache_purge_test_sync_count();
        auto pending = point(1, false, 1);
        op = ngx_http_cache_purge_test_checkpoint(ops, op, pending);
        check(ngx_http_cache_purge_test_sync_count() == before,
            "executor progress shares a later WAL barrier");
        auto replay = ops.checkpoint("sync", op.revision, pending);
        check(replay.visits == op.visits
                  && ngx_http_cache_purge_test_sync_count() == before + 1,
            "synchronous replay flushes deferred progress without duplicating "
            "it");
        before = ngx_http_cache_purge_test_sync_count();
        op = ngx_http_cache_purge_test_checkpoint(ops, op, point(2, true));
        check(ngx_http_cache_purge_test_sync_count() == before,
            "scan completion alone is not a durable completion receipt");
        op = ops.materialize("sync", op.revision, op.sequence);
        check(op.stage == OperationStage::materialized
                  && ngx_http_cache_purge_test_sync_count() == before + 1,
            "materialization flushes progress before FENCE retirement");
    }
    close(store);
}

static void
ngx_http_cache_purge_test_wal_boundary(const char *path)
{
    for (const auto &entry : std::filesystem::directory_iterator(path)) {
        if (entry.path().extension() == ".log") {
            std::cout << "WAL " << entry.path().string() << ' '
                      << entry.file_size() << std::endl;
        }
    }
}

static void
ngx_http_cache_purge_test_checkpoint_loss(
    const char *path, unsigned phase, bool recover)
{
    check(phase < 3, "valid checkpoint rollback phase");
    auto *store = open(path);
    Index index(store);
    Operations ops(store);
    File file;
    file.id[15] = 1;
    file.key = "progress";
    file.url = "https://example.test/progress";
    file.host = "example.test";
    file.tag_namespace = "team";
    file.key = identity_key(file.tag_namespace, file.url, file.misc);
    file.root = "/cache/a";
    file.zone = "a";
    file.path = "0/00/progress";
    file.tags = { "photo" };
    if (!recover) {
        index.prepare(file);
        index.commit(file.id, 1);
        auto op = ops.accept(input("progress"), 1000);
        op = ops.activate("progress", op.revision, 1100);
        if (phase == 0) {
            ngx_http_cache_purge_test_wal_boundary(path);
        }
        op = ngx_http_cache_purge_test_checkpoint(ops, op, point(1, false, 1));
        if (phase != 0) {
            index.retire(file.id, 2);
            if (phase == 1) {
                ngx_http_cache_purge_test_wal_boundary(path);
            }
            op = ngx_http_cache_purge_test_checkpoint(
                ops, op, point(2, phase == 2));
            if (phase == 2) {
                ops.materialize("progress", op.revision, op.sequence);
                ngx_http_cache_purge_test_wal_boundary(path);
                ngx_http_cache_rocksdb_mutation_t tail{
                    NGX_HTTP_CACHE_ROCKSDB_DEFAULT, NGX_HTTP_CACHE_ROCKSDB_PUT,
                    bytes("Z:tail"), bytes("cache data")
                };
                ngx_http_cache_rocksdb_error_t error{};
                check(ngx_http_cache_rocksdb_write_deferred(
                          store, &tail, 1, nullptr, 0, &error)
                          == 0,
                    "write disposable WAL tail");
            }
        }
        _exit(0);
    }
    auto op = ops.get("progress");
    auto saved = index.get(file.id);
    check(op.accepted_at_ms == 1000 && op.effective_at_ms == 1100,
        "progress loss preserves durable acceptance and activation");
    if (phase == 0) {
        check(op.checkpoint_id == 0 && op.pending == 0
                  && saved.state == FileState::committed,
            "lost reservation leaves the instance available for enumeration");
    } else if (phase == 1) {
        check(op.checkpoint_id == 1 && op.pending == 1
                  && saved.state == FileState::retired,
            "retirement barrier preserves the exact pending reservation");
    } else {
        check(op.checkpoint_id == 2 && op.pending == 0 && op.scan_done
                  && op.stage == OperationStage::materialized
                  && ops.usage().retired_sequence == op.sequence,
            "durable completion preserves prior progress and the write floor");
    }
    if (phase != 2) {
        check(op.stage == OperationStage::active && !op.scan_done
                  && ops.usage().retired_sequence == 0,
            "incomplete replay keeps FENCE and does not advance the write "
            "floor");
        fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
            [&] { ops.materialize("progress", op.revision, op.sequence); });
    }
    close(store);
    std::cout << "PASS checkpoint WAL rollback phase " << phase << std::endl;
}

static void
ngx_http_cache_purge_test_tag_groups()
{
    char path[] = "/tmp/cache-purge-tag-operations-XXXXXX";
    check(mkdtemp(path) != nullptr, "TAG operation directory");
    auto *store = open(path);
    std::vector<std::string> members;
    for (unsigned i = 0; i < 64; i++) {
        members.push_back("tag-" + std::to_string(100 + i));
    }
    auto request = input("tag-group");
    request.selector = { SelectorKind::namespace_tags, tag_group(members),
        "team" };
    uint64_t sequence;
    ngx_http_cache_rocksdb_error_t error{};
    auto require = [&](Operations &ops, const Selector &selector, size_t budget,
                       uint64_t expected) {
        ngx_http_cache_purge_selector_t raw{ static_cast<uint32_t>(
                                                 selector.kind),
            bytes(selector.value), bytes(selector.scope) };
        uint64_t invalidate = 0, remove = 0;
        check(ngx_http_cache_purge_operations_requirements(
                  ops.native(), &raw, &budget, &invalidate, &remove, &error)
                      == 0
                  && invalidate == expected && remove == 0,
            "TAG member requirement shares the group operation sequence");
    };
    {
        Index index(store);
        Operations ops(store);
        raw_write(store, "M:operations", "3");
        auto before = writes(store);
        Operations upgraded(store);
        check(writes(store) == before + 1,
            "schema 3 upgrades with one metadata write and no index scan");
        before = writes(store);
        auto accepted = ops.accept(request, 1000);
        sequence = accepted.sequence;
        check(writes(store) == before + 1
                  && ops.get(request.id).input.selector.value
                         == request.selector.value,
            "one write atomically accepts the TAG set and all member rules");
        for (const auto &member : members) {
            require(ops, { SelectorKind::namespace_tag, member, "team" }, 1,
                sequence);
            require(
                ops, { SelectorKind::namespace_tag, member, "other" }, 1, 0);
        }
        require(ops, request.selector, 64, sequence);
        auto invalid = request;
        for (auto bad : { tag_group({ "b", "a" }), tag_group({ "a", "a" }),
                 tag_group({ "a" }), request.selector.value + "suffix" })
        {
            invalid.id = "invalid";
            invalid.selector.value = bad;
            before = writes(store);
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID,
                [&] { ops.accept(invalid, 2000); });
            check(writes(store) == before,
                "bad TAG lists cannot partially write requirements");
        }
        before = writes(store);
        check(ops.accept(request, 9999).sequence == sequence
                  && writes(store) == before,
            "TAG group replay keeps its operation without writes");
        auto changed = request;
        changed.selector.value = tag_group({ "different", "tags" });
        fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            [&] { ops.accept(changed, 3000); });
        auto active = ops.activate(request.id, accepted.revision, 1001);
        active = ops.checkpoint(request.id, active.revision, point(1, true));
        ops.materialize(request.id, active.revision, active.sequence);
        require(ops, request.selector, 64, sequence);
    }
    close(store);
    store = open(path);
    {
        Index index(store);
        auto before = writes(store);
        Operations ops(store);
        check(
            writes(store) == before && ops.get(request.id).sequence == sequence,
            "group receipt reopens without rebuilding or rewriting");
        for (const auto &member : members) {
            require(ops, { SelectorKind::namespace_tag, member, "team" }, 1,
                sequence);
        }
        ngx_http_cache_purge_selector_t raw{
            NGX_HTTP_CACHE_PURGE_NAMESPACE_TAGS, bytes(request.selector.value),
            bytes(request.selector.scope)
        };
        size_t budget = 63;
        uint64_t invalidate = 99, remove = 99;
        check(ngx_http_cache_purge_operations_requirements(
                  ops.native(), &raw, &budget, &invalidate, &remove, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_LIMIT
                  && invalidate == 0 && remove == 0,
            "group requirement budget failure cannot expose partial coverage");
    }
    close(store);
}

static void
ngx_http_cache_purge_test_submission()
{
    char path[] = "/tmp/cache-purge-submit-XXXXXX";
    check(mkdtemp(path) != nullptr, "submission test directory");
    auto *store = open(path);
    ngx_http_cache_rocksdb_error_t error{};
    ngx_http_cache_purge_operation_t accepted{}, queued{};
    ngx_http_cache_purge_operation_input_t request{};
    request.id = bytes("submitted");
    request.mode = NGX_HTTP_CACHE_PURGE_DELETE;
    request.issued_at_ms = 10;
    request.selector = { NGX_HTTP_CACHE_PURGE_URL, bytes("https://a.test/a"),
        {} };
    request.binding = bytes("request-v1");
    {
        Index index(store);
        Operations ops(store);
        auto before = writes(store);
        check(ngx_http_cache_purge_operations_submit(
                  ops.native(), &request, 20, 300000, &accepted, &error)
                      == 0
                  && writes(store) == before + 1 && accepted.sequence == 1
                  && accepted.stage == NGX_HTTP_CACHE_PURGE_PREPARING,
            "acceptance and activation intent share one durable write");
        check(ngx_http_cache_purge_operations_activation(
                  ops.native(), &queued, &error)
                      == 0
                  && queued.sequence == 1,
            "peek the bounded durable activation queue");
        ngx_http_cache_purge_operation_free(&queued);
        ngx_http_cache_purge_operation_free(&accepted);
        before = writes(store);
        check(ngx_http_cache_purge_operations_submit(
                  ops.native(), &request, 30, 1, &accepted, &error)
                      == 0
                  && writes(store) == before && accepted.accepted_at_ms == 20
                  && accepted.propagation_ms == 300000,
            "queued retry preserves the first acceptance without writes");
        ngx_http_cache_purge_operation_free(&accepted);
        auto changed = request;
        changed.binding = bytes("different");
        check(ngx_http_cache_purge_operations_submit(
                  ops.native(), &changed, 30, 300000, &accepted, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "same identity with changed input cannot alter the queue");
    }
    close(store);
    store = open(path);
    {
        Index index(store);
        Operations ops(store);
        check(ngx_http_cache_purge_operations_activation(
                  ops.native(), &queued, &error)
                      == 0
                  && queued.sequence == 1,
            "activation survives owner reopen");
        auto before = writes(store);
        check(ngx_http_cache_purge_operations_activate(ops.native(), request.id,
                  queued.revision, 40, &accepted, &error)
                      == 0
                  && writes(store) == before + 1
                  && accepted.effective_at_ms == 40,
            "activation atomically removes the durable intent");
        ngx_http_cache_purge_operation_free(&queued);
        ngx_http_cache_purge_operation_free(&accepted);
        check(ngx_http_cache_purge_operations_activation(
                  ops.native(), &queued, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
            "activated work leaves no orphan activation entry");
        auto ordinary = input("ordinary");
        auto receipt = ops.accept(ordinary, 50);
        ngx_http_cache_purge_operation_input_t promoted{ bytes(ordinary.id),
            { static_cast<uint32_t>(ordinary.selector.kind),
                bytes(ordinary.selector.value),
                bytes(ordinary.selector.scope) },
            static_cast<uint32_t>(ordinary.mode), ordinary.issued_at_ms,
            bytes(ordinary.binding) };
        check(ngx_http_cache_purge_operations_submit(
                  ops.native(), &promoted, 60, 300000, &accepted, &error)
                      == 0
                  && accepted.sequence == receipt.sequence
                  && accepted.accepted_at_ms == 50,
            "an existing diagnostic acceptance can be submitted without a new "
            "identity");
        ngx_http_cache_purge_operation_free(&accepted);
        check(ngx_http_cache_purge_operations_activation(
                  ops.native(), &queued, &error)
                      == 0
                  && queued.sequence == receipt.sequence,
            "promoted operation is queued for autonomous activation");
        ngx_http_cache_purge_operation_free(&queued);
    }
    close(store);
}

static void
ngx_http_cache_purge_test_requirements()
{
    char path[] = "/tmp/cache-purge-rules-XXXXXX";
    check(mkdtemp(path) != nullptr, "selector requirement test directory");
    auto *store = open(path);
    struct Rule {
        Selector selector;
        PurgeMode mode;
        uint64_t sequence;
    };
    std::vector<Rule> rules;
    std::vector<std::pair<std::string, std::string>> saved;
    auto query = [&](Operations &ops, Selector selector, size_t limit = 512) {
        uint64_t actual[2]{}, expected[2]{};
        ngx_http_cache_purge_selector_t raw{ static_cast<uint32_t>(
                                                 selector.kind),
            bytes(selector.value), bytes(selector.scope) };
        ngx_http_cache_rocksdb_error_t error{};
        size_t budget = limit;
        check(ngx_http_cache_purge_operations_requirements(
                  ops.native(), &raw, &budget, &actual[0], &actual[1], &error)
                  == 0,
            error.message);
        for (const auto &rule : rules) {
            bool prefix =
                (selector.kind == SelectorKind::url
                    && rule.selector.kind == SelectorKind::url_prefix)
                || (selector.kind == SelectorKind::key
                    && rule.selector.kind == SelectorKind::key_prefix);
            if (rule.selector.scope == selector.scope
                && ((rule.selector.kind == selector.kind
                        && rule.selector.value == selector.value)
                    || (prefix
                        && selector.value.compare(0, rule.selector.value.size(),
                               rule.selector.value)
                               == 0)))
            {
                auto slot = rule.mode == PurgeMode::remove;
                expected[slot] = std::max(expected[slot], rule.sequence);
            }
        }
        check(actual[0] == expected[0] && actual[1] == expected[1],
            "ordered requirements match a full-history reference");
        return limit - budget;
    };
    auto verify = [&](Operations &ops) {
        for (const auto &value : { "https://a.test/", "https://a.test/dir/abc",
                 "https://a.test/dir/aa", "https://a.test/dir/y",
                 "https://a.test/dir/z", "https://a.test/elsewhere" })
        {
            query(ops, { SelectorKind::url, value, "" });
        }
        check(
            query(ops, { SelectorKind::url, "https://a.test/dir/y", "" }) <= 8,
            "predecessor seeks skip hundreds of unrelated sibling prefixes");
        query(
            ops, { SelectorKind::key, std::string("k\0\xff", 3) + "tail", "" });
        query(ops, { SelectorKind::key, "ordinary-key", "" });
        query(ops, { SelectorKind::host, "a.test", "" });
        query(ops, { SelectorKind::namespace_tag, "tag", "one" });
        query(ops, { SelectorKind::namespace_tag, "tag", "two" });
        query(ops, { SelectorKind::namespace_tag, "tag", "" });
        query(ops, { SelectorKind::host_tag, "tag", "a.test" });
        query(ops, { SelectorKind::host_tag, "tag", "other.test" });
    };
    {
        Index index(store);
        Operations ops(store);
        auto add = [&](SelectorKind kind, const std::string &value,
                       PurgeMode mode, const std::string &scope = "") {
            auto request = input("rule-" + std::to_string(rules.size()), mode);
            request.selector = { kind, value, scope };
            auto operation = ops.accept(request, 1000);
            rules.push_back({ request.selector, mode, operation.sequence });
        };
        add(SelectorKind::url_prefix, "https://a.test/", PurgeMode::invalidate);
        add(SelectorKind::url_prefix, "https://a.test/dir/", PurgeMode::remove);
        add(SelectorKind::url, "https://a.test/dir/abc", PurgeMode::invalidate);
        add(SelectorKind::url_prefix, "https://a.test/dir/ab",
            PurgeMode::invalidate);
        for (unsigned i = 0; i < 500; i++) {
            add(SelectorKind::url_prefix,
                "https://a.test/dir/x/" + std::to_string(i),
                PurgeMode::invalidate);
        }
        add(SelectorKind::url_prefix, "https://a.test/", PurgeMode::remove);
        add(SelectorKind::key_prefix, "", PurgeMode::invalidate);
        add(SelectorKind::key_prefix, std::string("k\0", 2), PurgeMode::remove);
        add(SelectorKind::host, "a.test", PurgeMode::invalidate);
        add(SelectorKind::namespace_tag, "tag", PurgeMode::remove, "one");
        add(SelectorKind::namespace_tag, "tag", PurgeMode::invalidate, "");
        add(SelectorKind::host_tag, "tag", PurgeMode::invalidate, "a.test");
        auto before = writes(store);
        verify(ops);
        check(writes(store) == before, "requirement lookup is read-only");
        auto retired = ops.get("rule-0");
        retired = ops.activate("rule-0", retired.revision, 2000);
        retired = ops.checkpoint("rule-0", retired.revision, point(1, true));
        ops.materialize("rule-0", retired.revision, retired.sequence);
        ops.expire_propagation(400000);
        verify(ops);
        uint64_t invalidate = 123, remove = 123;
        size_t budget = 0;
        ngx_http_cache_rocksdb_error_t error{};
        ngx_http_cache_purge_selector_t selected{ NGX_HTTP_CACHE_PURGE_URL,
            bytes("https://a.test/dir/abc"), bytes("") };
        check(ngx_http_cache_purge_operations_requirements(ops.native(),
                  &selected, &budget, &invalidate, &remove, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_LIMIT
                  && invalidate == 0 && remove == 0,
            "exhausted lookup budget never returns partial authorization");
        ngx_http_cache_rocksdb_cursor_t *cursor = nullptr;
        check(ngx_http_cache_rocksdb_cursor_open(store,
                  NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes("T"), bytes(""),
                  &cursor, &error)
                  == 0,
            "inspect sparse requirement rows");
        unsigned char keys[65536], values[16];
        ngx_http_cache_rocksdb_buffer_t key{ keys, sizeof(keys), 0 },
            value{ values, sizeof(values), 0 };
        while (ngx_http_cache_rocksdb_cursor_next(cursor, &key, &value, &error)
               == 0)
        {
            saved.emplace_back(
                std::string(reinterpret_cast<char *>(keys), key.length),
                std::string(reinterpret_cast<char *>(values), value.length));
        }
        check(ngx_http_cache_rocksdb_cursor_close(&cursor, &error) == 0,
            "close requirement inspection");
        check(saved.size() == rules.size() - 1,
            "repeated selectors share one row with independent mode "
            "requirements");
        auto host =
            std::find_if(saved.begin(), saved.end(), [](const auto &row) {
                return row.first.size() >= 6
                       && row.first[1] == NGX_HTTP_CACHE_PURGE_HOST;
            });
        check(host != saved.end(), "host requirement row exists");
        selected = { NGX_HTTP_CACHE_PURGE_HOST, bytes("a.test"), bytes("") };
        for (const auto &corrupt :
            { std::string("bad"), std::string(16, '\0') })
        {
            raw_write(store, host->first, corrupt);
            budget = 512;
            invalidate = remove = 123;
            check(ngx_http_cache_purge_operations_requirements(ops.native(),
                      &selected, &budget, &invalidate, &remove, &error)
                          == NGX_HTTP_CACHE_ROCKSDB_CORRUPT
                      && invalidate == 0 && remove == 0,
                "corrupt selector requirement never produces authorization");
        }
        raw_write(store, host->first, host->second);
        for (const auto &row : saved) {
            raw_write(store, row.first, "", true);
        }
        raw_write(store, "M:operations", "1");
        raw_write(store, "M:rule-replay", "", true);
    }
    close(store);
    store = open(path);
    {
        Operations migrated(store);
        verify(migrated);
        auto before = writes(store);
        Operations again(store);
        verify(again);
        check(writes(store) == before,
            "completed rule migration is not repeated on reopen");
        auto lost =
            std::find_if(saved.begin(), saved.end(), [&](const auto &row) {
                return row.first.size() == 6 + rules[99].selector.value.size()
                       && row.first.compare(
                              6, std::string::npos, rules[99].selector.value)
                              == 0;
            });
        check(lost != saved.end(),
            "choose a row beyond the migration checkpoint");
        raw_write(store, lost->first, "", true);
        raw_write(store, "M:rule-replay", std::string(7, '\0') + '\1');
        raw_write(store, "M:rule-cursor", "Drule-0");
        Operations resumed(store);
        query(resumed,
            { SelectorKind::url, rules[99].selector.value + "/object", "" });
        verify(resumed);
        before = writes(store);
        Operations completed(store);
        check(writes(store) == before,
            "resumed migration commits its completion marker");
        ngx_http_cache_rocksdb_stats_t stats{};
        ngx_http_cache_rocksdb_error_t error{};
        check(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0
                  && stats.cursor_handles == 0,
            "requirement lookups retain no database cursors");
    }
    close(store);
}

int
main(int argc, char **argv)
{
    try {
        if (argc == 4
            && (std::string(argv[1]) == "--crash-progress"
                || std::string(argv[1]) == "--recover-progress"))
        {
            ngx_http_cache_purge_test_checkpoint_loss(argv[2],
                static_cast<unsigned>(std::strtoul(argv[3], nullptr, 10)),
                std::string(argv[1]) == "--recover-progress");
            return 0;
        }
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
            ngx_http_cache_purge_selector_t selector{
                NGX_HTTP_CACHE_PURGE_NAMESPACE_TAG, bytes("photo"),
                bytes("team")
            };
            ngx_http_cache_rocksdb_error_t error{};
            size_t budget = 8;
            uint64_t invalidate = 0, remove = 0;
            check(ngx_http_cache_purge_operations_requirements(ops.native(),
                      &selector, &budget, &invalidate, &remove, &error)
                          == 0
                      && invalidate == active.sequence && remove == 0,
                "accepted selector requirement survives abrupt exit atomically "
                "with the ledger");
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
            invalid.selector.kind = SelectorKind::host_tag;
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
            raw_write(store, "M:operations", "2");
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
            ngx_http_cache_rocksdb_error_t boundary_error;
            check(ngx_http_cache_purge_operations_origin(
                      ops.native(), nullptr, &boundary_error)
                      == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "publication rejects a malformed retired boundary");
            raw_write(store, "M:op-retired", std::string(8, '\xff'));
            fails(NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                [&] { Operations future(store); });
            check(ngx_http_cache_purge_operations_origin(
                      ops.native(), nullptr, &boundary_error)
                      == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "publication rejects an inverted writable interval");
            raw_write(store, "M:op-retired", "", true);
            check(ngx_http_cache_purge_operations_origin(
                      ops.native(), nullptr, &boundary_error)
                      == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "publication rejects a missing retired boundary before upgrade");
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
            check(ngx_http_cache_purge_operations_origin(
                      nullptr, nullptr, &boundary_error)
                      == NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "publication rejects a missing operation handle");
            raw_write(store, "M:op-next", std::string(8, '\0'));
            check(ngx_http_cache_purge_operations_origin(
                      again.native(), nullptr, &boundary_error)
                      == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "publication rejects a zero sequence allocator");
        }
        close(store);
        ngx_http_cache_purge_test_submission();
        ngx_http_cache_purge_test_requirements();
        ngx_http_cache_purge_test_tag_groups();
        ngx_http_cache_purge_test_checkpoint_sync();
        std::cout << "PASS " << assertions << " operation assertions; database "
                  << path << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "FAIL " << e.what() << std::endl;
        _exit(1);
    }
}
