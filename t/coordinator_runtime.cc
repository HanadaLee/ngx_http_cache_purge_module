#include "ngx_http_cache_purge_test.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>
#include <atomic>
#include <thread>

using namespace ngx_http_cache_purge;
static unsigned assertions;
static ngx_http_cache_rocksdb_error_t error;
static void
check(bool value, const char *message)
{
    if (!value) {
        throw std::runtime_error(message);
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
    throw std::runtime_error("expected coordinator failure");
}
static OperationInput
input(std::string id)
{
    return { std::move(id), { SelectorKind::url_prefix, "https://a.test/", "" },
        PurgeMode::invalidate, 1000, "config-v1" };
}
static void
ready(Coordinator &coordinator)
{
    for (unsigned n = 0; n != 100; ++n) {
        if (coordinator.state() == Coordination::ready) {
            return;
        }
        check(coordinator.step(), "unblocked coordinator makes progress");
    }
    throw std::runtime_error("coordinator recovery did not finish");
}
static int
visit(void *value, const ngx_http_cache_purge_control_match_t *m)
{
    auto *out = static_cast<uint64_t *>(value);
    ++out[0];
    out[1] |= m->flags;
    return 0;
}
static int
lookup(ngx_http_cache_purge_control_reader_t *reader, uint64_t *out,
    uint64_t &version, bool proxy = true, uint64_t now = 5000)
{
    const unsigned char url[] = "https://a.test/file";
    ngx_http_cache_purge_control_context_t context{};
    context.url = { url, sizeof(url) - 1 };
    context.proxy = proxy;
    context.now_ms = now;
    out[0] = out[1] = 0;
    return ngx_http_cache_purge_control_match(
        reader, &context, 64, 65536, visit, out, &version, &error);
}

static void
ngx_http_cache_purge_test_file_propagation()
{
    char path[] = "/tmp/ncp-file-propagation-XXXXXX";
    check(mkdtemp(path) != nullptr, "file propagation directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0,
        error.message);
    size_t size = ngx_http_cache_purge_control_size(262144, 8);
    void *shared = mmap(nullptr, size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    check(shared != MAP_FAILED
              && ngx_http_cache_purge_control_init(
                     shared, size, 262144, 8, &error)
                     == 0,
        "file propagation shared region");
    {
        Index index(store);
        Operations operations(store);
        File red;
        red.id[15] = 1;
        red.key = "shared-key";
        red.url = "https://cache.test/item";
        red.host = "cache.test";
        red.tag_namespace = "tenant";
        red.root = "/cache";
        red.zone = "a";
        red.path = "red";
        red.tags = { "red" };
        red.vary = "Accept-Language";
        File blue = red;
        blue.id[15] = 2;
        blue.path = "blue";
        blue.tags = { "blue" };
        for (const auto &file : { red, blue }) {
            auto prepared = index.prepare(file);
            index.commit(file.id, prepared.revision);
        }
        Coordinator coordinator(operations, shared, size);
        auto query =
            [&](const File &file, uint64_t now, int code = 0,
                uint64_t revision = 2,
                const ngx_http_cache_purge_control_context_t *observed =
                    nullptr) {
                auto expected =
                    ngx_http_cache_purge_native_expected(file.id, revision);
                ngx_http_cache_purge_fence_t result{ 99, 99 };
                uint64_t publication = 99;
                int rc = ngx_http_cache_purge_coordinator_file_propagation(
                    coordinator.native(), &expected, observed, now, &result,
                    &publication, &error);
                check(rc == code, error.message);
                if (rc) {
                    check(result.invalidate_ms == 0 && result.delete_ms == 0
                              && publication == 0,
                        "failed local query exposes no partial cutoffs");
                } else {
                    check(
                        publication == coordinator.publication() && publication,
                        "file propagation returns its control publication");
                }
                return result;
            };
        query(red, 2000, NGX_HTTP_CACHE_ROCKSDB_BUSY);
        ready(coordinator);
        auto result = query(red, 2000);
        check(result.invalidate_ms == 0 && result.delete_ms == 0,
            "empty control image has no propagation requirement");
        auto add = [&](const std::string &id, Selector selector, PurgeMode mode,
                       uint64_t issued) {
            OperationInput input{ id, std::move(selector), mode, issued,
                "file-propagation" };
            auto operation = coordinator.accept(input, 2000, 100);
            ready(coordinator);
            return operation;
        };
        add("red", { SelectorKind::namespace_tag, "red", "tenant" },
            PurgeMode::invalidate, 1000);
        result = query(red, 900000);
        check(result.invalidate_ms == 1000 && result.delete_ms == 0,
            "published PREPARING propagates without an activation window");
        result = query(blue, 2000);
        check(result.invalidate_ms == 0 && result.delete_ms == 0,
            "same-key Vary files do not inherit another file's TAGs");
        add("wrong-scope", { SelectorKind::namespace_tag, "red", "other" },
            PurgeMode::remove, 9000);
        result = query(red, 2000);
        check(result.invalidate_ms == 1000 && result.delete_ms == 0,
            "namespace isolation uses indexed file metadata");
        check(ngx_http_cache_purge_index_observe(store,
                  ngx_http_cache_purge_native_bytes(red.key),
                  ngx_http_cache_purge_native_bytes("https://alias.test/item"),
                  ngx_http_cache_purge_native_bytes("alias.test"), &error)
                  == 0,
            "add URL and business-host aliases to the shared key");
        add("host-blue", { SelectorKind::host_tag, "blue", "alias.test" },
            PurgeMode::remove, 2000);
        add("alias", { SelectorKind::url, "https://alias.test/item", "" },
            PurgeMode::remove, 1100);
        add("prefix", { SelectorKind::url_prefix, "https://alias.test/", "" },
            PurgeMode::invalidate, 1300);
        add("current-host", { SelectorKind::host_tag, "red", "fresh.test" },
            PurgeMode::remove, 2200);
        add("current-url", { SelectorKind::url, "https://fresh.test/item", "" },
            PurgeMode::invalidate, 2300);
        ngx_http_cache_purge_control_context_t observed{};
        const std::string fresh_url = "https://fresh.test/item";
        const std::string fresh_host = "fresh.test";
        observed.key = ngx_http_cache_purge_native_bytes(red.key);
        observed.url = ngx_http_cache_purge_native_bytes(fresh_url);
        observed.host = ngx_http_cache_purge_native_bytes(fresh_host);
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
            "read propagation baseline statistics");
        result = query(red, 2000);
        check(result.invalidate_ms == 1300 && result.delete_ms == 1100,
            "historical URL alias contributes independent mode maxima");
        result = query(blue, 2000);
        check(result.invalidate_ms == 1300 && result.delete_ms == 2000,
            "host TAG checks alias scope and this file's actual TAGs");
        result = query(red, 2000, 0, 2, &observed);
        check(result.invalidate_ms == 2300 && result.delete_ms == 2200,
            "new request identities match before their aliases are persisted");
        result = query(blue, 2000, 0, 2, &observed);
        check(result.invalidate_ms == 2300 && result.delete_ms == 2000,
            "current host TAG still uses the selected file's membership");
        auto wrong = observed;
        const std::string wrong_key = "other-key";
        wrong.key = ngx_http_cache_purge_native_bytes(wrong_key);
        query(red, 2000, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, 2, &wrong);
        wrong = observed;
        wrong.host = { nullptr, 10 };
        query(red, 2000, NGX_HTTP_CACHE_ROCKSDB_INVALID, 2, &wrong);
        check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && before.writes == after.writes,
            "local propagation queries perform no WAL writes");
        for (const auto &id :
            { "red", "wrong-scope", "host-blue", "alias", "prefix" })
        {
            auto operation = operations.get(id);
            operations.activate(id, operation.revision, 3000);
        }
        coordinator.refresh();
        ready(coordinator);
        result = query(red, 3099);
        check(result.invalidate_ms == 1300 && result.delete_ms == 1100,
            "active local windows preserve object cutoffs");
        result = query(red, 3100);
        check(result.invalidate_ms == 0 && result.delete_ms == 0,
            "expiry stops propagation while local FENCEs may remain");
        auto operation = operations.get("red");
        operation = operations.checkpoint("red", operation.revision,
            OperationCheckpoint{ 1, "", true, 3000, 0, 0, 0, "" });
        operations.materialize("red", operation.revision, operation.sequence);
        coordinator.refresh();
        ready(coordinator);
        result = query(red, 3099);
        check(result.invalidate_ms == 1300,
            "file propagation is independent of local FENCE retirement");
        query(red, 3001, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, 1);
        File missing = red;
        missing.id[15] = 99;
        query(missing, 3001, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        query(red, 0, NGX_HTTP_CACHE_ROCKSDB_INVALID);
        index.retire(blue.id, 2);
        query(blue, 3001, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, 3);
        for (unsigned n = 0; n < 63; n++) {
            index.add_url(red.key, "https://many.test/" + std::to_string(n));
        }
        query(red, 3001, NGX_HTTP_CACHE_ROCKSDB_LIMIT);
        File bounded = red;
        bounded.id[15] = 3;
        bounded.key = "budget-key";
        bounded.path = "budget";
        bounded.tags.clear();
        auto prepared = index.prepare(bounded);
        index.commit(bounded.id, prepared.revision);
        for (unsigned n = 0; n < 62; n++) {
            index.add_url(
                bounded.key, "https://budget.test/" + std::to_string(n));
        }
        for (unsigned n = 0; n < 20; n++) {
            add("all-" + std::to_string(n),
                { SelectorKind::url_prefix, "", "" }, PurgeMode::invalidate,
                n + 1);
        }
        query(bounded, 3001, NGX_HTTP_CACHE_ROCKSDB_LIMIT);
        query(red, 3001, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, 1);
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
    munmap(shared, size);
}

static void
ngx_http_cache_purge_test_retention()
{
    char path[] = "/tmp/ncp-automatic-retention-XXXXXX";
    check(mkdtemp(path) != nullptr, "retention coordinator directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0,
        error.message);
    size_t size = ngx_http_cache_purge_control_size(262144, 8);
    void *shared = mmap(nullptr, size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    check(shared != MAP_FAILED
              && ngx_http_cache_purge_control_init(
                     shared, size, 262144, 8, &error)
                     == 0,
        "retention coordinator shared region");
    {
        Index index(store);
        Operations operations(store);
        File file;
        file.id[15] = 1;
        file.key = "tenant\037https\037cache.test\037/object\037";
        file.url = "https://cache.test/object";
        file.host = "cache.test";
        file.tag_namespace = "tenant";
        file.structured_key = true;
        file.structured_url = true;
        file.components = { "tenant", "https", "cache.test", "/object", "",
            "" };
        file.tags = { "red" };
        file.root = "/cache";
        file.zone = "cache";
        file.path = std::string(31, '0') + '1';
        auto prepared = index.prepare(file);
        index.commit(file.id, prepared.revision);
        check(ngx_http_cache_purge_index_observe(store,
                  ngx_http_cache_purge_native_bytes(file.key),
                  ngx_http_cache_purge_native_bytes("https://alias.test/item"),
                  ngx_http_cache_purge_native_bytes("alias.test"), &error)
                  == 0,
            "retention source has historical URL and host aliases");
        Coordinator coordinator(operations, shared, size);
        ready(coordinator);
        unsigned char main[16] = {}, variant[16] = {};
        main[15] = 1;
        auto expected = ngx_http_cache_purge_native_expected(file.id, 2);
        auto get = [&](uint64_t now, int code = 0) {
            ngx_http_cache_purge_fence_t result{};
            check(ngx_http_cache_purge_index_get_propagation(store,
                      ngx_http_cache_purge_native_bytes(file.key), main,
                      ngx_http_cache_purge_native_bytes(file.vary), variant,
                      now, &result, &error)
                      == code,
                error.message);
            return result;
        };
        auto accept = [&](const std::string &name, Selector selector,
                          PurgeMode mode, uint64_t issued, uint64_t now,
                          uint32_t window) {
            OperationInput input{ name, selector, mode, issued, "retention" };
            auto op = coordinator.accept(input, now, window);
            ready(coordinator);
            return op;
        };
        auto activate = [&](const Operation &op, uint64_t now) {
            operations.activate(op.input.id, op.revision, now);
            coordinator.refresh();
            ready(coordinator);
        };
        auto first =
            accept("first", { SelectorKind::namespace_tag, "red", "tenant" },
                PurgeMode::invalidate, 10, 100, 50);
        check(ngx_http_cache_purge_coordinator_keep_file(
                  coordinator.native(), &expected, 100, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "PREPARING cannot invent a persistent local deadline");
        get(100, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        activate(first, 100);
        check(ngx_http_cache_purge_coordinator_keep_file(
                  coordinator.native(), &expected, 110, &error)
                  == 0,
            error.message);
        check(get(110).invalidate_ms == 10, "file TAG match is retained");
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
            "retention statistics");
        check(ngx_http_cache_purge_coordinator_keep_file(
                  coordinator.native(), &expected, 111, &error)
                  == 0,
            error.message);
        check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && before.writes == after.writes,
            "preservation retry performs no WAL write");
        auto updated = index.update_metadata(file.id, 2, "new", { "blue" });
        auto retired = index.retire(file.id, updated.revision);
        index.collect_file(file.id, retired.revision);
        while (index.collect_keys(1)) {
        }
        auto second =
            accept("second", { SelectorKind::namespace_tag, "red", "tenant" },
                PurgeMode::remove, 20, 140, 100);
        size_t processed = 99;
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 151, 8, &processed, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_BUSY
                  && processed == 0,
            "new PREPARING rule prevents history collection");
        check(get(149).invalidate_ms == 10,
            "failed expiry retains the original snapshot");
        activate(second, 140);
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 151, 8, &processed, &error)
                      == 0
                  && processed == 1,
            error.message);
        auto result = get(151);
        check(!result.invalidate_ms && result.delete_ms == 20,
            "later TAG rule renews retained membership after file collection");
        auto wrong =
            accept("wrong", { SelectorKind::namespace_tag, "red", "other" },
                PurgeMode::remove, 999, 200, 1000);
        activate(wrong, 200);
        auto host =
            accept("host", { SelectorKind::host_tag, "red", "alias.test" },
                PurgeMode::invalidate, 30, 220, 100);
        activate(host, 220);
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 241, 8, &processed, &error)
                      == 0
                  && processed == 1,
            error.message);
        result = get(241);
        check(result.invalidate_ms == 30 && !result.delete_ms,
            "history renewal uses retained TAGs and persistent host aliases "
            "with namespace isolation");
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 321, 8, &processed, &error)
                      == 0
                  && processed == 1,
            error.message);
        get(321, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        while (index.collect_keys(1)) {
        }
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
    munmap(shared, size);
}

int
main(int argc, char **argv)
{
    try {
        if (argc == 1) {
            ngx_http_cache_purge_test_file_propagation();
            ngx_http_cache_purge_test_retention();
        }
        char temporary[] = "/tmp/ncp-coordinator-XXXXXX";
        const char *path = argc == 3 ? argv[2] : mkdtemp(temporary);
        check(path != nullptr, "coordinator database directory");
        ngx_http_cache_rocksdb_options_t options;
        ngx_http_cache_rocksdb_options(&options);
        ngx_http_cache_rocksdb_t *store = nullptr;
        check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0,
            error.message);
        size_t size = ngx_http_cache_purge_control_size(65536, 8);
        check(ngx_http_cache_purge_control_size(65536, 9) - size == 64,
            "writer tracking retains the 64-byte reader slot footprint");
        void *shared = mmap(nullptr, size, PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        check(shared != MAP_FAILED
                  && ngx_http_cache_purge_control_init(
                         shared, size, 65536, 8, &error)
                         == 0,
            "coordinator shared region");
        ngx_http_cache_purge_control_reader_t *reader = nullptr;
        check(ngx_http_cache_purge_control_reader_open(
                  shared, size, &reader, &error)
                  == 0,
            "register test reader");
        Index index(store);
        Operations operations(store);
        uint64_t found[2], version = 0;
        ngx_http_cache_purge_watermark_t watermark{};
        uint64_t boundary = 0;
        {
            Coordinator coordinator(operations, shared, size);
            check(lookup(reader, found, version) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
                "startup stays closed before ledger recovery");
            check(ngx_http_cache_purge_control_watermark(
                      reader, &watermark, &boundary, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_BUSY,
                "no origin watermark before recovery");
            if (argc == 3) {
                std::string mode = argv[1];
                ready(coordinator);
                if (mode == "--crash-accept") {
                    auto op = coordinator.accept(input("crash"), 1000, 300000);
                    check(coordinator.state() == Coordination::publishing
                              && op.stage == OperationStage::preparing,
                        "crash point precedes publication");
                    check(lookup(reader, found, version)
                              == NGX_HTTP_CACHE_ROCKSDB_BUSY,
                        "durable/unpublished window blocks cache reads");
                    std::cout << "PASS crash after durable acceptance before "
                                 "publication"
                              << std::endl;
                    _exit(0);
                }
                check(mode == "--recover", "known child mode");
                check(lookup(reader, found, version) == 0 && found[0] == 1
                          && found[1] == 5,
                    "cold restart restores durable unpublished FENCE");
                auto op = operations.get("crash");
                check(op.accepted_at_ms == 1000 && !op.effective_at_ms,
                    "recovery does not forge an effective receipt");
            } else {
                auto preview = operations.preview(input("first"), 1000);
                check(preview.sequence == 1
                          && operations.usage().live_records == 0,
                    "preview does not allocate durable identity");
                ready(coordinator);
                check(lookup(reader, found, version) == 0 && !found[0],
                    "empty recovered table is usable");
                check(ngx_http_cache_purge_control_watermark(
                          reader, &watermark, &boundary, &error)
                              == 0
                          && watermark.sequence == 0 && boundary == version
                          && std::memcmp(watermark.ledger,
                                 operations.native()->ledger, 16)
                                 == 0,
                    "empty serving table exposes its durable ledger and zero "
                    "sequence");
                check(ngx_http_cache_purge_control_origins(
                          reader, nullptr, 1, &error)
                          == 0,
                    "announce unknown origins before taking their snapshot");
                check(ngx_http_cache_purge_control_guard_begin(
                          reader, version, &error)
                          == 0,
                    "pin earlier synchronous admission");
                auto earlier_admission = version;
                auto first = coordinator.accept(input("first"), 1000, 300000);
                check(first.stage == OperationStage::preparing
                          && operations.get("first").revision == 1,
                    "acceptance is durable preparing");
                check(coordinator.accept(input("first"), 5000, 1).propagation_ms
                          == 300000,
                    "replay remains available while publication is pending");
                fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                    [&] { coordinator.accept(input("second"), 1000, 300000); });
                check(coordinator.step() && !coordinator.step(),
                    "old admission blocks readiness but not publication work");
                check(
                    ngx_http_cache_purge_control_guard_end(reader, &error) == 0,
                    "release earlier admission");
                check(ngx_http_cache_purge_control_watermark(
                          reader, &watermark, &boundary, &error)
                          == NGX_HTTP_CACHE_ROCKSDB_BUSY,
                    "no watermark while acceptance publication is pending");
                ready(coordinator);
                check(lookup(reader, found, version) == 0 && found[0] == 1
                          && found[1] == 5,
                    "published preparing barrier matches");
                check(ngx_http_cache_purge_control_guard_begin(
                          reader, earlier_admission, &error)
                          == NGX_HTTP_CACHE_ROCKSDB_BUSY,
                    "a delayed admission stamp cannot pin a newer control "
                    "publication");
                check(!operations.get("first").effective_at_ms,
                    "SHM publication alone cannot claim native hook "
                    "effectiveness");
                check(coordinator.writers(1) == 1,
                    "unknown origin blocks the old-writer boundary");
                check(ngx_http_cache_purge_control_reader_close(&reader, &error)
                              == NGX_HTTP_CACHE_ROCKSDB_BUSY
                          && reader != nullptr,
                    "active origins cannot unregister their reader");
                std::memcpy(watermark.ledger, operations.native()->ledger, 16);
                watermark.sequence = 0;
                check(ngx_http_cache_purge_control_origins(
                          reader, &watermark, 1, &error)
                              == 0
                          && coordinator.writers(1) == 1,
                    "known pre-operation origins remain visible");
                watermark.sequence = 1;
                check(ngx_http_cache_purge_control_origins(
                          reader, &watermark, 1, &error)
                              == 0
                          && coordinator.writers(1) == 0,
                    "post-operation origins do not block an older cutoff");
                watermark.ledger[0] ^= 1;
                check(ngx_http_cache_purge_control_origins(
                          reader, &watermark, 1, &error)
                              == 0
                          && coordinator.writers(1) == 1,
                    "foreign-ledger origins are conservative blockers");
                check(ngx_http_cache_purge_control_origins(
                          reader, nullptr, 0, &error)
                              == 0
                          && coordinator.writers(1) == 0,
                    "completed origins release the boundary");
                fails(NGX_HTTP_CACHE_ROCKSDB_INVALID,
                    [&] { coordinator.writers(2); });

                std::atomic<bool> registered{ false }, racing{ false },
                    stop{ false };
                std::atomic<int> worker_error{ 0 };
                std::thread updater([&] {
                    ngx_http_cache_purge_control_reader_t *local = nullptr;
                    ngx_http_cache_rocksdb_error_t failure;
                    ngx_http_cache_purge_watermark_t old{}, foreign{};
                    std::memcpy(old.ledger, operations.native()->ledger, 16);
                    foreign = old;
                    foreign.ledger[0] ^= 1;
                    foreign.sequence = 1;
                    int rc = ngx_http_cache_purge_control_reader_open(
                        shared, size, &local, &failure);
                    if (!rc) {
                        rc = ngx_http_cache_purge_control_origins(
                            local, &old, 1, &failure);
                    }
                    worker_error = rc;
                    registered = true;
                    while (!racing && !stop) {
                        std::this_thread::yield();
                    }
                    while (!rc && !stop) {
                        rc = ngx_http_cache_purge_control_origins(
                            local, &foreign, 1, &failure);
                        if (!rc) {
                            rc = ngx_http_cache_purge_control_origins(
                                local, &old, 1, &failure);
                        }
                        std::this_thread::yield();
                    }
                    if (local) {
                        int cleared = ngx_http_cache_purge_control_origins(
                            local, nullptr, 0, &failure);
                        if (!rc) {
                            rc = cleared;
                        }
                        int closed;
                        do {
                            closed = ngx_http_cache_purge_control_reader_close(
                                &local, &failure);
                        } while (closed == NGX_HTTP_CACHE_ROCKSDB_BUSY);
                        if (!rc) {
                            rc = closed;
                        }
                    }
                    worker_error = rc;
                });
                while (!registered) {
                    std::this_thread::yield();
                }
                bool consistent = worker_error == 0;
                racing = true;
                for (unsigned i = 0; consistent && i < 1000; i++) {
                    try {
                        consistent = coordinator.writers(1) == 1;
                    } catch (const IndexError &e) {
                        consistent = e.code == NGX_HTTP_CACHE_ROCKSDB_BUSY;
                    }
                }
                stop = true;
                updater.join();
                check(consistent && worker_error == 0
                          && coordinator.writers(1) == 0,
                    "concurrent ledger/sequence updates never report a false "
                    "quiet boundary");

                /*
                 * Ledger transitions here simulate a future native
                 * coordinator;
                 */
                /*
                 * they are not claims about real files or response admission.
                 */
                first = operations.activate("first", first.revision, 2000);
                coordinator.refresh();
                ready(coordinator);
                check(lookup(reader, found, version) == 0 && found[1] == 3,
                    "confirmed ledger activation carries both duties");
                check(
                    lookup(reader, found, version, false) == 0 && found[1] == 1,
                    "non-proxy lookup filters propagation");
                OperationCheckpoint checkpoint{ 1, "", true, 2000, 0, 0, 0,
                    "" };
                first =
                    operations.checkpoint("first", first.revision, checkpoint);
                operations.materialize("first", first.revision, first.sequence);
                coordinator.refresh();
                ready(coordinator);
                check(lookup(reader, found, version) == 0 && found[1] == 2,
                    "materialized record retains propagation duty");
                check(operations.expire_propagation(302000) == 1,
                    "expire durable propagation");
                coordinator.refresh();
                ready(coordinator);
                check(lookup(reader, found, version) == 0 && !found[0],
                    "finished duties release shared control");
                check(ngx_http_cache_purge_control_watermark(
                          reader, &watermark, &boundary, &error)
                              == 0
                          && watermark.sequence == 1,
                    "watermark does not regress when all live rules are "
                    "removed");

                for (unsigned n = 0; n != 2; ++n) {
                    auto large = input("large-" + std::to_string(n));
                    large.selector = { SelectorKind::key_prefix,
                        std::string(32000, 'x'), "" };
                    coordinator.accept(large, 4000, 300000);
                    ready(coordinator);
                }
                auto large = input("too-large");
                large.selector = { SelectorKind::key_prefix,
                    std::string(32000, 'x'), "" };
                auto usage = operations.usage();
                fails(NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                    [&] { coordinator.accept(large, 4000, 300000); });
                fails(NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
                    [&] { operations.get("too-large"); });
                check(operations.usage().live_records == usage.live_records
                          && coordinator.state() == Coordination::ready
                          && lookup(reader, found, version) == 0,
                    "SHM budget rejection precedes persistence and preserves "
                    "gate");
            }
        }
        check(lookup(reader, found, version) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "owner shutdown closes gate before releasing DB");
        check(ngx_http_cache_purge_control_reader_close(&reader, &error) == 0,
            "close test reader");
        check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
        munmap(shared, size);
        std::cout << "PASS " << assertions << " coordinator assertions"
                  << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "FAIL " << e.what() << std::endl;
        _exit(1);
    }
}
