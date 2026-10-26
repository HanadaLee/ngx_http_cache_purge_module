#include "ngx_http_cache_purge_coordinator.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>

using namespace ngx_http_cache_purge;
static unsigned assertions;
static ngx_http_cache_rocksdb_error_t error;
static void check(bool value, const char *message) {
    if (!value) throw std::runtime_error(message);
    ++assertions;
}
template<class Function> static void fails(int code, Function function) {
    try { function(); } catch (const IndexError &e) { check(e.code == code, e.what()); return; }
    throw std::runtime_error("expected coordinator failure");
}
static OperationInput input(std::string id) {
    return {std::move(id), {SelectorKind::url_prefix, "https://a.test/", ""}, PurgeMode::invalidate, 1000, "config-v1"};
}
static void ready(Coordinator &coordinator) {
    for (unsigned n = 0; n != 100; ++n) {
        if (coordinator.state() == Coordination::ready) return;
        check(coordinator.step(), "unblocked coordinator makes progress");
    }
    throw std::runtime_error("coordinator recovery did not finish");
}
static int visit(void *value, const ngx_http_cache_purge_control_match_t *m) {
    auto *out = static_cast<uint64_t *>(value); ++out[0]; out[1] |= m->flags; return 0;
}
static int lookup(ngx_http_cache_purge_control_reader_t *reader, uint64_t *out, uint64_t &version,
                  bool proxy = true, uint64_t now = 5000) {
    const unsigned char url[] = "https://a.test/file";
    ngx_http_cache_purge_control_context_t context{}; context.url = {url, sizeof(url) - 1}; context.proxy = proxy; context.now_ms = now;
    out[0] = out[1] = 0;
    return ngx_http_cache_purge_control_match(reader, &context, 64, 65536, visit, out, &version, &error);
}

int main(int argc, char **argv) {
    try {
        char temporary[] = "/tmp/ncp-coordinator-XXXXXX";
        const char *path = argc == 3 ? argv[2] : mkdtemp(temporary);
        check(path != nullptr, "coordinator database directory");
        ngx_http_cache_rocksdb_options_t options; ngx_http_cache_rocksdb_options(&options);
        ngx_http_cache_rocksdb_t *store = nullptr;
        check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0, error.message);
        size_t size = ngx_http_cache_purge_control_size(65536, 8);
        void *shared = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        check(shared != MAP_FAILED && ngx_http_cache_purge_control_init(shared, size, 65536, 8, &error) == 0, "coordinator shared region");
        ngx_http_cache_purge_control_reader_t *reader = nullptr;
        check(ngx_http_cache_purge_control_reader_open(shared, size, &reader, &error) == 0, "register test reader");
        Index index(store); Operations operations(store);
        uint64_t found[2], version = 0;
        {
            Coordinator coordinator(operations, shared, size);
            check(lookup(reader, found, version) == NGX_HTTP_CACHE_ROCKSDB_BUSY, "startup stays closed before ledger recovery");
            if (argc == 3) {
                std::string mode = argv[1]; ready(coordinator);
                if (mode == "--crash-accept") {
                    auto op = coordinator.accept(input("crash"), 1000, 300000);
                    check(coordinator.state() == Coordination::publishing && op.stage == OperationStage::preparing, "crash point precedes publication");
                    check(lookup(reader, found, version) == NGX_HTTP_CACHE_ROCKSDB_BUSY, "durable/unpublished window blocks cache reads");
                    std::cout << "PASS crash after durable acceptance before publication" << std::endl; _exit(0);
                }
                check(mode == "--recover", "known child mode");
                check(lookup(reader, found, version) == 0 && found[0] == 1 && found[1] == 5, "cold restart restores durable unpublished FENCE");
                auto op = operations.get("crash");
                check(op.accepted_at_ms == 1000 && !op.effective_at_ms, "recovery does not forge an effective receipt");
            } else {
                auto preview = operations.preview(input("first"), 1000);
                check(preview.sequence == 1 && operations.usage().live_records == 0, "preview does not allocate durable identity");
                ready(coordinator);
                check(lookup(reader, found, version) == 0 && !found[0], "empty recovered table is usable");
                check(ngx_http_cache_purge_control_guard_begin(reader, version, &error) == 0, "pin earlier synchronous admission");
                auto first = coordinator.accept(input("first"), 1000, 300000);
                check(first.stage == OperationStage::preparing && operations.get("first").revision == 1, "acceptance is durable preparing");
                check(coordinator.accept(input("first"), 5000, 1).propagation_ms == 300000, "replay remains available while publication is pending");
                fails(NGX_HTTP_CACHE_ROCKSDB_BUSY, [&] { coordinator.accept(input("second"), 1000, 300000); });
                check(coordinator.step() && !coordinator.step(), "old admission blocks readiness but not publication work");
                check(ngx_http_cache_purge_control_guard_end(reader, &error) == 0, "release earlier admission");
                ready(coordinator);
                check(lookup(reader, found, version) == 0 && found[0] == 1 && found[1] == 5, "published preparing barrier matches");
                check(!operations.get("first").effective_at_ms, "SHM publication alone cannot claim native hook effectiveness");

                // Ledger transitions here simulate a future native coordinator;
                // they are not claims about real files or response admission.
                first = operations.activate("first", first.revision, 2000); coordinator.refresh(); ready(coordinator);
                check(lookup(reader, found, version) == 0 && found[1] == 3, "confirmed ledger activation carries both duties");
                check(lookup(reader, found, version, false) == 0 && found[1] == 1, "non-proxy lookup filters propagation");
                OperationCheckpoint checkpoint{1, "", true, 2000, 0, 0, 0, ""};
                first = operations.checkpoint("first", first.revision, checkpoint);
                operations.materialize("first", first.revision, first.sequence); coordinator.refresh(); ready(coordinator);
                check(lookup(reader, found, version) == 0 && found[1] == 2, "materialized record retains propagation duty");
                check(operations.expire_propagation(302000) == 1, "expire durable propagation");
                coordinator.refresh(); ready(coordinator);
                check(lookup(reader, found, version) == 0 && !found[0], "finished duties release shared control");

                for (unsigned n = 0; n != 2; ++n) {
                    auto large = input("large-" + std::to_string(n));
                    large.selector = {SelectorKind::key_prefix, std::string(32000, 'x'), ""};
                    coordinator.accept(large, 4000, 300000); ready(coordinator);
                }
                auto large = input("too-large"); large.selector = {SelectorKind::key_prefix, std::string(32000, 'x'), ""};
                auto usage = operations.usage();
                fails(NGX_HTTP_CACHE_ROCKSDB_LIMIT, [&] { coordinator.accept(large, 4000, 300000); });
                fails(NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, [&] { operations.get("too-large"); });
                check(operations.usage().live_records == usage.live_records && coordinator.state() == Coordination::ready
                      && lookup(reader, found, version) == 0, "SHM budget rejection precedes persistence and preserves gate");
            }
        }
        check(lookup(reader, found, version) == NGX_HTTP_CACHE_ROCKSDB_BUSY, "owner shutdown closes gate before releasing DB");
        check(ngx_http_cache_purge_control_reader_close(&reader, &error) == 0, "close test reader");
        check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message); munmap(shared, size);
        std::cout << "PASS " << assertions << " coordinator assertions" << std::endl;
        return 0;
    } catch (const std::exception &e) { std::cerr << "FAIL " << e.what() << std::endl; _exit(1); }
}
