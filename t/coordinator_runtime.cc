#include "ngx_http_cache_purge_test.hpp"
#include <openssl/evp.h>
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
ngx_http_cache_purge_test_object_propagation()
{
    char path[] = "/tmp/ncp-object-propagation-XXXXXX";
    check(mkdtemp(path) != nullptr, "object propagation directory");
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
        "object propagation shared region");
    {
        Index index(store);
        Operations operations(store);
        Coordinator coordinator(operations, shared, size);
        ready(coordinator);
        unsigned char main[16] = { 1 }, red_variant[16], blue_variant[16];
        auto hash = [&](const std::string &language, unsigned char *out) {
            std::string data(reinterpret_cast<char *>(main), 16);
            data += "accept-charset:UTF-8,ISO-8859-1\r\n"
                    "accept-encoding:gzip,br\r\naccept-language:";
            data += language;
            data += "\r\ncookie:a=1;  b=2,c=3\r\nx-empty:,\r\n";
            unsigned length;
            check(EVP_Digest(data.data(), data.size(), out, &length, EVP_md5(),
                      nullptr)
                          == 1
                      && length == 16,
                "independent expected native Vary input");
        };
        hash("en,fr,de", red_variant);
        hash("de", blue_variant);
        File red;
        red.id[15] = 1;
        red.key = "object-key";
        red.url = "https://cache.test/item";
        red.host = "cache.test";
        red.tag_namespace = "tenant";
        red.root = "/cache";
        red.zone = "a";
        red.path = "red";
        red.tags = { "red" };
        red.vary = " Accept-Charset, Accept-Encoding, Accept-Language, Cookie, "
                   "X-Empty ";
        File blue = red;
        blue.id[15] = 2;
        blue.path = "blue";
        blue.tags = { "blue" };
        for (const auto &file : { red, blue }) {
            ngx_http_cache_purge_index_match_t prepared{};
            auto raw = ngx_http_cache_purge_native_file(file);
            check(
                ngx_http_cache_purge_index_prepare_native(store, &raw, nullptr,
                    main, file.id == red.id ? red_variant : blue_variant,
                    &prepared, &error)
                    == 0,
                error.message);
            index.commit(file.id, prepared.revision);
        }
        auto encode = [](bool english) {
            std::vector<std::pair<std::string, std::string>> fields{
                { "Accept-Charset", " UTF-8 ,ISO-8859-1" },
                { "accept-Encoding", " gzip,  br " },
                { "Accept-Language", english ? " en, fr  " : "" },
                { "accept-language", "de" }, { "Cookie", "a=1;  b=2" },
                { "cookie", "c=3" }, { "X-Empty", "" }, { "x-empty", "" },
                { "X-Unrelated", "not indexed" }
            };
            std::string out;
            auto number = [&](uint32_t n) {
                for (int i = 3; i >= 0; i--) {
                    out += static_cast<char>(n >> (8 * i));
                }
            };
            number(fields.size());
            for (const auto &field : fields) {
                number(field.first.size());
                out += field.first;
                number(field.second.size());
                out += field.second;
            }
            return out;
        };
        auto red_headers = encode(true), blue_headers = encode(false);
        const std::string other_namespace = "other";
        const std::string unknown_key = "never-indexed";
        ngx_http_cache_purge_control_context_t request{};
        request.key = ngx_http_cache_purge_native_bytes(red.key);
        request.url = ngx_http_cache_purge_native_bytes(red.url);
        request.host = ngx_http_cache_purge_native_bytes(red.host);
        request.tag_namespace =
            ngx_http_cache_purge_native_bytes(other_namespace);
        auto query = [&](const std::string &headers, uint64_t now,
                         const unsigned char *identity = nullptr) {
            ngx_http_cache_purge_object_query_t *q = nullptr;
            check(
                ngx_http_cache_purge_coordinator_object_open(
                    coordinator.native(), &request, identity ? identity : main,
                    ngx_http_cache_purge_native_bytes(headers), now, &q, &error)
                    == 0,
                error.message);
            ngx_http_cache_purge_fence_t result{};
            uint64_t publication = 0;
            int done = 0;
            unsigned steps = 0;
            while (!done) {
                check(ngx_http_cache_purge_coordinator_object_step(
                          q, 1, &result, &publication, &done, &error)
                          == 0,
                    error.message);
                check(++steps < 1000, "object query makes bounded progress");
                check(done
                          || (!result.invalidate_ms && !result.delete_ms
                              && !publication),
                    "object query publishes no partial requirement");
            }
            check(publication == coordinator.publication(),
                "object query retains its exact publication");
            check(
                ngx_http_cache_purge_coordinator_object_close(&q, &error) == 0,
                "close object query");
            return result;
        };
        auto add = [&](const std::string &name, Selector selector,
                       PurgeMode mode, uint64_t issued) {
            OperationInput input{ name, selector, mode, issued,
                "object-query" };
            auto op = coordinator.accept(input, 2000, 120000);
            ready(coordinator);
            operations.activate(op.input.id, op.revision, 2000);
            coordinator.refresh();
            ready(coordinator);
        };
        add("red", { SelectorKind::namespace_tag, "red", "tenant" },
            PurgeMode::invalidate, 1000);
        add("blue", { SelectorKind::namespace_tag, "blue", "tenant" },
            PurgeMode::remove, 2000);
        add("wrong", { SelectorKind::namespace_tag, "red", "other" },
            PurgeMode::remove, 9000);
        auto result = query(red_headers, 2100);
        check(result.invalidate_ms == 1000 && result.delete_ms == 0,
            "native normalization selects red without caller-namespace "
            "pollution");
        result = query(blue_headers, 2100);
        check(result.invalidate_ms == 0 && result.delete_ms == 2000,
            "other Vary representation has independent TAG requirements");
        result = query(std::string(4, '\0'), 2100);
        check(result.invalidate_ms == 0 && result.delete_ms == 0,
            "unknown Vary has no TAG compensation");
        for (unsigned i = 0; i < 90; i++) {
            auto host = "alias-" + std::to_string(i) + ".test";
            check(ngx_http_cache_purge_index_observe(store, request.key,
                      ngx_http_cache_purge_native_bytes(
                          "https://" + host + "/item"),
                      ngx_http_cache_purge_native_bytes(host), &error)
                      == 0,
                "object aliases beyond the old fixed limit");
        }
        add("alias-red", { SelectorKind::host_tag, "red", "alias-89.test" },
            PurgeMode::remove, 1500);
        add("alias-url",
            { SelectorKind::url, "https://alias-88.test/item", "" },
            PurgeMode::invalidate, 1800);
        result = query(red_headers, 2100);
        check(result.invalidate_ms == 1800 && result.delete_ms == 1500,
            "paged host TAG and URL aliases both participate");
        {
            ngx_http_cache_purge_object_query_t *pending[4]{};
            for (auto &entry : pending) {
                check(ngx_http_cache_purge_coordinator_object_open(
                          coordinator.native(), &request, main,
                          ngx_http_cache_purge_native_bytes(red_headers), 2100,
                          &entry, &error)
                          == 0,
                    "open four interleaved object queries");
            }
            unsigned remaining = 4, rounds = 0;
            while (remaining) {
                check(++rounds < 1000,
                    "concurrent objects make progress within their budget");
                for (auto &entry : pending) {
                    if (entry == nullptr) {
                        continue;
                    }
                    ngx_http_cache_purge_fence_t fence{};
                    uint64_t version = 0;
                    int complete = 0;
                    check(ngx_http_cache_purge_coordinator_object_step(
                              entry, 8, &fence, &version, &complete, &error)
                              == 0,
                        "four objects share the default eight cursor slots");
                    if (complete) {
                        check(fence.invalidate_ms == 1800
                                  && fence.delete_ms == 1500
                                  && version == coordinator.publication(),
                            "concurrent object retains exact requirements");
                        check(ngx_http_cache_purge_coordinator_object_close(
                                  &entry, &error)
                                  == 0,
                            "release completed interleaved object");
                        remaining--;
                    }
                }
                ngx_http_cache_rocksdb_stats_t usage{};
                check(ngx_http_cache_rocksdb_stats(store, &usage, &error) == 0
                          && usage.cursor_handles == remaining
                          && usage.active_cursors == remaining,
                    "pending objects retain one cursor per snapshot");
                auto background = index.query(
                    { SelectorKind::namespace_tag, "red", "tenant" });
                Match matched{};
                check(background->next(matched) && matched.id == red.id
                          && !background->next(matched),
                    "TAG enumeration advances between object query batches");
            }
        }
        unsigned char other_main[16] = { 2 };
        result = query(red_headers, 2100, other_main);
        check(result.invalidate_ms == 1800 && result.delete_ms == 0,
            "unknown slice inherits identity rules but not another slice's "
            "TAGs");
        auto expected = ngx_http_cache_purge_native_expected(red.id, 2);
        ngx_http_cache_purge_retention_t retained{ 1000, 10000, 1500, 10000 };
        check(ngx_http_cache_purge_index_keep_propagation(
                  store, &expected, main, red_variant, 2100, &retained, &error)
                  == 0,
            "retain selected representation before its file is removed");
        auto retired = index.retire(red.id, 2);
        index.collect_file(red.id, retired.revision);
        add("later-red", { SelectorKind::namespace_tag, "red", "tenant" },
            PurgeMode::invalidate, 2500);
        result = query(red_headers, 3000);
        check(result.invalidate_ms == 2500 && result.delete_ms == 1500,
            "deleted known representation matches later local TAG rules");
        result = query(red_headers, 123000);
        check(result.invalidate_ms == 0 && result.delete_ms == 0,
            "expired outgoing windows do not emit requirements");
        request.key = ngx_http_cache_purge_native_bytes(unknown_key);
        result = query(red_headers, 3000);
        check(result.invalidate_ms == 0 && result.delete_ms == 0,
            "confirmed unknown key proceeds without TAG compensation");
        ngx_http_cache_purge_object_query_t *q = nullptr;
        std::string malformed = red_headers + "trailing";
        check(ngx_http_cache_purge_coordinator_object_open(coordinator.native(),
                  &request, main, ngx_http_cache_purge_native_bytes(malformed),
                  3000, &q, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_INVALID
                  && !q,
            "malformed header snapshot never starts a query");
        request.key = ngx_http_cache_purge_native_bytes(red.key);
        check(
            ngx_http_cache_purge_coordinator_object_open(coordinator.native(),
                &request, main, ngx_http_cache_purge_native_bytes(red_headers),
                3000, &q, &error)
                == 0,
            "open publication race query");
        uint64_t publication = 99;
        int done = 99;
        check(ngx_http_cache_purge_coordinator_object_step(
                  q, 1, &result, &publication, &done, &error)
                      == 0
                  && !done && !publication,
            "start object query before control change");
        add("racing", { SelectorKind::namespace_tag, "red", "tenant" },
            PurgeMode::remove, 3000);
        check(ngx_http_cache_purge_coordinator_object_step(
                  q, 1, &result, &publication, &done, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_BUSY
                  && !done && !publication && !result.invalidate_ms
                  && !result.delete_ms,
            "changed control image discards all partial requirements");
        check(ngx_http_cache_purge_coordinator_object_close(&q, &error) == 0,
            "close raced object query");
        check(
            ngx_http_cache_purge_coordinator_object_open(coordinator.native(),
                &request, main, ngx_http_cache_purge_native_bytes(red_headers),
                3000, &q, &error)
                == 0,
            "open expiring object query");
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        check(ngx_http_cache_purge_coordinator_object_step(
                  q, 1, &result, &publication, &done, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_EXPIRED
                  && !done && !publication && !result.invalidate_ms
                  && !result.delete_ms,
            "expired snapshot never becomes a successful empty object result");
        check(ngx_http_cache_purge_coordinator_object_close(&q, &error) == 0,
            "close expired object query");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close object propagation database");
    munmap(shared, size);
}

static void
ngx_http_cache_purge_test_propagation_policy()
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
        unsigned char main[16] = { 1 };
        for (const auto &file : { red, blue }) {
            std::string hash_input(reinterpret_cast<char *>(main), 16);
            hash_input += "accept-language:" + file.path + "\r\n";
            unsigned char variant[16];
            unsigned length;
            check(EVP_Digest(hash_input.data(), hash_input.size(), variant,
                      &length, EVP_md5(), nullptr)
                          == 1
                      && length == 16,
                "policy fixture native Vary identity");
            auto raw = ngx_http_cache_purge_native_file(file);
            ngx_http_cache_purge_index_match_t prepared{};
            check(ngx_http_cache_purge_index_prepare_native(
                      store, &raw, nullptr, main, variant, &prepared, &error)
                      == 0,
                error.message);
            index.commit(file.id, prepared.revision);
        }
        Coordinator coordinator(operations, shared, size);
        auto query = [&](const File &file, uint64_t now, int code = 0,
                         const ngx_http_cache_purge_control_context_t
                             *observed = nullptr) {
            ngx_http_cache_purge_control_context_t request{};
            request.key = ngx_http_cache_purge_native_bytes(file.key);
            request.url = ngx_http_cache_purge_native_bytes(file.url);
            request.host = ngx_http_cache_purge_native_bytes(file.host);
            request.tag_namespace =
                ngx_http_cache_purge_native_bytes(file.tag_namespace);
            if (observed) {
                request.key = observed->key;
                request.url = observed->url;
                request.host = observed->host;
            }
            std::string headers;
            auto number = [&](uint32_t value) {
                for (int n = 3; n >= 0; n--) {
                    headers += static_cast<char>(value >> (n * 8));
                }
            };
            number(1);
            number(15);
            headers += "Accept-Language";
            number(file.path.size());
            headers += file.path;
            ngx_http_cache_purge_fence_t result{};
            uint64_t publication = 0;
            ngx_http_cache_purge_object_query_t *q = nullptr;
            int rc = ngx_http_cache_purge_coordinator_object_open(
                coordinator.native(), &request, main,
                ngx_http_cache_purge_native_bytes(headers), now, &q, &error);
            int done = 0;
            unsigned steps = 0;
            while (rc == 0 && !done) {
                rc = ngx_http_cache_purge_coordinator_object_step(
                    q, 8, &result, &publication, &done, &error);
                check(++steps < 1000, "policy query makes bounded progress");
            }
            check(rc == code, error.message);
            if (q) {
                check(ngx_http_cache_purge_coordinator_object_close(&q, &error)
                          == 0,
                    "close policy object query");
            }
            if (rc) {
                check(result.invalidate_ms == 0 && result.delete_ms == 0
                          && publication == 0,
                    "failed local query exposes no partial cutoffs");
            } else {
                check(publication == coordinator.publication() && publication,
                    "object propagation returns its control publication");
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
        result = query(red, 2000, 0, &observed);
        check(result.invalidate_ms == 2300 && result.delete_ms == 2200,
            "new request identities match before their aliases are persisted");
        result = query(blue, 2000, 0, &observed);
        check(result.invalidate_ms == 2300 && result.delete_ms == 2000,
            "current host TAG still uses the selected file's membership");
        auto wrong = observed;
        const std::string wrong_key = "other-key";
        wrong.key = ngx_http_cache_purge_native_bytes(wrong_key);
        check(query(red, 2000, 0, &wrong).delete_ms == 0,
            "unknown key cannot borrow indexed TAG memberships");
        wrong = observed;
        wrong.host = { nullptr, 10 };
        query(red, 2000, NGX_HTTP_CACHE_ROCKSDB_INVALID, &wrong);
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
        query(red, 0, NGX_HTTP_CACHE_ROCKSDB_INVALID);
        index.retire(blue.id, 2);
        check(query(blue, 3001).delete_ms == 2000,
            "retired committed files retain known membership until collection");
        for (unsigned n = 0; n < 63; n++) {
            index.add_url(red.key, "https://many.test/" + std::to_string(n));
        }
        check(query(red, 3001).invalidate_ms == 1300,
            "paged request query exceeds the retired diagnostic alias limit");
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
        ngx_http_cache_purge_object_query_t *history = nullptr;
        ngx_http_cache_purge_control_context_t history_request{};
        history_request.key = ngx_http_cache_purge_native_bytes(file.key);
        history_request.url = ngx_http_cache_purge_native_bytes(file.url);
        history_request.host = ngx_http_cache_purge_native_bytes(file.host);
        history_request.tag_namespace =
            ngx_http_cache_purge_native_bytes(file.tag_namespace);
        const std::string empty_headers(4, '\0');
        auto history_open = [&](uint64_t now) {
            check(ngx_http_cache_purge_coordinator_object_open(
                      coordinator.native(), &history_request, main,
                      ngx_http_cache_purge_native_bytes(empty_headers), now,
                      &history, &error)
                      == 0,
                error.message);
        };
        auto history_result = [&] {
            ngx_http_cache_purge_fence_t result{ 99, 99 };
            uint64_t publication = 99;
            int done = 99;
            check(ngx_http_cache_purge_coordinator_object_step(
                      history, 1, &result, &publication, &done, &error)
                          == 0
                      && !done && !publication && !result.invalidate_ms
                      && !result.delete_ms,
                "partial history step does not publish an aggregate");
            unsigned steps = 0;
            while (!done) {
                check(ngx_http_cache_purge_coordinator_object_step(
                          history, 1, &result, &publication, &done, &error)
                          == 0,
                    error.message);
                check(++steps < 100, "historical object query makes progress");
            }
            check(publication == coordinator.publication(),
                "history completion carries the matching publication");
            check(
                ngx_http_cache_purge_coordinator_object_close(&history, &error)
                    == 0,
                "close completed history query");
            return result;
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
        history_open(151);
        auto immediate = history_result();
        check(!immediate.invalidate_ms && immediate.delete_ms == 20,
            "a new TAG rule matches stored history immediately before GC "
            "renews its expired original window");
        history_open(151);
        coordinator.refresh();
        ready(coordinator);
        ngx_http_cache_purge_fence_t stale{ 99, 99 };
        uint64_t publication = 99;
        int done = 99;
        check(ngx_http_cache_purge_coordinator_object_step(
                  history, 1, &stale, &publication, &done, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_BUSY
                  && !stale.invalidate_ms && !stale.delete_ms && !publication
                  && !done,
            "a replaced control image rejects the history aggregate");
        check(ngx_http_cache_purge_coordinator_object_close(&history, &error)
                  == 0,
            "close history after publication change");
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
        auto alias = accept("alias",
            { SelectorKind::url_prefix, "https://alias.test/", "" },
            PurgeMode::remove, 40, 220, 10);
        activate(alias, 220);
        history_open(221);
        auto alias_result = history_result();
        check(alias_result.invalidate_ms == 30 && alias_result.delete_ms == 40,
            "retained URL aliases participate in new prefix rules before GC");
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 241, 8, &processed, &error)
                      == 0
                  && processed == 1,
            error.message);
        result = get(241);
        check(result.invalidate_ms == 30 && !result.delete_ms,
            "history renewal uses retained TAGs and persistent host aliases "
            "with namespace isolation");
        history_open(241);
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 321, 8, &processed, &error)
                      == 0
                  && processed == 1,
            error.message);
        get(321, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        while (index.collect_keys(1)) {
        }
        auto historical = history_result();
        check(historical.invalidate_ms == 30 && !historical.delete_ms,
            "history TAGs and host aliases remain in one snapshot while "
            "live GC releases their dictionaries");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
    munmap(shared, size);
}

static void
ngx_http_cache_purge_test_keep_pages()
{
    char path[] = "/tmp/ncp-keep-pages-XXXXXX";
    check(mkdtemp(path) != nullptr, "paged retention directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    options.cursor_lifetime_ms = 5000;
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
        "paged retention shared memory");
    {
        Index index(store);
        Operations operations(store);
        Coordinator coordinator(operations, shared, size);
        ready(coordinator);
        File file;
        file.key = "keep-key";
        file.url = "https://keep.test/item";
        file.host = "keep.test";
        file.tag_namespace = "tenant";
        file.tags = { "red" };
        file.root = "/cache";
        file.zone = "a";
        unsigned char main[16] = {}, variant[16] = {};
        ngx_http_cache_purge_index_expected_t expected[5]{};
        for (unsigned i = 0; i < 5; i++) {
            file.id[15] = i + 1;
            file.path = std::to_string(i);
            main[15] = i + 1;
            auto raw = ngx_http_cache_purge_native_file(file);
            ngx_http_cache_purge_index_match_t prepared{};
            check(ngx_http_cache_purge_index_prepare_native(
                      store, &raw, nullptr, main, variant, &prepared, &error)
                      == 0,
                error.message);
            index.commit(file.id, prepared.revision);
            expected[i] = ngx_http_cache_purge_native_expected(file.id, 2);
        }
        for (unsigned i = 0; i < 160; i++) {
            auto host = "alias-" + std::to_string(i) + ".test";
            check(ngx_http_cache_purge_index_observe(store,
                      ngx_http_cache_purge_native_bytes(file.key),
                      ngx_http_cache_purge_native_bytes("https://" + host),
                      ngx_http_cache_purge_native_bytes(host), &error)
                      == 0,
                error.message);
        }
        auto add = [&](const char *name, Selector selector, PurgeMode mode,
                       uint64_t issued) {
            auto op = coordinator.accept(
                { name, selector, mode, issued, "keep-pages" }, 100, 60000);
            ready(coordinator);
            operations.activate(op.input.id, op.revision, 100);
            coordinator.refresh();
            ready(coordinator);
        };
        auto keep = [&](unsigned n) {
            return ngx_http_cache_purge_coordinator_keep_file(
                coordinator.native(), &expected[n], 110, &error);
        };
        auto retained = [&](unsigned n, int code = 0) {
            main[15] = n + 1;
            ngx_http_cache_purge_fence_t value{};
            check(ngx_http_cache_purge_index_get_propagation(store,
                      ngx_http_cache_purge_native_bytes(file.key), main,
                      ngx_http_cache_purge_native_bytes(file.vary), variant,
                      110, &value, &error)
                      == code,
                error.message);
            return value;
        };
        auto advance = [&] {
            for (unsigned i = 0; i < 64; i++) {
                coordinator.step();
            }
        };
        add("red", { SelectorKind::namespace_tag, "red", "tenant" },
            PurgeMode::invalidate, 10);
        add("late-host", { SelectorKind::host_tag, "red", "late.test" },
            PurgeMode::remove, 30);
        check(keep(0) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "large retention scan yields instead of a fixed-alias failure");
        retained(0, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        coordinator.step();
        check(ngx_http_cache_purge_index_observe(store,
                  ngx_http_cache_purge_native_bytes(file.key),
                  ngx_http_cache_purge_native_bytes("https://late.test/item"),
                  ngx_http_cache_purge_native_bytes("late.test"), &error)
                  == 0,
            "observe a matching host during a retention scan");
        advance();
        retained(0, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        check(keep(0) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "changed alias stamp restarts without publishing a partial result");
        advance();
        check(keep(0) == 0 && retained(0).invalidate_ms == 10
                  && retained(0).delete_ms == 30,
            "restarted retention includes late host TAG associations");
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0
                  && !before.cursor_handles,
            "completed retention unpins snapshots");
        check(keep(0) == 0
                  && ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && before.writes == after.writes,
            "completed retention retries need no alias rescan or WAL write");

        add("new-red", { SelectorKind::namespace_tag, "red", "tenant" },
            PurgeMode::invalidate, 40);
        for (unsigned i = 0; i < 5; i++) {
            check(keep(i) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
                "four retention slots bound concurrently retained metadata");
        }
        check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && !after.cursor_handles && !after.active_cursors,
            "four pending retention scans release cursors between batches");
        advance();
        check(keep(4) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "a new file cannot evict an unconsumed completed retention scan");
        for (unsigned i = 0; i < 4; i++) {
            check(keep(i) == 0 && retained(i).invalidate_ms == 40,
                "round-robin coordinator work completes each pending file");
        }
        check(keep(4) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "a completed slot can admit another file");
        add("during-scan", { SelectorKind::namespace_tag, "red", "tenant" },
            PurgeMode::remove, 50);
        coordinator.step();
        check(keep(4) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "new accepted controls invalidate pending retention work");
        advance();
        check(keep(4) == 0 && retained(4).delete_ms == 50,
            "restarted retention uses the new published controls");
        check(ngx_http_cache_purge_index_observe(store,
                  ngx_http_cache_purge_native_bytes(file.key),
                  ngx_http_cache_purge_native_bytes("https://another.test"),
                  ngx_http_cache_purge_native_bytes("another.test"), &error)
                  == 0,
            "change aliases after a completed retention result");
        check(keep(4) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "completed memo checks current aliases before reuse");
        auto changed = index.update_metadata(file.id, 2, "changed", { "blue" });
        check(keep(4) == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "completed memo cannot authorize a changed file revision");
        (void) changed;
        check(keep(0) == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "leave a pending retention scan for coordinator shutdown");
    }
    ngx_http_cache_rocksdb_stats_t stats{};
    check(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0
              && !stats.cursor_handles && !stats.active_cursors,
        "coordinator shutdown releases active retention queries");
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
    munmap(shared, size);
}

static void
ngx_http_cache_purge_test_expiry_pages()
{
    char path[] = "/tmp/ncp-expiry-pages-XXXXXX";
    check(mkdtemp(path) != nullptr, "paged expiry directory");
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
        "paged expiry shared memory");
    {
        Index index(store);
        Operations operations(store);
        Coordinator coordinator(operations, shared, size);
        ready(coordinator);
        File file;
        file.key = "expiry-key";
        file.url = "https://expiry.test/item";
        file.host = "expiry.test";
        file.tag_namespace = "tenant";
        file.root = "/cache";
        file.zone = "a";
        unsigned char main[16] = {}, variant[16] = {};
        for (unsigned i = 1; i <= 2; i++) {
            file.id[15] = i;
            file.path = std::to_string(i);
            file.tags = { i == 1 ? "red" : "blue" };
            main[15] = i;
            auto raw = ngx_http_cache_purge_native_file(file);
            ngx_http_cache_purge_index_match_t prepared{};
            check(ngx_http_cache_purge_index_prepare_native(
                      store, &raw, nullptr, main, variant, &prepared, &error)
                      == 0,
                error.message);
            index.commit(file.id, prepared.revision);
            auto expected = ngx_http_cache_purge_native_expected(file.id, 2);
            ngx_http_cache_purge_retention_t times{ 10, 200, 0, 0 };
            check(ngx_http_cache_purge_index_keep_propagation(
                      store, &expected, main, variant, 100, &times, &error)
                      == 0,
                error.message);
        }
        for (unsigned i = 0; i < 96; i++) {
            auto host = "expiry-alias-" + std::to_string(i) + ".test";
            check(ngx_http_cache_purge_index_observe(store,
                      ngx_http_cache_purge_native_bytes(file.key),
                      ngx_http_cache_purge_native_bytes("https://" + host),
                      ngx_http_cache_purge_native_bytes(host), &error)
                      == 0,
                error.message);
        }
        for (unsigned i = 1; i <= 2; i++) {
            file.id[15] = i;
            auto retired = index.retire(file.id, 2);
            index.collect_file(file.id, retired.revision);
        }
        auto op = coordinator.accept(
            { "host-red",
                { SelectorKind::host_tag, "red", "expiry-alias-95.test" },
                PurgeMode::remove, 20, "expiry-pages" },
            150, 1000);
        ready(coordinator);
        operations.activate(op.input.id, op.revision, 150);
        coordinator.refresh();
        ready(coordinator);
        auto get = [&](unsigned n, int code = 0) {
            main[15] = n;
            ngx_http_cache_purge_fence_t value{};
            check(ngx_http_cache_purge_index_get_propagation(store,
                      ngx_http_cache_purge_native_bytes(file.key), main,
                      ngx_http_cache_purge_native_bytes(file.vary), variant,
                      201, &value, &error)
                      == code,
                error.message);
            return value;
        };
        size_t processed = 99;
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 201, 8, &processed, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_BUSY
                  && !processed,
            "large retained identity starts bounded expiry work");
        ngx_http_cache_rocksdb_stats_t stats{};
        check(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0
                  && !stats.cursor_handles,
            "pending expiry retains no database cursor between steps");
        file.id[15] = 3;
        file.path = "new-alias-owner";
        file.url = "https://expiry-new.test/item";
        file.host = "expiry-new.test";
        main[15] = 3;
        auto raw = ngx_http_cache_purge_native_file(file);
        ngx_http_cache_purge_index_match_t prepared{};
        check(ngx_http_cache_purge_index_prepare_native(
                  store, &raw, nullptr, main, variant, &prepared, &error)
                  == 0,
            error.message);
        index.commit(file.id, prepared.revision);
        for (unsigned i = 0; i < 16; i++) {
            coordinator.step();
        }
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 201, 8, &processed, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_BUSY
                  && !processed,
            "new alias from a concurrent fill restarts history expiry");
        auto retired_new = index.retire(file.id, 2);
        index.collect_file(file.id, retired_new.revision);
        for (unsigned i = 0; i < 16; i++) {
            coordinator.step();
        }
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 201, 8, &processed, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_BUSY
                  && processed == 1,
            "completed first membership commits before the next scan yields");
        check(get(1).delete_ms == 20,
            "renewal matches a host TAG beyond the old alias count limit");
        check(!get(2).invalidate_ms && !get(2).delete_ms,
            "pending second membership remains indexed until checked");
        for (unsigned i = 0; i < 16; i++) {
            coordinator.step();
        }
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 201, 8, &processed, &error)
                      == 0
                  && processed == 1,
            "second membership with no live match is collected independently");
        get(2, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 1151, 8, &processed, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_BUSY
                  && !processed,
            "renewed membership retains its original next deadline");
        for (unsigned i = 0; i < 16; i++) {
            coordinator.step();
        }
        check(ngx_http_cache_purge_coordinator_expire_propagation(
                  coordinator.native(), 1151, 8, &processed, &error)
                      == 0
                  && processed == 1,
            "expired later operation does not reopen a propagation window");
        get(1, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        while (index.collect_keys(32)) {
        }
        check(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0
                  && !stats.cursor_handles && !stats.active_cursors,
            "completed expiry releases its retained input and snapshots");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
    munmap(shared, size);
}

int
main(int argc, char **argv)
{
    try {
        if (argc == 1) {
            ngx_http_cache_purge_test_propagation_policy();
            ngx_http_cache_purge_test_object_propagation();
            ngx_http_cache_purge_test_retention();
            ngx_http_cache_purge_test_keep_pages();
            ngx_http_cache_purge_test_expiry_pages();
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
