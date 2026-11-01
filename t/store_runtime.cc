
/*
 * Copyright (C) Hanada
 */


#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

extern "C" int ngx_http_cache_purge_store_c_probe(
    ngx_http_cache_rocksdb_t *store);
extern "C" unsigned ngx_http_cache_purge_test_sync_count(void);
extern "C" void ngx_http_cache_purge_test_sync_fail(unsigned fail);
extern "C" void ngx_http_cache_purge_test_direct_fail(unsigned mode);
extern "C" unsigned ngx_http_cache_purge_test_direct_count(void);
extern "C" unsigned ngx_http_cache_purge_test_direct_supported(void);
static void ngx_http_cache_purge_test_require(bool ok, const char *message);
#if (defined(__linux__))
static void ngx_http_cache_purge_test_background_priority(int inherited);
#endif
static void ngx_http_cache_purge_test_expect(int result, int wanted,
    const char *operation);
static ngx_http_cache_rocksdb_bytes_t ngx_http_cache_purge_test_bytes(
    const std::string &value);
static ngx_http_cache_rocksdb_mutation_t ngx_http_cache_purge_test_put(
    ngx_http_cache_rocksdb_cf_e cf, const std::string &key,
    const std::string &value);
static void ngx_http_cache_purge_test_write(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, const std::string &key,
    const std::string &value);
static std::string ngx_http_cache_purge_test_get(
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
    const std::string &key, ngx_http_cache_rocksdb_cursor_t *cursor = nullptr);
static void ngx_http_cache_purge_test_missing(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, const std::string &key);
static std::vector<std::string> ngx_http_cache_purge_test_scan(
    ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
    const std::string &prefix, const std::string &after = "",
    bool cached = false);
static ngx_http_cache_rocksdb_t *ngx_http_cache_purge_test_open(
    const char *path, const ngx_http_cache_rocksdb_options_t &options);
static void ngx_http_cache_purge_test_close(ngx_http_cache_rocksdb_t *&store);
static void ngx_http_cache_purge_test_options();
static void ngx_http_cache_purge_test_direct_io();
static void ngx_http_cache_purge_test_reader(ngx_http_cache_rocksdb_t *store);
static void ngx_http_cache_purge_test_cached_cursor(
    ngx_http_cache_rocksdb_t *store);
static void ngx_http_cache_purge_test_deferred();
static void ngx_http_cache_purge_test_schema_change(const char *path,
    const std::string &value);


static unsigned                        ngx_http_cache_purge_test_checks;
static ngx_http_cache_rocksdb_error_t  ngx_http_cache_purge_test_error;

static void
ngx_http_cache_purge_test_require(bool ok, const char *message)
{
    if (!ok) {
        throw std::runtime_error(message);
    }

    ++ngx_http_cache_purge_test_checks;
}


#if (defined(__linux__))
static void
ngx_http_cache_purge_test_background_priority(int inherited)
{
    for (auto pool : { rocksdb::Env::LOW, rocksdb::Env::HIGH }) {
        auto  *result = new std::promise<int>;
        auto   future = result->get_future();

        rocksdb::Env::Default()->Schedule(
            [](void *data) {
                std::unique_ptr<std::promise<int>> result(
                    static_cast<std::promise<int> *>(data));

                result->set_value(getpriority(PRIO_PROCESS, 0));
            },
            result, pool);
        ngx_http_cache_purge_test_require(
            future.get() == (pool == rocksdb::Env::LOW ? 19 : inherited),
            "compaction yields CPU while flushes keep the owner priority");
    }

    ngx_http_cache_purge_test_require(
        getpriority(PRIO_PROCESS, 0) == inherited,
        "RocksDB scheduling leaves the caller priority unchanged");
}


#endif

static void
ngx_http_cache_purge_test_expect(int result, int wanted, const char *operation)
{
    if (result != wanted) {
        throw std::runtime_error(std::string(operation)
                                 + ": result=" + std::to_string(result)
                                 + " expected=" + std::to_string(wanted) + " "
                                 + ngx_http_cache_purge_test_error.message);
    }

    ++ngx_http_cache_purge_test_checks;
}


static ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_test_bytes(const std::string &value)
{
    return { reinterpret_cast<const unsigned char *>(value.data()),
             value.size() };
}


static ngx_http_cache_rocksdb_mutation_t
ngx_http_cache_purge_test_put(ngx_http_cache_rocksdb_cf_e cf,
    const std::string &key, const std::string &value)
{
    return { cf, NGX_HTTP_CACHE_ROCKSDB_PUT,
             ngx_http_cache_purge_test_bytes(key),
             ngx_http_cache_purge_test_bytes(value) };
}


static void
ngx_http_cache_purge_test_write(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, const std::string &key,
    const std::string &value)
{
    auto  mutation = ngx_http_cache_purge_test_put(cf, key, value);

    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_write(store, &mutation, 1, nullptr, 0,
                                     &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "write");
}


static std::string
ngx_http_cache_purge_test_get(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, const std::string &key,
    ngx_http_cache_rocksdb_cursor_t *cursor)
{
    std::array<unsigned char, 4096>  data{};
    ngx_http_cache_rocksdb_buffer_t  buffer{ data.data(), data.size(), 0 };
    int                              rc =
        cursor ? ngx_http_cache_rocksdb_cursor_get(
                     cursor, cf, ngx_http_cache_purge_test_bytes(key),
                     &buffer, &ngx_http_cache_purge_test_error)
               : ngx_http_cache_rocksdb_get(
                     store, cf, ngx_http_cache_purge_test_bytes(key),
                     &buffer, &ngx_http_cache_purge_test_error);

    ngx_http_cache_purge_test_expect(rc, NGX_HTTP_CACHE_ROCKSDB_OK, "get");

    return std::string(reinterpret_cast<char *>(data.data()), buffer.length);
}


static void
ngx_http_cache_purge_test_missing(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, const std::string &key)
{
    ngx_http_cache_rocksdb_buffer_t  buffer{ nullptr, 0, 0 };

    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_get(store, cf,
                                   ngx_http_cache_purge_test_bytes(key),
                                   &buffer, &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "missing");
}


static std::vector<std::string>
ngx_http_cache_purge_test_scan(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_rocksdb_cf_e cf, const std::string &prefix,
    const std::string &after, bool cached)
{
    ngx_http_cache_rocksdb_cursor_t  *cursor = nullptr;
    auto                              open_cursor =
        cached ? ngx_http_cache_rocksdb_cursor_open_cached
               : ngx_http_cache_rocksdb_cursor_open;

    ngx_http_cache_purge_test_expect(
        open_cursor(store, cf, ngx_http_cache_purge_test_bytes(prefix),
                    ngx_http_cache_purge_test_bytes(after), &cursor,
                    &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "open cursor");
    std::vector<unsigned char>       key_data(65536), value_data(4096);
    ngx_http_cache_rocksdb_buffer_t  key{ key_data.data(), key_data.size(), 0 };
    ngx_http_cache_rocksdb_buffer_t  value{ value_data.data(),
                                            value_data.size(), 0 };
    std::vector<std::string>         keys;

    for (;;) {
        int  rc = ngx_http_cache_rocksdb_cursor_next(
            cursor, &key, &value, &ngx_http_cache_purge_test_error);

        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            break;
        }

        ngx_http_cache_purge_test_expect(rc, NGX_HTTP_CACHE_ROCKSDB_OK,
                                         "next cursor");
        keys.emplace_back(reinterpret_cast<char *>(key.data), key.length);
    }

    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_close(&cursor,
                                            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "close cursor");
    ngx_http_cache_purge_test_require(cursor == nullptr,
                                      "cursor handle consumed");

    return keys;
}


static ngx_http_cache_rocksdb_t *
ngx_http_cache_purge_test_open(const char *path,
    const ngx_http_cache_rocksdb_options_t &options)
{
    ngx_http_cache_rocksdb_t  *store = nullptr;

    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_open(path, &options, &store,
                                    &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "open");

    return store;
}


static void
ngx_http_cache_purge_test_close(ngx_http_cache_rocksdb_t *&store)
{
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_close(&store, &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "close");
    ngx_http_cache_purge_test_require(store == nullptr,
                                      "store handle consumed");
}


static void
ngx_http_cache_purge_test_options()
{
    ngx_http_cache_rocksdb_options_t  options;

    ngx_http_cache_rocksdb_init_options(&options);
    ngx_http_cache_rocksdb_t  *store = nullptr;
    options.abi = NGX_HTTP_CACHE_ROCKSDB_ABI - 1;
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_open("/unused", &options, &store,
                                    &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_INVALID, "reject an unsupported options ABI");
    options.abi = NGX_HTTP_CACHE_ROCKSDB_ABI;

    for (uint64_t rate : { uint64_t(1), uint64_t(1023), UINT64_MAX }) {
        options.io_rate = rate;
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_open("/unused", &options, &store,
                                        &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "reject invalid I/O rate");
        ngx_http_cache_purge_test_require(
            store == nullptr, "invalid options do not create a store");
    }

    char  path[] = "/tmp/cache-purge-unlimited-XXXXXX";
    ngx_http_cache_purge_test_require(mkdtemp(path) != nullptr,
                                      "create unlimited I/O fixture");
    options.io_rate = 0;
    store = ngx_http_cache_purge_test_open(path, options);
    ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                    "rate", "unlimited");
    ngx_http_cache_purge_test_close(store);
    ngx_http_cache_rocksdb_init_options(&options);
    store = ngx_http_cache_purge_test_open(path, options);
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                      "rate")
        == "unlimited",
        "I/O rate changes preserve the existing database");
    ngx_http_cache_purge_test_close(store);
}


static void
ngx_http_cache_purge_test_direct_io()
{
    char  path[] = "/tmp/cache-purge-direct-XXXXXX";

    ngx_http_cache_purge_test_require(mkdtemp(path) != nullptr,
                                      "create direct I/O fixture");
    auto                              configuration = [&]() {
        std::filesystem::path  latest;

        for (auto &entry : std::filesystem::directory_iterator(path)) {
            auto  name = entry.path().filename().string();

            if (name.rfind("OPTIONS-", 0) == 0 && entry.path() > latest) {
                latest = entry.path();
            }
        }

        ngx_http_cache_purge_test_require(!latest.empty(),
                                          "database options were persisted");
        std::ifstream input(latest);
        ngx_http_cache_purge_test_require(input.good(),
                                          "read actual database options");

        return std::string(std::istreambuf_iterator<char>(input),
                           std::istreambuf_iterator<char>());
    };
    ngx_http_cache_rocksdb_options_t  options;
    ngx_http_cache_rocksdb_init_options(&options);
    ngx_http_cache_purge_test_direct_fail(0);
    auto  *store = ngx_http_cache_purge_test_open(path, options);
    bool   supported = ngx_http_cache_purge_test_direct_supported() != 0;
    auto   normal = configuration();
    ngx_http_cache_purge_test_require(normal.find("use_direct_reads=false")
                                      != std::string::npos,
                                      "point reads keep buffered I/O");
    ngx_http_cache_purge_test_require(
        normal.find(supported ? "use_direct_io_for_flush_and_compaction=true" :
                                "use_direct_io_for_flush_and_compaction=false")
        != std::string::npos,
        "background I/O follows the filesystem probe");
    ngx_http_cache_purge_test_close(store);

    for (unsigned  mode = 1; mode <= (supported ? 3U : 1U); mode++) {
        ngx_http_cache_purge_test_direct_fail(mode);
        store = ngx_http_cache_purge_test_open(path, options);
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_direct_count() == 1,
            "probe failure was injected before database work");
        ngx_http_cache_purge_test_direct_fail(0);
        ngx_http_cache_purge_test_require(
            configuration().find(
                "use_direct_io_for_flush_and_compaction=false")
            != std::string::npos,
            "unsupported probe falls back without failing the database");
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                        "io/recover", std::to_string(mode));
        ngx_http_cache_purge_test_close(store);
        store = ngx_http_cache_purge_test_open(path, options);
        ngx_http_cache_purge_test_require(
            configuration().find(
                supported ? "use_direct_io_for_flush_and_compaction=true" :
                            "use_direct_io_for_flush_and_compaction=false")
            != std::string::npos,
            "a failed probe does not disable later supported opens");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(
                store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "io/recover")
            == std::to_string(mode),
            "fallback writes survive reopening with the normal I/O mode");
        ngx_http_cache_purge_test_close(store);
    }

    std::cout << "direct I/O probe supported=" << supported << std::endl;
}


static void
ngx_http_cache_purge_test_reader(ngx_http_cache_rocksdb_t *store)
{
    ngx_http_cache_rocksdb_t  *reader = nullptr, *extra = nullptr;

    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_reader_create(store, 3, &reader,
                                             &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_INVALID, "reader cannot exceed writer budget");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_reader_create(store, 2, &reader,
                                             &ngx_http_cache_purge_test_error),
        0, "create read handle");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_reader_create(store, 1, &extra,
                                             &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_BUSY, "only one read handle");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_close(&store, &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_BUSY, "unattached reader pins DB lifetime");
    ngx_http_cache_rocksdb_buffer_t  buffer{ nullptr, 0, 0 };
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_get(
            reader, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_test_bytes("reader/key"), &buffer,
            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OWNER, "unattached reader cannot read");
    auto  child = fork();
    ngx_http_cache_purge_test_require(child >= 0,
                                      "fork reader ownership check");

    if (child == 0) {
        _exit(ngx_http_cache_rocksdb_reader_attach(
                  reader, &ngx_http_cache_purge_test_error)
              == NGX_HTTP_CACHE_ROCKSDB_OWNER ?
                  0 :
                  1);
    }

    int  status = 0;
    ngx_http_cache_purge_test_require(waitpid(child, &status, 0) == child
                                      && WIFEXITED(status)
                                      && WEXITSTATUS(status) == 0,
                                      "reader rejects fork handoff");
    ngx_http_cache_purge_test_close(reader);
    ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                    "reader/key", "old");
    ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_TAG,
                                    "reader/key", "old");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_reader_create(store, 2, &reader,
                                             &ngx_http_cache_purge_test_error),
        0, "recreate reader after cancelled handoff");
    std::promise<void>  ready, changed;
    auto                resume = changed.get_future();
    auto                worker = std::async(std::launch::async, [&] {
        ngx_http_cache_rocksdb_error_t    local{};
        ngx_http_cache_rocksdb_cursor_t  *cursor = nullptr, *forked = nullptr,
                                         *overflow = nullptr;
        std::vector<int>                  results;

        results.push_back(
            ngx_http_cache_rocksdb_reader_attach(reader, &local));
        results.push_back(ngx_http_cache_rocksdb_reader_attach(reader, &local)
                          == NGX_HTTP_CACHE_ROCKSDB_INVALID ?
                              0 :
                              -1);
        results.push_back(ngx_http_cache_rocksdb_cursor_open(
            reader, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_test_bytes("reader/"),
            ngx_http_cache_purge_test_bytes(""), &cursor, &local));
        results.push_back(ngx_http_cache_rocksdb_cursor_fork(
            cursor, NGX_HTTP_CACHE_ROCKSDB_TAG,
            ngx_http_cache_purge_test_bytes("reader/"),
            ngx_http_cache_purge_test_bytes(""), &forked, &local));
        results.push_back(ngx_http_cache_rocksdb_cursor_open(
                              reader, NGX_HTTP_CACHE_ROCKSDB_URL,
                              ngx_http_cache_purge_test_bytes("reader/"),
                              ngx_http_cache_purge_test_bytes(""), &overflow,
                              &local)
                          == NGX_HTTP_CACHE_ROCKSDB_LIMIT ?
                              0 :
                              -1);
        ready.set_value();
        resume.wait();
        unsigned char                    raw[16];
        ngx_http_cache_rocksdb_buffer_t  value{ raw, sizeof(raw), 0 };
        results.push_back(ngx_http_cache_rocksdb_cursor_get(
            forked, NGX_HTTP_CACHE_ROCKSDB_TAG,
            ngx_http_cache_purge_test_bytes("reader/key"), &value, &local));
        results.push_back(
            value.length == 3 && !std::memcmp(raw, "old", 3) ? 0 : -1);
        results.push_back(ngx_http_cache_rocksdb_get(
            reader, NGX_HTTP_CACHE_ROCKSDB_TAG,
            ngx_http_cache_purge_test_bytes("reader/key"), &value, &local));
        results.push_back(
            value.length == 3 && !std::memcmp(raw, "new", 3) ? 0 : -1);
        const std::string  key = "reader/key", forbidden = "forbidden";
        auto               mutation = ngx_http_cache_purge_test_put(
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, forbidden);
        results.push_back(ngx_http_cache_rocksdb_write(reader, &mutation, 1,
                                                       nullptr, 0, &local)
                          == NGX_HTTP_CACHE_ROCKSDB_INVALID ?
                              0 :
                              -1);
        results.push_back(ngx_http_cache_rocksdb_write_deferred(
                              reader, &mutation, 1, nullptr, 0, &local)
                          == NGX_HTTP_CACHE_ROCKSDB_INVALID ?
                              0 :
                              -1);
        results.push_back(ngx_http_cache_rocksdb_sync(reader, &local)
                          == NGX_HTTP_CACHE_ROCKSDB_INVALID ?
                              0 :
                              -1);
        uint64_t  written = 1, synced = 1;
        results.push_back(ngx_http_cache_rocksdb_durability(reader, &written,
                                                            &synced, &local)
                          == NGX_HTTP_CACHE_ROCKSDB_INVALID
                          && !written && !synced ?
                              0 :
                              -1);
        ngx_http_cache_rocksdb_t  *nested = nullptr;
        results.push_back(
            ngx_http_cache_rocksdb_reader_create(reader, 1, &nested, &local)
            == NGX_HTTP_CACHE_ROCKSDB_INVALID ?
                0 :
                -1);
        results.push_back(ngx_http_cache_rocksdb_close(&reader, &local)
                          == NGX_HTTP_CACHE_ROCKSDB_BUSY ?
                              0 :
                              -1);
        results.push_back(
            ngx_http_cache_rocksdb_cursor_close(&cursor, &local));
        results.push_back(
            ngx_http_cache_rocksdb_cursor_close(&forked, &local));
        results.push_back(ngx_http_cache_rocksdb_close(&reader, &local));

        return results;
    });
    ready.get_future().wait();
    auto                                              blocked =
        ngx_http_cache_rocksdb_close(&store, &ngx_http_cache_purge_test_error);
    auto                                              wrong_thread =
        ngx_http_cache_rocksdb_get(
        reader, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
        ngx_http_cache_purge_test_bytes("reader/key"), &buffer,
        &ngx_http_cache_purge_test_error);
    const std::string                                 key = "reader/key",
                                                      replacement = "new";
    std::array<ngx_http_cache_rocksdb_mutation_t, 2>  batch{
        ngx_http_cache_purge_test_put(NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key,
                                      replacement),
        ngx_http_cache_purge_test_put(NGX_HTTP_CACHE_ROCKSDB_TAG, key,
                                      replacement)
    };
    auto                                              updated =
        ngx_http_cache_rocksdb_write(
        store, batch.data(), batch.size(), nullptr, 0,
        &ngx_http_cache_purge_test_error);
    changed.set_value();
    auto  results = worker.get();
    ngx_http_cache_purge_test_expect(blocked, NGX_HTTP_CACHE_ROCKSDB_BUSY,
                                     "reader pins writer lifetime");
    ngx_http_cache_purge_test_expect(
        wrong_thread, NGX_HTTP_CACHE_ROCKSDB_OWNER, "reader owner enforced");
    ngx_http_cache_purge_test_expect(
        updated, 0, "writer updates while read snapshots are open");
    ngx_http_cache_purge_test_require(
        std::all_of(results.begin(), results.end(),
                    [](int rc) { return rc == 0; }),
        "read-only handoff, bounds, shared snapshots and mutation rejection");
    ngx_http_cache_purge_test_require(!reader,
                                      "reader closed on its owner thread");
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                      key)
        == replacement,
        "rejected reader write leaves the writer's value intact");
}


static void
ngx_http_cache_purge_test_cached_cursor(ngx_http_cache_rocksdb_t *store)
{
    const std::string  prefix("cached\0", 7),
                       first = prefix + "a", last = prefix + "b",
                       outside = "cached!";

    ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL, first,
                                    "first");
    ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL, last,
                                    "old");
    ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL, outside,
                                    "outside");
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_scan(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                       prefix, first, true)
        == std::vector<std::string>{ last },
        "cached binary prefix with exclusive resume");
    ngx_http_cache_rocksdb_cursor_t  *parent = nullptr, *child = nullptr,
                                     *extra = nullptr;
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_open_cached(
            store, NGX_HTTP_CACHE_ROCKSDB_URL,
            ngx_http_cache_purge_test_bytes(prefix),
            ngx_http_cache_purge_test_bytes(outside), &parent,
            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_INVALID, "cached resume rejects another range");
    ngx_http_cache_purge_test_require(!parent,
                                      "invalid cached range leaves no handle");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_open_cached(
            store, NGX_HTTP_CACHE_ROCKSDB_URL,
            ngx_http_cache_purge_test_bytes(prefix),
            ngx_http_cache_purge_test_bytes(first), &parent,
            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "cached snapshot");
    ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL, last,
                                    "new");
    ngx_http_cache_rocksdb_buffer_t  key{ nullptr, 0, 0 },
                                     value{ nullptr, 0, 0 };
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_next(parent, &key, &value,
                                           &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_BUFFER, "cached short buffer");
    ngx_http_cache_purge_test_require(key.length == last.size()
                                      && value.length == 3,
                                      "cached required buffer sizes");
    std::array<unsigned char, 64>  k{}, v{};
    key = { k.data(), k.size(), 0 };
    value = { v.data(), v.size(), 0 };
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_next(parent, &key, &value,
                                           &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "cached retry does not consume row");
    ngx_http_cache_purge_test_require(
        std::string(reinterpret_cast<char *>(key.data), key.length) == last
        && std::string(reinterpret_cast<char *>(value.data), value.length)
           == "old",
        "cached read preserves pre-write snapshot");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_next(parent, &key, &value,
                                           &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_DONE, "cached upper bound excludes neighbor");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_fork(
            parent, NGX_HTTP_CACHE_ROCKSDB_URL,
            ngx_http_cache_purge_test_bytes(prefix),
            ngx_http_cache_purge_test_bytes(""), &child,
            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "bulk child from cached parent");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_open_cached(
            store, NGX_HTTP_CACHE_ROCKSDB_URL,
            ngx_http_cache_purge_test_bytes(prefix),
            ngx_http_cache_purge_test_bytes(""), &extra,
            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_LIMIT,
        "cached cursors share the handle budget");
    ngx_http_cache_purge_test_require(
        !extra, "saturated cached open leaves no handle");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_close(&parent,
                                            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "close cached parent");
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_get(store, NGX_HTTP_CACHE_ROCKSDB_URL, last,
                                      child)
        == "old",
        "bulk child retains cached parent's snapshot");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_cursor_close(&child,
                                            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "close cached child");
}


static void
ngx_http_cache_purge_test_deferred()
{
    char  path[] = "/tmp/cache-purge-deferred-XXXXXX";

    ngx_http_cache_purge_test_require(mkdtemp(path) != nullptr,
                                      "deferred test directory");
    ngx_http_cache_rocksdb_options_t  options;
    ngx_http_cache_rocksdb_init_options(&options);
    auto              *store = ngx_http_cache_purge_test_open(path, options);
    const std::string  key = "deferred", value = "cache data";
    auto               checkpoint = [&](uint64_t written, uint64_t synced) {
        uint64_t  actual_written = UINT64_MAX, actual_synced = UINT64_MAX;
        auto      syncs = ngx_http_cache_purge_test_sync_count();

        ngx_http_cache_purge_test_expect(ngx_http_cache_rocksdb_durability(
                                             store, &actual_written,
                                             &actual_synced,
                                             &ngx_http_cache_purge_test_error),
                                         0, "read batch durability tickets");
        ngx_http_cache_purge_test_require(
            actual_written == written && actual_synced == synced,
            "durability tickets distinguish issued and synchronized batches");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_sync_count() == syncs,
            "reading a durability ticket never synchronizes the WAL");
    };
    checkpoint(0, 0);
    uint64_t  invalid_written = 99, invalid_synced = 99;
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_durability(store, &invalid_written,
                                          &invalid_written,
                                          &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_INVALID, "aliased durability outputs rejected");
    ngx_http_cache_purge_test_require(invalid_written == 0,
                                      "invalid durability output is cleared");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_durability(store, nullptr, &invalid_synced,
                                          &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing durability output rejected");
    ngx_http_cache_purge_test_require(invalid_synced == 0,
                                      "other durability output is cleared");
    int          foreign = 0;
    std::thread  other([&] {
        ngx_http_cache_rocksdb_error_t  local{};

        invalid_written = invalid_synced = 99;
        foreign = ngx_http_cache_rocksdb_durability(store, &invalid_written,
                                                    &invalid_synced, &local);
    });
    other.join();
    ngx_http_cache_purge_test_require(
        foreign == NGX_HTTP_CACHE_ROCKSDB_OWNER && invalid_written == 0
        && invalid_synced == 0,
        "durability snapshots preserve executor ownership and clear errors");
    std::array<ngx_http_cache_rocksdb_mutation_t, 3>  batch;

    for (unsigned  i = 0; i < batch.size(); i++) {
        batch[i] = ngx_http_cache_purge_test_put(
            static_cast<ngx_http_cache_rocksdb_cf_e>(i), key, value);
    }

    auto  before = ngx_http_cache_purge_test_sync_count();
    ngx_http_cache_purge_test_require(
        before != 0, "WAL sync interception observes initialization");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_write_deferred(
            store, batch.data(), batch.size(), nullptr, 0,
            &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "defer cross-CF durability");
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_sync_count() == before,
        "deferred write does not sync WAL immediately");
    checkpoint(1, 0);

    for (unsigned  i = 0; i < batch.size(); i++) {
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(
                store, static_cast<ngx_http_cache_rocksdb_cf_e>(i), key)
            == value,
            "deferred cross-CF rows are immediately queryable");
    }

    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_sync(store, &ngx_http_cache_purge_test_error),
        0, "flush deferred WAL");
    ngx_http_cache_purge_test_require(ngx_http_cache_purge_test_sync_count()
                                      == before + 1,
                                      "one barrier syncs the deferred batch");
    checkpoint(1, 1);
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_sync(store, &ngx_http_cache_purge_test_error),
        0, "idle WAL barrier");
    ngx_http_cache_purge_test_require(ngx_http_cache_purge_test_sync_count()
                                      == before + 1,
                                      "idle barriers do not issue sync calls");
    checkpoint(1, 1);
    before = ngx_http_cache_purge_test_sync_count();

    for (unsigned  i = 0; i < 64; i++) {
        ngx_http_cache_purge_test_expect(ngx_http_cache_rocksdb_write_deferred(
                                             store, batch.data(), batch.size(),
                                             nullptr, 0,
                                             &ngx_http_cache_purge_test_error),
                                         0, "bounded WAL group");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_sync_count() == before + (i == 63),
            "the sixty-fourth deferred write forces a barrier");
        checkpoint(i + 2, i == 63 ? 65 : 1);
    }

    before = ngx_http_cache_purge_test_sync_count();
    ngx_http_cache_purge_test_expect(ngx_http_cache_rocksdb_write_deferred(
                                         store, batch.data(), batch.size(),
                                         nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
                                     0, "cache before control");
    checkpoint(66, 65);
    ngx_http_cache_rocksdb_condition_t  absent{
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, ngx_http_cache_purge_test_bytes(key),
        ngx_http_cache_purge_test_bytes(""), 0
    };
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_write(store, batch.data(), batch.size(),
                                     &absent, 1,
                                     &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "rejected write has no ticket");
    checkpoint(66, 65);
    ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                    "control", "durable");
    checkpoint(67, 67);
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_sync_count() == before + 1,
        "ordinary control write still syncs WAL immediately");
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_sync(store, &ngx_http_cache_purge_test_error),
        0, "control barrier");
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_sync_count() == before + 1,
        "control durability also covers preceding cache batches");
    ngx_http_cache_purge_test_expect(ngx_http_cache_rocksdb_write_deferred(
                                         store, batch.data(), batch.size(),
                                         nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
                                     0, "defer before close");
    checkpoint(68, 67);
    before = ngx_http_cache_purge_test_sync_count();
    ngx_http_cache_purge_test_close(store);
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_sync_count() > before,
        "graceful close syncs outstanding cache batches");
    store = ngx_http_cache_purge_test_open(path, options);
    checkpoint(0, 0);
    ngx_http_cache_purge_test_require(
        ngx_http_cache_purge_test_get(store, NGX_HTTP_CACHE_ROCKSDB_TAG, key)
        == value,
        "closed deferred batch survives reopening");
    ngx_http_cache_purge_test_expect(ngx_http_cache_rocksdb_write_deferred(
                                         store, batch.data(), batch.size(),
                                         nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
                                     0, "defer before sync failure");
    checkpoint(1, 0);
    ngx_http_cache_purge_test_sync_fail(1);
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_sync(store, &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_IO, "WAL synchronization failure is reported");
    checkpoint(1, 0);
    ngx_http_cache_purge_test_sync_fail(0);
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_sync(store, &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_IO,
        "a failed WAL cannot silently clear its pending durability");
    checkpoint(1, 0);
    ngx_http_cache_purge_test_expect(
        ngx_http_cache_rocksdb_close(&store, &ngx_http_cache_purge_test_error),
        NGX_HTTP_CACHE_ROCKSDB_IO,
        "close reports the failed durability barrier");
    ngx_http_cache_purge_test_require(store == nullptr,
                                      "failed close still consumes the store");
}


static void
ngx_http_cache_purge_test_schema_change(const char *path,
    const std::string &value)
{
    rocksdb::Options                              options;
    std::vector<rocksdb::ColumnFamilyDescriptor>  descriptions;

    for (const char *name : { "default", "url", "tag" }) {
        descriptions.emplace_back(name, options);
    }

    std::vector<rocksdb::ColumnFamilyHandle *>  handles;
    rocksdb::DB                                *db = nullptr;
    ngx_http_cache_purge_test_require(
        rocksdb::DB::Open(options, path, descriptions, &handles, &db).ok(),
        "raw open");
    const char             marker[] = "\0ngx_http_cache_purge/store";
    rocksdb::WriteOptions  write;
    write.sync = true;
    ngx_http_cache_purge_test_require(
        db->Put(write, handles[0], rocksdb::Slice(marker, sizeof(marker) - 1),
                value)
            .ok(),
        "raw schema put");

    for (auto *handle : handles) {
        db->DestroyColumnFamilyHandle(handle);
    }

    delete db;
}


int
main(int argc, char **argv)
{
    try {
        ngx_http_cache_rocksdb_options_t  options;

        ngx_http_cache_rocksdb_init_options(&options);

        if (argc == 3) {
            const std::string  mode = argv[1];

            if (mode == "--locked") {
                ngx_http_cache_rocksdb_t  *store = nullptr;

                ngx_http_cache_purge_test_expect(
                    ngx_http_cache_rocksdb_open(
                        argv[2], &options, &store,
                        &ngx_http_cache_purge_test_error),
                    NGX_HTTP_CACHE_ROCKSDB_BUSY, "owner lock");
                ngx_http_cache_purge_test_require(
                    !store, "busy open must not return handle");

                return 0;
            }

            auto  *store = ngx_http_cache_purge_test_open(argv[2], options);

            if (mode == "--hold") {
                std::cout << "READY" << std::endl;
                std::getchar();
            } else if (mode == "--crash-write" || mode == "--deferred-write"
                       || mode == "--deferred-barrier")
            {
                const std::string                                 key =
                    "Z:committed",
                                                                  value =
                    "cross-CF durable";
                std::array<ngx_http_cache_rocksdb_mutation_t, 3>  entries;

                for (unsigned  i = 0; i != entries.size(); ++i) {
                    entries[i] = ngx_http_cache_purge_test_put(
                        static_cast<ngx_http_cache_rocksdb_cf_e>(i), key,
                        value);
                }

                int  rc =
                    mode == "--crash-write" ?
                        ngx_http_cache_rocksdb_write(
                            store, entries.data(), entries.size(), nullptr, 0,
                            &ngx_http_cache_purge_test_error) :
                        ngx_http_cache_rocksdb_write_deferred(
                            store, entries.data(), entries.size(), nullptr, 0,
                            &ngx_http_cache_purge_test_error);
                ngx_http_cache_purge_test_expect(rc, NGX_HTTP_CACHE_ROCKSDB_OK,
                                                 "cross-CF write");

                if (mode == "--deferred-barrier") {
                    ngx_http_cache_purge_test_write(
                        store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "B:accepted",
                        "durable barrier");

                    for (const auto &entry :
                         std::filesystem::directory_iterator(argv[2]))
                    {
                        if (entry.path().extension() == ".log") {
                            std::cout << "WAL " << entry.path().string() << ' '
                            << entry.file_size() << std::endl;
                        }
                    }

                    const std::string  tail = "Z:tail";

                    for (unsigned  i = 0; i < entries.size(); i++) {
                        entries[i] = ngx_http_cache_purge_test_put(
                            static_cast<ngx_http_cache_rocksdb_cf_e>(i), tail,
                            value);
                    }

                    ngx_http_cache_purge_test_expect(
                        ngx_http_cache_rocksdb_write_deferred(
                            store, entries.data(), entries.size(), nullptr, 0,
                            &ngx_http_cache_purge_test_error),
                        0, "unsynchronized tail after barrier");
                }

                _exit(0);
            } else if (mode == "--recover" || mode == "--recover-barrier") {
                for (unsigned  i = 0; i != 3; ++i) {
                    ngx_http_cache_purge_test_require(
                        ngx_http_cache_purge_test_get(
                            store, static_cast<ngx_http_cache_rocksdb_cf_e>(i),
                            "Z:committed")
                        == "cross-CF durable",
                        "recover batch");

                    if (mode == "--recover-barrier") {
                        ngx_http_cache_purge_test_missing(
                            store, static_cast<ngx_http_cache_rocksdb_cf_e>(i),
                            "Z:tail");
                    }
                }

                if (mode == "--recover-barrier") {
                    ngx_http_cache_purge_test_require(
                        ngx_http_cache_purge_test_get(
                            store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                            "B:accepted")
                        == "durable barrier",
                        "a later lost cache tail preserves the control "
                        "barrier");
                }
            } else {
                throw std::runtime_error("unknown mode");
            }

            ngx_http_cache_purge_test_close(store);

            return 0;
        }

        ngx_http_cache_purge_test_options();
        ngx_http_cache_purge_test_direct_io();
        ngx_http_cache_purge_test_deferred();
        char  path[] = "/tmp/cache-purge-store-test-XXXXXX";
        ngx_http_cache_purge_test_require(mkdtemp(path) != nullptr,
                                          "create test directory");
        options.max_cursors = 2;
#if (defined(__linux__))
        int  inherited = getpriority(PRIO_PROCESS, 0);
#endif
        auto  *store = ngx_http_cache_purge_test_open(path, options);
#if (defined(__linux__))
        ngx_http_cache_purge_test_background_priority(inherited);
#endif
        ngx_http_cache_purge_test_reader(store);
        ngx_http_cache_purge_test_cached_cursor(store);
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                        "C:abi", "C ABI");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_store_c_probe(store) == 0, "C ABI probe");
        const std::string  binary_key("K:a\0b\xff", 6), binary_value("v\0x", 3);
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                        binary_key, binary_value);
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(
                store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, binary_key)
            == binary_value,
            "binary equality");
        ngx_http_cache_rocksdb_buffer_t  short_buffer{ nullptr, 0, 0 };
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_get(
                store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_test_bytes(binary_key), &short_buffer,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_BUFFER, "small get buffer");
        ngx_http_cache_purge_test_require(
            short_buffer.length == binary_value.size(), "required get size");
        ngx_http_cache_purge_test_missing(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                          binary_key);

        const std::string                                 key = "A:object",
                                                          initial = "old",
                                                          replacement = "new";
        std::array<ngx_http_cache_rocksdb_mutation_t, 3>  batch;

        for (unsigned  i = 0; i != batch.size(); ++i) {
            batch[i] = ngx_http_cache_purge_test_put(
                static_cast<ngx_http_cache_rocksdb_cf_e>(i), key, initial);
        }

        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, batch.data(), batch.size(),
                                         nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "atomic initial");
        ngx_http_cache_rocksdb_condition_t  condition{
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_test_bytes(key),
            ngx_http_cache_purge_test_bytes(initial), 1
        };

        for (unsigned  i = 0; i != batch.size(); ++i) {
            batch[i] = ngx_http_cache_purge_test_put(
                static_cast<ngx_http_cache_rocksdb_cf_e>(i), key, replacement);
        }

        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, batch.data(), batch.size(),
                                         &condition, 1,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "compare and write");

        for (auto &entry : batch) {
            entry.value = ngx_http_cache_purge_test_bytes(initial);
        }

        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, batch.data(), batch.size(),
                                         &condition, 1,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "reject stale precondition");

        for (unsigned  i = 0; i != 3; ++i) {
            ngx_http_cache_purge_test_require(
                ngx_http_cache_purge_test_get(
                    store, static_cast<ngx_http_cache_rocksdb_cf_e>(i), key)
                == replacement,
                "all CFs replaced");
        }

        auto  invalid = batch;
        invalid[1].key = { nullptr, 2 };
        invalid[0].value = ngx_http_cache_purge_test_bytes(initial);
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, invalid.data(), invalid.size(),
                                         nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid batch rejected");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(store,
                                          NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key)
            == replacement,
            "invalid batch had no partial update");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, batch.data(), SIZE_MAX,
                                         nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "entry count overflow rejected");
        const std::string                                 large(
            options.max_value_bytes, 'x');
        std::array<ngx_http_cache_rocksdb_mutation_t, 5>  oversized;

        for (auto &entry : oversized) {
            entry = ngx_http_cache_purge_test_put(
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, large);
        }

        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, oversized.data(),
                                         oversized.size(), nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "byte budget rejected");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(store,
                                          NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key)
            == replacement,
            "over-budget batch had no partial update");
        const std::string  empty_key_name = "E:empty", empty_value_name;
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                        empty_key_name, empty_value_name);
        ngx_http_cache_rocksdb_condition_t  empty_condition{
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
            ngx_http_cache_purge_test_bytes(empty_key_name),
            ngx_http_cache_purge_test_bytes(empty_value_name), 0
        };
        auto                                empty_mutation =
            ngx_http_cache_purge_test_put(
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, empty_key_name, replacement);
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, &empty_mutation, 1,
                                         &empty_condition, 1,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "empty value is present");
        empty_condition.exists = 1;
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, &empty_mutation, 1,
                                         &empty_condition, 1,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "compare empty value");
        const std::string  deleted = "D:delete";

        for (unsigned  i = 0; i != 3; ++i) {
            batch[i] = ngx_http_cache_purge_test_put(
                static_cast<ngx_http_cache_rocksdb_cf_e>(i), deleted, initial);
        }

        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, batch.data(), batch.size(),
                                         nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "prepare cross-CF deletion");

        for (auto &entry : batch) {
            entry.type = NGX_HTTP_CACHE_ROCKSDB_DELETE;
            entry.value = ngx_http_cache_purge_test_bytes(empty_value_name);
        }

        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_write(store, batch.data(), batch.size(),
                                         nullptr, 0,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "cross-CF delete");

        for (unsigned  i = 0; i != 3; ++i) {
            ngx_http_cache_purge_test_missing(
                store, static_cast<ngx_http_cache_rocksdb_cf_e>(i), deleted);
        }

        const std::string  long_prefix =
            "U:https://example.test/" + std::string(6000, 'a') + '/';
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                        long_prefix + "one", "1");
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                        long_prefix + "two", "2");
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                        long_prefix.substr(0, 1000) + "else",
                                        "3");
        auto  long_keys = ngx_http_cache_purge_test_scan(
            store, NGX_HTTP_CACHE_ROCKSDB_URL, long_prefix);
        ngx_http_cache_purge_test_require(
            long_keys
            == std::vector<std::string>{ long_prefix + "one",
                                         long_prefix + "two" },
            "complete long prefix range");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_scan(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                           long_prefix, long_keys[0])
            == std::vector<std::string>{ long_keys[1] },
            "exclusive resume");
        const std::string  high(2, static_cast<char>(255));
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                        high, "0");
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                        high + "a", "1");
        ngx_http_cache_purge_test_write(
            store, NGX_HTTP_CACHE_ROCKSDB_URL,
            std::string(1, static_cast<char>(254)) + "z", "2");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_scan(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                           high)
            == std::vector<std::string>{ high, high + "a" },
            "all-FF prefix without successor");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_scan(store, NGX_HTTP_CACHE_ROCKSDB_URL,
                                           high, "", true)
            == std::vector<std::string>{ high, high + "a" },
            "cached all-FF prefix without successor");
        const std::string  scope1("T:ns1\0tag\0", 10),
                           scope2("T:ns2\0tag\0", 10);
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_TAG,
                                        scope1 + "1", "a");
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_TAG,
                                        scope2 + "2", "b");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_scan(store, NGX_HTTP_CACHE_ROCKSDB_TAG,
                                           scope1)
            == std::vector<std::string>{ scope1 + "1" },
            "binary namespace/tag isolation");

        ngx_http_cache_rocksdb_cursor_t  *cursor = nullptr, *second = nullptr,
                                         *third = nullptr;
        const std::string                 prefix = "A:";
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_open(
                store, NGX_HTTP_CACHE_ROCKSDB_URL,
                ngx_http_cache_purge_test_bytes(prefix),
                ngx_http_cache_purge_test_bytes(""), &cursor,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "snapshot cursor");
        ngx_http_cache_purge_test_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                        key, "later");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(
                store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, cursor)
            == replacement,
            "cross-CF snapshot consistent");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(store,
                                          NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key)
            == "later",
            "current read sees newer commit");
        ngx_http_cache_rocksdb_buffer_t  empty_key{ nullptr, 0, 0 },
                                         empty_value{ nullptr, 0, 0 };
        ngx_http_cache_purge_test_expect(ngx_http_cache_rocksdb_cursor_next(
                                             cursor, &empty_key, &empty_value,
                                             &ngx_http_cache_purge_test_error),
                                         NGX_HTTP_CACHE_ROCKSDB_BUFFER,
                                         "short cursor buffers");
        ngx_http_cache_purge_test_require(empty_key.length == key.size()
                                          && empty_value.length
                                             == replacement.size(),
                                          "cursor required sizes");
        std::array<unsigned char, 64>    k{}, v{};
        ngx_http_cache_rocksdb_buffer_t  key_buffer{ k.data(), k.size(), 0 },
                                         value_buffer{ v.data(), v.size(), 0 };
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_next(
                cursor, &key_buffer, &value_buffer,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "short read did not advance");
        ngx_http_cache_purge_test_require(
            std::string(reinterpret_cast<char *>(k.data()), key_buffer.length)
            == key,
            "retry returns original record");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_next(
                cursor, &key_buffer, &value_buffer,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_DONE, "range end");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_open(
                store, NGX_HTTP_CACHE_ROCKSDB_TAG,
                ngx_http_cache_purge_test_bytes(prefix),
                ngx_http_cache_purge_test_bytes(""), &second,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "second cursor");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_open(
                store, NGX_HTTP_CACHE_ROCKSDB_TAG,
                ngx_http_cache_purge_test_bytes(prefix),
                ngx_http_cache_purge_test_bytes(""), &third,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cursor limit");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_close(&store,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "refuse close with live handles");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_close(
                &second, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close second");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_fork(
                cursor, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_test_bytes(prefix),
                ngx_http_cache_purge_test_bytes(""), &second,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "fork exact snapshot");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_close(
                &cursor, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close first");
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(
                store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, second)
            == replacement,
            "child retains snapshot after parent closes");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_seek_le(
                second, ngx_http_cache_purge_test_bytes(key), &empty_key,
                &empty_value, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_BUFFER, "predecessor short buffer");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_seek_le(
                second, ngx_http_cache_purge_test_bytes(key), &key_buffer,
                &value_buffer, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK,
            "inclusive predecessor retries selected row");
        ngx_http_cache_purge_test_require(
            std::string(reinterpret_cast<char *>(k.data()), key_buffer.length)
            == key
            && std::string(reinterpret_cast<char *>(v.data()),
                           value_buffer.length)
               == replacement,
            "predecessor uses the inherited snapshot after a later write");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_seek_le(
                second, ngx_http_cache_purge_test_bytes(prefix), &key_buffer,
                &value_buffer, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_DONE,
            "predecessor cannot cross the prefix lower bound");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_seek_le(
                second, ngx_http_cache_purge_test_bytes("B:"), &key_buffer,
                &value_buffer, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "predecessor target must belong to its range");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_close(
                &second, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close child");

        int          wrong_thread = 0;
        std::thread  thread([&] {
            ngx_http_cache_rocksdb_error_t   local;
            ngx_http_cache_rocksdb_buffer_t  buffer{ nullptr, 0, 0 };

            wrong_thread = ngx_http_cache_rocksdb_get(
                store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_test_bytes(key), &buffer, &local);
        });
        thread.join();
        ngx_http_cache_purge_test_require(wrong_thread
                                          == NGX_HTTP_CACHE_ROCKSDB_OWNER,
                                          "reject cross-thread handle use");
        ngx_http_cache_rocksdb_t  *contender = nullptr;
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_open(path, &options, &contender,
                                        &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "same-process second owner rejected");
        ngx_http_cache_rocksdb_stats_t  stats;
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_stats(store, &stats,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "stats");
        ngx_http_cache_purge_test_require(stats.cache_capacity
                                          == options.cache_bytes
                                          && stats.conflicts == 2,
                                          "budget and conflict accounting");
        ngx_http_cache_purge_test_close(store);
        options.cursor_lifetime_ms = 10;
        store = ngx_http_cache_purge_test_open(path, options);
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(store,
                                          NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key)
            == "later",
            "reopen persistent data");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_open_cached(
                store, NGX_HTTP_CACHE_ROCKSDB_URL,
                ngx_http_cache_purge_test_bytes(prefix),
                ngx_http_cache_purge_test_bytes(""), &cursor,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "expiring cursor");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_fork(
                cursor, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_test_bytes(prefix),
                ngx_http_cache_purge_test_bytes(""), &second,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "fork expiring view");
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_stats(store, &stats,
                                         &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "expire via stats");
        ngx_http_cache_purge_test_require(
            stats.active_cursors == 0 && stats.cursor_handles == 2,
            "expired cursors release shared snapshot, retain bounded handles");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_next(
                cursor, &key_buffer, &value_buffer,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_EXPIRED, "expired read rejected");
        ngx_http_cache_purge_test_expect(ngx_http_cache_rocksdb_cursor_next(
                                             second, &key_buffer,
                                             &value_buffer,
                                             &ngx_http_cache_purge_test_error),
                                         NGX_HTTP_CACHE_ROCKSDB_EXPIRED,
                                         "child shares original expiration");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_fork(
                cursor, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                ngx_http_cache_purge_test_bytes(prefix),
                ngx_http_cache_purge_test_bytes(""), &third,
                &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_EXPIRED, "expired parent cannot fork");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_close(
                &second, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close expired child");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_cursor_close(
                &cursor, &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close expired cursor");
        ngx_http_cache_purge_test_close(store);
        auto  reduced = options;
        reduced.max_value_bytes = 2;
        store = ngx_http_cache_purge_test_open(path, reduced);
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                       ngx_http_cache_purge_test_bytes(key),
                                       &value_buffer,
                                       &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "lower read budget is not disk corruption");
        ngx_http_cache_purge_test_close(store);
        ngx_http_cache_purge_test_schema_change(path, "future");
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_open(path, &options, &store,
                                        &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "future schema rejected");
        ngx_http_cache_purge_test_require(
            store == nullptr, "schema rejection returns no handle");
        ngx_http_cache_purge_test_schema_change(path, "1");
        store = ngx_http_cache_purge_test_open(path, options);
        ngx_http_cache_purge_test_require(
            ngx_http_cache_purge_test_get(store,
                                          NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key)
            == "later",
            "schema rejection preserves records");
        ngx_http_cache_purge_test_close(store);

        char  partial[] = "/tmp/cache-purge-store-partial-XXXXXX";
        ngx_http_cache_purge_test_require(
            mkdtemp(partial) != nullptr,
            "create partial initialization fixture");
        rocksdb::Options  raw_options;
        raw_options.create_if_missing = true;
        rocksdb::DB  *raw = nullptr;
        ngx_http_cache_purge_test_require(
            rocksdb::DB::Open(raw_options, partial, &raw).ok(),
            "create empty default CF");
        delete raw;
        store = ngx_http_cache_purge_test_open(partial, options);
        ngx_http_cache_purge_test_close(store);
        std::vector<std::string>  names;
        ngx_http_cache_purge_test_require(
            rocksdb::DB::ListColumnFamilies(raw_options, partial, &names).ok(),
            "list initialized CFs");
        std::sort(names.begin(), names.end());
        ngx_http_cache_purge_test_require(
            names == std::vector<std::string>{ "default", "tag", "url" },
            "complete interrupted empty initialization");

        char  foreign[] = "/tmp/cache-purge-store-foreign-XXXXXX";
        ngx_http_cache_purge_test_require(mkdtemp(foreign) != nullptr,
                                          "create foreign database fixture");
        ngx_http_cache_purge_test_require(
            rocksdb::DB::Open(raw_options, foreign, &raw).ok(),
            "create foreign DB");
        ngx_http_cache_purge_test_require(
            raw->Put(rocksdb::WriteOptions(), "foreign", "preserved").ok(),
            "write foreign data");
        delete raw;
        ngx_http_cache_purge_test_expect(
            ngx_http_cache_rocksdb_open(foreign, &options, &store,
                                        &ngx_http_cache_purge_test_error),
            NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
            "do not adopt non-empty foreign DB");
        ngx_http_cache_purge_test_require(
            !store, "foreign rejection returns no handle");
        ngx_http_cache_purge_test_require(
            rocksdb::DB::ListColumnFamilies(raw_options, foreign, &names).ok(),
            "list foreign CFs");
        ngx_http_cache_purge_test_require(
            names == std::vector<std::string>{ "default" },
            "foreign schema unmodified");
        ngx_http_cache_purge_test_require(
            rocksdb::DB::Open(raw_options, foreign, &raw).ok(),
            "reopen foreign DB");
        std::string  foreign_value;
        ngx_http_cache_purge_test_require(
            raw->Get(rocksdb::ReadOptions(), "foreign", &foreign_value).ok()
            && foreign_value == "preserved",
            "foreign data unmodified");
        delete raw;
        std::cout << "PASS " << ngx_http_cache_purge_test_checks
        << " assertions; database " << path << std::endl;

        return 0;
    } catch (const std::exception &e) {
        std::cerr << "FAIL: " << e.what() << std::endl;

        return 1;
    }
}
