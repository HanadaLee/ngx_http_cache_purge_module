#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <rocksdb/db.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

extern "C" int ngx_http_cache_purge_store_c_probe(
    ngx_http_cache_rocksdb_t *store);
extern "C" unsigned ngx_http_cache_purge_test_sync_count(void);
extern "C" void ngx_http_cache_purge_test_sync_fail(unsigned fail);
static unsigned checks;
static ngx_http_cache_rocksdb_error_t error;

static void
require(bool ok, const char *message)
{
    if (!ok) {
        throw std::runtime_error(message);
    }
    ++checks;
}
static void
expect(int result, int wanted, const char *operation)
{
    if (result != wanted) {
        throw std::runtime_error(
            std::string(operation) + ": result=" + std::to_string(result)
            + " expected=" + std::to_string(wanted) + " " + error.message);
    }
    ++checks;
}
static ngx_http_cache_rocksdb_bytes_t
bytes(const std::string &value)
{
    return { reinterpret_cast<const unsigned char *>(value.data()),
        value.size() };
}
static ngx_http_cache_rocksdb_mutation_t
put(ngx_http_cache_rocksdb_cf_e cf, const std::string &key,
    const std::string &value)
{
    return { cf, NGX_HTTP_CACHE_ROCKSDB_PUT, bytes(key), bytes(value) };
}
static void
write(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
    const std::string &key, const std::string &value)
{
    auto mutation = put(cf, key, value);
    expect(
        ngx_http_cache_rocksdb_write(store, &mutation, 1, nullptr, 0, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "write");
}
static std::string
get(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
    const std::string &key, ngx_http_cache_rocksdb_cursor_t *cursor = nullptr)
{
    std::array<unsigned char, 4096> data{};
    ngx_http_cache_rocksdb_buffer_t buffer{ data.data(), data.size(), 0 };
    int rc = cursor ? ngx_http_cache_rocksdb_cursor_get(
                          cursor, cf, bytes(key), &buffer, &error) :
                      ngx_http_cache_rocksdb_get(
                          store, cf, bytes(key), &buffer, &error);
    expect(rc, NGX_HTTP_CACHE_ROCKSDB_OK, "get");
    return std::string(reinterpret_cast<char *>(data.data()), buffer.length);
}
static void
missing(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
    const std::string &key)
{
    ngx_http_cache_rocksdb_buffer_t buffer{ nullptr, 0, 0 };
    expect(ngx_http_cache_rocksdb_get(store, cf, bytes(key), &buffer, &error),
        NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "missing");
}
static std::vector<std::string>
scan(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
    const std::string &prefix, const std::string &after = "",
    bool cached = false)
{
    ngx_http_cache_rocksdb_cursor_t *cursor = nullptr;
    auto open_cursor = cached ? ngx_http_cache_rocksdb_cursor_open_cached :
                                ngx_http_cache_rocksdb_cursor_open;
    expect(open_cursor(store, cf, bytes(prefix), bytes(after), &cursor, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "open cursor");
    std::vector<unsigned char> key_data(65536), value_data(4096);
    ngx_http_cache_rocksdb_buffer_t key{ key_data.data(), key_data.size(), 0 };
    ngx_http_cache_rocksdb_buffer_t value{ value_data.data(), value_data.size(),
        0 };
    std::vector<std::string> keys;
    for (;;) {
        int rc =
            ngx_http_cache_rocksdb_cursor_next(cursor, &key, &value, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            break;
        }
        expect(rc, NGX_HTTP_CACHE_ROCKSDB_OK, "next cursor");
        keys.emplace_back(reinterpret_cast<char *>(key.data), key.length);
    }
    expect(ngx_http_cache_rocksdb_cursor_close(&cursor, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "close cursor");
    require(cursor == nullptr, "cursor handle consumed");
    return keys;
}
static ngx_http_cache_rocksdb_t *
open(const char *path, const ngx_http_cache_rocksdb_options_t &options)
{
    ngx_http_cache_rocksdb_t *store = nullptr;
    expect(ngx_http_cache_rocksdb_open(path, &options, &store, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "open");
    return store;
}
static void
close(ngx_http_cache_rocksdb_t *&store)
{
    expect(ngx_http_cache_rocksdb_close(&store, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "close");
    require(store == nullptr, "store handle consumed");
}

static void
ngx_http_cache_purge_test_cached_cursor(ngx_http_cache_rocksdb_t *store)
{
    const std::string prefix("cached\0", 7),
        first = prefix + "a", last = prefix + "b", outside = "cached!";
    write(store, NGX_HTTP_CACHE_ROCKSDB_URL, first, "first");
    write(store, NGX_HTTP_CACHE_ROCKSDB_URL, last, "old");
    write(store, NGX_HTTP_CACHE_ROCKSDB_URL, outside, "outside");
    require(scan(store, NGX_HTTP_CACHE_ROCKSDB_URL, prefix, first, true)
                == std::vector<std::string>{ last },
        "cached binary prefix with exclusive resume");
    ngx_http_cache_rocksdb_cursor_t *parent = nullptr, *child = nullptr,
                                    *extra = nullptr;
    expect(ngx_http_cache_rocksdb_cursor_open_cached(store,
               NGX_HTTP_CACHE_ROCKSDB_URL, bytes(prefix), bytes(outside),
               &parent, &error),
        NGX_HTTP_CACHE_ROCKSDB_INVALID, "cached resume rejects another range");
    require(!parent, "invalid cached range leaves no handle");
    expect(ngx_http_cache_rocksdb_cursor_open_cached(store,
               NGX_HTTP_CACHE_ROCKSDB_URL, bytes(prefix), bytes(first), &parent,
               &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "cached snapshot");
    write(store, NGX_HTTP_CACHE_ROCKSDB_URL, last, "new");
    ngx_http_cache_rocksdb_buffer_t key{ nullptr, 0, 0 },
        value{ nullptr, 0, 0 };
    expect(ngx_http_cache_rocksdb_cursor_next(parent, &key, &value, &error),
        NGX_HTTP_CACHE_ROCKSDB_BUFFER, "cached short buffer");
    require(key.length == last.size() && value.length == 3,
        "cached required buffer sizes");
    std::array<unsigned char, 64> k{}, v{};
    key = { k.data(), k.size(), 0 };
    value = { v.data(), v.size(), 0 };
    expect(ngx_http_cache_rocksdb_cursor_next(parent, &key, &value, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "cached retry does not consume row");
    require(
        std::string(reinterpret_cast<char *>(key.data), key.length) == last
            && std::string(reinterpret_cast<char *>(value.data), value.length)
                   == "old",
        "cached read preserves pre-write snapshot");
    expect(ngx_http_cache_rocksdb_cursor_next(parent, &key, &value, &error),
        NGX_HTTP_CACHE_ROCKSDB_DONE, "cached upper bound excludes neighbor");
    expect(
        ngx_http_cache_rocksdb_cursor_fork(parent, NGX_HTTP_CACHE_ROCKSDB_URL,
            bytes(prefix), bytes(""), &child, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "bulk child from cached parent");
    expect(ngx_http_cache_rocksdb_cursor_open_cached(store,
               NGX_HTTP_CACHE_ROCKSDB_URL, bytes(prefix), bytes(""), &extra,
               &error),
        NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cached cursors share the handle budget");
    require(!extra, "saturated cached open leaves no handle");
    expect(ngx_http_cache_rocksdb_cursor_close(&parent, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "close cached parent");
    require(get(store, NGX_HTTP_CACHE_ROCKSDB_URL, last, child) == "old",
        "bulk child retains cached parent's snapshot");
    expect(ngx_http_cache_rocksdb_cursor_close(&child, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "close cached child");
}

static void
ngx_http_cache_purge_test_deferred()
{
    char path[] = "/tmp/cache-purge-deferred-XXXXXX";
    require(mkdtemp(path) != nullptr, "deferred test directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    auto *store = open(path, options);
    const std::string key = "deferred", value = "cache data";
    auto checkpoint = [&](uint64_t written, uint64_t synced) {
        uint64_t actual_written = UINT64_MAX, actual_synced = UINT64_MAX;
        auto syncs = ngx_http_cache_purge_test_sync_count();
        expect(ngx_http_cache_rocksdb_durability(store, &actual_written,
                   &actual_synced, &error),
            0, "read batch durability tickets");
        require(actual_written == written && actual_synced == synced,
            "durability tickets distinguish issued and synchronized batches");
        require(ngx_http_cache_purge_test_sync_count() == syncs,
            "reading a durability ticket never synchronizes the WAL");
    };
    checkpoint(0, 0);
    uint64_t invalid_written = 99, invalid_synced = 99;
    expect(ngx_http_cache_rocksdb_durability(store, &invalid_written,
               &invalid_written, &error),
        NGX_HTTP_CACHE_ROCKSDB_INVALID, "aliased durability outputs rejected");
    require(invalid_written == 0, "invalid durability output is cleared");
    expect(ngx_http_cache_rocksdb_durability(store, nullptr, &invalid_synced,
               &error),
        NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing durability output rejected");
    require(invalid_synced == 0, "other durability output is cleared");
    int foreign = 0;
    std::thread other([&] {
        ngx_http_cache_rocksdb_error_t local{};
        invalid_written = invalid_synced = 99;
        foreign = ngx_http_cache_rocksdb_durability(store, &invalid_written,
            &invalid_synced, &local);
    });
    other.join();
    require(foreign == NGX_HTTP_CACHE_ROCKSDB_OWNER
                && invalid_written == 0 && invalid_synced == 0,
        "durability snapshots preserve executor ownership and clear errors");
    std::array<ngx_http_cache_rocksdb_mutation_t, 3> batch;
    for (unsigned i = 0; i < batch.size(); i++) {
        batch[i] = put(static_cast<ngx_http_cache_rocksdb_cf_e>(i), key, value);
    }
    auto before = ngx_http_cache_purge_test_sync_count();
    require(before != 0, "WAL sync interception observes initialization");
    expect(ngx_http_cache_rocksdb_write_deferred(
               store, batch.data(), batch.size(), nullptr, 0, &error),
        NGX_HTTP_CACHE_ROCKSDB_OK, "defer cross-CF durability");
    require(ngx_http_cache_purge_test_sync_count() == before,
        "deferred write does not sync WAL immediately");
    checkpoint(1, 0);
    for (unsigned i = 0; i < batch.size(); i++) {
        require(get(store, static_cast<ngx_http_cache_rocksdb_cf_e>(i), key)
                    == value,
            "deferred cross-CF rows are immediately queryable");
    }
    expect(ngx_http_cache_rocksdb_sync(store, &error), 0, "flush deferred WAL");
    require(ngx_http_cache_purge_test_sync_count() == before + 1,
        "one barrier syncs the deferred batch");
    checkpoint(1, 1);
    expect(ngx_http_cache_rocksdb_sync(store, &error), 0, "idle WAL barrier");
    require(ngx_http_cache_purge_test_sync_count() == before + 1,
        "idle barriers do not issue sync calls");
    checkpoint(1, 1);
    before = ngx_http_cache_purge_test_sync_count();
    for (unsigned i = 0; i < 64; i++) {
        expect(ngx_http_cache_rocksdb_write_deferred(
                   store, batch.data(), batch.size(), nullptr, 0, &error),
            0, "bounded WAL group");
        require(ngx_http_cache_purge_test_sync_count() == before + (i == 63),
            "the sixty-fourth deferred write forces a barrier");
        checkpoint(i + 2, i == 63 ? 65 : 1);
    }
    before = ngx_http_cache_purge_test_sync_count();
    expect(ngx_http_cache_rocksdb_write_deferred(
               store, batch.data(), batch.size(), nullptr, 0, &error),
        0, "cache before control");
    checkpoint(66, 65);
    ngx_http_cache_rocksdb_condition_t absent{
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes(key), bytes(""), 0
    };
    expect(ngx_http_cache_rocksdb_write(store, batch.data(), batch.size(),
               &absent, 1, &error),
        NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "rejected write has no ticket");
    checkpoint(66, 65);
    write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "control", "durable");
    checkpoint(67, 67);
    require(ngx_http_cache_purge_test_sync_count() == before + 1,
        "ordinary control write still syncs WAL immediately");
    expect(ngx_http_cache_rocksdb_sync(store, &error), 0, "control barrier");
    require(ngx_http_cache_purge_test_sync_count() == before + 1,
        "control durability also covers preceding cache batches");
    expect(ngx_http_cache_rocksdb_write_deferred(
               store, batch.data(), batch.size(), nullptr, 0, &error),
        0, "defer before close");
    checkpoint(68, 67);
    before = ngx_http_cache_purge_test_sync_count();
    close(store);
    require(ngx_http_cache_purge_test_sync_count() > before,
        "graceful close syncs outstanding cache batches");
    store = open(path, options);
    checkpoint(0, 0);
    require(get(store, NGX_HTTP_CACHE_ROCKSDB_TAG, key) == value,
        "closed deferred batch survives reopening");
    expect(ngx_http_cache_rocksdb_write_deferred(
               store, batch.data(), batch.size(), nullptr, 0, &error),
        0, "defer before sync failure");
    checkpoint(1, 0);
    ngx_http_cache_purge_test_sync_fail(1);
    expect(ngx_http_cache_rocksdb_sync(store, &error),
        NGX_HTTP_CACHE_ROCKSDB_IO, "WAL synchronization failure is reported");
    checkpoint(1, 0);
    ngx_http_cache_purge_test_sync_fail(0);
    expect(ngx_http_cache_rocksdb_sync(store, &error),
        NGX_HTTP_CACHE_ROCKSDB_IO,
        "a failed WAL cannot silently clear its pending durability");
    checkpoint(1, 0);
    expect(ngx_http_cache_rocksdb_close(&store, &error),
        NGX_HTTP_CACHE_ROCKSDB_IO,
        "close reports the failed durability barrier");
    require(store == nullptr, "failed close still consumes the store");
}

static void
schema_change(const char *path, const std::string &value)
{
    rocksdb::Options options;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptions;
    for (const char *name : { "default", "url", "tag" }) {
        descriptions.emplace_back(name, options);
    }
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
    rocksdb::DB *db = nullptr;
    require(rocksdb::DB::Open(options, path, descriptions, &handles, &db).ok(),
        "raw open");
    const char marker[] = "\0ngx_http_cache_purge/store";
    rocksdb::WriteOptions write;
    write.sync = true;
    require(db->Put(write, handles[0],
                  rocksdb::Slice(marker, sizeof(marker) - 1), value)
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
        ngx_http_cache_rocksdb_options_t options;
        ngx_http_cache_rocksdb_options(&options);
        if (argc == 3) {
            const std::string mode = argv[1];
            if (mode == "--locked") {
                ngx_http_cache_rocksdb_t *store = nullptr;
                expect(ngx_http_cache_rocksdb_open(
                           argv[2], &options, &store, &error),
                    NGX_HTTP_CACHE_ROCKSDB_BUSY, "owner lock");
                require(!store, "busy open must not return handle");
                return 0;
            }
            auto *store = open(argv[2], options);
            if (mode == "--hold") {
                std::cout << "READY" << std::endl;
                std::getchar();
            } else if (mode == "--crash-write" || mode == "--deferred-write"
                       || mode == "--deferred-barrier")
            {
                const std::string key = "Z:committed",
                                  value = "cross-CF durable";
                std::array<ngx_http_cache_rocksdb_mutation_t, 3> entries;
                for (unsigned i = 0; i != entries.size(); ++i) {
                    entries[i] =
                        put(static_cast<ngx_http_cache_rocksdb_cf_e>(i), key,
                            value);
                }
                int rc =
                    mode == "--crash-write" ?
                        ngx_http_cache_rocksdb_write(store, entries.data(),
                            entries.size(), nullptr, 0, &error) :
                        ngx_http_cache_rocksdb_write_deferred(store,
                            entries.data(), entries.size(), nullptr, 0, &error);
                expect(rc, NGX_HTTP_CACHE_ROCKSDB_OK, "cross-CF write");
                if (mode == "--deferred-barrier") {
                    write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "B:accepted",
                        "durable barrier");
                    for (const auto &entry :
                        std::filesystem::directory_iterator(argv[2]))
                    {
                        if (entry.path().extension() == ".log") {
                            std::cout << "WAL " << entry.path().string() << ' '
                                      << entry.file_size() << std::endl;
                        }
                    }
                    const std::string tail = "Z:tail";
                    for (unsigned i = 0; i < entries.size(); i++) {
                        entries[i] =
                            put(static_cast<ngx_http_cache_rocksdb_cf_e>(i),
                                tail, value);
                    }
                    expect(
                        ngx_http_cache_rocksdb_write_deferred(store,
                            entries.data(), entries.size(), nullptr, 0, &error),
                        0, "unsynchronized tail after barrier");
                }
                _exit(0);
            } else if (mode == "--recover" || mode == "--recover-barrier") {
                for (unsigned i = 0; i != 3; ++i) {
                    require(
                        get(store, static_cast<ngx_http_cache_rocksdb_cf_e>(i),
                            "Z:committed")
                            == "cross-CF durable",
                        "recover batch");
                    if (mode == "--recover-barrier") {
                        missing(store,
                            static_cast<ngx_http_cache_rocksdb_cf_e>(i),
                            "Z:tail");
                    }
                }
                if (mode == "--recover-barrier") {
                    require(
                        get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "B:accepted")
                            == "durable barrier",
                        "a later lost cache tail preserves the control "
                        "barrier");
                }
            } else {
                throw std::runtime_error("unknown mode");
            }
            close(store);
            return 0;
        }

        ngx_http_cache_purge_test_deferred();
        char path[] = "/tmp/cache-purge-store-test-XXXXXX";
        require(mkdtemp(path) != nullptr, "create test directory");
        options.max_cursors = 2;
        auto *store = open(path, options);
        ngx_http_cache_purge_test_cached_cursor(store);
        write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "C:abi", "C ABI");
        require(ngx_http_cache_purge_store_c_probe(store) == 0, "C ABI probe");
        const std::string binary_key("K:a\0b\xff", 6), binary_value("v\0x", 3);
        write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, binary_key, binary_value);
        require(get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, binary_key)
                    == binary_value,
            "binary equality");
        ngx_http_cache_rocksdb_buffer_t short_buffer{ nullptr, 0, 0 };
        expect(ngx_http_cache_rocksdb_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                   bytes(binary_key), &short_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUFFER, "small get buffer");
        require(
            short_buffer.length == binary_value.size(), "required get size");
        missing(store, NGX_HTTP_CACHE_ROCKSDB_URL, binary_key);

        const std::string key = "A:object", initial = "old",
                          replacement = "new";
        std::array<ngx_http_cache_rocksdb_mutation_t, 3> batch;
        for (unsigned i = 0; i != batch.size(); ++i) {
            batch[i] =
                put(static_cast<ngx_http_cache_rocksdb_cf_e>(i), key, initial);
        }
        expect(ngx_http_cache_rocksdb_write(
                   store, batch.data(), batch.size(), nullptr, 0, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "atomic initial");
        ngx_http_cache_rocksdb_condition_t condition{
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes(key), bytes(initial), 1
        };
        for (unsigned i = 0; i != batch.size(); ++i) {
            batch[i] = put(
                static_cast<ngx_http_cache_rocksdb_cf_e>(i), key, replacement);
        }
        expect(ngx_http_cache_rocksdb_write(
                   store, batch.data(), batch.size(), &condition, 1, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "compare and write");
        for (auto &entry : batch) {
            entry.value = bytes(initial);
        }
        expect(ngx_http_cache_rocksdb_write(
                   store, batch.data(), batch.size(), &condition, 1, &error),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "reject stale precondition");
        for (unsigned i = 0; i != 3; ++i) {
            require(get(store, static_cast<ngx_http_cache_rocksdb_cf_e>(i), key)
                        == replacement,
                "all CFs replaced");
        }
        auto invalid = batch;
        invalid[1].key = { nullptr, 2 };
        invalid[0].value = bytes(initial);
        expect(ngx_http_cache_rocksdb_write(
                   store, invalid.data(), invalid.size(), nullptr, 0, &error),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid batch rejected");
        require(get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key) == replacement,
            "invalid batch had no partial update");
        expect(ngx_http_cache_rocksdb_write(
                   store, batch.data(), SIZE_MAX, nullptr, 0, &error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "entry count overflow rejected");
        const std::string large(options.max_value_bytes, 'x');
        std::array<ngx_http_cache_rocksdb_mutation_t, 5> oversized;
        for (auto &entry : oversized) {
            entry = put(NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, large);
        }
        expect(ngx_http_cache_rocksdb_write(store, oversized.data(),
                   oversized.size(), nullptr, 0, &error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "byte budget rejected");
        require(get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key) == replacement,
            "over-budget batch had no partial update");
        const std::string empty_key_name = "E:empty", empty_value_name;
        write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, empty_key_name,
            empty_value_name);
        ngx_http_cache_rocksdb_condition_t empty_condition{
            NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes(empty_key_name),
            bytes(empty_value_name), 0
        };
        auto empty_mutation =
            put(NGX_HTTP_CACHE_ROCKSDB_DEFAULT, empty_key_name, replacement);
        expect(ngx_http_cache_rocksdb_write(
                   store, &empty_mutation, 1, &empty_condition, 1, &error),
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "empty value is present");
        empty_condition.exists = 1;
        expect(ngx_http_cache_rocksdb_write(
                   store, &empty_mutation, 1, &empty_condition, 1, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "compare empty value");
        const std::string deleted = "D:delete";
        for (unsigned i = 0; i != 3; ++i) {
            batch[i] = put(
                static_cast<ngx_http_cache_rocksdb_cf_e>(i), deleted, initial);
        }
        expect(ngx_http_cache_rocksdb_write(
                   store, batch.data(), batch.size(), nullptr, 0, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "prepare cross-CF deletion");
        for (auto &entry : batch) {
            entry.type = NGX_HTTP_CACHE_ROCKSDB_DELETE;
            entry.value = bytes(empty_value_name);
        }
        expect(ngx_http_cache_rocksdb_write(
                   store, batch.data(), batch.size(), nullptr, 0, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "cross-CF delete");
        for (unsigned i = 0; i != 3; ++i) {
            missing(
                store, static_cast<ngx_http_cache_rocksdb_cf_e>(i), deleted);
        }

        const std::string long_prefix =
            "U:https://example.test/" + std::string(6000, 'a') + '/';
        write(store, NGX_HTTP_CACHE_ROCKSDB_URL, long_prefix + "one", "1");
        write(store, NGX_HTTP_CACHE_ROCKSDB_URL, long_prefix + "two", "2");
        write(store, NGX_HTTP_CACHE_ROCKSDB_URL,
            long_prefix.substr(0, 1000) + "else", "3");
        auto long_keys = scan(store, NGX_HTTP_CACHE_ROCKSDB_URL, long_prefix);
        require(long_keys
                    == std::vector<std::string>{ long_prefix + "one",
                        long_prefix + "two" },
            "complete long prefix range");
        require(
            scan(store, NGX_HTTP_CACHE_ROCKSDB_URL, long_prefix, long_keys[0])
                == std::vector<std::string>{ long_keys[1] },
            "exclusive resume");
        const std::string high(2, static_cast<char>(255));
        write(store, NGX_HTTP_CACHE_ROCKSDB_URL, high, "0");
        write(store, NGX_HTTP_CACHE_ROCKSDB_URL, high + "a", "1");
        write(store, NGX_HTTP_CACHE_ROCKSDB_URL,
            std::string(1, static_cast<char>(254)) + "z", "2");
        require(scan(store, NGX_HTTP_CACHE_ROCKSDB_URL, high)
                    == std::vector<std::string>{ high, high + "a" },
            "all-FF prefix without successor");
        require(scan(store, NGX_HTTP_CACHE_ROCKSDB_URL, high, "", true)
                    == std::vector<std::string>{ high, high + "a" },
            "cached all-FF prefix without successor");
        const std::string scope1("T:ns1\0tag\0", 10),
            scope2("T:ns2\0tag\0", 10);
        write(store, NGX_HTTP_CACHE_ROCKSDB_TAG, scope1 + "1", "a");
        write(store, NGX_HTTP_CACHE_ROCKSDB_TAG, scope2 + "2", "b");
        require(scan(store, NGX_HTTP_CACHE_ROCKSDB_TAG, scope1)
                    == std::vector<std::string>{ scope1 + "1" },
            "binary namespace/tag isolation");

        ngx_http_cache_rocksdb_cursor_t *cursor = nullptr, *second = nullptr,
                                        *third = nullptr;
        const std::string prefix = "A:";
        expect(ngx_http_cache_rocksdb_cursor_open(store,
                   NGX_HTTP_CACHE_ROCKSDB_URL, bytes(prefix), bytes(""),
                   &cursor, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "snapshot cursor");
        write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, "later");
        require(get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, cursor)
                    == replacement,
            "cross-CF snapshot consistent");
        require(get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key) == "later",
            "current read sees newer commit");
        ngx_http_cache_rocksdb_buffer_t empty_key{ nullptr, 0, 0 },
            empty_value{ nullptr, 0, 0 };
        expect(ngx_http_cache_rocksdb_cursor_next(
                   cursor, &empty_key, &empty_value, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUFFER, "short cursor buffers");
        require(empty_key.length == key.size()
                    && empty_value.length == replacement.size(),
            "cursor required sizes");
        std::array<unsigned char, 64> k{}, v{};
        ngx_http_cache_rocksdb_buffer_t key_buffer{ k.data(), k.size(), 0 },
            value_buffer{ v.data(), v.size(), 0 };
        expect(ngx_http_cache_rocksdb_cursor_next(
                   cursor, &key_buffer, &value_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "short read did not advance");
        require(
            std::string(reinterpret_cast<char *>(k.data()), key_buffer.length)
                == key,
            "retry returns original record");
        expect(ngx_http_cache_rocksdb_cursor_next(
                   cursor, &key_buffer, &value_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_DONE, "range end");
        expect(ngx_http_cache_rocksdb_cursor_open(store,
                   NGX_HTTP_CACHE_ROCKSDB_TAG, bytes(prefix), bytes(""),
                   &second, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "second cursor");
        expect(ngx_http_cache_rocksdb_cursor_open(store,
                   NGX_HTTP_CACHE_ROCKSDB_TAG, bytes(prefix), bytes(""), &third,
                   &error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cursor limit");
        expect(ngx_http_cache_rocksdb_close(&store, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "refuse close with live handles");
        expect(ngx_http_cache_rocksdb_cursor_close(&second, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close second");
        expect(ngx_http_cache_rocksdb_cursor_fork(cursor,
                   NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes(prefix), bytes(""),
                   &second, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "fork exact snapshot");
        expect(ngx_http_cache_rocksdb_cursor_close(&cursor, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close first");
        require(get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key, second)
                    == replacement,
            "child retains snapshot after parent closes");
        expect(ngx_http_cache_rocksdb_cursor_seek_le(
                   second, bytes(key), &empty_key, &empty_value, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUFFER, "predecessor short buffer");
        expect(ngx_http_cache_rocksdb_cursor_seek_le(
                   second, bytes(key), &key_buffer, &value_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK,
            "inclusive predecessor retries selected row");
        require(
            std::string(reinterpret_cast<char *>(k.data()), key_buffer.length)
                    == key
                && std::string(
                       reinterpret_cast<char *>(v.data()), value_buffer.length)
                       == replacement,
            "predecessor uses the inherited snapshot after a later write");
        expect(ngx_http_cache_rocksdb_cursor_seek_le(
                   second, bytes(prefix), &key_buffer, &value_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_DONE,
            "predecessor cannot cross the prefix lower bound");
        expect(ngx_http_cache_rocksdb_cursor_seek_le(
                   second, bytes("B:"), &key_buffer, &value_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "predecessor target must belong to its range");
        expect(ngx_http_cache_rocksdb_cursor_close(&second, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close child");

        int wrong_thread = 0;
        std::thread thread([&] {
            ngx_http_cache_rocksdb_error_t local;
            ngx_http_cache_rocksdb_buffer_t buffer{ nullptr, 0, 0 };
            wrong_thread = ngx_http_cache_rocksdb_get(store,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes(key), &buffer, &local);
        });
        thread.join();
        require(wrong_thread == NGX_HTTP_CACHE_ROCKSDB_OWNER,
            "reject cross-thread handle use");
        ngx_http_cache_rocksdb_t *contender = nullptr;
        expect(ngx_http_cache_rocksdb_open(path, &options, &contender, &error),
            NGX_HTTP_CACHE_ROCKSDB_BUSY, "same-process second owner rejected");
        ngx_http_cache_rocksdb_stats_t stats;
        expect(ngx_http_cache_rocksdb_stats(store, &stats, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "stats");
        require(
            stats.cache_capacity == options.cache_bytes && stats.conflicts == 2,
            "budget and conflict accounting");
        close(store);
        options.cursor_lifetime_ms = 10;
        store = open(path, options);
        require(get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key) == "later",
            "reopen persistent data");
        expect(ngx_http_cache_rocksdb_cursor_open_cached(store,
                   NGX_HTTP_CACHE_ROCKSDB_URL, bytes(prefix), bytes(""),
                   &cursor, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "expiring cursor");
        expect(ngx_http_cache_rocksdb_cursor_fork(cursor,
                   NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes(prefix), bytes(""),
                   &second, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "fork expiring view");
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        expect(ngx_http_cache_rocksdb_stats(store, &stats, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "expire via stats");
        require(stats.active_cursors == 0 && stats.cursor_handles == 2,
            "expired cursors release shared snapshot, retain bounded handles");
        expect(ngx_http_cache_rocksdb_cursor_next(
                   cursor, &key_buffer, &value_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_EXPIRED, "expired read rejected");
        expect(ngx_http_cache_rocksdb_cursor_next(
                   second, &key_buffer, &value_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_EXPIRED, "child shares original expiration");
        expect(ngx_http_cache_rocksdb_cursor_fork(cursor,
                   NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes(prefix), bytes(""),
                   &third, &error),
            NGX_HTTP_CACHE_ROCKSDB_EXPIRED, "expired parent cannot fork");
        expect(ngx_http_cache_rocksdb_cursor_close(&second, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close expired child");
        expect(ngx_http_cache_rocksdb_cursor_close(&cursor, &error),
            NGX_HTTP_CACHE_ROCKSDB_OK, "close expired cursor");
        close(store);
        auto reduced = options;
        reduced.max_value_bytes = 2;
        store = open(path, reduced);
        expect(ngx_http_cache_rocksdb_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                   bytes(key), &value_buffer, &error),
            NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "lower read budget is not disk corruption");
        close(store);
        schema_change(path, "future");
        expect(ngx_http_cache_rocksdb_open(path, &options, &store, &error),
            NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "future schema rejected");
        require(store == nullptr, "schema rejection returns no handle");
        schema_change(path, "1");
        store = open(path, options);
        require(get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key) == "later",
            "schema rejection preserves records");
        close(store);

        char partial[] = "/tmp/cache-purge-store-partial-XXXXXX";
        require(mkdtemp(partial) != nullptr,
            "create partial initialization fixture");
        rocksdb::Options raw_options;
        raw_options.create_if_missing = true;
        rocksdb::DB *raw = nullptr;
        require(rocksdb::DB::Open(raw_options, partial, &raw).ok(),
            "create empty default CF");
        delete raw;
        store = open(partial, options);
        close(store);
        std::vector<std::string> names;
        require(
            rocksdb::DB::ListColumnFamilies(raw_options, partial, &names).ok(),
            "list initialized CFs");
        std::sort(names.begin(), names.end());
        require(names == std::vector<std::string>{ "default", "tag", "url" },
            "complete interrupted empty initialization");

        char foreign[] = "/tmp/cache-purge-store-foreign-XXXXXX";
        require(mkdtemp(foreign) != nullptr, "create foreign database fixture");
        require(rocksdb::DB::Open(raw_options, foreign, &raw).ok(),
            "create foreign DB");
        require(raw->Put(rocksdb::WriteOptions(), "foreign", "preserved").ok(),
            "write foreign data");
        delete raw;
        expect(ngx_http_cache_rocksdb_open(foreign, &options, &store, &error),
            NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "do not adopt non-empty foreign DB");
        require(!store, "foreign rejection returns no handle");
        require(
            rocksdb::DB::ListColumnFamilies(raw_options, foreign, &names).ok(),
            "list foreign CFs");
        require(names == std::vector<std::string>{ "default" },
            "foreign schema unmodified");
        require(rocksdb::DB::Open(raw_options, foreign, &raw).ok(),
            "reopen foreign DB");
        std::string foreign_value;
        require(raw->Get(rocksdb::ReadOptions(), "foreign", &foreign_value).ok()
                    && foreign_value == "preserved",
            "foreign data unmodified");
        delete raw;
        std::cout << "PASS " << checks << " assertions; database " << path
                  << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "FAIL: " << e.what() << std::endl;
        return 1;
    }
}
