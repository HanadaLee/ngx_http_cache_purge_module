#include "ngx_cache_purge_store.h"
#include <rocksdb/db.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

extern "C" int cache_purge_store_c_probe(ngx_cache_purge_store_t *store);
static unsigned checks;
static ngx_cache_purge_store_error_t error;

static void require(bool ok, const char *message) {
    if (!ok) throw std::runtime_error(message);
    ++checks;
}
static void expect(int result, int wanted, const char *operation) {
    if (result != wanted) {
        throw std::runtime_error(std::string(operation) + ": result=" + std::to_string(result)
                                 + " expected=" + std::to_string(wanted) + " " + error.message);
    }
    ++checks;
}
static ngx_cache_purge_bytes_t bytes(const std::string &value) {
    return {reinterpret_cast<const unsigned char *>(value.data()), value.size()};
}
static ngx_cache_purge_mutation_t put(ngx_cache_purge_cf_t cf, const std::string &key,
                                     const std::string &value) {
    return {cf, NGX_PURGE_STORE_PUT, bytes(key), bytes(value)};
}
static void write(ngx_cache_purge_store_t *store, ngx_cache_purge_cf_t cf,
                  const std::string &key, const std::string &value) {
    auto mutation = put(cf, key, value);
    expect(ngx_cache_purge_store_write(store, &mutation, 1, nullptr, 0, &error),
           NGX_PURGE_STORE_OK, "write");
}
static std::string get(ngx_cache_purge_store_t *store, ngx_cache_purge_cf_t cf,
                       const std::string &key, ngx_cache_purge_cursor_t *cursor = nullptr) {
    std::array<unsigned char, 4096> data{};
    ngx_cache_purge_buffer_t buffer{data.data(), data.size(), 0};
    int rc = cursor ? ngx_cache_purge_cursor_get(cursor, cf, bytes(key), &buffer, &error)
                    : ngx_cache_purge_store_get(store, cf, bytes(key), &buffer, &error);
    expect(rc, NGX_PURGE_STORE_OK, "get");
    return std::string(reinterpret_cast<char *>(data.data()), buffer.length);
}
static void missing(ngx_cache_purge_store_t *store, ngx_cache_purge_cf_t cf,
                    const std::string &key) {
    ngx_cache_purge_buffer_t buffer{nullptr, 0, 0};
    expect(ngx_cache_purge_store_get(store, cf, bytes(key), &buffer, &error),
           NGX_PURGE_STORE_NOT_FOUND, "missing");
}
static std::vector<std::string> scan(ngx_cache_purge_store_t *store, ngx_cache_purge_cf_t cf,
                                     const std::string &prefix, const std::string &after = "") {
    ngx_cache_purge_cursor_t *cursor = nullptr;
    expect(ngx_cache_purge_cursor_open(store, cf, bytes(prefix), bytes(after), &cursor, &error),
           NGX_PURGE_STORE_OK, "open cursor");
    std::vector<unsigned char> key_data(65536), value_data(4096);
    ngx_cache_purge_buffer_t key{key_data.data(), key_data.size(), 0};
    ngx_cache_purge_buffer_t value{value_data.data(), value_data.size(), 0};
    std::vector<std::string> keys;
    for (;;) {
        int rc = ngx_cache_purge_cursor_next(cursor, &key, &value, &error);
        if (rc == NGX_PURGE_STORE_DONE) break;
        expect(rc, NGX_PURGE_STORE_OK, "next cursor");
        keys.emplace_back(reinterpret_cast<char *>(key.data), key.length);
    }
    expect(ngx_cache_purge_cursor_close(&cursor, &error), NGX_PURGE_STORE_OK, "close cursor");
    require(cursor == nullptr, "cursor handle consumed");
    return keys;
}
static ngx_cache_purge_store_t *open(const char *path,
                                    const ngx_cache_purge_store_options_t &options) {
    ngx_cache_purge_store_t *store = nullptr;
    expect(ngx_cache_purge_store_open(path, &options, &store, &error), NGX_PURGE_STORE_OK, "open");
    return store;
}
static void close(ngx_cache_purge_store_t *&store) {
    expect(ngx_cache_purge_store_close(&store, &error), NGX_PURGE_STORE_OK, "close");
    require(store == nullptr, "store handle consumed");
}

static void schema_change(const char *path, const std::string &value) {
    rocksdb::Options options;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptions;
    for (const char *name : {"default", "url", "tag"}) descriptions.emplace_back(name, options);
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
    rocksdb::DB *db = nullptr;
    require(rocksdb::DB::Open(options, path, descriptions, &handles, &db).ok(), "raw open");
    const char marker[] = "\0ngx_http_cache_purge/store";
    rocksdb::WriteOptions write;
    write.sync = true;
    require(db->Put(write, handles[0], rocksdb::Slice(marker, sizeof(marker) - 1), value).ok(), "raw schema put");
    for (auto *handle : handles) db->DestroyColumnFamilyHandle(handle);
    delete db;
}

int main(int argc, char **argv) {
    try {
        ngx_cache_purge_store_options_t options;
        ngx_cache_purge_store_options(&options);
        if (argc == 3) {
            const std::string mode = argv[1];
            if (mode == "--locked") {
                ngx_cache_purge_store_t *store = nullptr;
                expect(ngx_cache_purge_store_open(argv[2], &options, &store, &error), NGX_PURGE_STORE_BUSY, "owner lock");
                require(!store, "busy open must not return handle");
                return 0;
            }
            auto *store = open(argv[2], options);
            if (mode == "--hold") {
                std::cout << "READY" << std::endl;
                std::getchar();
            } else if (mode == "--crash-write") {
                const std::string key = "Z:committed", value = "cross-CF durable";
                std::array<ngx_cache_purge_mutation_t, 3> entries;
                for (unsigned i = 0; i != entries.size(); ++i)
                    entries[i] = put(static_cast<ngx_cache_purge_cf_t>(i), key, value);
                expect(ngx_cache_purge_store_write(store, entries.data(), entries.size(), nullptr, 0, &error),
                       NGX_PURGE_STORE_OK, "cross-CF write");
                _exit(0);
            } else if (mode == "--recover") {
                for (unsigned i = 0; i != 3; ++i)
                    require(get(store, static_cast<ngx_cache_purge_cf_t>(i), "Z:committed") == "cross-CF durable", "recover batch");
            } else {
                throw std::runtime_error("unknown mode");
            }
            close(store);
            return 0;
        }

        char path[] = "/tmp/cache-purge-store-test-XXXXXX";
        require(mkdtemp(path) != nullptr, "create test directory");
        options.max_cursors = 2;
        auto *store = open(path, options);
        write(store, NGX_PURGE_STORE_DEFAULT, "C:abi", "C ABI");
        require(cache_purge_store_c_probe(store) == 0, "C ABI probe");
        const std::string binary_key("K:a\0b\xff", 6), binary_value("v\0x", 3);
        write(store, NGX_PURGE_STORE_DEFAULT, binary_key, binary_value);
        require(get(store, NGX_PURGE_STORE_DEFAULT, binary_key) == binary_value, "binary equality");
        ngx_cache_purge_buffer_t short_buffer{nullptr, 0, 0};
        expect(ngx_cache_purge_store_get(store, NGX_PURGE_STORE_DEFAULT, bytes(binary_key), &short_buffer, &error),
               NGX_PURGE_STORE_BUFFER, "small get buffer");
        require(short_buffer.length == binary_value.size(), "required get size");
        missing(store, NGX_PURGE_STORE_URL, binary_key);

        const std::string key = "A:object", initial = "old", replacement = "new";
        std::array<ngx_cache_purge_mutation_t, 3> batch;
        for (unsigned i = 0; i != batch.size(); ++i)
            batch[i] = put(static_cast<ngx_cache_purge_cf_t>(i), key, initial);
        expect(ngx_cache_purge_store_write(store, batch.data(), batch.size(), nullptr, 0, &error), NGX_PURGE_STORE_OK, "atomic initial");
        ngx_cache_purge_condition_t condition{NGX_PURGE_STORE_DEFAULT, bytes(key), bytes(initial), 1};
        for (unsigned i = 0; i != batch.size(); ++i)
            batch[i] = put(static_cast<ngx_cache_purge_cf_t>(i), key, replacement);
        expect(ngx_cache_purge_store_write(store, batch.data(), batch.size(), &condition, 1, &error), NGX_PURGE_STORE_OK, "compare and write");
        for (auto &entry : batch) entry.value = bytes(initial);
        expect(ngx_cache_purge_store_write(store, batch.data(), batch.size(), &condition, 1, &error), NGX_PURGE_STORE_CONFLICT, "reject stale precondition");
        for (unsigned i = 0; i != 3; ++i)
            require(get(store, static_cast<ngx_cache_purge_cf_t>(i), key) == replacement, "all CFs replaced");
        auto invalid = batch;
        invalid[1].key = {nullptr, 2};
        invalid[0].value = bytes(initial);
        expect(ngx_cache_purge_store_write(store, invalid.data(), invalid.size(), nullptr, 0, &error), NGX_PURGE_STORE_INVALID, "invalid batch rejected");
        require(get(store, NGX_PURGE_STORE_DEFAULT, key) == replacement, "invalid batch had no partial update");
        expect(ngx_cache_purge_store_write(store, batch.data(), SIZE_MAX, nullptr, 0, &error), NGX_PURGE_STORE_LIMIT, "entry count overflow rejected");
        const std::string large(options.max_value_bytes, 'x');
        std::array<ngx_cache_purge_mutation_t, 5> oversized;
        for (auto &entry : oversized) entry = put(NGX_PURGE_STORE_DEFAULT, key, large);
        expect(ngx_cache_purge_store_write(store, oversized.data(), oversized.size(), nullptr, 0, &error), NGX_PURGE_STORE_LIMIT, "byte budget rejected");
        require(get(store, NGX_PURGE_STORE_DEFAULT, key) == replacement, "over-budget batch had no partial update");
        const std::string empty_key_name = "E:empty", empty_value_name;
        write(store, NGX_PURGE_STORE_DEFAULT, empty_key_name, empty_value_name);
        ngx_cache_purge_condition_t empty_condition{NGX_PURGE_STORE_DEFAULT, bytes(empty_key_name), bytes(empty_value_name), 0};
        auto empty_mutation = put(NGX_PURGE_STORE_DEFAULT, empty_key_name, replacement);
        expect(ngx_cache_purge_store_write(store, &empty_mutation, 1, &empty_condition, 1, &error), NGX_PURGE_STORE_CONFLICT, "empty value is present");
        empty_condition.exists = 1;
        expect(ngx_cache_purge_store_write(store, &empty_mutation, 1, &empty_condition, 1, &error), NGX_PURGE_STORE_OK, "compare empty value");
        const std::string deleted = "D:delete";
        for (unsigned i = 0; i != 3; ++i)
            batch[i] = put(static_cast<ngx_cache_purge_cf_t>(i), deleted, initial);
        expect(ngx_cache_purge_store_write(store, batch.data(), batch.size(), nullptr, 0, &error), NGX_PURGE_STORE_OK, "prepare cross-CF deletion");
        for (auto &entry : batch) { entry.type = NGX_PURGE_STORE_DELETE; entry.value = bytes(empty_value_name); }
        expect(ngx_cache_purge_store_write(store, batch.data(), batch.size(), nullptr, 0, &error), NGX_PURGE_STORE_OK, "cross-CF delete");
        for (unsigned i = 0; i != 3; ++i) missing(store, static_cast<ngx_cache_purge_cf_t>(i), deleted);

        const std::string long_prefix = "U:https://example.test/" + std::string(6000, 'a') + '/';
        write(store, NGX_PURGE_STORE_URL, long_prefix + "one", "1");
        write(store, NGX_PURGE_STORE_URL, long_prefix + "two", "2");
        write(store, NGX_PURGE_STORE_URL, long_prefix.substr(0, 1000) + "else", "3");
        auto long_keys = scan(store, NGX_PURGE_STORE_URL, long_prefix);
        require(long_keys == std::vector<std::string>{long_prefix + "one", long_prefix + "two"}, "complete long prefix range");
        require(scan(store, NGX_PURGE_STORE_URL, long_prefix, long_keys[0]) == std::vector<std::string>{long_keys[1]}, "exclusive resume");
        const std::string high(2, static_cast<char>(255));
        write(store, NGX_PURGE_STORE_URL, high, "0");
        write(store, NGX_PURGE_STORE_URL, high + "a", "1");
        write(store, NGX_PURGE_STORE_URL, std::string(1, static_cast<char>(254)) + "z", "2");
        require(scan(store, NGX_PURGE_STORE_URL, high) == std::vector<std::string>{high, high + "a"}, "all-FF prefix without successor");
        const std::string scope1("T:ns1\0tag\0", 10), scope2("T:ns2\0tag\0", 10);
        write(store, NGX_PURGE_STORE_TAG, scope1 + "1", "a");
        write(store, NGX_PURGE_STORE_TAG, scope2 + "2", "b");
        require(scan(store, NGX_PURGE_STORE_TAG, scope1) == std::vector<std::string>{scope1 + "1"}, "binary namespace/tag isolation");

        ngx_cache_purge_cursor_t *cursor = nullptr, *second = nullptr, *third = nullptr;
        const std::string prefix = "A:";
        expect(ngx_cache_purge_cursor_open(store, NGX_PURGE_STORE_URL, bytes(prefix), bytes(""), &cursor, &error), NGX_PURGE_STORE_OK, "snapshot cursor");
        write(store, NGX_PURGE_STORE_DEFAULT, key, "later");
        require(get(store, NGX_PURGE_STORE_DEFAULT, key, cursor) == replacement, "cross-CF snapshot consistent");
        require(get(store, NGX_PURGE_STORE_DEFAULT, key) == "later", "current read sees newer commit");
        ngx_cache_purge_buffer_t empty_key{nullptr, 0, 0}, empty_value{nullptr, 0, 0};
        expect(ngx_cache_purge_cursor_next(cursor, &empty_key, &empty_value, &error), NGX_PURGE_STORE_BUFFER, "short cursor buffers");
        require(empty_key.length == key.size() && empty_value.length == replacement.size(), "cursor required sizes");
        std::array<unsigned char, 64> k{}, v{};
        ngx_cache_purge_buffer_t key_buffer{k.data(), k.size(), 0}, value_buffer{v.data(), v.size(), 0};
        expect(ngx_cache_purge_cursor_next(cursor, &key_buffer, &value_buffer, &error), NGX_PURGE_STORE_OK, "short read did not advance");
        require(std::string(reinterpret_cast<char *>(k.data()), key_buffer.length) == key, "retry returns original record");
        expect(ngx_cache_purge_cursor_next(cursor, &key_buffer, &value_buffer, &error), NGX_PURGE_STORE_DONE, "range end");
        expect(ngx_cache_purge_cursor_open(store, NGX_PURGE_STORE_TAG, bytes(prefix), bytes(""), &second, &error), NGX_PURGE_STORE_OK, "second cursor");
        expect(ngx_cache_purge_cursor_open(store, NGX_PURGE_STORE_TAG, bytes(prefix), bytes(""), &third, &error), NGX_PURGE_STORE_LIMIT, "cursor limit");
        expect(ngx_cache_purge_store_close(&store, &error), NGX_PURGE_STORE_BUSY, "refuse close with live handles");
        expect(ngx_cache_purge_cursor_close(&second, &error), NGX_PURGE_STORE_OK, "close second");
        expect(ngx_cache_purge_cursor_close(&cursor, &error), NGX_PURGE_STORE_OK, "close first");

        int wrong_thread = 0;
        std::thread thread([&] {
            ngx_cache_purge_store_error_t local;
            ngx_cache_purge_buffer_t buffer{nullptr, 0, 0};
            wrong_thread = ngx_cache_purge_store_get(store, NGX_PURGE_STORE_DEFAULT, bytes(key), &buffer, &local);
        });
        thread.join();
        require(wrong_thread == NGX_PURGE_STORE_OWNER, "reject cross-thread handle use");
        ngx_cache_purge_store_t *contender = nullptr;
        expect(ngx_cache_purge_store_open(path, &options, &contender, &error), NGX_PURGE_STORE_BUSY, "same-process second owner rejected");
        ngx_cache_purge_store_stats_t stats;
        expect(ngx_cache_purge_store_stats(store, &stats, &error), NGX_PURGE_STORE_OK, "stats");
        require(stats.cache_capacity == options.cache_bytes && stats.conflicts == 2, "budget and conflict accounting");
        close(store);
        options.cursor_lifetime_ms = 10;
        store = open(path, options);
        require(get(store, NGX_PURGE_STORE_DEFAULT, key) == "later", "reopen persistent data");
        expect(ngx_cache_purge_cursor_open(store, NGX_PURGE_STORE_URL, bytes(prefix), bytes(""), &cursor, &error), NGX_PURGE_STORE_OK, "expiring cursor");
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        expect(ngx_cache_purge_store_stats(store, &stats, &error), NGX_PURGE_STORE_OK, "expire via stats");
        require(stats.active_cursors == 0 && stats.cursor_handles == 1, "expired cursor releases snapshot, retains bounded handle");
        expect(ngx_cache_purge_cursor_next(cursor, &key_buffer, &value_buffer, &error), NGX_PURGE_STORE_EXPIRED, "expired read rejected");
        expect(ngx_cache_purge_cursor_close(&cursor, &error), NGX_PURGE_STORE_OK, "close expired cursor");
        close(store);
        auto reduced = options;
        reduced.max_value_bytes = 2;
        store = open(path, reduced);
        expect(ngx_cache_purge_store_get(store, NGX_PURGE_STORE_DEFAULT, bytes(key), &value_buffer, &error),
               NGX_PURGE_STORE_LIMIT, "lower read budget is not disk corruption");
        close(store);
        schema_change(path, "future");
        expect(ngx_cache_purge_store_open(path, &options, &store, &error), NGX_PURGE_STORE_SCHEMA, "future schema rejected");
        require(store == nullptr, "schema rejection returns no handle");
        schema_change(path, "1");
        store = open(path, options);
        require(get(store, NGX_PURGE_STORE_DEFAULT, key) == "later", "schema rejection preserves records");
        close(store);

        char partial[] = "/tmp/cache-purge-store-partial-XXXXXX";
        require(mkdtemp(partial) != nullptr, "create partial initialization fixture");
        rocksdb::Options raw_options;
        raw_options.create_if_missing = true;
        rocksdb::DB *raw = nullptr;
        require(rocksdb::DB::Open(raw_options, partial, &raw).ok(), "create empty default CF");
        delete raw;
        store = open(partial, options);
        close(store);
        std::vector<std::string> names;
        require(rocksdb::DB::ListColumnFamilies(raw_options, partial, &names).ok(), "list initialized CFs");
        std::sort(names.begin(), names.end());
        require(names == std::vector<std::string>{"default", "tag", "url"}, "complete interrupted empty initialization");

        char foreign[] = "/tmp/cache-purge-store-foreign-XXXXXX";
        require(mkdtemp(foreign) != nullptr, "create foreign database fixture");
        require(rocksdb::DB::Open(raw_options, foreign, &raw).ok(), "create foreign DB");
        require(raw->Put(rocksdb::WriteOptions(), "foreign", "preserved").ok(), "write foreign data");
        delete raw;
        expect(ngx_cache_purge_store_open(foreign, &options, &store, &error), NGX_PURGE_STORE_SCHEMA, "do not adopt non-empty foreign DB");
        require(!store, "foreign rejection returns no handle");
        require(rocksdb::DB::ListColumnFamilies(raw_options, foreign, &names).ok(), "list foreign CFs");
        require(names == std::vector<std::string>{"default"}, "foreign schema unmodified");
        require(rocksdb::DB::Open(raw_options, foreign, &raw).ok(), "reopen foreign DB");
        std::string foreign_value;
        require(raw->Get(rocksdb::ReadOptions(), "foreign", &foreign_value).ok() && foreign_value == "preserved", "foreign data unmodified");
        delete raw;
        std::cout << "PASS " << checks << " assertions; database " << path << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "FAIL: " << e.what() << std::endl;
        return 1;
    }
}
