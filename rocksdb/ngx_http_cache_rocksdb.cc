/* Copyright (C) Hanada */
#include "ngx_http_cache_rocksdb.h"

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <rocksdb/write_buffer_manager.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr char schema_key[] = "\0ngx_http_cache_purge/store";
constexpr char schema_value[] = "1";
constexpr const char *cf_names[] = {"default", "url", "tag"};

int fail(ngx_http_cache_rocksdb_error_t *error, int code, const char *message) noexcept {
    if (error) {
        error->code = code;
        size_t n = std::min(std::strlen(message), sizeof(error->message) - 1);
        std::memcpy(error->message, message, n);
        error->message[n] = 0;
    }
    return code;
}

template <class Function>
int boundary(ngx_http_cache_rocksdb_error_t *error, Function function) noexcept {
    if (error) { error->code = NGX_HTTP_CACHE_ROCKSDB_OK; error->message[0] = 0; }
    try { return function(); }
    catch (const std::bad_alloc &) {
        return fail(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cache index allocation failed");
    } catch (const std::exception &e) {
        return fail(error, NGX_HTTP_CACHE_ROCKSDB_IO, e.what());
    } catch (...) {
        return fail(error, NGX_HTTP_CACHE_ROCKSDB_IO, "cache index unexpected exception");
    }
}

int result(const rocksdb::Status &status, ngx_http_cache_rocksdb_error_t *error) {
    if (status.ok()) return NGX_HTTP_CACHE_ROCKSDB_OK;
    int code = NGX_HTTP_CACHE_ROCKSDB_IO;
    if (status.IsNotFound()) code = NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
    else if (status.IsCorruption()) code = NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
    else if (status.IsInvalidArgument()) code = NGX_HTTP_CACHE_ROCKSDB_INVALID;
    else if (status.IsMemoryLimit() || status.IsIncomplete()) code = NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    else if (status.IsBusy() || status.IsTryAgain()) code = NGX_HTTP_CACHE_ROCKSDB_BUSY;
    return fail(error, code, status.ToString().c_str());
}

rocksdb::Slice slice(ngx_http_cache_rocksdb_bytes_t value) {
    return rocksdb::Slice(value.len ? reinterpret_cast<const char *>(value.data) : "", value.len);
}

bool bytes_valid(ngx_http_cache_rocksdb_bytes_t value, size_t maximum) {
    return value.len <= maximum && (value.len == 0 || value.data != nullptr);
}

bool cf_valid(ngx_http_cache_rocksdb_cf_e cf) {
    return static_cast<unsigned>(cf) < NGX_HTTP_CACHE_ROCKSDB_CF_COUNT;
}

bool buffer_valid(const ngx_http_cache_rocksdb_buffer_t *buffer) {
    return buffer && (!buffer->capacity || buffer->data);
}

int copy(const rocksdb::Slice &source, ngx_http_cache_rocksdb_buffer_t *destination) {
    destination->length = source.size();
    if (destination->capacity < source.size()) return NGX_HTTP_CACHE_ROCKSDB_BUFFER;
    if (!source.empty()) std::memcpy(destination->data, source.data(), source.size());
    return NGX_HTTP_CACHE_ROCKSDB_OK;
}
} // namespace

struct ngx_http_cache_rocksdb_s {
    ngx_http_cache_rocksdb_options_t options{};
    pid_t pid = getpid();
    std::thread::id thread = std::this_thread::get_id();
    int lease = -1;
    std::unique_ptr<rocksdb::DB> db;
    std::array<rocksdb::ColumnFamilyHandle *, NGX_HTTP_CACHE_ROCKSDB_CF_COUNT> cf{};
    std::shared_ptr<rocksdb::Cache> cache;
    std::shared_ptr<rocksdb::WriteBufferManager> buffers;
    std::vector<ngx_http_cache_rocksdb_cursor_t *> cursors;
    uint64_t writes = 0;
    uint64_t conflicts = 0;

    ~ngx_http_cache_rocksdb_s() {
        if (db) {
            for (auto *handle : cf) if (handle) db->DestroyColumnFamilyHandle(handle);
            db.reset();
        }
        if (lease >= 0) close(lease);
    }

    bool owner() const noexcept {
        return pid == getpid() && thread == std::this_thread::get_id();
    }
    void expire();
};

struct StoreSnapshot {
    ngx_http_cache_rocksdb_t *store;
    const rocksdb::Snapshot *snapshot = nullptr;
    Clock::time_point deadline;
    explicit StoreSnapshot(ngx_http_cache_rocksdb_t *s) : store(s) {}
    ~StoreSnapshot() { if (snapshot) store->db->ReleaseSnapshot(snapshot); }
};

struct ngx_http_cache_rocksdb_cursor_s {
    ngx_http_cache_rocksdb_t *store;
    std::shared_ptr<StoreSnapshot> view;
    std::string prefix;
    std::string upper;
    rocksdb::Slice bound;
    std::unique_ptr<rocksdb::Iterator> iterator;
    bool expired = false;

    explicit ngx_http_cache_rocksdb_cursor_s(ngx_http_cache_rocksdb_t *s) : store(s) {}
    void release() noexcept {
        iterator.reset();
        view.reset();
    }
    ~ngx_http_cache_rocksdb_cursor_s() { release(); }
};

void ngx_http_cache_rocksdb_s::expire() {
    const auto now = Clock::now();
    for (auto *cursor : cursors) {
        if (!cursor->expired && now >= cursor->view->deadline) {
            cursor->release();
            cursor->expired = true;
        }
    }
}

namespace {
int check(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_error_t *error) {
    if (!store) return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null cache index handle");
    if (!store->owner()) return fail(error, NGX_HTTP_CACHE_ROCKSDB_OWNER, "cache index process/thread owner mismatch");
    store->expire();
    return NGX_HTTP_CACHE_ROCKSDB_OK;
}

int check(ngx_http_cache_rocksdb_cursor_t *cursor, ngx_http_cache_rocksdb_error_t *error) {
    if (!cursor) return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null cache index cursor");
    int code = check(cursor->store, error);
    if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
    if (cursor->expired) return fail(error, NGX_HTTP_CACHE_ROCKSDB_EXPIRED, "cache index cursor expired; resume with a new snapshot");
    return NGX_HTTP_CACHE_ROCKSDB_OK;
}

bool key_valid(const ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_bytes_t key) {
    return key.len && bytes_valid(key, store->options.max_key_bytes) && key.data[0] != 0;
}

bool budget(size_t &remaining, ngx_http_cache_rocksdb_bytes_t key, ngx_http_cache_rocksdb_bytes_t value) {
    if (remaining < 64 || key.len > remaining - 64) return false;
    remaining -= 64 + key.len;
    if (value.len > remaining) return false;
    remaining -= value.len;
    return true;
}

int get(ngx_http_cache_rocksdb_t *store, const rocksdb::Snapshot *snapshot,
        ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_rocksdb_bytes_t key,
        ngx_http_cache_rocksdb_buffer_t *value, ngx_http_cache_rocksdb_error_t *error) {
    if (!cf_valid(cf) || !key_valid(store, key) || !buffer_valid(value))
        return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache index get input");
    value->length = 0;
    rocksdb::ReadOptions read;
    read.snapshot = snapshot;
    rocksdb::PinnableSlice found;
    auto status = store->db->Get(read, store->cf[cf], slice(key), &found);
    if (!status.ok()) return result(status, error);
    if (found.size() > store->options.max_value_bytes)
        return fail(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cache index value exceeds configured limit");
    return copy(found, value);
}

int initialize(ngx_http_cache_rocksdb_t *store, const std::string &path,
               ngx_http_cache_rocksdb_error_t *error) {
    store->cache = rocksdb::NewLRUCache(store->options.cache_bytes, 2, true);
    store->buffers = std::make_shared<rocksdb::WriteBufferManager>(
        store->options.write_buffer_bytes, store->cache, true);
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = false;
    options.write_buffer_manager = store->buffers;
    options.write_buffer_size = store->options.write_buffer_bytes / 2;
    options.max_write_buffer_number = 2;
    options.max_open_files = static_cast<int>(store->options.max_open_files);
    options.max_background_jobs = static_cast<int>(store->options.background_jobs);
    options.compression = rocksdb::kSnappyCompression;
    options.max_log_file_size = 1 << 20;
    options.keep_log_file_num = 2;
    options.info_log_level = rocksdb::WARN_LEVEL;
    rocksdb::BlockBasedTableOptions table;
    table.block_cache = store->cache;
    table.cache_index_and_filter_blocks = true;
    table.cache_index_and_filter_blocks_with_high_priority = true;
    table.pin_l0_filter_and_index_blocks_in_cache = false;
    table.pin_top_level_index_and_filter = false;
    table.index_type = rocksdb::BlockBasedTableOptions::kTwoLevelIndexSearch;
    table.partition_filters = true;
    table.metadata_block_size = 4096;
    table.index_block_restart_interval = 16;
    table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));

    std::vector<std::string> names;
    struct stat info;
    if (stat((path + "/CURRENT").c_str(), &info) == 0) {
        auto status = rocksdb::DB::ListColumnFamilies(options, path, &names);
        if (!status.ok()) return result(status, error);
    } else if (errno == ENOENT) {
        names.emplace_back("default");
    } else {
        return fail(error, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot inspect cache index CURRENT");
    }
    for (const auto &name : names) {
        if (std::find(std::begin(cf_names), std::end(cf_names), name) == std::end(cf_names))
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unexpected cache index column family");
    }
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptions;
    for (const auto &name : names) descriptions.emplace_back(name, options);
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
    rocksdb::DB *db = nullptr;
    auto status = rocksdb::DB::Open(options, path, descriptions, &handles, &db);
    if (!status.ok()) return result(status, error);
    store->db.reset(db);
    for (size_t i = 0; i < names.size(); ++i) {
        for (unsigned n = 0; n < NGX_HTTP_CACHE_ROCKSDB_CF_COUNT; ++n) {
            if (names[i] == cf_names[n]) store->cf[n] = handles[i];
        }
    }
    if (!store->cf[0]) return fail(error, NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "missing default column family");
    const rocksdb::Slice marker(schema_key, sizeof(schema_key) - 1);
    std::string version;
    status = db->Get(rocksdb::ReadOptions(), store->cf[0], marker, &version);
    if (status.ok()) {
        if (version != schema_value || names.size() != NGX_HTTP_CACHE_ROCKSDB_CF_COUNT)
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unsupported cache index storage schema");
        return NGX_HTTP_CACHE_ROCKSDB_OK;
    }
    if (!status.IsNotFound()) return result(status, error);

    // Recover an interrupted first open only while all existing CFs are empty.
    // Never adopt a non-empty database or rebuild it by silently dropping data.
    for (auto *handle : store->cf) {
        if (!handle) continue;
        std::unique_ptr<rocksdb::Iterator> iterator(db->NewIterator(rocksdb::ReadOptions(), handle));
        iterator->SeekToFirst();
        if (!iterator->status().ok()) return result(iterator->status(), error);
        if (iterator->Valid()) return fail(error, NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "non-empty cache index has no storage schema");
    }
    for (unsigned i = 0; i < NGX_HTTP_CACHE_ROCKSDB_CF_COUNT; ++i) {
        if (!store->cf[i]) {
            status = db->CreateColumnFamily(options, cf_names[i], &store->cf[i]);
            if (!status.ok()) return result(status, error);
        }
    }
    rocksdb::WriteOptions write;
    write.sync = true;
    return result(db->Put(write, store->cf[0], marker, schema_value), error);
}
} // namespace

extern "C" void
ngx_http_cache_rocksdb_options(ngx_http_cache_rocksdb_options_t *options) {
    if (!options) return;
    std::memset(options, 0, sizeof(*options));
    options->abi = NGX_HTTP_CACHE_ROCKSDB_ABI;
    options->cache_bytes = 16 << 20;
    options->write_buffer_bytes = 4 << 20;
    options->max_key_bytes = 64 << 10;
    options->max_value_bytes = 256 << 10;
    options->max_batch_bytes = 1 << 20;
    options->max_batch_entries = 1024;
    options->max_cursors = 8;
    options->cursor_lifetime_ms = 1000;
    options->max_open_files = 64;
    options->background_jobs = 2;
}

extern "C" uint32_t
ngx_http_cache_rocksdb_abi_1(void) {
    return NGX_HTTP_CACHE_ROCKSDB_ABI;
}

extern "C" int
ngx_http_cache_rocksdb_open(const char *path, const ngx_http_cache_rocksdb_options_t *options,
                          ngx_http_cache_rocksdb_t **output, ngx_http_cache_rocksdb_error_t *error) {
    if (output) *output = nullptr;
    return boundary(error, [&]() -> int {
        if (!output || !path || !*path || std::strlen(path) > 4096 || !options
            || options->abi != NGX_HTTP_CACHE_ROCKSDB_ABI
            || options->cache_bytes < (4U << 20)
            || options->write_buffer_bytes < (1U << 20)
            || options->write_buffer_bytes > options->cache_bytes / 2
            || !options->max_key_bytes || !options->max_value_bytes
            || options->max_key_bytes > options->max_batch_bytes
            || options->max_value_bytes > options->max_batch_bytes
            || !options->max_batch_entries || options->max_batch_entries > 65536
            || !options->max_cursors || options->max_cursors > 64
            || !options->cursor_lifetime_ms || options->cursor_lifetime_ms > 60000
            || options->max_open_files < 16 || options->max_open_files > 4096
            || !options->background_jobs || options->background_jobs > 8)
        {
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache index options");
        }
        auto store = std::make_unique<ngx_http_cache_rocksdb_t>();
        store->options = *options;
        store->cursors.reserve(options->max_cursors);
        const std::string name(path);
        store->lease = open((name + "/index.lock").c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (store->lease < 0) return fail(error, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot open cache index owner lock");
        if (flock(store->lease, LOCK_EX | LOCK_NB) != 0)
            return fail(error, (errno == EWOULDBLOCK || errno == EINTR) ? NGX_HTTP_CACHE_ROCKSDB_BUSY : NGX_HTTP_CACHE_ROCKSDB_IO,
                        "cache index owner lock unavailable");
        int code = initialize(store.get(), name, error);
        if (code == NGX_HTTP_CACHE_ROCKSDB_OK) *output = store.release();
        return code;
    });
}

extern "C" int
ngx_http_cache_rocksdb_close(ngx_http_cache_rocksdb_t **pointer, ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        if (!pointer || !*pointer) return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null cache index handle");
        auto *store = *pointer;
        int code = check(store, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        if (!store->cursors.empty()) return fail(error, NGX_HTTP_CACHE_ROCKSDB_BUSY, "close cache index cursors before the store");
        for (auto *&handle : store->cf) {
            store->db->DestroyColumnFamilyHandle(handle);
            handle = nullptr;
        }
        auto status = store->db->Close();
        delete store;
        *pointer = nullptr;
        return result(status, error);
    });
}

extern "C" int
ngx_http_cache_rocksdb_get(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
                         ngx_http_cache_rocksdb_bytes_t key, ngx_http_cache_rocksdb_buffer_t *value,
                         ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        int code = check(store, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        return get(store, nullptr, cf, key, value, error);
    });
}

extern "C" int
ngx_http_cache_rocksdb_write(ngx_http_cache_rocksdb_t *store,
                           const ngx_http_cache_rocksdb_mutation_t *mutations, size_t count,
                           const ngx_http_cache_rocksdb_condition_t *conditions, size_t condition_count,
                           ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        int code = check(store, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        if (!count || !mutations || (condition_count && !conditions))
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache index batch");
        if (count > store->options.max_batch_entries
            || condition_count > store->options.max_batch_entries - count)
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cache index batch entry limit");
        size_t remaining = store->options.max_batch_bytes;
        for (size_t i = 0; i < count; ++i) {
            const auto &op = mutations[i];
            if (!cf_valid(op.cf) || !key_valid(store, op.key)
                || !bytes_valid(op.value, store->options.max_value_bytes)
                || (op.type != NGX_HTTP_CACHE_ROCKSDB_PUT && op.type != NGX_HTTP_CACHE_ROCKSDB_DELETE)
                || (op.type == NGX_HTTP_CACHE_ROCKSDB_DELETE && op.value.len))
                return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache index mutation");
            if (!budget(remaining, op.key, op.value))
                return fail(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cache index batch byte limit");
        }
        for (size_t i = 0; i < condition_count; ++i) {
            const auto &condition = conditions[i];
            if (!cf_valid(condition.cf) || !key_valid(store, condition.key)
                || !bytes_valid(condition.value, store->options.max_value_bytes)
                || (condition.exists != 0 && condition.exists != 1)
                || (!condition.exists && condition.value.len))
                return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache index precondition");
            if (!budget(remaining, condition.key, condition.value))
                return fail(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cache index batch byte limit");
        }
        for (size_t i = 0; i < condition_count; ++i) {
            const auto &condition = conditions[i];
            rocksdb::PinnableSlice found;
            auto status = store->db->Get(rocksdb::ReadOptions(), store->cf[condition.cf], slice(condition.key), &found);
            if (!status.ok() && !status.IsNotFound()) return result(status, error);
            if (status.ok() && found.size() > store->options.max_value_bytes)
                return fail(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cache index precondition record exceeds configured limits");
            if ((condition.exists && (!status.ok() || found != slice(condition.value)))
                || (!condition.exists && !status.IsNotFound()))
            {
                ++store->conflicts;
                return fail(error, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "cache index precondition changed");
            }
        }
        rocksdb::WriteBatch batch;
        for (size_t i = 0; i < count; ++i) {
            const auto &op = mutations[i];
            if (op.type == NGX_HTTP_CACHE_ROCKSDB_PUT) batch.Put(store->cf[op.cf], slice(op.key), slice(op.value));
            else batch.Delete(store->cf[op.cf], slice(op.key));
        }
        rocksdb::WriteOptions write;
        write.sync = true;
        auto status = store->db->Write(write, &batch);
        if (status.ok()) ++store->writes;
        return result(status, error);
    });
}

static int
open_cursor(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
            ngx_http_cache_rocksdb_bytes_t prefix, ngx_http_cache_rocksdb_bytes_t after,
            std::shared_ptr<StoreSnapshot> view, ngx_http_cache_rocksdb_cursor_t **output,
            ngx_http_cache_rocksdb_error_t *error) {
        int code = check(store, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        if (!output || !cf_valid(cf) || !bytes_valid(prefix, store->options.max_key_bytes)
            || !bytes_valid(after, store->options.max_key_bytes)
            || (after.len && !slice(after).starts_with(slice(prefix))))
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache index cursor range");
        if (store->cursors.size() >= store->options.max_cursors)
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cache index cursor handle limit");
        if (view && Clock::now() >= view->deadline)
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_EXPIRED, "cache index parent snapshot expired");
        auto cursor = std::make_unique<ngx_http_cache_rocksdb_cursor_t>(store);
        cursor->prefix = slice(prefix).ToString();
        cursor->upper = cursor->prefix;
        while (!cursor->upper.empty() && static_cast<unsigned char>(cursor->upper.back()) == 255)
            cursor->upper.pop_back();
        if (!cursor->upper.empty()) ++cursor->upper.back();
        cursor->bound = cursor->upper;
        if (!view) {
            view = std::make_shared<StoreSnapshot>(store);
            view->snapshot = store->db->GetSnapshot();
            if (!view->snapshot)
                return fail(error, NGX_HTTP_CACHE_ROCKSDB_IO, "cache index snapshot unavailable");
            view->deadline = Clock::now() + std::chrono::milliseconds(store->options.cursor_lifetime_ms);
        }
        cursor->view = std::move(view);
        rocksdb::ReadOptions read;
        read.snapshot = cursor->view->snapshot;
        read.fill_cache = false;
        read.total_order_seek = true;
        if (!cursor->upper.empty()) read.iterate_upper_bound = &cursor->bound;
        cursor->iterator.reset(store->db->NewIterator(read, store->cf[cf]));
        cursor->iterator->Seek(after.len ? slice(after) : slice(prefix));
        if (after.len && cursor->iterator->Valid() && cursor->iterator->key() == slice(after))
            cursor->iterator->Next();
        if (!cursor->iterator->status().ok()) return result(cursor->iterator->status(), error);
        store->cursors.push_back(cursor.get());
        *output = cursor.release();
        return NGX_HTTP_CACHE_ROCKSDB_OK;
}

extern "C" int
ngx_http_cache_rocksdb_cursor_open(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
                           ngx_http_cache_rocksdb_bytes_t prefix, ngx_http_cache_rocksdb_bytes_t after,
                           ngx_http_cache_rocksdb_cursor_t **output, ngx_http_cache_rocksdb_error_t *error) {
    if (output) *output = nullptr;
    return boundary(error, [&]() -> int {
        return open_cursor(store, cf, prefix, after, nullptr, output, error);
    });
}

extern "C" int
ngx_http_cache_rocksdb_cursor_fork(ngx_http_cache_rocksdb_cursor_t *parent, ngx_http_cache_rocksdb_cf_e cf,
                           ngx_http_cache_rocksdb_bytes_t prefix, ngx_http_cache_rocksdb_bytes_t after,
                           ngx_http_cache_rocksdb_cursor_t **output, ngx_http_cache_rocksdb_error_t *error) {
    if (output) *output = nullptr;
    return boundary(error, [&]() -> int {
        int code = check(parent, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        return open_cursor(parent->store, cf, prefix, after, parent->view, output, error);
    });
}

extern "C" int
ngx_http_cache_rocksdb_cursor_next(ngx_http_cache_rocksdb_cursor_t *cursor,
                           ngx_http_cache_rocksdb_buffer_t *key, ngx_http_cache_rocksdb_buffer_t *value,
                           ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        int code = check(cursor, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        if (!buffer_valid(key) || !buffer_valid(value))
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache index cursor buffers");
        key->length = 0; value->length = 0;
        if (!cursor->iterator->status().ok()) return result(cursor->iterator->status(), error);
        if (!cursor->iterator->Valid() || !cursor->iterator->key().starts_with(cursor->prefix))
            return NGX_HTTP_CACHE_ROCKSDB_DONE;
        auto k = cursor->iterator->key(), v = cursor->iterator->value();
        if (k.size() > cursor->store->options.max_key_bytes || v.size() > cursor->store->options.max_value_bytes)
            return fail(error, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "cache index cursor record exceeds configured limits");
        key->length = k.size(); value->length = v.size();
        if (key->capacity < k.size() || value->capacity < v.size()) return NGX_HTTP_CACHE_ROCKSDB_BUFFER;
        copy(k, key); copy(v, value);
        cursor->iterator->Next();
        return NGX_HTTP_CACHE_ROCKSDB_OK;
    });
}

extern "C" int
ngx_http_cache_rocksdb_cursor_get(ngx_http_cache_rocksdb_cursor_t *cursor, ngx_http_cache_rocksdb_cf_e cf,
                          ngx_http_cache_rocksdb_bytes_t key, ngx_http_cache_rocksdb_buffer_t *value,
                          ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        int code = check(cursor, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        return get(cursor->store, cursor->view->snapshot, cf, key, value, error);
    });
}

extern "C" int
ngx_http_cache_rocksdb_cursor_close(ngx_http_cache_rocksdb_cursor_t **pointer, ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        if (!pointer || !*pointer) return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null cache index cursor");
        auto *cursor = *pointer;
        int code = check(cursor->store, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        auto &cursors = cursor->store->cursors;
        auto found = std::find(cursors.begin(), cursors.end(), cursor);
        if (found == cursors.end()) return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "unregistered cache index cursor");
        cursors.erase(found);
        delete cursor;
        *pointer = nullptr;
        return NGX_HTTP_CACHE_ROCKSDB_OK;
    });
}

extern "C" int
ngx_http_cache_rocksdb_stats(ngx_http_cache_rocksdb_t *store,
                          ngx_http_cache_rocksdb_stats_t *stats, ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&]() -> int {
        int code = check(store, error);
        if (code != NGX_HTTP_CACHE_ROCKSDB_OK) return code;
        if (!stats) return fail(error, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null cache index stats");
        std::memset(stats, 0, sizeof(*stats));
        stats->cache_capacity = store->cache->GetCapacity();
        stats->cache_usage = store->cache->GetUsage();
        stats->cache_pinned = store->cache->GetPinnedUsage();
        stats->write_buffer_usage = store->buffers->memory_usage();
        stats->cursor_handles = store->cursors.size();
        for (auto *cursor : store->cursors) if (!cursor->expired) ++stats->active_cursors;
        for (auto *cf : store->cf) {
            uint64_t value = 0;
            store->db->GetIntProperty(cf, "rocksdb.estimate-table-readers-mem", &value);
            stats->table_reader_memory += value;
            value = 0;
            store->db->GetIntProperty(cf, "rocksdb.cur-size-all-mem-tables", &value);
            stats->memtable_memory += value;
        }
        stats->writes = store->writes;
        stats->conflicts = store->conflicts;
        return NGX_HTTP_CACHE_ROCKSDB_OK;
    });
}
