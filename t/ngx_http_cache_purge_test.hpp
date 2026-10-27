/*
 * Copyright (C) Hanada
 */

#ifndef _NGX_HTTP_CACHE_PURGE_TEST_HPP_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_TEST_HPP_INCLUDED_

/*
 * Test-only C++ adapters and reference wire encoding for the production C
 * APIs.
 */
#include "ngx_http_cache_purge_internal.h"

/* Copyright (C) Hanada */

#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace ngx_http_cache_purge {

using FileId = std::array<unsigned char, 16>;
enum class FileState : uint8_t {
    prepared = 1,
    committed = 2,
    retired = 3
};

class IndexError : public std::runtime_error {
public:
    int code;
    IndexError(int value, const std::string &message)
        : std::runtime_error(message), code(value)
    {}
};

struct File {
    FileId id{};
    FileState state = FileState::prepared;
    uint64_t revision = 0;
    std::string key, url, host, tag_namespace;
    std::string root, zone, path, range, vary;
    std::vector<std::string> tags;
    std::string metadata;
    /*
         * bounded, versioned file-policy data owned by the component
         */
    bool structured_key = false;
    bool structured_url = false;
    std::array<std::string, 6> components;
};

struct Match {
    FileId id{};
    uint64_t revision = 0;
    FileState state = FileState::prepared;
};
struct Fill {
    File file;
    FileId previous{};
};
struct ExpectedFile {
    FileId id{};
    uint64_t revision = 0;
};

enum class SelectorKind : uint8_t {
    key,
    url,
    key_prefix,
    url_prefix,
    host,
    namespace_tag,
    host_tag,
    namespace_tags,
    host_tags
};
struct Selector {
    SelectorKind kind = SelectorKind::url;
    std::string value;
    std::string scope; /* namespace or host for the two TAG selectors */
};

inline std::string
tag_group(const std::vector<std::string> &tags)
{
    std::string value(1, static_cast<char>(tags.size()));
    for (const auto &tag : tags) {
        value += static_cast<char>(tag.size() >> 8);
        value += static_cast<char>(tag.size());
        value += tag;
    }
    return value;
}

inline ngx_http_cache_rocksdb_bytes_t
ngx_http_cache_purge_native_bytes(const std::string &s)
{
    return { reinterpret_cast<const unsigned char *>(s.data()), s.size() };
}
inline std::string
ngx_http_cache_purge_cpp_bytes(ngx_http_cache_rocksdb_bytes_t s)
{
    return s.len ? std::string(reinterpret_cast<const char *>(s.data), s.len) :
                   std::string{};
}

/* Conversion-only adapters: all index policy, schema and mutations are C. */
inline void
ngx_http_cache_purge_index_checked(
    int rc, const ngx_http_cache_rocksdb_error_t &error)
{
    if (rc) {
        throw IndexError(rc, error.message);
    }
}
inline Match
ngx_http_cache_purge_cpp_match(const ngx_http_cache_purge_index_match_t &value)
{
    Match result;
    std::memcpy(result.id.data(), value.id, result.id.size());
    result.revision = value.revision;
    result.state = static_cast<FileState>(value.state);
    return result;
}
inline ngx_http_cache_purge_index_expected_t
ngx_http_cache_purge_native_expected(const FileId &id, uint64_t revision)
{
    ngx_http_cache_purge_index_expected_t result{};
    std::memcpy(result.id, id.data(), id.size());
    result.revision = revision;
    return result;
}
inline ngx_http_cache_purge_index_file_t
ngx_http_cache_purge_native_file(const File &file)
{
    ngx_http_cache_purge_index_file_t out{};
    std::memcpy(out.id, file.id.data(), file.id.size());
    out.state = static_cast<uint32_t>(file.state);
    out.revision = file.revision;
    out.key = ngx_http_cache_purge_native_bytes(file.key);
    out.url = ngx_http_cache_purge_native_bytes(file.url);
    out.host = ngx_http_cache_purge_native_bytes(file.host);
    out.tag_namespace = ngx_http_cache_purge_native_bytes(file.tag_namespace);
    out.root = ngx_http_cache_purge_native_bytes(file.root);
    out.zone = ngx_http_cache_purge_native_bytes(file.zone);
    out.path = ngx_http_cache_purge_native_bytes(file.path);
    out.range = ngx_http_cache_purge_native_bytes(file.range);
    out.vary = ngx_http_cache_purge_native_bytes(file.vary);
    out.metadata = ngx_http_cache_purge_native_bytes(file.metadata);
    out.structured_key = file.structured_key;
    out.structured_url = file.structured_url;
    for (size_t i = 0; i < 6; i++) {
        out.components[i] =
            ngx_http_cache_purge_native_bytes(file.components[i]);
    }
    out.tag_count = file.tags.size();
    for (size_t i = 0; i < file.tags.size() && i < 64; i++) {
        out.tags[i] = ngx_http_cache_purge_native_bytes(file.tags[i]);
    }
    return out;
}
class Query {
public:
    ~Query()
    {
        ngx_http_cache_purge_query_close(&native_, nullptr);
    }
    Query(const Query &) = delete;
    Query &operator=(const Query &) = delete;
    bool
    next(Match &match)
    {
        ngx_http_cache_purge_index_match_t value{};
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_query_next(native_, &value, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            return false;
        }
        ngx_http_cache_purge_index_checked(rc, error);
        match = ngx_http_cache_purge_cpp_match(value);
        return true;
    }
    std::string
    bookmark() const
    {
        return ngx_http_cache_purge_cpp_bytes(
            ngx_http_cache_purge_query_bookmark(native_));
    }

private:
    ngx_http_cache_purge_query_t *native_ = nullptr;
    Query() = default;
    friend class Index;
};
class Index {
    ngx_http_cache_rocksdb_t *store_;
    struct OwnedFile {
        ngx_http_cache_purge_index_file_t value{};
        OwnedFile() = default;
        OwnedFile(const OwnedFile &) = delete;
        ~OwnedFile()
        {
            ngx_http_cache_purge_index_file_free(&value);
        }
    };
    static void
    count(size_t size)
    {
        if (!size || size > 32) {
            throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                "index batch must contain 1-32 files");
        }
    }
    Match
    apply(ngx_http_cache_purge_index_action_e action, const FileId &id,
        uint64_t revision)
    {
        auto input = ngx_http_cache_purge_native_expected(id, revision);
        ngx_http_cache_purge_index_match_t output{};
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_apply(
            store_, action, &input, 1, &output, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        return ngx_http_cache_purge_cpp_match(output);
    }
    std::vector<Match>
    apply_batch(ngx_http_cache_purge_index_action_e action,
        const std::vector<ExpectedFile> &files)
    {
        count(files.size());
        std::array<ngx_http_cache_purge_index_expected_t, 32> input{};
        std::array<ngx_http_cache_purge_index_match_t, 32> output{};
        std::vector<Match> result(files.size());
        for (size_t i = 0; i < files.size(); i++) {
            input[i] = ngx_http_cache_purge_native_expected(
                files[i].id, files[i].revision);
        }
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_apply(
            store_, action, input.data(), files.size(), output.data(), &error);
        ngx_http_cache_purge_index_checked(rc, error);
        for (size_t i = 0; i < files.size(); i++) {
            result[i] = ngx_http_cache_purge_cpp_match(output[i]);
        }
        return result;
    }
    std::unique_ptr<Query>
    open_query(
        const Selector *selector, unsigned mode, const std::string &bookmark)
    {
        std::unique_ptr<Query> result(new Query);
        ngx_http_cache_purge_selector_t input{};
        if (selector) {
            input = { static_cast<uint32_t>(selector->kind),
                ngx_http_cache_purge_native_bytes(selector->value),
                ngx_http_cache_purge_native_bytes(selector->scope) };
        }
        ngx_http_cache_rocksdb_error_t error;
        int rc =
            ngx_http_cache_purge_query_open(store_, selector ? &input : nullptr,
                mode, ngx_http_cache_purge_native_bytes(bookmark),
                &result->native_, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        return result;
    }

public:
    ngx_http_cache_rocksdb_t *
    native()
    {
        return store_;
    }
    explicit Index(ngx_http_cache_rocksdb_t *store) : store_(store)
    {
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_init(store, &error);
        ngx_http_cache_purge_index_checked(rc, error);
    }
    Match
    prepare(const File &file, const FileId &previous = {})
    {
        auto input = ngx_http_cache_purge_native_file(file);
        ngx_http_cache_purge_index_match_t output{};
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_prepare(
            store_, &input, previous.data(), &output, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        return ngx_http_cache_purge_cpp_match(output);
    }
    std::vector<Match>
    prepare_batch(const std::vector<Fill> &files)
    {
        count(files.size());
        std::vector<ngx_http_cache_purge_index_fill_t> input(files.size());
        std::array<ngx_http_cache_purge_index_match_t, 32> output{};
        std::vector<Match> result(files.size());
        for (size_t i = 0; i < files.size(); i++) {
            input[i].file = ngx_http_cache_purge_native_file(files[i].file);
            std::memcpy(input[i].previous, files[i].previous.data(),
                files[i].previous.size());
        }
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_prepare_batch(
            store_, input.data(), files.size(), output.data(), &error);
        ngx_http_cache_purge_index_checked(rc, error);
        for (size_t i = 0; i < files.size(); i++) {
            result[i] = ngx_http_cache_purge_cpp_match(output[i]);
        }
        return result;
    }
    Match
    commit(const FileId &id, uint64_t revision)
    {
        return apply(NGX_HTTP_CACHE_PURGE_INDEX_COMMIT, id, revision);
    }
    std::vector<Match>
    commit_batch(const std::vector<ExpectedFile> &files)
    {
        return apply_batch(NGX_HTTP_CACHE_PURGE_INDEX_COMMIT, files);
    }
    Match
    commit_pending(const FileId &id, uint64_t revision)
    {
        return apply(NGX_HTTP_CACHE_PURGE_INDEX_COMMIT_PENDING, id, revision);
    }
    Match
    publish(const FileId &id, uint64_t revision)
    {
        return apply(NGX_HTTP_CACHE_PURGE_INDEX_PUBLISH, id, revision);
    }
    Match
    discard_publication(const FileId &id, uint64_t revision)
    {
        return apply(NGX_HTTP_CACHE_PURGE_INDEX_DISCARD, id, revision);
    }
    Match
    abort_prepare(const FileId &id, uint64_t revision)
    {
        return apply(NGX_HTTP_CACHE_PURGE_INDEX_ABORT, id, revision);
    }
    Match
    retire(const FileId &id, uint64_t revision)
    {
        return apply(NGX_HTTP_CACHE_PURGE_INDEX_RETIRE, id, revision);
    }
    std::vector<Match>
    retire_batch(const std::vector<ExpectedFile> &files)
    {
        return apply_batch(NGX_HTTP_CACHE_PURGE_INDEX_RETIRE, files);
    }
    std::optional<Match>
    current(const std::string &root, const std::string &path)
    {
        ngx_http_cache_purge_index_match_t output{};
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_current(store_,
            ngx_http_cache_purge_native_bytes(root),
            ngx_http_cache_purge_native_bytes(path), &output, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            return {};
        }
        ngx_http_cache_purge_index_checked(rc, error);
        return ngx_http_cache_purge_cpp_match(output);
    }
    bool
    has_fill_intent(const FileId &id)
    {
        int output = 0;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_has_intent(
            store_, id.data(), &output, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        return output != 0;
    }
    Match
    update_metadata(const FileId &id, uint64_t revision,
        const std::string &metadata, const std::vector<std::string> &tags)
    {
        auto input = ngx_http_cache_purge_native_expected(id, revision);
        std::array<ngx_http_cache_rocksdb_bytes_t, 64> values{};
        for (size_t i = 0; i < tags.size() && i < 64; i++) {
            values[i] = ngx_http_cache_purge_native_bytes(tags[i]);
        }
        ngx_http_cache_purge_index_match_t output{};
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_update(store_, &input,
            ngx_http_cache_purge_native_bytes(metadata), values.data(),
            tags.size(), &output, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        return ngx_http_cache_purge_cpp_match(output);
    }
    File
    get(const FileId &id)
    {
        OwnedFile output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_get_file(
            store_, id.data(), &output.value, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        const auto &value = output.value;
        File result;
        std::memcpy(result.id.data(), value.id, result.id.size());
        result.revision = value.revision;
        result.state = static_cast<FileState>(value.state);
        result.key = ngx_http_cache_purge_cpp_bytes(value.key);
        result.url = ngx_http_cache_purge_cpp_bytes(value.url);
        result.host = ngx_http_cache_purge_cpp_bytes(value.host);
        result.tag_namespace =
            ngx_http_cache_purge_cpp_bytes(value.tag_namespace);
        result.root = ngx_http_cache_purge_cpp_bytes(value.root);
        result.zone = ngx_http_cache_purge_cpp_bytes(value.zone);
        result.path = ngx_http_cache_purge_cpp_bytes(value.path);
        result.range = ngx_http_cache_purge_cpp_bytes(value.range);
        result.vary = ngx_http_cache_purge_cpp_bytes(value.vary);
        result.metadata = ngx_http_cache_purge_cpp_bytes(value.metadata);
        result.tags.reserve(value.tag_count);
        for (size_t i = 0; i < value.tag_count; i++) {
            result.tags.push_back(
                ngx_http_cache_purge_cpp_bytes(value.tags[i]));
        }
        return result;
    }
    void
    add_url(const std::string &key, const std::string &url)
    {
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_add_url(store_,
            ngx_http_cache_purge_native_bytes(key),
            ngx_http_cache_purge_native_bytes(url), &error);
        ngx_http_cache_purge_index_checked(rc, error);
    }
    void
    collect_file(const FileId &id, uint64_t revision)
    {
        auto input = ngx_http_cache_purge_native_expected(id, revision);
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_collect(store_, &input, 1, &error);
        ngx_http_cache_purge_index_checked(rc, error);
    }
    void
    collect_files(const std::vector<ExpectedFile> &files)
    {
        count(files.size());
        std::array<ngx_http_cache_purge_index_expected_t, 32> input{};
        for (size_t i = 0; i < files.size(); i++) {
            input[i] = ngx_http_cache_purge_native_expected(
                files[i].id, files[i].revision);
        }
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_collect(
            store_, input.data(), files.size(), &error);
        ngx_http_cache_purge_index_checked(rc, error);
    }
    size_t
    collect_keys(size_t budget = 8)
    {
        size_t output = 0;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_index_collect_keys(
            store_, budget, &output, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        return output;
    }
    std::unique_ptr<Query>
    query(const Selector &selector, const std::string &bookmark = "")
    {
        return open_query(
            &selector, NGX_HTTP_CACHE_PURGE_QUERY_SELECT, bookmark);
    }
    std::unique_ptr<Query>
    pending_fills(const std::string &bookmark = "")
    {
        return open_query(
            nullptr, NGX_HTTP_CACHE_PURGE_QUERY_PENDING, bookmark);
    }
    std::unique_ptr<Query>
    retired_files(const std::string &bookmark = "")
    {
        return open_query(
            nullptr, NGX_HTTP_CACHE_PURGE_QUERY_RETIRED, bookmark);
    }
};

} /* namespace ngx_http_cache_purge */


/* Copyright (C) Hanada */


namespace ngx_http_cache_purge {

enum class PurgeMode : uint8_t {
    invalidate = 1,
    remove = 2
};
enum class OperationStage : uint8_t {
    preparing = 1,
    active = 2,
    draining = 3,
    materialized = 4
};

struct OperationInput {
    std::string id;
    Selector selector;
    PurgeMode mode = PurgeMode::invalidate;
    uint64_t issued_at_ms = 0;
    std::string binding;
    /*
         * Frozen configuration/authorization contract, supplied by admission.
         */
};

struct Operation {
    OperationInput input;
    uint64_t sequence =
        0; /* Ordering of control operations; never changes a cache key. */
    uint64_t revision = 0;
    OperationStage stage = OperationStage::preparing;
    uint64_t accepted_at_ms = 0, effective_at_ms = 0, propagation_until_ms = 0;
    uint32_t propagation_ms = 300000;
    bool propagate = false;
    std::string bookmark;
    bool scan_done = false;
    uint64_t next_run_ms = 0;
    uint64_t visits = 0; /* Attempts, not a deduplicated object count. */
    uint64_t pending = 0, failed = 0;
    uint64_t checkpoint_id = 0;
    std::string last_error;
};

struct OperationCheckpoint {
    uint64_t id = 0;
    /*
         * Strictly increasing per operation; replay of the last ID is
         * idempotent.
         */
    std::string bookmark;
    bool scan_done = false;
    uint64_t next_run_ms = 0;
    uint64_t visits_delta = 0;
    uint64_t pending = 0, failed = 0; /* Absolute outstanding work counts. */
    std::string last_error;
};

struct OperationLimits {
    uint32_t live_records = 4096;
    uint64_t live_selector_bytes = 4 * 1024 * 1024;
};

struct OperationPage {
    std::vector<Operation> items;
    std::string after;
    bool done = false;
};

struct OperationUsage {
    uint64_t live_records = 0, live_selector_bytes = 0, last_sequence = 0;
    uint64_t retired_sequence = 0;
};

/* Durable ledger, called only by the single component executor. It does not
 * publish SHM, access files or prove that old readers/writers have drained.
 * activate follows real FENCE publication. materialize requires a verified
 * quiescence watermark from the future native-cache coordinator; EOF alone is
 * insufficient. Do not expose these transitions as client assertions in HTTP.
 * All accepted operations retain their idempotency record on disk, even after
 * their bounded resident-control slot is released. */
/* Conversion-only bridge for backend consumers still being migrated to C. */
inline ngx_http_cache_purge_operation_input_t
ngx_http_cache_purge_native_input(const OperationInput &input)
{
    return { ngx_http_cache_purge_native_bytes(input.id),
        { static_cast<uint32_t>(input.selector.kind),
            ngx_http_cache_purge_native_bytes(input.selector.value),
            ngx_http_cache_purge_native_bytes(input.selector.scope) },
        static_cast<uint32_t>(input.mode), input.issued_at_ms,
        ngx_http_cache_purge_native_bytes(input.binding) };
}
inline Operation
ngx_http_cache_purge_cpp_operation(
    const ngx_http_cache_purge_operation_t &input)
{
    Operation out;
    out.input.id = ngx_http_cache_purge_cpp_bytes(input.input.id);
    out.input.selector = { static_cast<SelectorKind>(input.input.selector.kind),
        ngx_http_cache_purge_cpp_bytes(input.input.selector.value),
        ngx_http_cache_purge_cpp_bytes(input.input.selector.scope) };
    out.input.mode = static_cast<PurgeMode>(input.input.mode);
    out.input.issued_at_ms = input.input.issued_at_ms;
    out.input.binding = ngx_http_cache_purge_cpp_bytes(input.input.binding);
    out.sequence = input.sequence;
    out.revision = input.revision;
    out.stage = static_cast<OperationStage>(input.stage);
    out.accepted_at_ms = input.accepted_at_ms;
    out.effective_at_ms = input.effective_at_ms;
    out.propagation_until_ms = input.propagation_until_ms;
    out.propagation_ms = input.propagation_ms;
    out.propagate = input.propagate != 0;
    out.bookmark = ngx_http_cache_purge_cpp_bytes(input.bookmark);
    out.scan_done = input.scan_done != 0;
    out.next_run_ms = input.next_run_ms;
    out.visits = input.visits;
    out.pending = input.pending;
    out.failed = input.failed;
    out.checkpoint_id = input.checkpoint_id;
    out.last_error = ngx_http_cache_purge_cpp_bytes(input.last_error);
    return out;
}
class Operations {
    ngx_http_cache_purge_operations_t native_{};
    struct Result {
        ngx_http_cache_purge_operation_t value{};
        Result() = default;
        Result(const Result &) = delete;
        ~Result()
        {
            ngx_http_cache_purge_operation_free(&value);
        }
    };
    struct Page {
        ngx_http_cache_purge_operation_page_t value{};
        Page() = default;
        Page(const Page &) = delete;
        ~Page()
        {
            ngx_http_cache_purge_operation_page_free(&value);
        }
    };
    static void
    checked(int rc, const ngx_http_cache_rocksdb_error_t &error)
    {
        if (rc) {
            throw IndexError(rc, error.message);
        }
    }
    OperationPage
    page(bool due, uint64_t now, const std::string &after, size_t limit)
    {
        Page output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_operations_page(&native_, due, now,
            ngx_http_cache_purge_native_bytes(after), limit, &output.value,
            &error);
        checked(rc, error);
        OperationPage result;
        result.done = output.value.done != 0;
        result.after.assign(reinterpret_cast<char *>(output.value.after),
            output.value.after_len);
        result.items.reserve(output.value.count);
        for (size_t i = 0; i < output.value.count; i++) {
            result.items.push_back(
                ngx_http_cache_purge_cpp_operation(output.value.items[i]));
        }
        return result;
    }

public:
    ngx_http_cache_purge_operations_t *
    native()
    {
        return &native_;
    }
    explicit Operations(
        ngx_http_cache_rocksdb_t *store, OperationLimits limits = {})
    {
        ngx_http_cache_purge_operation_limits_t raw{ limits.live_records,
            limits.live_selector_bytes };
        ngx_http_cache_rocksdb_error_t error;
        int rc =
            ngx_http_cache_purge_operations_init(&native_, store, &raw, &error);
        checked(rc, error);
    }
    Operation
    preview(const OperationInput &input, uint64_t now,
        uint32_t propagation_ms = 300000)
    {
        auto raw = ngx_http_cache_purge_native_input(input);
        Result output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_operations_preview(
            &native_, &raw, now, propagation_ms, &output.value, &error);
        checked(rc, error);
        return ngx_http_cache_purge_cpp_operation(output.value);
    }
    Operation
    accept(const OperationInput &input, uint64_t now,
        uint32_t propagation_ms = 300000)
    {
        auto raw = ngx_http_cache_purge_native_input(input);
        Result output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_operations_accept(
            &native_, &raw, now, propagation_ms, &output.value, &error);
        checked(rc, error);
        return ngx_http_cache_purge_cpp_operation(output.value);
    }
    Operation
    get(const std::string &id)
    {
        Result output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_operations_get(&native_,
            ngx_http_cache_purge_native_bytes(id), &output.value, &error);
        checked(rc, error);
        return ngx_http_cache_purge_cpp_operation(output.value);
    }
    OperationUsage
    usage()
    {
        ngx_http_cache_purge_operation_usage_t output{};
        ngx_http_cache_rocksdb_error_t error;
        int rc =
            ngx_http_cache_purge_operations_usage(&native_, &output, &error);
        checked(rc, error);
        return { output.live_records, output.live_selector_bytes,
            output.last_sequence, output.retired_sequence };
    }
    Operation
    activate(const std::string &id, uint64_t revision, uint64_t now)
    {
        Result output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_operations_activate(&native_,
            ngx_http_cache_purge_native_bytes(id), revision, now, &output.value,
            &error);
        checked(rc, error);
        return ngx_http_cache_purge_cpp_operation(output.value);
    }
    Operation
    checkpoint(const std::string &id, uint64_t revision,
        const OperationCheckpoint &point)
    {
        ngx_http_cache_purge_checkpoint_t raw{ point.id,
            ngx_http_cache_purge_native_bytes(point.bookmark), point.scan_done,
            point.next_run_ms, point.visits_delta, point.pending, point.failed,
            ngx_http_cache_purge_native_bytes(point.last_error) };
        Result output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_operations_checkpoint(&native_,
            ngx_http_cache_purge_native_bytes(id), revision, &raw,
            &output.value, &error);
        checked(rc, error);
        return ngx_http_cache_purge_cpp_operation(output.value);
    }
    Operation
    materialize(
        const std::string &id, uint64_t revision, uint64_t quiescent_sequence)
    {
        Result output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_operations_materialize(&native_,
            ngx_http_cache_purge_native_bytes(id), revision, quiescent_sequence,
            &output.value, &error);
        checked(rc, error);
        return ngx_http_cache_purge_cpp_operation(output.value);
    }
    OperationPage
    live(const std::string &after = "", size_t limit = 32)
    {
        return page(false, 0, after, limit);
    }
    OperationPage
    due(uint64_t now, const std::string &after = "", size_t limit = 32)
    {
        return page(true, now, after, limit);
    }
    size_t
    expire_propagation(uint64_t now, size_t limit = 32)
    {
        size_t output = 0;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_operations_expire(
            &native_, now, limit, &output, &error);
        checked(rc, error);
        return output;
    }
};
inline uint64_t
operation_control_bytes(const OperationInput &input)
{
    auto raw = ngx_http_cache_purge_native_input(input);
    return ngx_http_cache_purge_operation_charge(&raw);
}


} /* namespace ngx_http_cache_purge */


/* Copyright (C) Hanada */


namespace ngx_http_cache_purge {

class ControlImage {
    ngx_http_cache_purge_control_image_t *native_ = nullptr;
    explicit ControlImage(std::nullptr_t)
    {}

public:
    ControlImage(ControlImage &&other) noexcept : native_(other.native_)
    {
        other.native_ = nullptr;
    }
    ControlImage &
    operator=(ControlImage &&other) noexcept
    {
        if (this != &other) {
            ngx_http_cache_purge_control_image_free(&native_);
            native_ = other.native_;
            other.native_ = nullptr;
        }
        return *this;
    }
    ~ControlImage()
    {
        ngx_http_cache_purge_control_image_free(&native_);
    }
    ControlImage(const ControlImage &) = delete;
    ControlImage &operator=(const ControlImage &) = delete;
    friend ControlImage prepare_controls(
        ngx_http_cache_purge_control_writer_t *,
        const std::vector<Operation> &);
    friend uint64_t publish_controls(
        ngx_http_cache_purge_control_writer_t *, const ControlImage &);
};

/* Conversion-only bridge; image construction and publication execute in C. */
inline ControlImage
prepare_controls(ngx_http_cache_purge_control_writer_t *writer,
    const std::vector<Operation> &operations)
{
    std::vector<ngx_http_cache_purge_operation_t> raw;
    raw.reserve(operations.size());
    for (const auto &op : operations) {
        ngx_http_cache_purge_operation_t value{};
        value.input = ngx_http_cache_purge_native_input(op.input);
        value.sequence = op.sequence;
        value.stage = static_cast<uint32_t>(op.stage);
        value.propagation_until_ms = op.propagation_until_ms;
        value.propagate = op.propagate;
        raw.push_back(value);
    }
    ControlImage result(nullptr);
    ngx_http_cache_rocksdb_error_t error;
    int rc = ngx_http_cache_purge_control_prepare(
        writer, raw.data(), raw.size(), &result.native_, &error);
    if (rc) {
        throw IndexError(rc, error.message);
    }
    return result;
}
inline uint64_t
publish_controls(
    ngx_http_cache_purge_control_writer_t *writer, const ControlImage &image)
{
    uint64_t publication = 0;
    ngx_http_cache_rocksdb_error_t error;
    int rc = ngx_http_cache_purge_control_publish(
        writer, image.native_, &publication, &error);
    if (rc) {
        throw IndexError(rc, error.message);
    }
    return publication;
}
inline uint64_t
publish_controls(ngx_http_cache_purge_control_writer_t *writer,
    const std::vector<Operation> &operations)
{
    return publish_controls(writer, prepare_controls(writer, operations));
}

enum class Coordination : uint32_t {
    claiming = 1,
    loading,
    building,
    publishing,
    draining,
    ready
};

/* Conversion-only coordinator facade for consumers still being migrated. */
class Coordinator {
    ngx_http_cache_purge_coordinator_t *native_ = nullptr;
    static void
    checked(int rc, const ngx_http_cache_rocksdb_error_t &error)
    {
        if (rc) {
            throw IndexError(rc, error.message);
        }
    }

public:
    Coordinator(Operations &operations, void *shared, size_t shared_bytes)
    {
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_coordinator_open(
            operations.native(), shared, shared_bytes, &native_, &error);
        checked(rc, error);
    }
    ~Coordinator()
    {
        ngx_http_cache_purge_coordinator_close(&native_, nullptr);
    }
    Coordinator(const Coordinator &) = delete;
    Coordinator &operator=(const Coordinator &) = delete;
    bool
    step()
    {
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_coordinator_step(native_, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            return false;
        }
        checked(rc, error);
        return true;
    }
    Operation
    accept(const OperationInput &input, uint64_t now, uint32_t propagation_ms)
    {
        auto raw = ngx_http_cache_purge_native_input(input);
        struct Result {
            ngx_http_cache_purge_operation_t value{};
            ~Result()
            {
                ngx_http_cache_purge_operation_free(&value);
            }
        } output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_coordinator_accept(
            native_, &raw, now, propagation_ms, &output.value, &error);
        checked(rc, error);
        return ngx_http_cache_purge_cpp_operation(output.value);
    }
    void
    refresh()
    {
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_coordinator_refresh(native_, &error);
        checked(rc, error);
    }
    uint32_t
    writers(uint64_t sequence)
    {
        uint32_t count = 0;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_coordinator_writers(
            native_, sequence, &count, &error);
        checked(rc, error);
        return count;
    }
    Coordination
    state() const
    {
        uint32_t state;
        uint64_t publication;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_coordinator_status(
            native_, &state, &publication, &error);
        checked(rc, error);
        return static_cast<Coordination>(state);
    }
    uint64_t
    publication() const
    {
        uint32_t state;
        uint64_t publication;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_coordinator_status(
            native_, &state, &publication, &error);
        checked(rc, error);
        return publication;
    }
};


} /* namespace ngx_http_cache_purge */


/* Copyright (C) Hanada */
namespace ngx_http_cache_purge {
struct RootSpec {
    std::string path, zone;
    unsigned kind;
};
struct PublicationTarget {
    std::string root, address;
    FileId previous{};
};
/* Executor-thread filesystem policy; pinned roots are configuration-sized,
 * never an in-memory copy of the file index. Construction may perform I/O. */
class Publications {
    ngx_http_cache_purge_publications_t *native_ = nullptr;

public:
    ngx_http_cache_purge_publications_t *
    native()
    {
        return native_;
    }
    Publications(
        const std::vector<RootSpec> &roots, const std::string &database)
    {
        std::vector<ngx_http_cache_purge_root_t> raw;
        raw.reserve(roots.size());
        for (const auto &root : roots) {
            raw.push_back({ ngx_http_cache_purge_native_bytes(root.path),
                ngx_http_cache_purge_native_bytes(root.zone), root.kind });
        }
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_publications_open(
            raw.data(), raw.size(), database.c_str(), &native_, &error);
        ngx_http_cache_purge_index_checked(rc, error);
    }
    ~Publications()
    {
        ngx_http_cache_purge_publications_close(&native_, nullptr);
    }
    Publications(const Publications &) = delete;
    Publications &operator=(const Publications &) = delete;
    PublicationTarget
    target(Index &index, const std::string &root, const std::string &zone,
        const std::string &path)
    {
        struct Result {
            ngx_http_cache_purge_publication_target_t value{};
            ~Result()
            {
                ngx_http_cache_purge_publication_target_free(&value);
            }
        } output;
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_publications_target(native_,
            index.native(), ngx_http_cache_purge_native_bytes(root),
            ngx_http_cache_purge_native_bytes(zone),
            ngx_http_cache_purge_native_bytes(path), &output.value, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        PublicationTarget result{ ngx_http_cache_purge_cpp_bytes(
                                      output.value.root),
            ngx_http_cache_purge_cpp_bytes(output.value.address), {} };
        std::memcpy(result.previous.data(), output.value.previous, 16);
        return result;
    }
    Match
    prepare(Index &index, const Fill &fill, const std::string &temporary,
        int anchor_fd, int lease_fd, int temporary_fd, uint32_t peer_uid)
    {
        ngx_http_cache_purge_index_fill_t raw{};
        raw.file = ngx_http_cache_purge_native_file(fill.file);
        std::memcpy(raw.previous, fill.previous.data(), 16);
        ngx_http_cache_purge_index_match_t output{};
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_publications_prepare(native_,
            index.native(), &raw, ngx_http_cache_purge_native_bytes(temporary),
            anchor_fd, lease_fd, temporary_fd, peer_uid, &output, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        return ngx_http_cache_purge_cpp_match(output);
    }
    Match
    finish(Index &index, const FileId &id, uint64_t revision, int anchor_fd,
        int lease_fd, uint32_t peer_uid)
    {
        ngx_http_cache_purge_index_match_t output{};
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_purge_publications_finish(native_,
            index.native(), id.data(), revision, anchor_fd, lease_fd, peer_uid,
            &output, &error);
        ngx_http_cache_purge_index_checked(rc, error);
        return ngx_http_cache_purge_cpp_match(output);
    }
};
} /* namespace ngx_http_cache_purge */


/* Copyright (C) Hanada */

#include <cstring>
#include <string_view>

namespace ngx_http_cache_purge::protocol {

/*
 * Internal IPC v1, all integers big-endian. Frame: "NCP1", u16 version, u16
 * opcode, u64 correlation ID, u32 payload length, u32 result (0 in requests).
 * Correlation IDs are transport labels, not durable idempotency keys. Strings:
 * u32 length + bytes; IDs: 16 raw bytes; bools: one byte 0/1. No struct
 * padding, pointers, C++ objects or platform-sized fields on wire.
 */
struct Encoder {
    std::string data;
    void
    byte(unsigned value)
    {
        data += static_cast<char>(value);
    }
    void
    integer(uint64_t value, unsigned width)
    {
        for (unsigned n = width; n; --n) {
            byte(value >> ((n - 1) * 8));
        }
    }
    void
    string(const std::string &value)
    {
        integer(value.size(), 4);
        data += value;
    }
    void
    id(const FileId &value)
    {
        data.append(reinterpret_cast<const char *>(value.data()), value.size());
    }
};
struct Decoder {
    std::string_view data;
    size_t at = 0;
    std::string_view
    take(size_t count)
    {
        if (count > data.size() - at) {
            throw IndexError(
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "truncated IPC payload");
        }
        auto value = data.substr(at, count);
        at += count;
        return value;
    }
    uint64_t
    integer(unsigned width)
    {
        uint64_t value = 0;
        for (unsigned char c : take(width)) {
            value = (value << 8) | c;
        }
        return value;
    }
    bool
    flag()
    {
        auto value = integer(1);
        if (value > 1) {
            throw IndexError(
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid IPC boolean");
        }
        return value != 0;
    }
    std::string
    string(size_t limit = 32768)
    {
        auto size = integer(4);
        if (size > limit) {
            throw IndexError(
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC string exceeds limit");
        }
        return std::string(take(size));
    }
    FileId
    id()
    {
        FileId value;
        auto raw = take(value.size());
        std::memcpy(value.data(), raw.data(), value.size());
        return value;
    }
    void
    end()
    {
        if (at != data.size()) {
            throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "unexpected IPC payload suffix");
        }
    }
};
struct Frame {
    uint16_t opcode;
    uint64_t id;
    uint32_t size;
    uint32_t result;
};
inline Frame
frame(std::string_view value)
{
    Decoder d{ value };
    if (d.take(4) != "NCP1"
        || d.integer(2) != NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION)
    {
        throw IndexError(
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "unknown IPC protocol");
    }
    Frame f{ static_cast<uint16_t>(d.integer(2)), d.integer(8),
        static_cast<uint32_t>(d.integer(4)),
        static_cast<uint32_t>(d.integer(4)) };
    d.end();
    return f;
}
inline std::string
frame(uint16_t opcode, uint64_t id, const std::string &payload,
    uint32_t result = 0)
{
    Encoder e;
    e.data = "NCP1";
    e.integer(NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION, 2);
    e.integer(opcode, 2);
    e.integer(id, 8);
    e.integer(payload.size(), 4);
    e.integer(result, 4);
    e.data += payload;
    return e.data;
}
inline std::vector<std::string>
tags(Decoder &d)
{
    auto n = d.integer(1);
    if (n > 64) {
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC TAG count");
    }
    std::vector<std::string> result;
    result.reserve(n);
    while (n--) {
        result.push_back(d.string(1024));
    }
    return result;
}
inline void
tags(Encoder &e, const std::vector<std::string> &values)
{
    e.byte(values.size());
    for (const auto &value : values) {
        e.string(value);
    }
}
inline File
file(Decoder &d)
{
    File f;
    f.id = d.id();
    for (auto *field : { &f.key, &f.url, &f.host, &f.tag_namespace, &f.root,
             &f.zone, &f.path, &f.range, &f.vary })
    {
        *field = d.string();
    }
    f.metadata = d.string(16384);
    f.tags = tags(d);
    f.structured_key = d.flag();
    f.structured_url = d.flag();
    for (auto &part : f.components) {
        part = d.string();
    }
    return f;
}
inline void
file(Encoder &e, const File &f)
{
    e.id(f.id);
    for (auto *field : { &f.key, &f.url, &f.host, &f.tag_namespace, &f.root,
             &f.zone, &f.path, &f.range, &f.vary })
    {
        e.string(*field);
    }
    e.string(f.metadata);
    tags(e, f.tags);
    e.byte(f.structured_key);
    e.byte(f.structured_url);
    for (auto &part : f.components) {
        e.string(part);
    }
}
inline void
match(Encoder &e, const Match &m)
{
    e.id(m.id);
    e.integer(m.revision, 8);
    e.byte(static_cast<unsigned>(m.state));
}
inline Selector
selector(Decoder &d)
{
    Selector s;
    s.kind = static_cast<SelectorKind>(d.integer(1));
    s.value = d.string();
    s.scope = d.string();
    return s;
}
inline std::vector<ExpectedFile>
expected(Decoder &d)
{
    auto n = d.integer(1);
    if (!n || n > 32) {
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC batch count");
    }
    std::vector<ExpectedFile> result;
    result.reserve(n);
    while (n--) {
        auto id = d.id();
        result.push_back({ id, d.integer(8) });
    }
    return result;
}
inline OperationInput
operation_input(Decoder &d)
{
    OperationInput in;
    in.id = d.string(128);
    in.mode = static_cast<PurgeMode>(d.integer(1));
    in.issued_at_ms = d.integer(8);
    in.selector = selector(d);
    in.binding = d.string(256);
    return in;
}
inline void
operation(Encoder &e, const Operation &op)
{
    e.string(op.input.id);
    e.byte(static_cast<unsigned>(op.input.mode));
    e.integer(op.input.issued_at_ms, 8);
    e.byte(static_cast<unsigned>(op.input.selector.kind));
    e.string(op.input.selector.value);
    e.string(op.input.selector.scope);
    e.string(op.input.binding);
    for (auto n : { op.sequence, op.revision, op.accepted_at_ms,
             op.effective_at_ms, op.propagation_until_ms })
    {
        e.integer(n, 8);
    }
    e.byte(static_cast<unsigned>(op.stage));
    e.integer(op.propagation_ms, 4);
    e.byte(op.propagate);
    e.string(op.bookmark);
    e.byte(op.scan_done);
    for (auto n :
        { op.next_run_ms, op.visits, op.pending, op.failed, op.checkpoint_id })
    {
        e.integer(n, 8);
    }
    e.string(op.last_error);
}

} /* namespace ngx_http_cache_purge::protocol */

#endif /* _NGX_HTTP_CACHE_PURGE_TEST_HPP_INCLUDED_ */
