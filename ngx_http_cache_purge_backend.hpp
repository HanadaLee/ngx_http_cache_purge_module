/*
 * Copyright (C) Hanada
 */

#ifndef _NGX_HTTP_CACHE_PURGE_BACKEND_HPP_INCLUDED_
#define _NGX_HTTP_CACHE_PURGE_BACKEND_HPP_INCLUDED_

/* Transitional C++ contracts, removed as the backend is converted to C. */
#include "ngx_http_cache_purge_internal.h"

/* Copyright (C) Hanada */

#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace ngx_http_cache_purge {

using FileId = std::array<unsigned char, 16>;
enum class FileState : uint8_t { prepared = 1, committed = 2, retired = 3 };

class IndexError : public std::runtime_error {
public:
    int code;
    IndexError(int value, const std::string &message) : std::runtime_error(message), code(value) {}
};

struct File {
    FileId id{};
    FileState state = FileState::prepared;
    uint64_t revision = 0;
    std::string key, url, host, tag_namespace;
    std::string root, zone, path, range, vary;
    std::vector<std::string> tags;
    std::string metadata; /* bounded, versioned file-policy data owned by the component */
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

enum class SelectorKind : uint8_t { key, url, key_prefix, url_prefix, host, namespace_tag, host_tag };
struct Selector {
    SelectorKind kind = SelectorKind::url;
    std::string value;
    std::string scope; /* namespace or host for the two TAG selectors */
};

class Query {
public:
    ~Query();
    Query(const Query &) = delete;
    Query &operator=(const Query &) = delete;
    bool next(Match &match);
    std::string bookmark() const; /* persist only after the returned match is handled */
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit Query(std::unique_ptr<Impl> impl);
    friend class Index;
};

/* Internal C++ API, used on the component executor thread. The store outlives
 * Index and Query. IndexError carries a store result code; the component's C
 * entry point must catch it (and allocation failures) before returning to C. */
class Index {
public:
    explicit Index(ngx_http_cache_rocksdb_t *store);
    Match prepare(const File &file, const FileId &expected_previous = {});
    std::vector<Match> prepare_batch(const std::vector<Fill> &files);
    Match commit(const FileId &id, uint64_t expected_revision);
    std::vector<Match> commit_batch(const std::vector<ExpectedFile> &files);
    /* Native file publication keeps its durable lease after the DB commit.
     * Publish only after the file's readable marker is durable. These APIs
     * perform no filesystem operations and do not prove writer quiescence. */
    Match commit_pending(const FileId &id, uint64_t expected_revision);
    Match publish(const FileId &id, uint64_t expected_revision);
    Match discard_publication(const FileId &id, uint64_t expected_revision);
    std::optional<Match> current(const std::string &root, const std::string &path);
    bool has_fill_intent(const FileId &id);
    Match abort_prepare(const FileId &id, uint64_t expected_revision);
    Match retire(const FileId &id, uint64_t expected_revision);
    std::vector<Match> retire_batch(const std::vector<ExpectedFile> &files);
    Match update_metadata(const FileId &id, uint64_t expected_revision,
                          const std::string &metadata, const std::vector<std::string> &tags);
    File get(const FileId &id);
    void add_url(const std::string &key, const std::string &url);
    void collect_file(const FileId &id, uint64_t expected_revision);
    void collect_files(const std::vector<ExpectedFile> &files);
    size_t collect_keys(size_t edge_budget = 8);
    std::unique_ptr<Query> query(const Selector &selector, const std::string &bookmark = "");
    /* Includes PREPARED and COMMITTED-but-unpublished instances. */
    std::unique_ptr<Query> pending_fills(const std::string &bookmark = "");
    std::unique_ptr<Query> retired_files(const std::string &bookmark = "");
private:
    ngx_http_cache_rocksdb_t *store_;
};

} // namespace ngx_http_cache_purge


/* Copyright (C) Hanada */


namespace ngx_http_cache_purge {

enum class PurgeMode : uint8_t { invalidate = 1, remove = 2 };
enum class OperationStage : uint8_t { preparing = 1, active = 2, draining = 3, materialized = 4 };

struct OperationInput {
    std::string id;
    Selector selector;
    PurgeMode mode = PurgeMode::invalidate;
    uint64_t issued_at_ms = 0;
    std::string binding; // Frozen configuration/authorization contract, supplied by admission.
};

struct Operation {
    OperationInput input;
    uint64_t sequence = 0; // Ordering of control operations; never changes a cache key.
    uint64_t revision = 0;
    OperationStage stage = OperationStage::preparing;
    uint64_t accepted_at_ms = 0, effective_at_ms = 0, propagation_until_ms = 0;
    uint32_t propagation_ms = 300000;
    bool propagate = false;
    std::string bookmark;
    bool scan_done = false;
    uint64_t next_run_ms = 0;
    uint64_t visits = 0; // Attempts, not a deduplicated object count.
    uint64_t pending = 0, failed = 0;
    uint64_t checkpoint_id = 0;
    std::string last_error;
};

struct OperationCheckpoint {
    uint64_t id = 0; // Strictly increasing per operation; replay of the last ID is idempotent.
    std::string bookmark;
    bool scan_done = false;
    uint64_t next_run_ms = 0;
    uint64_t visits_delta = 0;
    uint64_t pending = 0, failed = 0; // Absolute outstanding work counts.
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
};

/* Durable ledger, called only by the single component executor. It does not
 * publish SHM, access files or prove that old readers/writers have drained.
 * activate follows real FENCE publication. materialize requires a verified
 * quiescence watermark from the future native-cache coordinator; EOF alone is
 * insufficient. Do not expose these transitions as client assertions in HTTP.
 * All accepted operations retain their idempotency record on disk, even after
 * their bounded resident-control slot is released. */
class Operations {
public:
    explicit Operations(ngx_http_cache_rocksdb_t *store, OperationLimits limits = {});
    Operation preview(const OperationInput &input, uint64_t now_ms, uint32_t propagation_ms = 300000);
    Operation accept(const OperationInput &input, uint64_t now_ms, uint32_t propagation_ms = 300000);
    Operation get(const std::string &id);
    OperationUsage usage(); // Recovery verifies this budget against paged live records.
    Operation activate(const std::string &id, uint64_t revision, uint64_t now_ms);
    Operation checkpoint(const std::string &id, uint64_t revision, const OperationCheckpoint &checkpoint);
    Operation materialize(const std::string &id, uint64_t revision, uint64_t quiescent_sequence);
    OperationPage live(const std::string &after = "", size_t limit = 32);
    OperationPage due(uint64_t now_ms, const std::string &after = "", size_t limit = 32);
    size_t expire_propagation(uint64_t now_ms, size_t limit = 32);
private:
    ngx_http_cache_rocksdb_t *store_;
    OperationLimits limits_;
};

uint64_t operation_control_bytes(const OperationInput &input);

} // namespace ngx_http_cache_purge


/* Copyright (C) Hanada */


namespace ngx_http_cache_purge {

class ControlImage {
public:
    ControlImage(ControlImage &&) noexcept;
    ControlImage &operator=(ControlImage &&) noexcept;
    ~ControlImage();
    ControlImage(const ControlImage &) = delete;
    ControlImage &operator=(const ControlImage &) = delete;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit ControlImage(std::unique_ptr<Impl> impl);
    friend ControlImage prepare_controls(ngx_http_cache_purge_control_writer_t *, const std::vector<Operation> &);
    friend uint64_t publish_controls(ngx_http_cache_purge_control_writer_t *, const ControlImage &);
};

/* Build before closing the serving gate when admission ordering permits it.
 * A prepared image binds its owner and prior publication; it cannot overwrite
 * an intervening update. BUSY publication can retry the same prepared image. */
ControlImage prepare_controls(ngx_http_cache_purge_control_writer_t *writer,
                              const std::vector<Operation> &operations);
uint64_t publish_controls(ngx_http_cache_purge_control_writer_t *writer, const ControlImage &image);

/* Rebuild an immutable, bounded image from live control operations only. The
 * full file index never enters this structure. Publication requires a closed
 * gate. BUSY means the inactive bank is still pinned; retry without opening
 * the gate or discarding the pending operation. No cached key is rewritten. */
uint64_t publish_controls(ngx_http_cache_purge_control_writer_t *writer,
                          const std::vector<Operation> &operations);

} // namespace ngx_http_cache_purge


/* Copyright (C) Hanada */


namespace ngx_http_cache_purge {

enum class Coordination : uint32_t { claiming = 1, loading, building, publishing, draining, ready };

/* Single executor only, while that executor holds the DB owner lease.
 * A step advances at most one recovery page or one publication transition.
 * Controls may be readable while operations remain PREPARING. This class does
 * not attest native cache hooks, activate operations or complete file work. */
class Coordinator {
public:
    Coordinator(Operations &operations, void *shared, size_t shared_bytes);
    ~Coordinator();
    Coordinator(const Coordinator &) = delete;
    Coordinator &operator=(const Coordinator &) = delete;
    bool step(); // false means a live reader/registration still blocks progress.
    Operation accept(const OperationInput &input, uint64_t now_ms, uint32_t propagation_ms);
    void refresh(); // Call after durable control changes; old stronger constraints remain until publication.
    Coordination state() const { return state_; }
    uint64_t publication() const { return publication_; }
private:
    Operations &operations_;
    void *shared_;
    size_t shared_bytes_;
    ngx_http_cache_purge_control_writer_t *writer_ = nullptr;
    Coordination state_ = Coordination::claiming;
    OperationUsage expected_{};
    std::vector<Operation> controls_;
    std::string after_;
    uint64_t loaded_bytes_ = 0, publication_ = 0;
    std::unique_ptr<ControlImage> pending_;
    void load_begin();
};

} // namespace ngx_http_cache_purge


/* Copyright (C) Hanada */
namespace ngx_http_cache_purge {
struct RootSpec { std::string path, zone; unsigned kind; };
struct PublicationTarget { std::string root, address; FileId previous{}; };
/* Executor-thread filesystem policy; pinned roots are configuration-sized,
 * never an in-memory copy of the file index. Construction may perform I/O. */
class Publications {
public:
    Publications(const std::vector<RootSpec> &roots, const std::string &database);
    ~Publications();
    PublicationTarget target(Index &index, const std::string &root,
                             const std::string &zone, const std::string &path);
    Match prepare(Index &index, Fill fill, const std::string &temporary,
                  int anchor_fd, int lease_fd, int temporary_fd, uint32_t peer_uid);
    /* Caller must have finished/abandoned native writing and retain the lease.
     * Reconcile the actual final instance; do not trust a reported rename code.
     * A committed result proves identity publication, not purge freshness. */
    Match finish(Index &index, const FileId &id, uint64_t prepared_revision,
                 int anchor_fd, int lease_fd, uint32_t peer_uid);
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}


/* Copyright (C) Hanada */

#include <cstring>
#include <string_view>

namespace ngx_http_cache_purge::protocol {

/* Internal IPC v1, all integers big-endian. Frame: "NCP1", u16 version,
 * u16 opcode, u64 correlation ID, u32 payload length, u32 result (0 in requests).
 * Correlation IDs are transport labels, not durable idempotency keys.
 * Strings: u32 length + bytes; IDs: 16 raw bytes; bools: one byte 0/1.
 * No struct padding, pointers, C++ objects or platform-sized fields on wire. */
struct Encoder {
    std::string data;
    void byte(unsigned value) { data += static_cast<char>(value); }
    void integer(uint64_t value, unsigned width) {
        for (unsigned n = width; n; --n) byte(value >> ((n - 1) * 8));
    }
    void string(const std::string &value) { integer(value.size(), 4); data += value; }
    void id(const FileId &value) { data.append(reinterpret_cast<const char *>(value.data()), value.size()); }
};
struct Decoder {
    std::string_view data;
    size_t at = 0;
    std::string_view take(size_t count) {
        if (count > data.size() - at) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "truncated IPC payload");
        auto value = data.substr(at, count); at += count; return value;
    }
    uint64_t integer(unsigned width) {
        uint64_t value = 0;
        for (unsigned char c : take(width)) value = (value << 8) | c;
        return value;
    }
    bool flag() {
        auto value = integer(1);
        if (value > 1) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid IPC boolean");
        return value != 0;
    }
    std::string string(size_t limit = 32768) {
        auto size = integer(4);
        if (size > limit) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC string exceeds limit");
        return std::string(take(size));
    }
    FileId id() { FileId value; auto raw = take(value.size()); std::memcpy(value.data(), raw.data(), value.size()); return value; }
    void end() {
        if (at != data.size()) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "unexpected IPC payload suffix");
    }
};
struct Frame { uint16_t opcode; uint64_t id; uint32_t size; uint32_t result; };
inline Frame frame(std::string_view value) {
    Decoder d{value};
    if (d.take(4) != "NCP1" || d.integer(2) != NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION)
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "unknown IPC protocol");
    Frame f{static_cast<uint16_t>(d.integer(2)), d.integer(8), static_cast<uint32_t>(d.integer(4)),
            static_cast<uint32_t>(d.integer(4))};
    d.end(); return f;
}
inline std::string frame(uint16_t opcode, uint64_t id, const std::string &payload, uint32_t result = 0) {
    Encoder e; e.data = "NCP1"; e.integer(NGX_HTTP_CACHE_PURGE_PROTOCOL_VERSION, 2);
    e.integer(opcode, 2); e.integer(id, 8); e.integer(payload.size(), 4); e.integer(result, 4);
    e.data += payload; return e.data;
}
inline std::vector<std::string> tags(Decoder &d) {
    auto n = d.integer(1);
    if (n > 64) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC TAG count");
    std::vector<std::string> result; result.reserve(n);
    while (n--) result.push_back(d.string(1024));
    return result;
}
inline void tags(Encoder &e, const std::vector<std::string> &values) {
    e.byte(values.size()); for (const auto &value : values) e.string(value);
}
inline File file(Decoder &d) {
    File f; f.id = d.id();
    for (auto *field : {&f.key, &f.url, &f.host, &f.tag_namespace, &f.root, &f.zone, &f.path, &f.range, &f.vary})
        *field = d.string();
    f.metadata = d.string(16384); f.tags = tags(d);
    f.structured_key = d.flag(); f.structured_url = d.flag();
    for (auto &part : f.components) part = d.string();
    return f;
}
inline void file(Encoder &e, const File &f) {
    e.id(f.id);
    for (auto *field : {&f.key, &f.url, &f.host, &f.tag_namespace, &f.root, &f.zone, &f.path, &f.range, &f.vary})
        e.string(*field);
    e.string(f.metadata); tags(e, f.tags); e.byte(f.structured_key); e.byte(f.structured_url);
    for (auto &part : f.components) e.string(part);
}
inline void match(Encoder &e, const Match &m) { e.id(m.id); e.integer(m.revision, 8); e.byte(static_cast<unsigned>(m.state)); }
inline Selector selector(Decoder &d) {
    Selector s; s.kind = static_cast<SelectorKind>(d.integer(1)); s.value = d.string(); s.scope = d.string(); return s;
}
inline std::vector<ExpectedFile> expected(Decoder &d) {
    auto n = d.integer(1);
    if (!n || n > 32) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_LIMIT, "IPC batch count");
    std::vector<ExpectedFile> result; result.reserve(n);
    while (n--) { auto id = d.id(); result.push_back({id, d.integer(8)}); }
    return result;
}
inline OperationInput operation_input(Decoder &d) {
    OperationInput in; in.id = d.string(128); in.mode = static_cast<PurgeMode>(d.integer(1));
    in.issued_at_ms = d.integer(8); in.selector = selector(d); in.binding = d.string(256); return in;
}
inline void operation(Encoder &e, const Operation &op) {
    e.string(op.input.id); e.byte(static_cast<unsigned>(op.input.mode)); e.integer(op.input.issued_at_ms, 8);
    e.byte(static_cast<unsigned>(op.input.selector.kind)); e.string(op.input.selector.value);
    e.string(op.input.selector.scope); e.string(op.input.binding);
    for (auto n : {op.sequence, op.revision, op.accepted_at_ms, op.effective_at_ms, op.propagation_until_ms}) e.integer(n, 8);
    e.byte(static_cast<unsigned>(op.stage)); e.integer(op.propagation_ms, 4); e.byte(op.propagate);
    e.string(op.bookmark); e.byte(op.scan_done);
    for (auto n : {op.next_run_ms, op.visits, op.pending, op.failed, op.checkpoint_id}) e.integer(n, 8);
    e.string(op.last_error);
}

} // namespace ngx_http_cache_purge::protocol

#endif /* _NGX_HTTP_CACHE_PURGE_BACKEND_HPP_INCLUDED_ */
