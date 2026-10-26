/* Copyright (C) Hanada */
#include "ngx_http_cache_purge_operations.hpp"
#include "ngx_http_cache_purge_protocol.hpp"
#include <openssl/sha.h>
#include <map>
#include <optional>

namespace ngx_http_cache_purge {
namespace {
namespace wire = protocol;
constexpr auto cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
using Value = std::optional<std::string>;
constexpr size_t max_record = 131072;

void require(bool condition, int code, const char *message) { if (!condition) throw IndexError(code, message); }
void checked(int rc, const ngx_http_cache_rocksdb_error_t &e) { if (rc) throw IndexError(rc, e.message); }
ngx_http_cache_rocksdb_bytes_t bytes(const std::string &s) { return {reinterpret_cast<const unsigned char *>(s.data()), s.size()}; }
std::string number(uint64_t n) { wire::Encoder e; e.integer(n, 8); return e.data; }
uint64_t integer(const std::string &s) {
    require(s.size() == 8, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid operation integer");
    wire::Decoder d{s}; return d.integer(8);
}
std::string hash(const std::string &s) {
    std::string result(SHA256_DIGEST_LENGTH, '\0');
    require(SHA256(reinterpret_cast<const unsigned char *>(s.data()), s.size(),
                   reinterpret_cast<unsigned char *>(result.data())) != nullptr,
            NGX_HTTP_CACHE_ROCKSDB_IO, "operation digest failed");
    return result;
}
std::string row(const std::string &id) { return "D" + id; }
std::string live_key(const Operation &op) { return "L" + number(op.sequence); }
std::string due_key(const Operation &op) { return "Q" + number(op.next_run_ms) + number(op.sequence); }
std::string expiry_key(const Operation &op) { return "E" + number(op.propagation_until_ms) + number(op.sequence); }
bool scheduled(const Operation &op) { return op.stage == OperationStage::active || op.stage == OperationStage::draining; }
bool resident(const Operation &op) { return op.stage != OperationStage::materialized || op.propagate; }
uint64_t charge(const OperationInput &in) { return 256 + in.id.size() + in.selector.value.size() + in.selector.scope.size() + in.binding.size(); }
void valid_id(const std::string &id) {
    require(!id.empty() && id.size() <= 128, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation ID length");
    for (unsigned char c : id) require(c > 32 && c < 127, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation ID byte");
}
void validate(const OperationInput &in) {
    valid_id(in.id);
    require(in.mode == PurgeMode::invalidate || in.mode == PurgeMode::remove, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid purge mode");
    require(in.issued_at_ms && in.binding.size() <= 256, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation timestamp/binding");
    auto kind = in.selector.kind;
    require(kind >= SelectorKind::key && kind <= SelectorKind::host_tag, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation selector");
    bool tag = kind == SelectorKind::namespace_tag || kind == SelectorKind::host_tag;
    require(in.selector.value.size() <= (tag ? 1024U : 32768U) && in.selector.scope.size() <= 32768,
            NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation selector exceeds limit");
    require(tag ? !in.selector.scope.empty() : in.selector.scope.empty(), NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid selector scope");
    require(!in.selector.value.empty() || kind == SelectorKind::key_prefix || kind == SelectorKind::url_prefix,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "empty operation selector");
}
std::string encode_input(const OperationInput &in) {
    wire::Encoder e; e.string(in.id); e.byte(static_cast<unsigned>(in.mode)); e.integer(in.issued_at_ms, 8);
    e.byte(static_cast<unsigned>(in.selector.kind)); e.string(in.selector.value); e.string(in.selector.scope); e.string(in.binding);
    return e.data;
}
OperationInput decode_input(wire::Decoder &d) {
    OperationInput in; in.id = d.string(128); in.mode = static_cast<PurgeMode>(d.integer(1)); in.issued_at_ms = d.integer(8);
    in.selector = wire::selector(d); in.binding = d.string(256); validate(in); return in;
}
std::string checkpoint_hash(const OperationCheckpoint &point) {
    wire::Encoder e; e.integer(point.id, 8); e.string(point.bookmark); e.byte(point.scan_done);
    for (auto n : {point.next_run_ms, point.visits_delta, point.pending, point.failed}) e.integer(n, 8);
    e.string(point.last_error); return hash(e.data);
}
struct Stored {
    Operation op;
    std::string checkpoint_digest;
};
std::string encode(const Stored &value) {
    const auto &op = value.op;
    wire::Encoder e; e.byte(1); e.data += encode_input(op.input);
    for (auto n : {op.sequence, op.revision, op.accepted_at_ms, op.effective_at_ms, op.propagation_until_ms}) e.integer(n, 8);
    e.byte(static_cast<unsigned>(op.stage)); e.integer(op.propagation_ms, 4); e.byte(op.propagate);
    e.string(op.bookmark); e.byte(op.scan_done);
    for (auto n : {op.next_run_ms, op.visits, op.pending, op.failed, op.checkpoint_id}) e.integer(n, 8);
    e.string(op.last_error); e.string(value.checkpoint_digest);
    require(e.data.size() <= max_record, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation record exceeds limit");
    return e.data;
}
Stored decode(const std::string &raw) {
    try {
        wire::Decoder d{raw}; Stored value; auto &op = value.op;
        require(d.integer(1) == 1, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid operation record version");
        op.input = decode_input(d);
        for (auto *n : {&op.sequence, &op.revision, &op.accepted_at_ms, &op.effective_at_ms, &op.propagation_until_ms}) *n = d.integer(8);
        op.stage = static_cast<OperationStage>(d.integer(1)); op.propagation_ms = d.integer(4); op.propagate = d.flag();
        op.bookmark = d.string(33024); op.scan_done = d.flag();
        for (auto *n : {&op.next_run_ms, &op.visits, &op.pending, &op.failed, &op.checkpoint_id}) *n = d.integer(8);
        op.last_error = d.string(512); value.checkpoint_digest = d.string(32); d.end();
        require(op.sequence && op.revision && op.accepted_at_ms
                && op.stage >= OperationStage::preparing && op.stage <= OperationStage::materialized
                && (op.checkpoint_id ? value.checkpoint_digest.size() == 32 : value.checkpoint_digest.empty()),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent operation record");
        require(op.stage == OperationStage::preparing ? !op.effective_at_ms && !op.propagate
                : op.effective_at_ms != 0, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent operation activation");
        require(op.stage == OperationStage::preparing
                ? !op.propagation_until_ms && !op.scan_done && !op.checkpoint_id && !op.next_run_ms
                : op.effective_at_ms <= UINT64_MAX - op.propagation_ms
                  && op.propagation_until_ms == op.effective_at_ms + op.propagation_ms,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "inconsistent operation clock/progress");
        require(!op.propagate || op.propagation_ms != 0, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "zero-window propagation record");
        require(op.stage != OperationStage::active || !op.scan_done, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "active operation already enumerated");
        require(op.stage != OperationStage::draining || op.scan_done, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "draining operation has unfinished scan");
        require(op.stage != OperationStage::materialized || (op.scan_done && !op.pending && !op.failed),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "incomplete materialized operation");
        return value;
    } catch (const IndexError &e) { throw IndexError(NGX_HTTP_CACHE_ROCKSDB_CORRUPT, e.what()); }
}

Value read(ngx_http_cache_rocksdb_t *store, const std::string &key, ngx_http_cache_rocksdb_cursor_t *view = nullptr) {
    std::string data(512, '\0');
    for (;;) {
        ngx_http_cache_rocksdb_buffer_t buffer{reinterpret_cast<unsigned char *>(data.data()), data.size(), 0};
        ngx_http_cache_rocksdb_error_t error;
        int rc = view ? ngx_http_cache_rocksdb_cursor_get(view, cf, bytes(key), &buffer, &error)
                      : ngx_http_cache_rocksdb_get(store, cf, bytes(key), &buffer, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) return {};
        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUFFER) {
            require(buffer.length <= max_record, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "oversized operation data");
            data.resize(buffer.length); continue;
        }
        checked(rc, error); data.resize(buffer.length); return data;
    }
}
class Tx {
    ngx_http_cache_rocksdb_t *store_;
    std::map<std::string, Value> before_, changes_;
public:
    explicit Tx(ngx_http_cache_rocksdb_t *store) : store_(store) {}
    Value get(const std::string &key) {
        auto changed = changes_.find(key); if (changed != changes_.end()) return changed->second;
        auto old = before_.find(key); if (old != before_.end()) return old->second;
        auto value = read(store_, key); before_[key] = value; return value;
    }
    std::string need(const std::string &key) {
        auto value = get(key); require(value.has_value(), NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing operation metadata"); return *value;
    }
    void put(const std::string &key, const std::string &value) { changes_[key] = value; }
    void del(const std::string &key) { changes_[key] = {}; }
    Stored load(const std::string &id) {
        valid_id(id); auto raw = get(row(id)); require(raw.has_value(), NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND, "operation not found");
        auto result = decode(*raw); const auto &op = result.op;
        require(op.input.id == id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "operation ID mismatch");
        if (resident(op)) require(need(live_key(op)) == id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing resident operation link");
        if (scheduled(op)) require(need(due_key(op)) == id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing scheduled operation link");
        if (op.propagate) require(need(expiry_key(op)) == id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing propagation expiry link");
        return result;
    }
    void commit() {
        if (changes_.empty()) return;
        std::vector<ngx_http_cache_rocksdb_condition_t> conditions;
        std::vector<ngx_http_cache_rocksdb_mutation_t> mutations;
        const std::string empty;
        for (const auto &entry : before_) conditions.push_back({cf, bytes(entry.first), bytes(entry.second ? *entry.second : empty), entry.second ? 1 : 0});
        for (const auto &entry : changes_) mutations.push_back({cf, entry.second ? NGX_HTTP_CACHE_ROCKSDB_PUT : NGX_HTTP_CACHE_ROCKSDB_DELETE,
                                                              bytes(entry.first), bytes(entry.second ? *entry.second : empty)});
        ngx_http_cache_rocksdb_error_t error;
        checked(ngx_http_cache_rocksdb_write(store_, mutations.data(), mutations.size(), conditions.data(), conditions.size(), &error), error);
    }
};
struct Cursor {
    ngx_http_cache_rocksdb_cursor_t *handle = nullptr;
    Cursor(ngx_http_cache_rocksdb_t *store, const std::string &prefix, const std::string &after = "") {
        ngx_http_cache_rocksdb_error_t error;
        checked(ngx_http_cache_rocksdb_cursor_open(store, cf, bytes(prefix), bytes(after), &handle, &error), error);
    }
    ~Cursor() { ngx_http_cache_rocksdb_error_t error; ngx_http_cache_rocksdb_cursor_close(&handle, &error); }
    bool next(std::string &key, std::string &value) {
        std::array<unsigned char, 256> k{}, v{};
        ngx_http_cache_rocksdb_buffer_t kb{k.data(), k.size(), 0}, vb{v.data(), v.size(), 0};
        ngx_http_cache_rocksdb_error_t error;
        int rc = ngx_http_cache_rocksdb_cursor_next(handle, &kb, &vb, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) return false;
        require(rc != NGX_HTTP_CACHE_ROCKSDB_BUFFER, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "oversized operation queue row");
        checked(rc, error); key.assign(reinterpret_cast<char *>(k.data()), kb.length); value.assign(reinterpret_cast<char *>(v.data()), vb.length);
        return true;
    }
};
void revision(const Operation &op, uint64_t expected) {
    require(op.revision == expected, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "operation revision changed");
}
void advance(Operation &op) {
    require(op.revision != UINT64_MAX, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation revision exhausted"); ++op.revision;
}
void save(Tx &tx, const Stored &before, const Stored &after) {
    const auto &a = before.op, &b = after.op;
    if (scheduled(a)) {
        require(tx.need(due_key(a)) == a.input.id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "operation schedule mismatch"); tx.del(due_key(a));
    }
    if (scheduled(b)) tx.put(due_key(b), b.input.id);
    if (a.propagate && !b.propagate) {
        require(tx.need(expiry_key(a)) == a.input.id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "operation propagation expiry mismatch"); tx.del(expiry_key(a));
    }
    if (!a.propagate && b.propagate) tx.put(expiry_key(b), b.input.id);
    if (resident(a) && !resident(b)) {
        require(tx.need(live_key(a)) == a.input.id, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "operation resident index mismatch");
        auto count = integer(tx.need("M:op-live")), size = integer(tx.need("M:op-bytes"));
        require(count && size >= charge(a.input), NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "operation budget underflow");
        tx.put("M:op-live", number(count - 1)); tx.put("M:op-bytes", number(size - charge(a.input))); tx.del(live_key(a));
    }
    tx.put(row(b.input.id), encode(after));
}
OperationPage page(ngx_http_cache_rocksdb_t *store, bool ready, uint64_t now, const std::string &after, size_t limit) {
    const std::string prefix = ready ? "Q" : "L";
    require(limit && limit <= 32, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation page size");
    require(after.empty() || (after[0] == prefix[0] && after.size() == (ready ? 17U : 9U)),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation queue bookmark");
    Cursor cursor(store, prefix, after); OperationPage result; result.after = after;
    size_t remaining = 262144;
    std::string key, value;
    while (result.items.size() < limit) {
        if (!cursor.next(key, value)) { result.done = true; break; }
        require(key.size() == (ready ? 17U : 9U), NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid operation queue key");
        if (ready && integer(key.substr(1, 8)) > now) { result.done = true; break; }
        auto raw = read(store, row(value), cursor.handle);
        require(raw.has_value(), NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "queue has no operation");
        if (raw->size() > remaining && !result.items.empty()) break;
        remaining -= raw->size();
        auto op = decode(*raw).op;
        require(op.input.id == value && (ready ? scheduled(op) && key == due_key(op) : resident(op) && key == live_key(op)),
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "queue operation mismatch");
        result.items.push_back(std::move(op)); result.after = key;
    }
    return result;
}
} // namespace

Operations::Operations(ngx_http_cache_rocksdb_t *store, OperationLimits limits) : store_(store), limits_(limits) {
    require(limits.live_records && limits.live_records <= 65536 && limits.live_selector_bytes >= 1024
            && limits.live_selector_bytes <= 64 * 1024 * 1024, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation capacity");
    Tx tx(store_);
    require(tx.need("M:index") == "2", NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "operation ledger requires supported file index");
    auto schema = tx.get("M:operations");
    if (!schema) {
        for (const char *prefix : {"D", "L", "Q", "E"}) {
            Cursor cursor(store_, prefix); std::string key, value;
            require(!cursor.next(key, value), NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unversioned operation ledger");
        }
        for (const char *key : {"M:op-next", "M:op-live", "M:op-bytes"})
            require(!tx.get(key), NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unversioned operation metadata");
        tx.put("M:operations", "1"); tx.put("M:op-next", number(1));
        tx.put("M:op-live", number(0)); tx.put("M:op-bytes", number(0)); tx.commit();
    } else {
        require(*schema == "1", NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unsupported operation ledger schema");
        require(integer(tx.need("M:op-next")) != 0, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid operation sequence allocator");
        // Recovery must retain existing controls even if an administrator has
        // lowered admission limits. Only new admissions are capacity checked.
        integer(tx.need("M:op-live")); integer(tx.need("M:op-bytes"));
    }
}
Operation Operations::get(const std::string &id) { Tx tx(store_); return tx.load(id).op; }
OperationUsage Operations::usage() {
    Tx tx(store_); auto next = integer(tx.need("M:op-next"));
    require(next != 0, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid operation sequence allocator");
    return {integer(tx.need("M:op-live")), integer(tx.need("M:op-bytes")), next - 1};
}
uint64_t operation_control_bytes(const OperationInput &input) { return charge(input); }
static Operation preview_accept(Tx &tx, const OperationInput &input, uint64_t now, uint32_t propagation_ms, OperationLimits limits) {
    validate(input); require(now != 0, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation clock");
    if (auto old = tx.get(row(input.id))) {
        auto value = tx.load(input.id);
        require(encode_input(value.op.input) == encode_input(input), NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "operation ID reused with different input");
        return value.op;
    }
    uint64_t count = integer(tx.need("M:op-live")), size = integer(tx.need("M:op-bytes"));
    require(count < limits.live_records && size <= limits.live_selector_bytes
            && charge(input) <= limits.live_selector_bytes - size, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "resident operation capacity exhausted");
    auto sequence = integer(tx.need("M:op-next"));
    require(sequence && sequence != UINT64_MAX, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation sequence exhausted");
    Operation op; op.input = input; op.sequence = sequence; op.revision = 1;
    op.accepted_at_ms = now; op.propagation_ms = propagation_ms;
    return op;
}
Operation Operations::preview(const OperationInput &input, uint64_t now, uint32_t propagation_ms) {
    Tx tx(store_); return preview_accept(tx, input, now, propagation_ms, limits_);
}
Operation Operations::accept(const OperationInput &input, uint64_t now, uint32_t propagation_ms) {
    Tx tx(store_); auto op = preview_accept(tx, input, now, propagation_ms, limits_);
    if (tx.get(row(input.id))) return op;
    uint64_t count = integer(tx.need("M:op-live")), size = integer(tx.need("M:op-bytes")), sequence = op.sequence;
    tx.put("M:op-next", number(sequence + 1)); tx.put("M:op-live", number(count + 1)); tx.put("M:op-bytes", number(size + charge(input)));
    tx.put(row(input.id), encode({op, ""})); tx.put(live_key(op), input.id); tx.commit(); return op;
}
Operation Operations::activate(const std::string &id, uint64_t expected, uint64_t now) {
    require(now != 0, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid activation clock");
    Tx tx(store_); auto before = tx.load(id); revision(before.op, expected);
    if (before.op.stage != OperationStage::preparing) return before.op;
    auto after = before; auto &op = after.op;
    require(now <= UINT64_MAX - op.propagation_ms, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "propagation deadline overflow");
    op.stage = OperationStage::active; op.effective_at_ms = now; op.propagation_until_ms = now + op.propagation_ms;
    op.propagate = op.propagation_ms != 0; op.next_run_ms = now; advance(op);
    save(tx, before, after); tx.commit(); return op;
}
Operation Operations::checkpoint(const std::string &id, uint64_t expected, const OperationCheckpoint &point) {
    require(point.id && point.bookmark.size() <= 33024 && point.last_error.size() <= 512,
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid operation checkpoint");
    Tx tx(store_); auto before = tx.load(id); auto digest = checkpoint_hash(point);
    if (point.id == before.op.checkpoint_id) {
        require(before.checkpoint_digest == digest, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "checkpoint ID reused with different progress");
        return before.op;
    }
    revision(before.op, expected);
    require(scheduled(before.op) && point.id > before.op.checkpoint_id, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "operation cannot accept this checkpoint");
    require(!before.op.scan_done || point.scan_done, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "completed enumeration cannot regress");
    require(point.visits_delta <= UINT64_MAX - before.op.visits, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation visit counter overflow");
    auto after = before; auto &op = after.op;
    op.bookmark = point.bookmark; op.scan_done = point.scan_done; op.next_run_ms = point.next_run_ms;
    op.visits += point.visits_delta; op.pending = point.pending; op.failed = point.failed;
    op.last_error = point.last_error; op.checkpoint_id = point.id; after.checkpoint_digest = digest;
    if (op.scan_done) op.stage = OperationStage::draining;
    advance(op); save(tx, before, after); tx.commit(); return op;
}
Operation Operations::materialize(const std::string &id, uint64_t expected, uint64_t quiescent_sequence) {
    Tx tx(store_); auto before = tx.load(id); revision(before.op, expected);
    if (before.op.stage == OperationStage::materialized) return before.op;
    require(before.op.stage == OperationStage::draining && before.op.scan_done && !before.op.pending && !before.op.failed
            && quiescent_sequence >= before.op.sequence, NGX_HTTP_CACHE_ROCKSDB_BUSY, "operation still has files/readers/writers to drain");
    auto after = before; after.op.stage = OperationStage::materialized; advance(after.op);
    save(tx, before, after); tx.commit(); return after.op;
}
OperationPage Operations::live(const std::string &after, size_t limit) { return page(store_, false, 0, after, limit); }
OperationPage Operations::due(uint64_t now, const std::string &after, size_t limit) { return page(store_, true, now, after, limit); }
size_t Operations::expire_propagation(uint64_t now, size_t limit) {
    require(limit && limit <= 32, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid expiration batch size");
    Cursor cursor(store_, "E"); std::string key, id; size_t expired = 0;
    Tx tx(store_); size_t remaining = 262144;
    while (expired < limit && cursor.next(key, id)) {
        require(key.size() == 17, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid propagation expiry key");
        if (integer(key.substr(1, 8)) > now) break;
        auto before = tx.load(id);
        size_t cost = encode(before).size() * 2 + 512;
        if (cost > remaining && expired) break;
        require(cost <= remaining, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "operation expiration batch budget");
        remaining -= cost;
        require(before.op.propagate && key == expiry_key(before.op), NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "stale propagation expiry index");
        auto after = before; after.op.propagate = false; advance(after.op);
        save(tx, before, after); ++expired;
    }
    tx.commit();
    return expired;
}

} // namespace ngx_http_cache_purge
