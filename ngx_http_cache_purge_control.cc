/* Copyright (C) Hanada */

#include "ngx_http_cache_purge_backend.hpp"
#include <pthread.h>
#include <signal.h>
#include <sys/random.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string_view>
#include <thread>

#if defined(NGX_HTTP_CACHE_PURGE_CONTROL_FAULT_TEST)
static int fault_point, fault_fd;
extern "C" void ngx_http_cache_purge_control_test_fault(int point, int fd) { fault_point = point; fault_fd = fd; }
static void fault(int point) {
    if (point != fault_point) return;
    const char byte = 1;
    if (write(fault_fd, &byte, 1) != 1) _exit(90);
    for (;;) pause();
}
#else
static void fault(int) {}
#endif

namespace {
using namespace ngx_http_cache_purge;
constexpr uint64_t magic = 0x4e43504354524c31ULL;
constexpr uint32_t none = UINT32_MAX;
static_assert(__atomic_always_lock_free(8, nullptr), "control table requires lock-free 64-bit atomics");
template<class T> T load(const T *value) { return __atomic_load_n(value, __ATOMIC_SEQ_CST); }
template<class T> void put(T *value, T next) { __atomic_store_n(value, next, __ATOMIC_SEQ_CST); }
size_t align64(size_t value) { return (value + 63) & ~size_t(63); }
void require(bool ok, int code, const char *message) { if (!ok) throw IndexError(code, message); }
struct alignas(64) Slot {
    uint64_t token, pin;
    int64_t pid;
    uint64_t birth;
};
struct alignas(64) Shared {
    uint64_t magic_value, bytes, bank_bytes, bank_offset;
    uint64_t publication, serving, next_token, owner_token;
    int64_t owner_pid;
    uint64_t owner_birth, salt[2];
    uint32_t abi, readers;
    pthread_mutex_t mutex;
};
Slot *slots(Shared *s) { return reinterpret_cast<Slot *>(reinterpret_cast<char *>(s) + align64(sizeof(Shared))); }
unsigned char *bank(Shared *s, unsigned i) { return reinterpret_cast<unsigned char *>(s) + s->bank_offset + i * s->bank_bytes; }
uint64_t birth(pid_t pid) {
    std::ifstream file("/proc/" + std::to_string(pid) + "/stat");
    std::string data; if (!std::getline(file, data)) return 0;
    auto end = data.rfind(')'); if (end == std::string::npos) return 0;
    std::istringstream fields(data.substr(end + 2)); std::string field;
    for (unsigned i = 3; i <= 22; ++i) {
        if (!(fields >> field)) return 0;
        if (i == 22) {
            try { return std::stoull(field); } catch (...) { return 0; }
        }
    }
    return 0;
}
bool gone(int64_t pid, uint64_t expected) {
    if (pid <= 0 || !expected) return false;
    auto current = birth(static_cast<pid_t>(pid));
    if (current) return current != expected;
    // A failed /proc read is not proof of death (permissions/resource errors).
    return kill(static_cast<pid_t>(pid), 0) < 0 && errno == ESRCH;
}
struct Lock {
    Shared *shared;
    explicit Lock(Shared *s) : shared(s) {
        int rc = pthread_mutex_trylock(&s->mutex);
        if (rc == EOWNERDEAD) {
            put(&s->serving, uint64_t(0));
            rc = pthread_mutex_consistent(&s->mutex);
            if (rc) { pthread_mutex_unlock(&s->mutex); throw IndexError(NGX_HTTP_CACHE_ROCKSDB_IO, "control mutex recovery failed"); }
            return;
        }
        require(rc != EBUSY, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control publisher/registration busy");
        require(rc == 0, NGX_HTTP_CACHE_ROCKSDB_IO, "control mutex failure");
        fault(1);
    }
    ~Lock() { pthread_mutex_unlock(&shared->mutex); }
};
Shared *attach(void *memory, size_t bytes) {
    require(memory && reinterpret_cast<uintptr_t>(memory) % 64 == 0 && bytes >= sizeof(Shared),
            NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid shared control memory");
    auto *s = static_cast<Shared *>(memory);
    require(s->magic_value == magic && s->abi == NGX_HTTP_CACHE_PURGE_CONTROL_ABI && s->bytes == bytes
            && s->bank_offset == align64(sizeof(Shared)) + s->readers * sizeof(Slot)
            && ngx_http_cache_purge_control_size(s->bank_bytes, s->readers) == bytes,
            NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "incompatible shared control layout");
    return s;
}
uint64_t token(Shared *s) {
    require(s->next_token && s->next_token != UINT64_MAX, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control registration tokens exhausted");
    return s->next_token++;
}
void reclaim(Shared *s) {
    for (uint32_t i = 0; i < s->readers; ++i) {
        auto &slot = slots(s)[i];
        if (load(&slot.token) && gone(slot.pid, slot.birth)) {
            put(&slot.pin, uint64_t(0)); put(&slot.token, uint64_t(0));
        }
    }
}
struct Slice { uint32_t offset = 0, size = 0; };
struct Record {
    uint64_t sequence = 0, issued = 0, until = 0;
    uint32_t flags = 0, mode = 0, kind = 0, next = none;
    Slice id, value, scope;
};
struct Node { Slice edge; uint32_t child = none, sibling = none, record = none; };
struct Image {
    uint64_t magic_value = magic;
    uint32_t bytes = 0, records = 0, nodes = 0, buckets = 0;
    uint32_t records_at = 0, nodes_at = 0, buckets_at = 0, strings_at = 0;
};
std::string_view view(const unsigned char *base, Slice s) {
    return {reinterpret_cast<const char *>(base + s.offset), s.size};
}
uint64_t rotate(uint64_t n, unsigned bits) { return (n << bits) | (n >> (64 - bits)); }
// SipHash-2-4 with a per-SHM random key, including scope/value lengths.
class Hash {
    uint64_t v0_, v1_, v2_, v3_, word_ = 0, count_ = 0;
    unsigned used_ = 0;
    void round() {
        v0_ += v1_; v1_ = rotate(v1_, 13); v1_ ^= v0_; v0_ = rotate(v0_, 32);
        v2_ += v3_; v3_ = rotate(v3_, 16); v3_ ^= v2_;
        v0_ += v3_; v3_ = rotate(v3_, 21); v3_ ^= v0_;
        v2_ += v1_; v1_ = rotate(v1_, 17); v1_ ^= v2_; v2_ = rotate(v2_, 32);
    }
    void block(uint64_t value) { v3_ ^= value; round(); round(); v0_ ^= value; }
public:
    explicit Hash(const uint64_t *salt) : v0_(salt[0] ^ 0x736f6d6570736575ULL), v1_(salt[1] ^ 0x646f72616e646f6dULL),
        v2_(salt[0] ^ 0x6c7967656e657261ULL), v3_(salt[1] ^ 0x7465646279746573ULL) {}
    void byte(unsigned char c) { word_ |= uint64_t(c) << (used_++ * 8); ++count_; if (used_ == 8) { block(word_); word_ = 0; used_ = 0; } }
    void string(std::string_view value) {
        for (unsigned i = 0; i != 4; ++i) byte(value.size() >> (i * 8));
        for (unsigned char c : value) byte(c);
    }
    uint64_t finish() { block(word_ | ((count_ & 255) << 56)); v2_ ^= 255; for (int i = 0; i != 4; ++i) round(); return v0_ ^ v1_ ^ v2_ ^ v3_; }
};
uint64_t key_hash(const uint64_t *salt, uint32_t kind, std::string_view value, std::string_view scope) {
    Hash h(salt); h.byte(kind); h.string(scope); h.string(value); return h.finish();
}
struct Build {
    std::vector<Record> records;
    std::vector<Node> nodes{2};
    std::vector<uint32_t> buckets;
    std::string strings;
    size_t capacity;
    explicit Build(size_t size) : capacity(size) {}
    Slice append(const std::string &value) {
        require(value.size() <= capacity - strings.size(), NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control selector budget");
        Slice slice{static_cast<uint32_t>(strings.size()), static_cast<uint32_t>(value.size())}; strings += value; return slice;
    }
    void prefix(uint32_t root, uint32_t record) {
        Slice remaining = records[record].value;
        uint32_t parent = root;
        while (remaining.size) {
            auto first = static_cast<unsigned char>(strings[remaining.offset]);
            uint32_t child = nodes[parent].child, previous = none;
            while (child != none && static_cast<unsigned char>(strings[nodes[child].edge.offset]) < first) {
                previous = child; child = nodes[child].sibling;
            }
            if (child == none || static_cast<unsigned char>(strings[nodes[child].edge.offset]) != first) {
                Node node; node.edge = remaining; node.sibling = child; node.record = record;
                auto id = static_cast<uint32_t>(nodes.size()); nodes.push_back(node);
                if (previous == none) nodes[parent].child = id; else nodes[previous].sibling = id;
                return;
            }
            auto edge = nodes[child].edge; uint32_t common = 0;
            while (common < edge.size && common < remaining.size && strings[edge.offset + common] == strings[remaining.offset + common]) ++common;
            if (common < edge.size) {
                Node split; split.edge = {edge.offset, common}; split.child = child; split.sibling = nodes[child].sibling;
                auto id = static_cast<uint32_t>(nodes.size()); nodes.push_back(split);
                nodes[child].edge = {edge.offset + common, edge.size - common}; nodes[child].sibling = none;
                if (previous == none) nodes[parent].child = id; else nodes[previous].sibling = id;
                child = id;
            }
            remaining.offset += common; remaining.size -= common; parent = child;
        }
        records[record].next = nodes[parent].record; nodes[parent].record = record;
    }
    std::string finish() {
        Image header;
        header.records = records.size(); header.nodes = nodes.size(); header.buckets = buckets.size();
        header.records_at = align64(sizeof(Image));
        header.nodes_at = header.records_at + records.size() * sizeof(Record);
        header.buckets_at = header.nodes_at + nodes.size() * sizeof(Node);
        header.strings_at = header.buckets_at + buckets.size() * sizeof(uint32_t);
        require(header.strings_at <= capacity && strings.size() <= capacity - header.strings_at,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "shared control image exceeds bank capacity");
        header.bytes = header.strings_at + strings.size();
        for (auto &record : records) for (auto *slice : {&record.id, &record.value, &record.scope}) slice->offset += header.strings_at;
        for (auto &node : nodes) node.edge.offset += header.strings_at;
        std::string result(header.bytes, '\0');
        std::memcpy(result.data(), &header, sizeof(header));
        if (!records.empty()) std::memcpy(result.data() + header.records_at, records.data(), records.size() * sizeof(Record));
        std::memcpy(result.data() + header.nodes_at, nodes.data(), nodes.size() * sizeof(Node));
        std::memcpy(result.data() + header.buckets_at, buckets.data(), buckets.size() * sizeof(uint32_t));
        std::memcpy(result.data() + header.strings_at, strings.data(), strings.size());
        return result;
    }
};
std::string build(Shared *s, const std::vector<Operation> &operations) {
    require(operations.size() <= 65536, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "too many control operations");
    Build b(s->bank_bytes); size_t buckets = 8;
    while (buckets < operations.size() * 2) buckets *= 2;
    b.buckets.assign(buckets, none); b.records.reserve(operations.size()); b.nodes.reserve(operations.size() * 2 + 2);
    std::set<std::string> ids; std::set<uint64_t> sequences;
    for (const auto &op : operations) {
        require(op.sequence && op.input.issued_at_ms && op.input.id.size() && op.input.id.size() <= 128
                && op.input.selector.value.size() <= 32768 && op.input.selector.scope.size() <= 32768,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid control operation identity");
        require(ids.insert(op.input.id).second && sequences.insert(op.sequence).second,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "duplicate control operation identity");
        require(op.stage >= OperationStage::preparing && op.stage <= OperationStage::materialized
                && (op.input.mode == PurgeMode::invalidate || op.input.mode == PurgeMode::remove),
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid control operation state/mode");
        auto kind = op.input.selector.kind;
        require(kind >= SelectorKind::key && kind <= SelectorKind::host_tag, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid control selector kind");
        bool tag = kind == SelectorKind::namespace_tag || kind == SelectorKind::host_tag;
        require(tag ? !op.input.selector.scope.empty() && !op.input.selector.value.empty() && op.input.selector.value.size() <= 1024
                    : op.input.selector.scope.empty(), NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid control selector scope");
        require(!op.input.selector.value.empty() || kind == SelectorKind::key_prefix || kind == SelectorKind::url_prefix,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "empty control selector");
        Record r; r.sequence = op.sequence; r.issued = op.input.issued_at_ms; r.until = op.propagation_until_ms;
        r.mode = static_cast<unsigned>(op.input.mode); r.kind = static_cast<unsigned>(kind);
        if (op.stage != OperationStage::materialized) r.flags |= NGX_HTTP_CACHE_PURGE_CONTROL_FENCE;
        if (op.stage == OperationStage::preparing) r.flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PREPARING;
        if (op.propagate) {
            require(op.stage != OperationStage::preparing && r.until, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid propagation control");
            r.flags |= NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
        }
        require(r.flags, NGX_HTTP_CACHE_ROCKSDB_INVALID, "completed operation has no resident duty");
        r.id = b.append(op.input.id); r.value = b.append(op.input.selector.value); r.scope = b.append(op.input.selector.scope);
        uint32_t index = b.records.size(); b.records.push_back(r);
        if (kind == SelectorKind::key_prefix || kind == SelectorKind::url_prefix) b.prefix(kind == SelectorKind::key_prefix ? 0 : 1, index);
        else {
            auto bucket = key_hash(s->salt, r.kind, op.input.selector.value, op.input.selector.scope) & (buckets - 1);
            b.records[index].next = b.buckets[bucket]; b.buckets[bucket] = index;
        }
    }
    return b.finish();
}
template<class Function> int boundary(ngx_http_cache_rocksdb_error_t *error, Function function) noexcept {
    if (error) { error->code = 0; error->message[0] = 0; }
    try { function(); return 0; }
    catch (const IndexError &e) { if (error) { error->code = e.code; std::snprintf(error->message, sizeof(error->message), "%s", e.what()); } return e.code; }
    catch (const std::bad_alloc &) { if (error) { error->code = NGX_HTTP_CACHE_ROCKSDB_LIMIT; std::snprintf(error->message, sizeof(error->message), "control allocation failed"); } return NGX_HTTP_CACHE_ROCKSDB_LIMIT; }
    catch (...) { if (error) { error->code = NGX_HTTP_CACHE_ROCKSDB_IO; std::snprintf(error->message, sizeof(error->message), "control internal error"); } return NGX_HTTP_CACHE_ROCKSDB_IO; }
}
} // namespace

struct ngx_http_cache_purge_control_reader_s {
    Shared *shared;
    uint32_t slot;
    uint64_t token;
    pid_t pid = getpid();
    std::thread::id thread = std::this_thread::get_id();
    bool pinned = false;
};
struct ngx_http_cache_purge_control_writer_s {
    Shared *shared;
    uint64_t token;
    uint64_t published = 0;
    pid_t pid = getpid();
    std::thread::id thread = std::this_thread::get_id();
};

namespace {
void reader_owner(ngx_http_cache_purge_control_reader_t *r) {
    require(r && r->pid == getpid() && r->thread == std::this_thread::get_id(), NGX_HTTP_CACHE_ROCKSDB_OWNER, "control reader owner mismatch");
    require(load(&slots(r->shared)[r->slot].token) == r->token, NGX_HTTP_CACHE_ROCKSDB_OWNER, "control reader registration changed");
}
void writer_owner(ngx_http_cache_purge_control_writer_t *w) {
    require(w && w->pid == getpid() && w->thread == std::this_thread::get_id(), NGX_HTTP_CACHE_ROCKSDB_OWNER, "control writer owner mismatch");
    require(load(&w->shared->owner_token) == w->token, NGX_HTTP_CACHE_ROCKSDB_OWNER, "control publisher lease changed");
}
uint64_t pin(ngx_http_cache_purge_control_reader_t *r, uint64_t expected = 0) {
    reader_owner(r); require(!r->pinned, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control reader already pinned");
    auto *s = r->shared;
    require(load(&s->serving) != 0, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control admission gate closed");
    auto version = load(&s->publication);
    require(version && (!expected || version == expected), NGX_HTTP_CACHE_ROCKSDB_BUSY, "control publication changed");
    put(&slots(s)[r->slot].pin, version);
    if (load(&s->publication) != version || !load(&s->serving)) {
        put(&slots(s)[r->slot].pin, uint64_t(0));
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_BUSY, "control publication changed during admission");
    }
    r->pinned = true; return version;
}
void unpin(ngx_http_cache_purge_control_reader_t *r) {
    put(&slots(r->shared)[r->slot].pin, uint64_t(0)); r->pinned = false;
}
struct Pin {
    ngx_http_cache_purge_control_reader_t *reader;
    uint64_t version;
    explicit Pin(ngx_http_cache_purge_control_reader_t *r) : reader(r), version(pin(r)) {}
    ~Pin() { unpin(reader); }
};
bool bank_pinned(Shared *s, unsigned target) {
    for (uint32_t i = 0; i < s->readers; ++i) {
        auto value = load(&slots(s)[i].pin);
        if (value && (value & 1) == target) return true;
    }
    return false;
}
std::string_view text(ngx_http_cache_rocksdb_bytes_t b, size_t limit = 32768) {
    require(b.len <= limit && (!b.len || b.data), NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid control request field");
    return {b.len ? reinterpret_cast<const char *>(b.data) : "", b.len};
}
struct Matcher {
    Shared *shared;
    const unsigned char *base;
    const Image *image;
    const Record *records;
    const Node *nodes;
    const uint32_t *buckets;
    const ngx_http_cache_purge_control_context_t &context;
    size_t count = 0, remaining;
    size_t limit;
    ngx_http_cache_purge_control_visit_pt visit;
    void *data;
    Matcher(Shared *s, uint64_t version, const ngx_http_cache_purge_control_context_t &c,
             size_t max_count, size_t max_bytes, ngx_http_cache_purge_control_visit_pt callback, void *opaque)
        : shared(s), base(bank(s, version & 1)), image(reinterpret_cast<const Image *>(base)),
          context(c), remaining(max_bytes), limit(max_count), visit(callback), data(opaque) {
        require(image->magic_value == magic && image->bytes <= s->bank_bytes && image->bytes >= sizeof(Image)
                && image->records <= 65536 && image->nodes >= 2 && image->nodes <= 2 * image->records + 2
                && image->buckets >= 8 && image->buckets <= 131072 && !(image->buckets & (image->buckets - 1))
                && image->records_at == align64(sizeof(Image))
                && image->nodes_at == image->records_at + image->records * sizeof(Record)
                && image->buckets_at == image->nodes_at + image->nodes * sizeof(Node)
                && image->strings_at == image->buckets_at + image->buckets * sizeof(uint32_t)
                && image->strings_at <= image->bytes, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid shared control image");
        records = reinterpret_cast<const Record *>(base + image->records_at);
        nodes = reinterpret_cast<const Node *>(base + image->nodes_at);
        buckets = reinterpret_cast<const uint32_t *>(base + image->buckets_at);
    }
    std::string_view string(Slice value) {
        require(value.offset >= image->strings_at && value.offset <= image->bytes && value.size <= image->bytes - value.offset,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "control string outside image");
        return view(base, value);
    }
    const Record &record(uint32_t i) {
        require(i < image->records, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "control record outside image"); return records[i];
    }
    const Node &node(uint32_t i) {
        require(i < image->nodes, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "control prefix node outside image"); return nodes[i];
    }
    void emit(const Record &r) {
        uint32_t flags = r.flags;
        if (!context.proxy || context.now_ms >= r.until) flags &= ~NGX_HTTP_CACHE_PURGE_CONTROL_PROPAGATE;
        if (!flags) return;
        auto id = string(r.id), value = string(r.value), scope = string(r.scope);
        size_t size = sizeof(ngx_http_cache_purge_control_match_t) + id.size() + value.size() + scope.size();
        require(count < limit && size <= remaining, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control match output budget exhausted");
        auto bytes = [](std::string_view text) { return ngx_http_cache_rocksdb_bytes_t{reinterpret_cast<const unsigned char *>(text.data()), text.size()}; };
        ngx_http_cache_purge_control_match_t m{r.sequence, r.issued, r.until, flags, r.mode, r.kind, bytes(id), bytes(value), bytes(scope)};
        int result = visit(data, &m);
        require(result == NGX_HTTP_CACHE_ROCKSDB_OK, result, "control match visitor failed"); ++count; remaining -= size;
    }
    void exact(uint32_t kind, std::string_view value, std::string_view scope = {}) {
        uint32_t i = buckets[key_hash(shared->salt, kind, value, scope) & (image->buckets - 1)];
        size_t steps = 0;
        while (i != none) {
            require(++steps <= image->records, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "control hash chain cycle");
            const auto &r = record(i);
            if (r.kind == kind && string(r.value) == value && string(r.scope) == scope) emit(r);
            i = r.next;
        }
    }
    void prefix(uint32_t root, std::string_view value) {
        uint32_t current = root; size_t offset = 0, steps = 0;
        for (;;) {
            require(++steps <= image->nodes, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "control prefix cycle");
            auto r = node(current).record; size_t linked = 0;
            while (r != none) {
                require(++linked <= image->records, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "control prefix record cycle");
                emit(record(r)); r = record(r).next;
            }
            if (offset == value.size()) return;
            auto child = node(current).child; size_t siblings = 0;
            while (child != none) {
                require(++siblings <= 256, NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "control prefix sibling overflow");
                auto edge = string(node(child).edge);
                require(!edge.empty(), NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "empty control prefix edge");
                auto first = static_cast<unsigned char>(edge[0]), sought = static_cast<unsigned char>(value[offset]);
                if (first > sought) return;
                if (first == sought) {
                    if (edge.size() > value.size() - offset || value.substr(offset, edge.size()) != edge) return;
                    offset += edge.size(); current = child; break;
                }
                child = node(child).sibling;
            }
            if (child == none) return;
        }
    }
    void run() {
        auto key = text(context.key), url = text(context.url), host = text(context.host), ns = text(context.tag_namespace);
        require(context.tag_count <= 64 && (!context.tag_count || context.tags), NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid control TAG input");
        for (size_t i = 0; i < context.tag_count; ++i) text(context.tags[i], 1024);
        if (!image->records) return;
        exact(static_cast<unsigned>(SelectorKind::key), key); prefix(0, key);
        exact(static_cast<unsigned>(SelectorKind::url), url); prefix(1, url);
        exact(static_cast<unsigned>(SelectorKind::host), host);
        for (size_t i = 0; i < context.tag_count; ++i) {
            auto tag = text(context.tags[i]); bool duplicate = false;
            for (size_t j = 0; j < i; ++j) if (tag == text(context.tags[j])) { duplicate = true; break; }
            if (duplicate) continue;
            exact(static_cast<unsigned>(SelectorKind::namespace_tag), tag, ns);
            exact(static_cast<unsigned>(SelectorKind::host_tag), tag, host);
        }
    }
};
} // namespace

extern "C" size_t ngx_http_cache_purge_control_size(size_t bank_bytes, uint32_t readers) {
    if (bank_bytes < 4096 || bank_bytes > 64 * 1024 * 1024 || bank_bytes % 64 || !readers || readers > 1024) return 0;
    return align64(sizeof(Shared)) + readers * sizeof(Slot) + bank_bytes * 2;
}
extern "C" int ngx_http_cache_purge_control_init(void *memory, size_t bytes, size_t bank_bytes,
    uint32_t readers, ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&] {
        require(memory && reinterpret_cast<uintptr_t>(memory) % 64 == 0 && bytes != 0
                && ngx_http_cache_purge_control_size(bank_bytes, readers) == bytes, NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid shared control allocation");
        auto *s = static_cast<Shared *>(memory);
        // Touch only the header/reader slots; unused banks remain demand paged.
        std::memset(memory, 0, align64(sizeof(Shared)) + readers * sizeof(Slot));
        s->bytes = bytes; s->bank_bytes = bank_bytes; s->bank_offset = align64(sizeof(Shared)) + readers * sizeof(Slot);
        s->abi = NGX_HTTP_CACHE_PURGE_CONTROL_ABI; s->readers = readers; s->next_token = 1;
        size_t obtained = 0;
        while (obtained < sizeof(s->salt)) {
            auto n = getrandom(reinterpret_cast<char *>(s->salt) + obtained, sizeof(s->salt) - obtained, 0);
            if (n < 0 && errno == EINTR) continue;
            require(n > 0, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot initialize control hash seed"); obtained += n;
        }
        pthread_mutexattr_t attr;
        require(pthread_mutexattr_init(&attr) == 0, NGX_HTTP_CACHE_ROCKSDB_IO, "control mutex attributes");
        int rc = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        if (!rc) rc = pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
        if (!rc) rc = pthread_mutex_init(&s->mutex, &attr);
        pthread_mutexattr_destroy(&attr);
        require(rc == 0, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot initialize robust shared control mutex");
        s->magic_value = magic;
    });
}
extern "C" int ngx_http_cache_purge_control_reader_open(void *memory, size_t bytes,
    ngx_http_cache_purge_control_reader_t **output, ngx_http_cache_rocksdb_error_t *error) {
    if (output) *output = nullptr;
    return boundary(error, [&] {
        require(output != nullptr, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null control reader output");
        auto *s = attach(memory, bytes); auto reader = std::make_unique<ngx_http_cache_purge_control_reader_s>();
        auto stamp = birth(getpid()); require(stamp != 0, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot identify control reader process");
        Lock lock(s);
        uint32_t selected = s->readers;
        for (unsigned pass = 0; pass != 2 && selected == s->readers; ++pass) {
            if (pass) reclaim(s);
            for (uint32_t i = 0; i < s->readers; ++i) if (!load(&slots(s)[i].token)) { selected = i; break; }
        }
        require(selected != s->readers, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control reader slots exhausted");
        reader->shared = s; reader->slot = selected; reader->token = token(s);
        auto &slot = slots(s)[selected]; slot.pid = getpid(); slot.birth = stamp; put(&slot.pin, uint64_t(0)); put(&slot.token, reader->token);
        *output = reader.release();
    });
}
extern "C" int ngx_http_cache_purge_control_reader_close(ngx_http_cache_purge_control_reader_t **output,
    ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&] {
        require(output && *output, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null control reader close");
        auto *r = *output; reader_owner(r); require(!r->pinned, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control reader still pinned");
        Lock lock(r->shared); put(&slots(r->shared)[r->slot].token, uint64_t(0)); delete r; *output = nullptr;
    });
}
extern "C" int ngx_http_cache_purge_control_writer_open(void *memory, size_t bytes,
    ngx_http_cache_purge_control_writer_t **output, ngx_http_cache_rocksdb_error_t *error) {
    if (output) *output = nullptr;
    return boundary(error, [&] {
        require(output != nullptr, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null control writer output");
        auto *s = attach(memory, bytes); auto writer = std::make_unique<ngx_http_cache_purge_control_writer_s>();
        auto stamp = birth(getpid()); require(stamp != 0, NGX_HTTP_CACHE_ROCKSDB_IO, "cannot identify control writer process");
        Lock lock(s);
        require(!load(&s->owner_token) || gone(s->owner_pid, s->owner_birth), NGX_HTTP_CACHE_ROCKSDB_BUSY, "another control publisher owns the table");
        writer->shared = s; writer->token = token(s); s->owner_pid = getpid(); s->owner_birth = stamp;
        put(&s->serving, uint64_t(0)); put(&s->owner_token, writer->token); *output = writer.release();
    });
}
extern "C" int ngx_http_cache_purge_control_writer_close(ngx_http_cache_purge_control_writer_t **output,
    ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&] {
        require(output && *output, NGX_HTTP_CACHE_ROCKSDB_INVALID, "null control writer close");
        auto *w = *output; writer_owner(w); put(&w->shared->serving, uint64_t(0));
        // Stop publishing before releasing ownership. Registration may hold
        // the mutex; shutdown must not wait for an unrelated live reader.
        put(&w->shared->owner_token, uint64_t(0)); delete w; *output = nullptr;
    });
}
extern "C" int ngx_http_cache_purge_control_gate_close(ngx_http_cache_purge_control_writer_t *w,
    ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&] { writer_owner(w); put(&w->shared->serving, uint64_t(0)); });
}
extern "C" int ngx_http_cache_purge_control_gate_open(ngx_http_cache_purge_control_writer_t *w,
    ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&] {
        writer_owner(w); auto *s = w->shared; Lock lock(s); auto version = load(&s->publication);
        require(version != 0 && version == w->published, NGX_HTTP_CACHE_ROCKSDB_BUSY, "publisher has not rebuilt recovered controls");
        bool old = false;
        for (uint32_t i = 0; i < s->readers; ++i) { auto v = load(&slots(s)[i].pin); if (v && v != version) old = true; }
        if (old) reclaim(s);
        for (uint32_t i = 0; i < s->readers; ++i) {
            auto v = load(&slots(s)[i].pin); require(!v || v == version, NGX_HTTP_CACHE_ROCKSDB_BUSY, "old control admission still pinned");
        }
        put(&s->serving, uint64_t(1));
    });
}
extern "C" int ngx_http_cache_purge_control_guard_begin(ngx_http_cache_purge_control_reader_t *r,
    uint64_t version, ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&] { require(version != 0, NGX_HTTP_CACHE_ROCKSDB_INVALID, "empty control publication"); pin(r, version); });
}
extern "C" int ngx_http_cache_purge_control_guard_end(ngx_http_cache_purge_control_reader_t *r,
    ngx_http_cache_rocksdb_error_t *error) {
    return boundary(error, [&] { reader_owner(r); require(r->pinned, NGX_HTTP_CACHE_ROCKSDB_INVALID, "no control guard"); unpin(r); });
}
extern "C" int ngx_http_cache_purge_control_match(ngx_http_cache_purge_control_reader_t *r,
    const ngx_http_cache_purge_control_context_t *context, size_t max_matches, size_t max_bytes,
    ngx_http_cache_purge_control_visit_pt visit, void *data, uint64_t *publication, ngx_http_cache_rocksdb_error_t *error) {
    if (publication) *publication = 0;
    int result = boundary(error, [&] {
        require(context && visit && publication && max_matches && max_matches <= 65536 && max_bytes <= 64 * 1024 * 1024,
                NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid control lookup output");
        Pin guard(r); Matcher match(r->shared, guard.version, *context, max_matches, max_bytes, visit, data); match.run();
        require(load(&r->shared->serving) && load(&r->shared->publication) == guard.version,
                NGX_HTTP_CACHE_ROCKSDB_BUSY, "control image changed during lookup");
        *publication = guard.version;
    });
    if (result == NGX_HTTP_CACHE_ROCKSDB_CORRUPT && r) put(&r->shared->serving, uint64_t(0));
    return result;
}

namespace ngx_http_cache_purge {
struct ControlImage::Impl {
    Shared *shared;
    uint64_t owner, previous;
    std::string data;
};
ControlImage::ControlImage(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
ControlImage::ControlImage(ControlImage &&) noexcept = default;
ControlImage &ControlImage::operator=(ControlImage &&) noexcept = default;
ControlImage::~ControlImage() = default;
ControlImage prepare_controls(ngx_http_cache_purge_control_writer_t *w, const std::vector<Operation> &operations) {
    writer_owner(w); auto image = std::make_unique<ControlImage::Impl>();
    image->shared = w->shared; image->owner = w->token; image->previous = load(&w->shared->publication);
    image->data = build(w->shared, operations);
    return ControlImage(std::move(image));
}
uint64_t publish_controls(ngx_http_cache_purge_control_writer_t *w, const ControlImage &prepared) {
    writer_owner(w); auto *s = w->shared;
    require(prepared.impl_ && prepared.impl_->shared == s && prepared.impl_->owner == w->token,
            NGX_HTTP_CACHE_ROCKSDB_OWNER, "prepared control image belongs to another publisher");
    require(!load(&s->serving), NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "close control gate before publishing");
    Lock lock(s); writer_owner(w);
    auto current = load(&s->publication);
    require(current == prepared.impl_->previous, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "prepared control image is stale");
    require(current != UINT64_MAX, NGX_HTTP_CACHE_ROCKSDB_LIMIT, "control publications exhausted");
    auto next = current ? current + 1 : 2; auto target = static_cast<unsigned>(next & 1);
    if (bank_pinned(s, target)) reclaim(s);
    require(!bank_pinned(s, target), NGX_HTTP_CACHE_ROCKSDB_BUSY, "inactive control image remains pinned");
    const auto &image = prepared.impl_->data;
#if defined(NGX_HTTP_CACHE_PURGE_CONTROL_FAULT_TEST)
    size_t split = image.size() / 2;
    std::memcpy(bank(s, target), image.data(), split); fault(2);
    std::memcpy(bank(s, target) + split, image.data() + split, image.size() - split);
#else
    std::memcpy(bank(s, target), image.data(), image.size());
#endif
    put(&s->publication, next); w->published = next; return next;
}
uint64_t publish_controls(ngx_http_cache_purge_control_writer_t *w, const std::vector<Operation> &operations) {
    writer_owner(w);
    require(!load(&w->shared->serving), NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "close control gate before publishing");
    return publish_controls(w, prepare_controls(w, operations));
}
} // namespace ngx_http_cache_purge


namespace ngx_http_cache_purge {
namespace {
void require(bool ok, int code, const char *message) { if (!ok) throw IndexError(code, message); }
void checked(int code, const ngx_http_cache_rocksdb_error_t &error) { if (code) throw IndexError(code, error.message); }
bool same(const OperationUsage &a, const OperationUsage &b) {
    return a.live_records == b.live_records && a.live_selector_bytes == b.live_selector_bytes && a.last_sequence == b.last_sequence;
}
Operation control(const Operation &source) {
    Operation result; result.input = source.input; result.sequence = source.sequence; result.revision = source.revision;
    result.stage = source.stage; result.accepted_at_ms = source.accepted_at_ms; result.effective_at_ms = source.effective_at_ms;
    result.propagation_until_ms = source.propagation_until_ms; result.propagation_ms = source.propagation_ms; result.propagate = source.propagate;
    // Do not retain per-operation bookmarks, error details or file statistics
    // in the resident control collection, including their string capacities.
    return result;
}
} // namespace

Coordinator::Coordinator(Operations &operations, void *shared, size_t shared_bytes)
    : operations_(operations), shared_(shared), shared_bytes_(shared_bytes) {
    require(shared && shared_bytes, NGX_HTTP_CACHE_ROCKSDB_INVALID, "missing coordinator shared memory");
}
Coordinator::~Coordinator() {
    if (writer_) {
        ngx_http_cache_rocksdb_error_t error;
        ngx_http_cache_purge_control_writer_close(&writer_, &error);
    }
}
void Coordinator::load_begin() {
    pending_.reset(); controls_.clear(); after_.clear(); loaded_bytes_ = 0;
    expected_ = operations_.usage();
    require(expected_.live_records <= 65536 && expected_.live_selector_bytes <= 64 * 1024 * 1024,
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "durable control usage exceeds supported bounds");
    controls_.reserve(static_cast<size_t>(expected_.live_records)); state_ = Coordination::loading;
}
void Coordinator::refresh() {
    require(state_ == Coordination::ready, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control synchronization already in progress");
    load_begin();
}
bool Coordinator::step() {
    ngx_http_cache_rocksdb_error_t error;
    try {
        switch (state_) {
        case Coordination::claiming: {
            int rc = ngx_http_cache_purge_control_writer_open(shared_, shared_bytes_, &writer_, &error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) return false;
            checked(rc, error); load_begin(); return true;
        }
        case Coordination::loading: {
            auto page = operations_.live(after_, 32);
            for (const auto &op : page.items) {
                auto size = operation_control_bytes(op.input);
                require(controls_.size() < expected_.live_records && loaded_bytes_ <= expected_.live_selector_bytes
                        && size <= expected_.live_selector_bytes - loaded_bytes_
                        && op.sequence <= expected_.last_sequence,
                        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "live control enumeration exceeds durable budget");
                require(controls_.empty() || controls_.back().sequence < op.sequence,
                        NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "live controls not strictly ordered");
                controls_.push_back(control(op)); loaded_bytes_ += size;
            }
            after_ = std::move(page.after);
            if (page.done) {
                require(controls_.size() == expected_.live_records && loaded_bytes_ == expected_.live_selector_bytes
                        && same(expected_, operations_.usage()), NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "live control recovery watermark mismatch");
                state_ = Coordination::building;
            }
            return true;
        }
        case Coordination::building:
            pending_ = std::make_unique<ControlImage>(prepare_controls(writer_, controls_));
            checked(ngx_http_cache_purge_control_gate_close(writer_, &error), error);
            state_ = Coordination::publishing; return true;
        case Coordination::publishing:
            publication_ = publish_controls(writer_, *pending_); pending_.reset(); state_ = Coordination::draining; return true;
        case Coordination::draining: {
            int rc = ngx_http_cache_purge_control_gate_open(writer_, &error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) return false;
            checked(rc, error); state_ = Coordination::ready; return true;
        }
        case Coordination::ready: return true;
        }
    } catch (const IndexError &e) {
        if (e.code == NGX_HTTP_CACHE_ROCKSDB_BUSY) return false;
        // Even a failure while prebuilding a refresh must not let a caller
        // accidentally continue with unverified recovered state.
        if (writer_) ngx_http_cache_purge_control_gate_close(writer_, &error);
        throw;
    }
    return false;
}
Operation Coordinator::accept(const OperationInput &input, uint64_t now, uint32_t propagation_ms) {
    auto durable = operations_.usage();
    auto preview = operations_.preview(input, now, propagation_ms);
    if (preview.sequence <= durable.last_sequence) return preview; // Replay remains available while publication waits.
    require(state_ == Coordination::ready, NGX_HTTP_CACHE_ROCKSDB_BUSY, "control publication is pending");
    require(expected_.last_sequence != UINT64_MAX && preview.sequence == expected_.last_sequence + 1,
            NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "operation sequence changed outside coordinator");
    controls_.push_back(control(preview));
    std::unique_ptr<ControlImage> image;
    try { image = std::make_unique<ControlImage>(prepare_controls(writer_, controls_)); }
    catch (...) { controls_.pop_back(); throw; }
    ngx_http_cache_rocksdb_error_t error;
    try { checked(ngx_http_cache_purge_control_gate_close(writer_, &error), error); }
    catch (...) { controls_.pop_back(); throw; }
    Operation accepted;
    try {
        accepted = operations_.accept(input, now, propagation_ms);
        require(accepted.sequence == preview.sequence && accepted.revision == preview.revision,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "accepted operation differs from prepared controls");
        expected_ = operations_.usage(); loaded_bytes_ = expected_.live_selector_bytes;
    } catch (...) {
        // A write error can be ambiguous. Keep the gate closed and recover
        // from the durable ledger instead of assuming no operation exists.
        load_begin(); throw;
    }
    pending_ = std::move(image); state_ = Coordination::publishing;
    return accepted; // Durable PREPARING; no claim of native cache effectiveness.
}

} // namespace ngx_http_cache_purge
