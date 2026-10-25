/* Copyright (C) Hanada */
#ifndef NGX_CACHE_PURGE_PROTOCOL_HPP
#define NGX_CACHE_PURGE_PROTOCOL_HPP

#include "ngx_cache_purge_component.h"
#include "ngx_cache_purge_index.hpp"
#include "ngx_cache_purge_operations.hpp"
#include <cstring>
#include <string_view>

namespace ngx_cache_purge::protocol {

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
        if (count > data.size() - at) throw IndexError(NGX_PURGE_STORE_INVALID, "truncated IPC payload");
        auto value = data.substr(at, count); at += count; return value;
    }
    uint64_t integer(unsigned width) {
        uint64_t value = 0;
        for (unsigned char c : take(width)) value = (value << 8) | c;
        return value;
    }
    bool flag() {
        auto value = integer(1);
        if (value > 1) throw IndexError(NGX_PURGE_STORE_INVALID, "invalid IPC boolean");
        return value != 0;
    }
    std::string string(size_t limit = 32768) {
        auto size = integer(4);
        if (size > limit) throw IndexError(NGX_PURGE_STORE_LIMIT, "IPC string exceeds limit");
        return std::string(take(size));
    }
    FileId id() { FileId value; auto raw = take(value.size()); std::memcpy(value.data(), raw.data(), value.size()); return value; }
    void end() {
        if (at != data.size()) throw IndexError(NGX_PURGE_STORE_INVALID, "unexpected IPC payload suffix");
    }
};
struct Frame { uint16_t opcode; uint64_t id; uint32_t size; uint32_t result; };
inline Frame frame(std::string_view value) {
    Decoder d{value};
    if (d.take(4) != "NCP1" || d.integer(2) != NGX_CACHE_PURGE_PROTOCOL_VERSION)
        throw IndexError(NGX_PURGE_STORE_INVALID, "unknown IPC protocol");
    Frame f{static_cast<uint16_t>(d.integer(2)), d.integer(8), static_cast<uint32_t>(d.integer(4)),
            static_cast<uint32_t>(d.integer(4))};
    d.end(); return f;
}
inline std::string frame(uint16_t opcode, uint64_t id, const std::string &payload, uint32_t result = 0) {
    Encoder e; e.data = "NCP1"; e.integer(NGX_CACHE_PURGE_PROTOCOL_VERSION, 2);
    e.integer(opcode, 2); e.integer(id, 8); e.integer(payload.size(), 4); e.integer(result, 4);
    e.data += payload; return e.data;
}
inline std::vector<std::string> tags(Decoder &d) {
    auto n = d.integer(1);
    if (n > 64) throw IndexError(NGX_PURGE_STORE_LIMIT, "IPC TAG count");
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
    if (!n || n > 32) throw IndexError(NGX_PURGE_STORE_LIMIT, "IPC batch count");
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

} // namespace ngx_cache_purge::protocol
#endif
