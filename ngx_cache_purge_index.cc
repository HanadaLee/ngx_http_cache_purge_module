/* Copyright (C) Hanada */
#include "ngx_cache_purge_index.hpp"
#include <openssl/sha.h>
#include <algorithm>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <utility>

namespace ngx_cache_purge {
namespace {
constexpr size_t max_string = 32768, max_metadata = 16384, max_tags = 64;
constexpr size_t max_input = 262144, max_transaction = 524288;
using Cf = ngx_cache_purge_cf_t;
constexpr Cf common = NGX_PURGE_STORE_DEFAULT, urls = NGX_PURGE_STORE_URL, tags = NGX_PURGE_STORE_TAG;
using RowKey = std::pair<Cf, std::string>;

[[noreturn]] void reject(int code, const char *message) { throw IndexError(code, message); }
void require(bool condition, int code, const char *message) { if (!condition) reject(code, message); }
void checked(int code, const ngx_cache_purge_store_error_t &error) {
    if (code != NGX_PURGE_STORE_OK) throw IndexError(code, error.message);
}
ngx_cache_purge_bytes_t bytes(const std::string &value) {
    return {reinterpret_cast<const unsigned char *>(value.data()), value.size()};
}
std::string number(uint64_t value) {
    std::string result(8, '\0');
    for (int i = 7; i >= 0; --i) { result[i] = static_cast<char>(value); value >>= 8; }
    return result;
}
uint64_t integer(const std::string &value) {
    require(value.size() == 8, NGX_PURGE_STORE_CORRUPT, "invalid index integer");
    uint64_t result = 0;
    for (unsigned char c : value) result = (result << 8) | c;
    return result;
}
std::string id_bytes(const FileId &id) { return {reinterpret_cast<const char *>(id.data()), id.size()}; }
FileId file_id(const std::string &value) {
    require(value.size() == 16, NGX_PURGE_STORE_CORRUPT, "invalid file instance ID");
    FileId id;
    std::memcpy(id.data(), value.data(), id.size());
    return id;
}
bool zero(const FileId &id) { return std::all_of(id.begin(), id.end(), [](unsigned char c) { return c == 0; }); }
std::string digest(const std::string &value) {
    std::string result(SHA256_DIGEST_LENGTH, '\0');
    require(SHA256(reinterpret_cast<const unsigned char *>(value.data()), value.size(),
                   reinterpret_cast<unsigned char *>(result.data())) != nullptr,
            NGX_PURGE_STORE_IO, "index digest failed");
    return result;
}
std::string row(char type, uint64_t id) { return std::string(1, type) + number(id); }
std::string object(const FileId &id) { return "O" + id_bytes(id); }
std::string file_edge(uint64_t key, const FileId &id) { return row('F', key) + id_bytes(id); }
std::string location(uint64_t root, const std::string &path) { return row('P', root) + path; }
std::string lease(uint64_t root, const std::string &path) { return row('I', root) + path; }
std::string tag_edge(char type, uint64_t scope, uint64_t tag, const FileId &id) {
    return row(type, scope) + number(tag) + id_bytes(id);
}

struct Writer {
    std::string data;
    void byte(unsigned value) { data += static_cast<char>(value); }
    void u64(uint64_t value) { data += number(value); }
    void string(const std::string &value) { u64(value.size()); data += value; }
    void id(const FileId &value) { data += id_bytes(value); }
};
struct Reader {
    const std::string &data;
    size_t offset = 0;
    std::string take(size_t count) {
        require(count <= data.size() - offset, NGX_PURGE_STORE_CORRUPT, "truncated index record");
        auto result = data.substr(offset, count); offset += count; return result;
    }
    unsigned byte() { return static_cast<unsigned char>(take(1)[0]); }
    uint64_t u64() { return integer(take(8)); }
    std::string string() {
        uint64_t count = u64();
        require(count <= data.size() - offset, NGX_PURGE_STORE_CORRUPT, "invalid index field size");
        return take(static_cast<size_t>(count));
    }
    FileId id() { return file_id(take(16)); }
    void end() { require(offset == data.size(), NGX_PURGE_STORE_CORRUPT, "unexpected index record suffix"); }
};

std::optional<std::string> read(ngx_cache_purge_store_t *store, Cf cf, const std::string &key,
                                ngx_cache_purge_cursor_t *cursor = nullptr) {
    std::string result(256, '\0');
    for (;;) {
        ngx_cache_purge_buffer_t buffer{reinterpret_cast<unsigned char *>(result.data()), result.size(), 0};
        ngx_cache_purge_store_error_t error;
        int code = cursor ? ngx_cache_purge_cursor_get(cursor, cf, bytes(key), &buffer, &error)
                          : ngx_cache_purge_store_get(store, cf, bytes(key), &buffer, &error);
        if (code == NGX_PURGE_STORE_NOT_FOUND) return std::nullopt;
        if (code == NGX_PURGE_STORE_BUFFER) {
            require(buffer.length <= max_transaction, NGX_PURGE_STORE_LIMIT, "index record exceeds transaction budget");
            result.resize(buffer.length); continue;
        }
        checked(code, error); result.resize(buffer.length); return result;
    }
}

class Cursor {
public:
    ngx_cache_purge_cursor_t *handle = nullptr;
    std::string key_buffer = std::string(256, '\0'), value_buffer = std::string(256, '\0');
    Cursor() = default;
    Cursor(const Cursor &) = delete;
    Cursor &operator=(const Cursor &) = delete;
    ~Cursor() { close(); }
    void close() noexcept {
        if (handle) { ngx_cache_purge_store_error_t error; ngx_cache_purge_cursor_close(&handle, &error); }
    }
    void open(ngx_cache_purge_store_t *store, Cf cf, const std::string &prefix,
              const std::string &after = "", ngx_cache_purge_cursor_t *parent = nullptr) {
        close(); ngx_cache_purge_store_error_t error;
        int code = parent ? ngx_cache_purge_cursor_fork(parent, cf, bytes(prefix), bytes(after), &handle, &error)
                          : ngx_cache_purge_cursor_open(store, cf, bytes(prefix), bytes(after), &handle, &error);
        checked(code, error);
    }
    bool next(std::string &key, std::string &value) {
        for (;;) {
            ngx_cache_purge_buffer_t k{reinterpret_cast<unsigned char *>(key_buffer.data()), key_buffer.size(), 0};
            ngx_cache_purge_buffer_t v{reinterpret_cast<unsigned char *>(value_buffer.data()), value_buffer.size(), 0};
            ngx_cache_purge_store_error_t error;
            int code = ngx_cache_purge_cursor_next(handle, &k, &v, &error);
            if (code == NGX_PURGE_STORE_DONE) return false;
            if (code == NGX_PURGE_STORE_BUFFER) {
                require(k.length + v.length <= max_transaction, NGX_PURGE_STORE_LIMIT, "index cursor row exceeds budget");
                key_buffer.resize(std::max(key_buffer.size(), k.length));
                value_buffer.resize(std::max(value_buffer.size(), v.length)); continue;
            }
            checked(code, error);
            key.assign(key_buffer.data(), k.length); value.assign(value_buffer.data(), v.length); return true;
        }
    }
};

class Transaction {
    size_t memory_ = 0;
    std::map<RowKey, std::optional<std::string>> original_, writes_;
    void charge(size_t amount) {
        require(amount <= max_transaction - memory_, NGX_PURGE_STORE_LIMIT, "index transaction memory budget");
        memory_ += amount;
    }
public:
    ngx_cache_purge_store_t *store;
    explicit Transaction(ngx_cache_purge_store_t *value) : store(value) {}
    std::optional<std::string> get(Cf cf, const std::string &key) {
        RowKey k{cf, key};
        auto written = writes_.find(k); if (written != writes_.end()) return written->second;
        auto found = original_.find(k); if (found != original_.end()) return found->second;
        auto value = read(store, cf, key);
        charge(64 + key.size() + (value ? value->size() : 0));
        original_.emplace(std::move(k), value); return value;
    }
    std::string need(Cf cf, const std::string &key) {
        auto value = get(cf, key);
        require(value.has_value(), NGX_PURGE_STORE_CORRUPT, "missing referenced index record"); return *value;
    }
    void set(Cf cf, const std::string &key, std::optional<std::string> value) {
        auto found = writes_.find({cf, key});
        if (found != writes_.end()) memory_ -= 64 + key.size() + (found->second ? found->second->size() : 0);
        charge(64 + key.size() + (value ? value->size() : 0));
        writes_[{cf, key}] = std::move(value);
    }
    void put(Cf cf, const std::string &key, const std::string &value) { set(cf, key, value); }
    void del(Cf cf, const std::string &key) { set(cf, key, std::nullopt); }
    uint64_t allocate() {
        uint64_t next = integer(need(common, "M:next"));
        require(next && next != UINT64_MAX, NGX_PURGE_STORE_LIMIT, "index ID space exhausted");
        put(common, "M:next", number(next + 1)); return next;
    }
    void commit() {
        if (writes_.empty()) return;
        std::vector<ngx_cache_purge_mutation_t> changes;
        std::vector<ngx_cache_purge_condition_t> conditions;
        const std::string empty;
        for (const auto &entry : writes_)
            changes.push_back({entry.first.first, entry.second ? NGX_PURGE_STORE_PUT : NGX_PURGE_STORE_DELETE,
                               bytes(entry.first.second), bytes(entry.second ? *entry.second : empty)});
        for (const auto &entry : original_)
            conditions.push_back({entry.first.first, bytes(entry.first.second),
                                  bytes(entry.second ? *entry.second : empty), entry.second ? 1 : 0});
        ngx_cache_purge_store_error_t error;
        checked(ngx_cache_purge_store_write(store, changes.data(), changes.size(),
                                            conditions.data(), conditions.size(), &error), error);
    }
};

struct Value {
    uint64_t references = 0;
    unsigned format = 0; /* raw, us-escape-v1 base, URL */
    std::string raw;
    std::vector<uint64_t> parts;
};
std::string encode(const Value &value) {
    Writer w; w.byte(1); w.u64(value.references); w.byte(value.format);
    if (!value.format) w.string(value.raw);
    else { w.byte(value.parts.size()); for (auto id : value.parts) w.u64(id); }
    return w.data;
}
Value decode_value(const std::string &data) {
    Reader r{data}; Value value;
    require(r.byte() == 1, NGX_PURGE_STORE_CORRUPT, "invalid value record version");
    value.references = r.u64(); value.format = r.byte();
    if (!value.format) {
        value.raw = r.string();
        require(value.raw.size() <= max_string, NGX_PURGE_STORE_CORRUPT, "oversized dictionary value");
    }
    else {
        require(value.format == 1 || value.format == 2, NGX_PURGE_STORE_CORRUPT, "unknown value representation");
        unsigned count = r.byte();
        require(count == (value.format == 1 ? 5U : 3U), NGX_PURGE_STORE_CORRUPT, "invalid component count");
        while (count--) value.parts.push_back(r.u64());
    }
    r.end(); return value;
}
std::string encoded_key(const std::vector<std::string> &parts) {
    std::string result;
    for (const auto &part : parts) {
        if (!result.empty() || &part != &parts.front()) result += '\x1f';
        for (unsigned char c : part) {
            if (c == '\\') result += "\\\\";
            else if (c == 31) result += "\\x1F";
            else result += static_cast<char>(c);
            require(result.size() <= max_string, NGX_PURGE_STORE_LIMIT, "encoded index key too long");
        }
    }
    return result;
}
std::string materialize(ngx_cache_purge_store_t *store, const Value &value,
                        Transaction *tx = nullptr, ngx_cache_purge_cursor_t *view = nullptr) {
    if (!value.format) return value.raw;
    std::vector<std::string> parts;
    for (auto id : value.parts) {
        auto body = tx ? tx->get(common, row('V', id)) : read(store, common, row('V', id), view);
        require(body.has_value(), NGX_PURGE_STORE_CORRUPT, "missing value component");
        auto component = decode_value(*body);
        require(component.format == 0, NGX_PURGE_STORE_CORRUPT, "recursive value components are unsupported");
        parts.push_back(std::move(component.raw));
    }
    if (value.format == 1) return encoded_key(parts);
    require(parts[0].size() + 3 + parts[1].size() + parts[2].size() <= max_string,
            NGX_PURGE_STORE_CORRUPT, "oversized reconstructed URL");
    return parts[0] + "://" + parts[1] + parts[2];
}

class Values {
    Transaction &tx_;
    std::map<std::string, uint64_t> pending_;
public:
    explicit Values(Transaction &tx) : tx_(tx) {}
    Value load(uint64_t id) { return decode_value(tx_.need(common, row('V', id))); }
    std::string text(uint64_t id) { return materialize(tx_.store, load(id), &tx_); }
    uint64_t find(const std::string &text) {
        auto known = pending_.find(text); if (known != pending_.end()) return known->second;
        const std::string prefix = "S" + digest(text);
        Cursor cursor; cursor.open(tx_.store, common, prefix);
        std::string key, ignored; unsigned candidates = 0;
        while (cursor.next(key, ignored)) {
            require(++candidates <= 16, NGX_PURGE_STORE_LIMIT, "index digest bucket limit");
            require(key.size() == prefix.size() + 8, NGX_PURGE_STORE_CORRUPT, "invalid digest entry");
            uint64_t id = integer(key.substr(prefix.size()));
            auto body = tx_.get(common, row('V', id));
            if (!body) continue; /* removed in this transaction */
            if (materialize(tx_.store, decode_value(*body), &tx_) == text) {
                pending_[text] = id; return id;
            }
        }
        return 0;
    }
    void retain(uint64_t id) {
        auto value = load(id);
        require(value.references != UINT64_MAX, NGX_PURGE_STORE_LIMIT, "index value reference overflow");
        ++value.references; tx_.put(common, row('V', id), encode(value));
    }
    void drop(uint64_t id) {
        auto value = load(id);
        require(value.references > 0, NGX_PURGE_STORE_CORRUPT, "index value reference underflow");
        if (--value.references) { tx_.put(common, row('V', id), encode(value)); return; }
        const std::string text = materialize(tx_.store, value, &tx_);
        tx_.del(common, "S" + digest(text) + number(id)); tx_.del(common, row('V', id));
        pending_.erase(text);
        for (auto part : value.parts) drop(part);
    }
    uint64_t intern(const std::string &text, unsigned format = 0,
                    const std::vector<std::string> &components = {}, bool leaf = false) {
        uint64_t id = find(text);
        if (id) {
            auto value = load(id);
            if (leaf && value.format) {
                auto old_parts = value.parts;
                value.format = 0; value.parts.clear(); value.raw = text;
                tx_.put(common, row('V', id), encode(value));
                for (auto part : old_parts) drop(part);
            }
            return id;
        }
        Value value; value.format = format;
        if (!format) value.raw = text;
        else for (const auto &component : components) {
            uint64_t part = intern(component, 0, {}, true);
            retain(part); value.parts.push_back(part);
        }
        id = tx_.allocate();
        tx_.put(common, row('V', id), encode(value));
        tx_.put(common, "S" + digest(text) + number(id), "");
        pending_[text] = id; return id;
    }
};

struct Record {
    FileId id{}, previous{};
    FileState state = FileState::prepared;
    uint64_t revision = 1;
    uint64_t key = 0, url = 0, host = 0, ns = 0, root = 0, zone = 0, range = 0, vary = 0;
    std::string path, metadata, input_digest;
    std::vector<uint64_t> tag_ids;
};
std::string encode(const Record &record) {
    Writer w; w.byte(1); w.id(record.id); w.id(record.previous);
    w.byte(static_cast<unsigned>(record.state)); w.u64(record.revision);
    for (auto id : {record.key, record.url, record.host, record.ns, record.root, record.zone, record.range, record.vary}) w.u64(id);
    w.string(record.path); w.string(record.metadata); w.string(record.input_digest);
    w.byte(record.tag_ids.size()); for (auto id : record.tag_ids) w.u64(id);
    return w.data;
}
Record decode_record(const std::string &data) {
    Reader r{data}; Record record;
    require(r.byte() == 1, NGX_PURGE_STORE_CORRUPT, "invalid file record version");
    record.id = r.id(); record.previous = r.id(); record.state = static_cast<FileState>(r.byte());
    require(record.state >= FileState::prepared && record.state <= FileState::retired, NGX_PURGE_STORE_CORRUPT, "invalid file state");
    record.revision = r.u64();
    require(record.revision != 0 && !zero(record.id), NGX_PURGE_STORE_CORRUPT, "invalid file identity/revision");
    for (auto *id : {&record.key, &record.url, &record.host, &record.ns, &record.root, &record.zone, &record.range, &record.vary}) {
        *id = r.u64(); require(*id != 0, NGX_PURGE_STORE_CORRUPT, "invalid value reference");
    }
    record.path = r.string(); record.metadata = r.string(); record.input_digest = r.string();
    require(!record.path.empty() && record.path.size() <= max_string && record.metadata.size() <= max_metadata
            && record.input_digest.size() == 32, NGX_PURGE_STORE_CORRUPT, "invalid file record fields");
    unsigned count = r.byte(); require(count <= max_tags, NGX_PURGE_STORE_CORRUPT, "invalid TAG count");
    while (count--) {
        auto id = r.u64();
        require(id != 0 && std::find(record.tag_ids.begin(), record.tag_ids.end(), id) == record.tag_ids.end(),
                NGX_PURGE_STORE_CORRUPT, "invalid or duplicate TAG reference");
        record.tag_ids.push_back(id);
    }
    r.end(); return record;
}
Record load(Transaction &tx, const FileId &id, uint64_t revision) {
    auto body = tx.get(common, object(id));
    require(body.has_value(), NGX_PURGE_STORE_NOT_FOUND, "file instance not indexed");
    auto record = decode_record(*body);
    require(record.id == id, NGX_PURGE_STORE_CORRUPT, "file record identity mismatch");
    require(record.revision == revision, NGX_PURGE_STORE_CONFLICT, "file revision changed");
    return record;
}
Match match(const Record &record) { return {record.id, record.revision, record.state}; }
void advance(Record &record) {
    require(record.revision != UINT64_MAX, NGX_PURGE_STORE_LIMIT, "file revision exhausted"); ++record.revision;
}
bool relative_path(const std::string &path) {
    if (path.empty() || path.front() == '/' || path.find('\0') != std::string::npos) return false;
    size_t start = 0;
    while (start < path.size()) {
        size_t end = path.find('/', start); if (end == std::string::npos) end = path.size();
        auto part = path.substr(start, end - start);
        if (part.empty() || part == "." || part == "..") return false;
        start = end + 1;
    }
    return path.back() != '/';
}
std::vector<std::string> canonical_tags(const std::vector<std::string> &input) {
    require(input.size() <= max_tags, NGX_PURGE_STORE_LIMIT, "too many TAGs");
    for (const auto &tag : input) require(!tag.empty() && tag.size() <= 1024, NGX_PURGE_STORE_INVALID, "invalid TAG length");
    auto result = input;
    std::sort(result.begin(), result.end()); result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}
std::string fingerprint(const File &file, const std::vector<std::string> &tag_values) {
    require(!zero(file.id), NGX_PURGE_STORE_INVALID, "empty file instance ID");
    require(file.state == FileState::prepared && file.revision == 0, NGX_PURGE_STORE_INVALID, "prepare requires a new file descriptor");
    require(!file.key.empty() && !file.url.empty() && !file.host.empty() && !file.tag_namespace.empty()
            && !file.zone.empty() && !file.root.empty() && file.root.front() == '/'
            && file.root.find('\0') == std::string::npos && relative_path(file.path),
            NGX_PURGE_STORE_INVALID, "invalid file identity or path");
    require(file.tag_namespace.size() <= 255, NGX_PURGE_STORE_INVALID, "TAG namespace too long");
    for (unsigned char c : file.tag_namespace)
        require(c > 32 && c < 127 && c != ',', NGX_PURGE_STORE_INVALID, "invalid TAG namespace");
    size_t total = file.metadata.size();
    require(total <= max_metadata, NGX_PURGE_STORE_LIMIT, "file metadata too large");
    for (const auto *field : {&file.key, &file.url, &file.host, &file.tag_namespace,
                              &file.root, &file.zone, &file.path, &file.range, &file.vary}) {
        require(field->size() <= max_string, NGX_PURGE_STORE_LIMIT, "index field too long"); total += field->size();
    }
    for (const auto &part : file.components) {
        require(part.size() <= max_string, NGX_PURGE_STORE_LIMIT, "component too long"); total += part.size();
    }
    for (const auto &tag : tag_values) total += tag.size();
    require(total <= max_input, NGX_PURGE_STORE_LIMIT, "file index input budget");
    Writer w; w.id(file.id);
    for (const auto *field : {&file.key, &file.url, &file.host, &file.tag_namespace,
                              &file.root, &file.zone, &file.path, &file.range, &file.vary}) {
        require(field->size() <= max_string, NGX_PURGE_STORE_LIMIT, "index field too long"); w.string(*field);
    }
    require(file.metadata.size() <= max_metadata, NGX_PURGE_STORE_LIMIT, "file metadata too large");
    w.string(file.metadata); for (const auto &tag : tag_values) w.string(tag);
    if (file.structured_key) {
        std::vector<std::string> parts(file.components.begin(), file.components.begin() + 5);
        require(encoded_key(parts) == file.key, NGX_PURGE_STORE_INVALID, "key components do not match final key bytes");
    }
    if (file.structured_url)
        require(file.components[1] + "://" + file.components[2] + file.components[3] == file.url,
                NGX_PURGE_STORE_INVALID, "URL components do not match final URL bytes");
    return digest(w.data);
}

void url_link(Transaction &tx, Values &pool, uint64_t key, uint64_t url, const std::string &text) {
    const auto edge = row('A', url) + number(key);
    auto count = tx.get(common, row('u', url));
    auto projection = tx.get(urls, "U" + text);
    require(count ? projection && *projection == number(url) && integer(*count) > 0 : !projection,
            NGX_PURGE_STORE_CORRUPT, "inconsistent URL projection");
    auto forward = tx.get(urls, edge), reverse = tx.get(common, row('a', key) + number(url));
    require(forward.has_value() == reverse.has_value() && (!forward || count),
            NGX_PURGE_STORE_CORRUPT, "inconsistent URL/key relationship");
    if (forward) return;
    if (!count) { pool.retain(url); tx.put(urls, "U" + text, number(url)); }
    uint64_t n = count ? integer(*count) : 0;
    require(n != UINT64_MAX, NGX_PURGE_STORE_LIMIT, "URL relationship overflow");
    tx.put(common, row('u', url), number(n + 1));
    tx.put(urls, edge, ""); tx.put(common, row('a', key) + number(url), "");
}
void tag_links(Transaction &tx, const Record &record, bool add) {
    for (auto tag : record.tag_ids) {
        auto n = tag_edge('N', record.ns, tag, record.id), h = tag_edge('H', record.host, tag, record.id);
        if (add) { tx.put(tags, n, ""); tx.put(tags, h, ""); }
        else { tx.del(tags, n); tx.del(tags, h); }
    }
}
} // namespace

Index::Index(ngx_cache_purge_store_t *store) : store_(store) {
    Transaction tx(store_);
    auto schema = tx.get(common, "M:index");
    if (schema) {
        require(*schema == "1" || *schema == "2", NGX_PURGE_STORE_SCHEMA, "unsupported file index schema");
        require(integer(tx.need(common, "M:next")) != 0, NGX_PURGE_STORE_CORRUPT, "invalid index ID allocator");
        /* Record encodings are unchanged. Version 2 additionally permits a
         * committed instance to retain its pending-publication lease. */
        if (*schema == "1") { tx.put(common, "M:index", "2"); tx.commit(); }
        return;
    }
    for (Cf cf : {common, urls, tags}) {
        Cursor cursor; cursor.open(store_, cf, "");
        std::string key, value;
        while (cursor.next(key, value))
            require(cf == common && !key.empty() && key[0] == 0,
                    NGX_PURGE_STORE_SCHEMA, "file index schema missing from non-empty store");
    }
    tx.put(common, "M:index", "2"); tx.put(common, "M:next", number(1)); tx.commit();
}

static Match prepare_record(Transaction &tx, Values &pool, const File &file, const FileId &expected_previous) {
    const auto tag_values = canonical_tags(file.tags);
    const std::string hash = fingerprint(file, tag_values);
    require(file.id != expected_previous, NGX_PURGE_STORE_INVALID, "file cannot replace itself");
    if (auto body = tx.get(common, object(file.id))) {
        auto existing = decode_record(*body);
        require(existing.id == file.id && existing.input_digest == hash && existing.previous == expected_previous
                && existing.state != FileState::retired, NGX_PURGE_STORE_CONFLICT, "file ID reused with another fill");
        if (existing.state == FileState::prepared)
            require(tx.need(common, lease(existing.root, existing.path)) == id_bytes(file.id),
                    NGX_PURGE_STORE_CORRUPT, "prepared file has no matching lease");
        return match(existing);
    }
    Record record; record.id = file.id; record.previous = expected_previous;
    record.path = file.path; record.metadata = file.metadata; record.input_digest = hash;
    record.root = pool.intern(file.root, 0, {}, true);
    pool.retain(record.root);
    auto current = tx.get(common, location(record.root, record.path));
    require((current ? file_id(*current) : FileId{}) == expected_previous,
            NGX_PURGE_STORE_CONFLICT, "cache path instance changed");
    require(!tx.get(common, lease(record.root, record.path)), NGX_PURGE_STORE_BUSY, "cache path has an unfinished fill");
    record.key = pool.intern(file.key, file.structured_key ? 1 : 0,
                             std::vector<std::string>(file.components.begin(), file.components.begin() + 5));
    auto count = tx.get(common, row('k', record.key));
    auto projection = tx.get(common, "K" + file.key);
    require(count ? projection && *projection == number(record.key) : !projection,
            NGX_PURGE_STORE_CORRUPT, "inconsistent key projection");
    if (!count) { pool.retain(record.key); tx.put(common, "K" + file.key, number(record.key)); }
    uint64_t files = count ? integer(*count) : 0;
    require(files != UINT64_MAX, NGX_PURGE_STORE_LIMIT, "key file count overflow");
    tx.put(common, row('k', record.key), number(files + 1));
    record.url = pool.intern(file.url, file.structured_url ? 2 : 0,
                             std::vector<std::string>(file.components.begin() + 1, file.components.begin() + 4));
    url_link(tx, pool, record.key, record.url, file.url);
    auto field = [&](const std::string &value) { auto id = pool.intern(value, 0, {}, true); pool.retain(id); return id; };
    record.host = field(file.host);
    record.ns = field(file.tag_namespace);
    record.zone = field(file.zone);
    record.range = field(file.range);
    record.vary = field(file.vary);
    for (const auto &tag : tag_values) { auto id = pool.intern(tag, 0, {}, true); pool.retain(id); record.tag_ids.push_back(id); }
    tx.put(common, file_edge(record.key, record.id), "");
    tx.put(common, row('H', record.host) + id_bytes(record.id), "");
    tag_links(tx, record, true);
    tx.put(common, object(record.id), encode(record));
    tx.put(common, lease(record.root, record.path), id_bytes(record.id));
    return match(record);
}

Match Index::prepare(const File &file, const FileId &previous) {
    Transaction tx(store_); Values pool(tx);
    auto result = prepare_record(tx, pool, file, previous); tx.commit(); return result;
}

std::vector<Match> Index::prepare_batch(const std::vector<Fill> &fills) {
    require(!fills.empty() && fills.size() <= 32, NGX_PURGE_STORE_LIMIT, "fill batch must contain 1-32 files");
    size_t remaining = max_input;
    for (const auto &fill : fills) {
        require(fill.file.tags.size() <= max_tags, NGX_PURGE_STORE_LIMIT, "too many TAGs");
        auto charge = [&](const std::string &value) {
            require(value.size() <= remaining, NGX_PURGE_STORE_LIMIT, "fill batch input budget"); remaining -= value.size();
        };
        for (const auto *field : {&fill.file.key, &fill.file.url, &fill.file.host, &fill.file.tag_namespace,
                                  &fill.file.root, &fill.file.zone, &fill.file.path, &fill.file.range,
                                  &fill.file.vary, &fill.file.metadata}) charge(*field);
        for (const auto &field : fill.file.components) charge(field);
        for (const auto &field : fill.file.tags) charge(field);
    }
    Transaction tx(store_); Values pool(tx); std::vector<Match> result;
    result.reserve(fills.size());
    for (const auto &fill : fills) result.push_back(prepare_record(tx, pool, fill.file, fill.previous));
    tx.commit(); return result;
}

static Match commit_record(Transaction &tx, const FileId &id, uint64_t revision,
                           bool pending_publication = false) {
    auto record = load(tx, id, revision);
    require(record.state == FileState::prepared, NGX_PURGE_STORE_CONFLICT, "file is not prepared");
    require(tx.need(common, lease(record.root, record.path)) == id_bytes(id), NGX_PURGE_STORE_CONFLICT, "fill lease changed");
    auto current = tx.get(common, location(record.root, record.path));
    require((current ? file_id(*current) : FileId{}) == record.previous, NGX_PURGE_STORE_CONFLICT, "cache path changed before commit");
    if (!zero(record.previous)) {
        auto old = decode_record(tx.need(common, object(record.previous)));
        require(old.id == record.previous && old.state == FileState::committed
                && old.root == record.root && old.path == record.path,
                NGX_PURGE_STORE_CORRUPT, "previous path record is inconsistent");
        old.state = FileState::retired; advance(old); tx.put(common, object(old.id), encode(old));
        tx.put(common, "g" + id_bytes(old.id), number(old.revision));
    }
    record.state = FileState::committed; advance(record);
    tx.put(common, object(id), encode(record)); tx.put(common, location(record.root, record.path), id_bytes(id));
    if (!pending_publication) tx.del(common, lease(record.root, record.path));
    return match(record);
}

Match Index::commit(const FileId &id, uint64_t revision) {
    Transaction tx(store_); auto result = commit_record(tx, id, revision); tx.commit(); return result;
}
std::vector<Match> Index::commit_batch(const std::vector<ExpectedFile> &files) {
    require(!files.empty() && files.size() <= 32, NGX_PURGE_STORE_LIMIT, "commit batch must contain 1-32 files");
    Transaction tx(store_); std::vector<Match> result; result.reserve(files.size());
    for (const auto &file : files) result.push_back(commit_record(tx, file.id, file.revision));
    tx.commit(); return result;
}

Match Index::commit_pending(const FileId &id, uint64_t revision) {
    Transaction tx(store_); auto result = commit_record(tx, id, revision, true);
    tx.commit(); return result;
}

Match Index::publish(const FileId &id, uint64_t revision) {
    Transaction tx(store_); auto record = load(tx, id, revision);
    require(record.state == FileState::committed, NGX_PURGE_STORE_CONFLICT, "file is not committed");
    require(tx.need(common, location(record.root, record.path)) == id_bytes(id),
            NGX_PURGE_STORE_CONFLICT, "cache path changed before publication");
    auto intent = tx.get(common, lease(record.root, record.path));
    if (intent) {
        require(*intent == id_bytes(id), NGX_PURGE_STORE_BUSY, "another fill owns the path lease");
        tx.del(common, lease(record.root, record.path));
    }
    tx.commit(); return match(record);
}

Match Index::discard_publication(const FileId &id, uint64_t revision) {
    Transaction tx(store_); auto record = load(tx, id, revision);
    require(record.state == FileState::committed, NGX_PURGE_STORE_CONFLICT, "file is not committed");
    auto intent = tx.get(common, lease(record.root, record.path));
    require(intent && *intent == id_bytes(id),
            NGX_PURGE_STORE_CONFLICT, "publication lease changed");
    require(tx.need(common, location(record.root, record.path)) == id_bytes(id),
            NGX_PURGE_STORE_CONFLICT, "cache path changed before publication discard");
    tx.del(common, lease(record.root, record.path));
    tx.del(common, location(record.root, record.path));
    record.state = FileState::retired; advance(record);
    tx.put(common, object(id), encode(record));
    tx.put(common, "g" + id_bytes(id), number(record.revision));
    tx.commit(); return match(record);
}

std::optional<Match> Index::current(const std::string &root, const std::string &path) {
    require(!root.empty() && root.front() == '/' && root.find('\0') == std::string::npos
            && relative_path(path), NGX_PURGE_STORE_INVALID, "invalid indexed path");
    require(root.size() <= max_string && path.size() <= max_string,
            NGX_PURGE_STORE_LIMIT, "indexed path too long");
    Transaction tx(store_); Values values(tx);
    auto root_id = values.find(root);
    if (!root_id) return {};
    auto value = tx.get(common, location(root_id, path));
    if (!value) return {};
    auto id = file_id(*value);
    auto record = decode_record(tx.need(common, object(id)));
    require(record.id == id && record.root == root_id && record.path == path
            && record.state == FileState::committed,
            NGX_PURGE_STORE_CORRUPT, "inconsistent current cache path");
    return match(record);
}

Match Index::abort_prepare(const FileId &id, uint64_t revision) {
    Transaction tx(store_); auto record = load(tx, id, revision);
    require(record.state == FileState::prepared, NGX_PURGE_STORE_CONFLICT, "file is not prepared");
    require(tx.need(common, lease(record.root, record.path)) == id_bytes(id), NGX_PURGE_STORE_CONFLICT, "fill lease changed");
    record.state = FileState::retired; advance(record);
    tx.del(common, lease(record.root, record.path)); tx.put(common, object(id), encode(record));
    tx.put(common, "g" + id_bytes(id), number(record.revision));
    tx.commit(); return match(record);
}

static Match retire_record(Transaction &tx, const FileId &id, uint64_t revision) {
    auto record = load(tx, id, revision);
    if (record.state == FileState::retired) return match(record);
    require(record.state == FileState::committed, NGX_PURGE_STORE_CONFLICT, "prepared file must be drained and aborted");
    require(!tx.get(common, lease(record.root, record.path)), NGX_PURGE_STORE_BUSY, "cache path has an unfinished fill");
    auto current = tx.get(common, location(record.root, record.path));
    if (current && file_id(*current) == id) tx.del(common, location(record.root, record.path));
    record.state = FileState::retired; advance(record); tx.put(common, object(id), encode(record));
    tx.put(common, "g" + id_bytes(id), number(record.revision));
    return match(record);
}

Match Index::retire(const FileId &id, uint64_t revision) {
    Transaction tx(store_); auto result = retire_record(tx, id, revision); tx.commit(); return result;
}
std::vector<Match> Index::retire_batch(const std::vector<ExpectedFile> &files) {
    require(!files.empty() && files.size() <= 32, NGX_PURGE_STORE_LIMIT, "retire batch must contain 1-32 files");
    Transaction tx(store_); std::vector<Match> result; result.reserve(files.size());
    for (const auto &file : files) result.push_back(retire_record(tx, file.id, file.revision));
    tx.commit(); return result;
}

Match Index::update_metadata(const FileId &id, uint64_t revision,
                             const std::string &metadata, const std::vector<std::string> &input_tags) {
    require(metadata.size() <= max_metadata, NGX_PURGE_STORE_LIMIT, "file metadata too large");
    auto tag_values = canonical_tags(input_tags);
    Transaction tx(store_); Values pool(tx); auto record = load(tx, id, revision);
    require(record.state == FileState::committed, NGX_PURGE_STORE_CONFLICT, "file is not committed");
    require(!tx.get(common, lease(record.root, record.path)), NGX_PURGE_STORE_BUSY, "cache path has an unfinished fill");
    std::map<std::string, uint64_t> old;
    for (auto tag : record.tag_ids) old.emplace(pool.text(tag), tag);
    std::vector<uint64_t> selected;
    for (const auto &tag : tag_values) {
        auto found = old.find(tag);
        if (found != old.end()) { selected.push_back(found->second); old.erase(found); continue; }
        auto value = pool.intern(tag, 0, {}, true); pool.retain(value); selected.push_back(value);
        tx.put(tags, tag_edge('N', record.ns, value, id), "");
        tx.put(tags, tag_edge('H', record.host, value, id), "");
    }
    for (const auto &removed : old) {
        tx.del(tags, tag_edge('N', record.ns, removed.second, id));
        tx.del(tags, tag_edge('H', record.host, removed.second, id)); pool.drop(removed.second);
    }
    record.tag_ids = std::move(selected); record.metadata = metadata; advance(record);
    tx.put(common, object(id), encode(record)); tx.commit(); return match(record);
}

File Index::get(const FileId &id) {
    Transaction tx(store_); Values pool(tx);
    auto body = tx.get(common, object(id)); require(body.has_value(), NGX_PURGE_STORE_NOT_FOUND, "file instance not indexed");
    auto record = decode_record(*body); require(record.id == id, NGX_PURGE_STORE_CORRUPT, "file identity mismatch");
    File result; result.id = id; result.state = record.state; result.revision = record.revision;
    result.key = pool.text(record.key); result.url = pool.text(record.url); result.host = pool.text(record.host);
    result.tag_namespace = pool.text(record.ns); result.root = pool.text(record.root); result.zone = pool.text(record.zone);
    result.range = pool.text(record.range); result.vary = pool.text(record.vary); result.path = record.path;
    result.metadata = record.metadata;
    for (auto tag : record.tag_ids) result.tags.push_back(pool.text(tag));
    return result;
}

void Index::add_url(const std::string &key, const std::string &url) {
    require(!key.empty() && !url.empty() && key.size() <= max_string && url.size() <= max_string,
            NGX_PURGE_STORE_INVALID, "invalid URL association");
    Transaction tx(store_); Values pool(tx);
    auto identity = tx.get(common, "K" + key); require(identity.has_value(), NGX_PURGE_STORE_NOT_FOUND, "key is not indexed");
    uint64_t id = integer(*identity);
    require(integer(tx.need(common, row('k', id))) != 0, NGX_PURGE_STORE_CONFLICT, "key has no file references");
    url_link(tx, pool, id, pool.intern(url), url); tx.commit();
}

static void collect_record(Transaction &tx, Values &pool, const FileId &id, uint64_t revision) {
    auto record = load(tx, id, revision);
    require(record.state == FileState::retired, NGX_PURGE_STORE_CONFLICT, "only retired file records can be collected");
    auto current = tx.get(common, location(record.root, record.path));
    require(!current || file_id(*current) != id, NGX_PURGE_STORE_CORRUPT, "retired file is still the current path instance");
    auto owner = tx.get(common, lease(record.root, record.path));
    require(!owner || file_id(*owner) != id, NGX_PURGE_STORE_BUSY, "file still owns its fill lease");
    tag_links(tx, record, false);
    tx.del(common, row('H', record.host) + id_bytes(id)); tx.del(common, file_edge(record.key, id));
    tx.del(common, object(id)); tx.del(common, "g" + id_bytes(id));
    for (auto value : {record.root, record.host, record.ns, record.zone, record.range, record.vary}) pool.drop(value);
    for (auto tag : record.tag_ids) pool.drop(tag);
    uint64_t count = integer(tx.need(common, row('k', record.key)));
    require(count > 0, NGX_PURGE_STORE_CORRUPT, "key file count underflow");
    tx.put(common, row('k', record.key), number(count - 1));
    if (count == 1) tx.put(common, row('G', record.key), "");
}

void Index::collect_file(const FileId &id, uint64_t revision) {
    Transaction tx(store_); Values pool(tx); collect_record(tx, pool, id, revision); tx.commit();
}
void Index::collect_files(const std::vector<ExpectedFile> &files) {
    require(!files.empty() && files.size() <= 32, NGX_PURGE_STORE_LIMIT, "collect batch must contain 1-32 files");
    Transaction tx(store_); Values pool(tx);
    for (const auto &file : files) collect_record(tx, pool, file.id, file.revision);
    tx.commit();
}

size_t Index::collect_keys(size_t edge_budget) {
    require(edge_budget > 0 && edge_budget <= 32, NGX_PURGE_STORE_INVALID, "invalid key collection budget");
    Cursor pending; pending.open(store_, common, "G");
    std::string key, ignored; if (!pending.next(key, ignored)) return 0;
    require(key.size() == 9, NGX_PURGE_STORE_CORRUPT, "invalid key collection entry");
    uint64_t id = integer(key.substr(1)); pending.close();
    Transaction tx(store_); Values pool(tx);
    auto count = tx.get(common, row('k', id));
    if (!count || integer(*count) != 0) { tx.del(common, key); tx.commit(); return 1; }
    Cursor edges; edges.open(store_, common, row('a', id));
    {
        Cursor files; files.open(store_, common, row('F', id));
        std::string file, value;
        require(!files.next(file, value), NGX_PURGE_STORE_CORRUPT, "empty key still has file postings");
    }
    size_t removed = 0; std::string edge;
    while (removed < edge_budget && edges.next(edge, ignored)) {
        require(edge.size() == 17, NGX_PURGE_STORE_CORRUPT, "invalid reverse URL relation");
        uint64_t url = integer(edge.substr(9));
        tx.del(common, edge); tx.del(urls, row('A', url) + number(id));
        uint64_t uses = integer(tx.need(common, row('u', url)));
        require(uses > 0, NGX_PURGE_STORE_CORRUPT, "URL relationship count underflow");
        if (uses == 1) {
            tx.del(urls, "U" + pool.text(url)); tx.del(common, row('u', url)); pool.drop(url);
        } else tx.put(common, row('u', url), number(uses - 1));
        ++removed;
    }
    bool remaining = edges.next(edge, ignored); edges.close();
    if (!remaining) {
        const std::string text = pool.text(id);
        require(tx.need(common, "K" + text) == number(id), NGX_PURGE_STORE_CORRUPT, "key projection identity mismatch");
        tx.del(common, "K" + text); tx.del(common, row('k', id)); tx.del(common, row('G', id)); pool.drop(id);
    }
    tx.commit(); return removed + (!remaining ? 1 : 0);
}

struct Query::Impl {
    ngx_cache_purge_store_t *store;
    Selector selector;
    Cursor outer, links, files;
    Cf outer_cf = common;
    std::string prefix, current_outer, current_link, last_file, token, signature;
    bool empty = false;
    char recovery = 0;

    Impl(ngx_cache_purge_store_t *s, const std::string &resume, char queue) : store(s), recovery(queue) {
        prefix.assign(1, queue); signature = digest("index-recovery-v1:" + prefix);
        std::string after;
        if (!resume.empty()) {
            require(resume.size() <= max_string + 256, NGX_PURGE_STORE_INVALID, "invalid pending-fill bookmark");
            Reader r{resume};
            require(r.byte() == 1 && r.take(32) == signature && r.string().empty() && r.string().empty(),
                    NGX_PURGE_STORE_INVALID, "bookmark is not for pending fills");
            after = r.string(); r.end();
            require((queue == 'I' ? after.size() > 9 : after.size() == 17) && after[0] == queue,
                    NGX_PURGE_STORE_INVALID, "invalid recovery position");
            token = resume;
        }
        outer.open(store, common, prefix, after);
    }

    Impl(ngx_cache_purge_store_t *s, Selector selected, const std::string &resume)
        : store(s), selector(std::move(selected)) {
        require(selector.kind >= SelectorKind::key && selector.kind <= SelectorKind::host_tag,
                NGX_PURGE_STORE_INVALID, "invalid index selector");
        require(selector.value.size() <= max_string && selector.scope.size() <= max_string,
                NGX_PURGE_STORE_LIMIT, "index selector too long");
        bool tag = selector.kind == SelectorKind::namespace_tag || selector.kind == SelectorKind::host_tag;
        require(!tag || selector.value.size() <= 1024, NGX_PURGE_STORE_INVALID, "TAG selector too long");
        require(tag ? !selector.scope.empty() : selector.scope.empty(), NGX_PURGE_STORE_INVALID, "invalid selector scope");
        require(!selector.value.empty() || selector.kind == SelectorKind::key_prefix || selector.kind == SelectorKind::url_prefix,
                NGX_PURGE_STORE_INVALID, "empty index selector");
        Writer identity; identity.byte(static_cast<unsigned>(selector.kind)); identity.string(selector.value); identity.string(selector.scope);
        signature = digest(identity.data);
        std::string saved_outer, saved_link, saved_file;
        if (!resume.empty()) {
            require(resume.size() <= max_string + 256, NGX_PURGE_STORE_INVALID, "invalid index bookmark size");
            Reader r{resume};
            require(r.byte() == 1 && r.take(32) == signature, NGX_PURGE_STORE_INVALID, "bookmark belongs to another selector");
            saved_outer = r.string(); saved_link = r.string(); saved_file = r.string(); r.end();
            token = resume;
        }
        Transaction tx(store); Values values(tx);
        uint64_t id = 0;
        switch (selector.kind) {
        case SelectorKind::key:
        case SelectorKind::url: {
            bool url = selector.kind == SelectorKind::url;
            auto value = tx.get(url ? urls : common, std::string(1, url ? 'U' : 'K') + selector.value);
            if (!value) { empty = true; return; }
            id = integer(*value); prefix = row(url ? 'A' : 'F', id); outer_cf = url ? urls : common;
            break;
        }
        case SelectorKind::key_prefix: prefix = "K" + selector.value; break;
        case SelectorKind::url_prefix: prefix = "U" + selector.value; outer_cf = urls; break;
        case SelectorKind::host:
            id = values.find(selector.value);
            if (!id) { empty = true; return; }
            prefix = row('H', id); break;
        case SelectorKind::namespace_tag:
        case SelectorKind::host_tag: {
            uint64_t scope = values.find(selector.scope), value = values.find(selector.value);
            if (!scope || !value) { empty = true; return; }
            prefix = row(selector.kind == SelectorKind::namespace_tag ? 'N' : 'H', scope) + number(value);
            outer_cf = tags; break;
        }
        }
        auto compatible = [](const std::string &key, const std::string &range, size_t size) {
            return key.empty() || (key.size() == size && key.compare(0, range.size(), range) == 0);
        };
        if (selector.kind == SelectorKind::key_prefix || selector.kind == SelectorKind::url_prefix) {
            require(saved_outer.empty() || saved_outer.compare(0, prefix.size(), prefix) == 0,
                    NGX_PURGE_STORE_INVALID, "bookmark is outside selector prefix");
            outer.open(store, outer_cf, prefix, saved_outer);
            if (!saved_outer.empty()) {
                auto value = read(store, outer_cf, saved_outer, outer.handle);
                if (value) {
                    current_outer = saved_outer;
                    if (selector.kind == SelectorKind::key_prefix) open_files(integer(*value), saved_file);
                    else open_links(integer(*value), saved_link, saved_file);
                }
            }
        } else if (selector.kind == SelectorKind::url) {
            require(saved_outer.empty(), NGX_PURGE_STORE_INVALID, "unexpected URL bookmark outer key");
            if (!compatible(saved_link, prefix, prefix.size() + 8)) saved_link.clear();
            outer.open(store, outer_cf, prefix, saved_link);
            if (!saved_link.empty() && read(store, urls, saved_link, outer.handle)) {
                current_link = saved_link; open_files(integer(saved_link.substr(9)), saved_file);
            }
        } else {
            require(saved_outer.empty() && saved_link.empty(), NGX_PURGE_STORE_INVALID, "unexpected direct bookmark join");
            if (!compatible(saved_file, prefix, prefix.size() + 16)) saved_file.clear();
            outer.open(store, outer_cf, prefix, saved_file);
        }
    }

    void open_files(uint64_t key, const std::string &after = "") {
        auto count = read(store, common, row('k', key), outer.handle);
        require(count.has_value(), NGX_PURGE_STORE_CORRUPT, "URL/key posting has no key record");
        const std::string range = row('F', key);
        std::string resume = after;
        if (resume.size() != range.size() + 16 || resume.compare(0, range.size(), range) != 0) resume.clear();
        files.open(store, common, range, resume, outer.handle);
    }
    void open_links(uint64_t url, const std::string &after = "", const std::string &file = "") {
        const std::string range = row('A', url);
        std::string resume = after;
        if (resume.size() != range.size() + 8 || resume.compare(0, range.size(), range) != 0) resume.clear();
        links.open(store, urls, range, resume, outer.handle);
        if (!resume.empty() && read(store, urls, resume, outer.handle)) {
            current_link = resume; open_files(integer(resume.substr(9)), file);
        }
    }
    bool candidate(const std::string &key, Cf cf, ngx_cache_purge_cursor_t *view, Match &output) {
        bool tagged = cf == tags;
        require(key.size() == (tagged ? 33U : 25U), NGX_PURGE_STORE_CORRUPT, "invalid file posting");
        FileId id = file_id(key.substr(key.size() - 16));
        auto value = read(store, common, object(id), view);
        require(value.has_value(), NGX_PURGE_STORE_CORRUPT, "file posting has no object");
        auto record = decode_record(*value);
        require(record.id == id, NGX_PURGE_STORE_CORRUPT, "file posting identity mismatch");
        uint64_t scope = integer(key.substr(1, 8));
        if (tagged) {
            require((key[0] == 'N' ? record.ns : record.host) == scope,
                    NGX_PURGE_STORE_CORRUPT, "TAG posting scope mismatch");
            uint64_t tag = integer(key.substr(9, 8));
            require(std::find(record.tag_ids.begin(), record.tag_ids.end(), tag) != record.tag_ids.end(),
                    NGX_PURGE_STORE_CORRUPT, "TAG posting is absent from file");
        } else {
            require((key[0] == 'F' ? record.key : record.host) == scope,
                    NGX_PURGE_STORE_CORRUPT, "file posting key/host mismatch");
        }
        if (record.state == FileState::retired) return false;
        output = match(record); last_file = key;
        Writer w; w.byte(1); w.data += signature; w.string(current_outer); w.string(current_link); w.string(last_file);
        token = std::move(w.data); return true;
    }
    bool next(Match &output) {
        if (empty) return false;
        std::string key, value;
        if (recovery) {
            if (!outer.next(key, value)) { outer.close(); empty = true; return false; }
            require(recovery == 'I' || key.size() == 17, NGX_PURGE_STORE_CORRUPT, "invalid retired-file queue key");
            auto id = recovery == 'I' ? file_id(value) : file_id(key.substr(1));
            auto body = read(store, common, object(id), outer.handle);
            require(body.has_value(), NGX_PURGE_STORE_CORRUPT, "pending fill has no file record");
            auto record = decode_record(*body);
            require(record.id == id, NGX_PURGE_STORE_CORRUPT, "recovery file ID mismatch");
            if (recovery == 'I') {
                require((record.state == FileState::prepared || record.state == FileState::committed)
                        && key == lease(record.root, record.path),
                        NGX_PURGE_STORE_CORRUPT, "invalid pending-fill relationship");
                if (record.state == FileState::committed) {
                    auto current = read(store, common, location(record.root, record.path), outer.handle);
                    require(current && *current == id_bytes(id), NGX_PURGE_STORE_CORRUPT,
                            "unpublished file has no matching current path");
                }
            }
            else require(record.state == FileState::retired && record.revision == integer(value),
                         NGX_PURGE_STORE_CORRUPT, "invalid retired-file relationship");
            output = match(record);
            Writer w; w.byte(1); w.data += signature; w.string(""); w.string(""); w.string(key);
            token = std::move(w.data); return true;
        }
        for (;;) {
            if (files.handle) {
                while (files.next(key, value)) if (candidate(key, common, files.handle, output)) return true;
                files.close();
            }
            if (links.handle) {
                if (links.next(key, value)) {
                    require(key.size() == 17, NGX_PURGE_STORE_CORRUPT, "invalid URL/key posting");
                    current_link = key; open_files(integer(key.substr(9))); continue;
                }
                links.close(); current_link.clear();
            }
            if (!outer.next(key, value)) { outer.close(); empty = true; return false; }
            switch (selector.kind) {
            case SelectorKind::key_prefix:
                current_outer = key; current_link.clear(); open_files(integer(value)); break;
            case SelectorKind::url_prefix:
                current_outer = key; current_link.clear(); open_links(integer(value)); break;
            case SelectorKind::url:
                require(key.size() == 17, NGX_PURGE_STORE_CORRUPT, "invalid URL/key posting");
                current_link = key; open_files(integer(key.substr(9))); break;
            default:
                if (candidate(key, outer_cf, outer.handle, output)) return true;
            }
        }
    }
};

Query::Query(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Query::~Query() = default;
bool Query::next(Match &match) { return impl_->next(match); }
std::string Query::bookmark() const { return impl_->token; }
std::unique_ptr<Query> Index::query(const Selector &selector, const std::string &bookmark) {
    return std::unique_ptr<Query>(new Query(std::make_unique<Query::Impl>(store_, selector, bookmark)));
}
std::unique_ptr<Query> Index::pending_fills(const std::string &bookmark) {
    return std::unique_ptr<Query>(new Query(std::make_unique<Query::Impl>(store_, bookmark, 'I')));
}
std::unique_ptr<Query> Index::retired_files(const std::string &bookmark) {
    return std::unique_ptr<Query>(new Query(std::make_unique<Query::Impl>(store_, bookmark, 'g')));
}

} // namespace ngx_cache_purge
