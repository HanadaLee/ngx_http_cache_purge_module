/* Copyright (C) Hanada */
#ifndef NGX_CACHE_PURGE_INDEX_HPP
#define NGX_CACHE_PURGE_INDEX_HPP

#include "ngx_cache_purge_store.h"
#include <array>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace ngx_cache_purge {

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
    explicit Index(ngx_cache_purge_store_t *store);
    Match prepare(const File &file, const FileId &expected_previous = {});
    std::vector<Match> prepare_batch(const std::vector<Fill> &files);
    Match commit(const FileId &id, uint64_t expected_revision);
    std::vector<Match> commit_batch(const std::vector<ExpectedFile> &files);
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
    std::unique_ptr<Query> pending_fills(const std::string &bookmark = "");
    std::unique_ptr<Query> retired_files(const std::string &bookmark = "");
private:
    ngx_cache_purge_store_t *store_;
};

} // namespace ngx_cache_purge
#endif
