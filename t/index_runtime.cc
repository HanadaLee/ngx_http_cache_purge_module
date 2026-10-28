#include "ngx_http_cache_purge_test.hpp"
#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <set>
#include <thread>

using namespace ngx_http_cache_purge;
extern "C" unsigned ngx_http_cache_purge_test_query_api(
    ngx_http_cache_rocksdb_t *, const unsigned char *, size_t,
    const unsigned char *);
static unsigned assertions;
static void
check(bool value, const char *message)
{
    if (!value) {
        throw std::runtime_error(message);
    }
    ++assertions;
}
template <class Function>
static void
fails(int code, Function function)
{
    try {
        function();
    } catch (const IndexError &error) {
        check(error.code == code, error.what());
        return;
    }
    throw std::runtime_error("expected an index failure");
}
static FileId
id(unsigned n)
{
    FileId value{};
    value[14] = n >> 8;
    value[15] = n;
    return value;
}
static unsigned
numeric(const FileId &value)
{
    return (value[14] << 8) | value[15];
}
static std::string
key(const std::array<std::string, 6> &parts)
{
    std::string result;
    for (unsigned i = 0; i != 5; ++i) {
        if (i) {
            result += '\x1f';
        }
        for (unsigned char c : parts[i]) {
            if (c == '\\') {
                result += "\\\\";
            } else if (c == 31) {
                result += "\\x1F";
            } else {
                result += static_cast<char>(c);
            }
        }
    }
    return result;
}
static File
file(unsigned n, std::string uri = "/dir/a", std::string misc = "",
    std::string host = "a.example", std::string root = "/cache/a")
{
    File result;
    result.id = id(n);
    result.host = host;
    result.tag_namespace = "team";
    result.root = root;
    result.zone = root == "/cache/a" ? "zone-a" : "zone-b";
    result.path = "a/00/file-" + std::to_string(n);
    result.components = { "team", "https", "shared.cache.test", uri, misc,
        "bytes=0-99" };
    result.key = key(result.components);
    result.url = "https://shared.cache.test" + uri;
    result.range = result.components[5];
    result.vary = "Accept-Language";
    result.tags = { "photo", "common", "photo" };
    result.metadata = "proof:v1";
    result.structured_key = result.structured_url = true;
    return result;
}
static Match
install(Index &index, const File &record, const FileId &previous = {})
{
    auto prepared = index.prepare(record, previous);
    check(prepared.state == FileState::prepared && prepared.revision == 1,
        "prepare state");
    return index.commit(record.id, prepared.revision);
}
static std::vector<unsigned>
scan(Index &index, const Selector &selector)
{
    auto query = index.query(selector);
    Match match;
    std::vector<unsigned> result;
    while (query->next(match)) {
        result.push_back(numeric(match.id));
    }
    return result;
}
static std::set<unsigned>
selected(Index &index, const Selector &selector)
{
    auto values = scan(index, selector);
    return { values.begin(), values.end() };
}
static void
pages(Index &index, const Selector &selector)
{
    auto expected = scan(index, selector);
    std::vector<unsigned> actual;
    std::string bookmark;
    for (unsigned i = 0; i != 512; ++i) {
        auto query = index.query(selector, bookmark);
        Match match;
        if (!query->next(match)) {
            break;
        }
        actual.push_back(numeric(match.id));
        bookmark = query->bookmark();
        check(!bookmark.empty(), "nonempty continuation");
        check(i != 511, "pagination terminates");
    }
    check(actual == expected, "paged traversal matches one-snapshot traversal");
}
static std::vector<std::string>
raw_keys(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf)
{
    ngx_http_cache_rocksdb_cursor_t *cursor = nullptr;
    ngx_http_cache_rocksdb_error_t error;
    ngx_http_cache_rocksdb_bytes_t empty{ nullptr, 0 };
    check(ngx_http_cache_rocksdb_cursor_open(
              store, cf, empty, empty, &cursor, &error)
              == 0,
        "raw cursor open");
    std::vector<unsigned char> k(65536), v(262144);
    std::vector<std::string> keys;
    for (;;) {
        ngx_http_cache_rocksdb_buffer_t kb{ k.data(), k.size(), 0 },
            vb{ v.data(), v.size(), 0 };
        int rc = ngx_http_cache_rocksdb_cursor_next(cursor, &kb, &vb, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            break;
        }
        check(rc == NGX_HTTP_CACHE_ROCKSDB_OK, "raw cursor next");
        keys.emplace_back(reinterpret_cast<char *>(k.data()), kb.length);
    }
    check(ngx_http_cache_rocksdb_cursor_close(&cursor, &error) == 0,
        "raw cursor close");
    return keys;
}

static ngx_http_cache_rocksdb_bytes_t
bytes(const std::string &value)
{
    return { reinterpret_cast<const unsigned char *>(value.data()),
        value.size() };
}
static void
raw_write(ngx_http_cache_rocksdb_t *store, ngx_http_cache_rocksdb_cf_e cf,
    const std::string &key, const std::string &value, bool remove = false)
{
    ngx_http_cache_rocksdb_mutation_t change{ cf,
        remove ? NGX_HTTP_CACHE_ROCKSDB_DELETE : NGX_HTTP_CACHE_ROCKSDB_PUT,
        bytes(key), bytes(value) };
    ngx_http_cache_rocksdb_error_t error;
    check(ngx_http_cache_rocksdb_write(store, &change, 1, nullptr, 0, &error)
              == 0,
        "raw test mutation");
}

static std::string
ngx_http_cache_purge_test_raw(
    ngx_http_cache_rocksdb_t *store, const std::string &key)
{
    std::string data(262144, '\0');
    ngx_http_cache_rocksdb_buffer_t value{
        reinterpret_cast<unsigned char *>(data.data()), data.size(), 0
    };
    ngx_http_cache_rocksdb_error_t error{};
    check(ngx_http_cache_rocksdb_get(
              store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, bytes(key), &value, &error)
              == 0,
        "read raw host fixture");
    data.resize(value.length);
    return data;
}

static uint64_t
ngx_http_cache_purge_test_number(const std::string &data, size_t offset)
{
    check(offset + 8 <= data.size(), "test integer bounds");
    uint64_t value = 0;
    for (size_t i = 0; i < 8; i++) {
        value = (value << 8) | static_cast<unsigned char>(data[offset + i]);
    }
    return value;
}

static std::string
ngx_http_cache_purge_test_number(uint64_t value)
{
    std::string result(8, '\0');
    for (size_t i = 0; i < 8; i++) {
        result[7 - i] = value & 255;
        value >>= 8;
    }
    return result;
}

static int
ngx_http_cache_purge_test_visit(void *data,
    ngx_http_cache_rocksdb_bytes_t value, ngx_http_cache_rocksdb_error_t *)
{
    auto values = static_cast<std::set<std::string> *>(data);
    values->emplace(reinterpret_cast<const char *>(value.data), value.len);
    return 0;
}

static void
ngx_http_cache_purge_test_hosts(const std::string &path)
{
    check(mkdir(path.c_str(), 0700) == 0, "host fixture directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error{};
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        "host fixture open");
    const auto cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
    auto a = file(401, "/host-shared", "", "first.test");
    auto b = file(402, "/host-shared", "", "second.test", "/cache/b");
    const std::string observed = "observed.test";
    {
        Index index(store);
        install(index, a);
        install(index, b);
        check(ngx_http_cache_purge_index_observe(
                  store, bytes(a.key), bytes(a.url), bytes(observed), &error)
                  == 0,
            "observe business host without rewriting file metadata");
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        ngx_http_cache_rocksdb_stats(store, &before, &error);
        check(ngx_http_cache_purge_index_observe(
                  store, bytes(a.key), bytes(a.url), bytes(observed), &error)
                  == 0,
            "repeat host observation");
        ngx_http_cache_rocksdb_stats(store, &after, &error);
        check(before.writes == after.writes,
            "repeated host observations require no WAL write");
        auto expected = ngx_http_cache_purge_native_expected(a.id, 2);
        std::set<std::string> hosts;
        check(ngx_http_cache_purge_index_visit_hosts(store, &expected, 8, 1024,
                  ngx_http_cache_purge_test_visit, &hosts, &error)
                      == 0
                  && hosts
                         == std::set<std::string>{ "first.test", "second.test",
                             observed },
            "host visitor covers original and observed business hosts");
        check(ngx_http_cache_purge_index_visit_hosts(store, &expected, 2, 1024,
                  ngx_http_cache_purge_test_visit, &hosts, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "host visitor rejects partial coverage beyond count budget");
        check(ngx_http_cache_purge_index_visit_hosts(store, &expected, 8, 1,
                  ngx_http_cache_purge_test_visit, &hosts, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_LIMIT,
            "host visitor enforces byte budget");
        for (const auto &host : hosts) {
            check(selected(index, { SelectorKind::host, host, "" })
                      == std::set<unsigned>{ 401, 402 },
                "each business host covers both zones");
        }
        check(selected(index, { SelectorKind::host, "shared.cache.test", "" })
                  .empty(),
            "cache-key host is not implicitly a business host");
        Selector selector{ SelectorKind::host_tag, "photo", observed };
        auto query = index.query(selector);
        Match match;
        check(query->next(match) && numeric(match.id) == 401,
            "host-first query starts at the first shared file");
        auto bookmark = query->bookmark();
        query.reset();
        check(ngx_http_cache_purge_test_number(bookmark, 41) == 17
                  && bookmark[49] == 'B',
            "bookmark records host-first plan");
        for (unsigned i = 0; i < 8; i++) {
            auto extra =
                file(410 + i, "/other-" + std::to_string(i), "", observed);
            extra.tags = { "unrelated" };
            install(index, extra);
        }
        query = index.query(selector);
        check(query->next(match), "new query uses current selectivity");
        auto direct = query->bookmark();
        query.reset();
        check(ngx_http_cache_purge_test_number(direct, 41) == 0
                  && direct[57] == 'T',
            "new query chooses tag-first after host cardinality grows");
        query = index.query(selector, bookmark);
        check(query->next(match) && numeric(match.id) == 402
                  && !query->next(match),
            "continuation preserves host-first plan after selectivity changes");
        query.reset();
        pages(index, selector);
        index.update_metadata(b.id, 2, "304-tag-update", { "new" });
        check(
            selected(index, selector) == std::set<unsigned>{ 401 }
                && selected(index, { SelectorKind::host_tag, "new", observed })
                       == std::set<unsigned>{ 402 },
            "TAG changes update all business-host views");
        auto object = ngx_http_cache_purge_test_raw(store,
            "O" + std::string(reinterpret_cast<const char *>(a.id.data()), 16));
        auto forward = "B" + object.substr(58, 8) + object.substr(42, 8);
        auto reverse = "b" + object.substr(42, 8) + object.substr(58, 8);
        raw_write(store, cf, reverse, "", true);
        fails(NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            [&] { selected(index, { SelectorKind::host, "first.test", "" }); });
        check(ngx_http_cache_purge_index_observe(
                  store, bytes(a.key), bytes(a.url), bytes(a.host), &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "one-sided host relationship cannot be silently rewritten");
        raw_write(store, cf, reverse, "");
        check(ngx_http_cache_purge_test_raw(store, forward).empty(),
            "forward host posting remains intact");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close host fixture");
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        "reopen host fixture");
    {
        Index index(store);
        check(selected(index, { SelectorKind::host, "first.test", "" })
                  == std::set<unsigned>{ 401, 402 },
            "host relations persist without a rebuild");
        for (unsigned n : scan(index, { SelectorKind::key_prefix, "", "" })) {
            auto value = index.get(id(n));
            auto retired = index.retire(value.id, value.revision);
            index.collect_file(value.id, retired.revision);
        }
        unsigned rounds = 0;
        while (index.collect_keys(1)) {
            check(++rounds < 100, "host GC progresses with one-item budget");
        }
        check(raw_keys(store, cf).size() == 3
                  && raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_TAG).empty(),
            "last-file GC reclaims every host link and dictionary reference");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close collected host fixture");
}

static void
ngx_http_cache_purge_test_host_migration(const std::string &path)
{
    check(mkdir(path.c_str(), 0700) == 0, "host migration directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error{};
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        "open host migration fixture");
    const auto cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
    const auto tag_cf = NGX_HTTP_CACHE_ROCKSDB_TAG;
    std::set<unsigned> all;
    std::string damaged, original;
    {
        Index index(store);
        for (unsigned i = 0; i < 40; i++) {
            install(index, file(500 + i, "/migration", "",
                               i % 2 ? "second.test" : "first.test"));
            all.insert(500 + i);
        }
        for (const auto &key : raw_keys(store, cf)) {
            if (key[0] == 'O') {
                auto value = ngx_http_cache_purge_test_raw(store, key);
                raw_write(
                    store, cf, "H" + value.substr(58, 8) + key.substr(1), "");
                if (static_cast<unsigned char>(key.back()) == (532 & 255)) {
                    damaged = key;
                    original = value;
                }
            } else if (key[0] == 'B') {
                auto dict_key = "V" + key.substr(1, 8);
                auto dict = ngx_http_cache_purge_test_raw(store, dict_key);
                auto count = ngx_http_cache_purge_test_number(dict, 1);
                check(count > 1, "migration host retains file references");
                dict.replace(1, 8, ngx_http_cache_purge_test_number(count - 1));
                raw_write(store, cf, dict_key, dict);
                raw_write(store, cf, key, "", true);
            } else if (key[0] == 'b') {
                raw_write(store, cf, key, "", true);
            }
        }
        for (const auto &key : raw_keys(store, tag_cf)) {
            if (key[0] == 'T') {
                auto object =
                    ngx_http_cache_purge_test_raw(store, "O" + key.substr(9));
                raw_write(store, tag_cf,
                    "H" + object.substr(58, 8) + key.substr(1), "");
                raw_write(store, tag_cf, key, "", true);
            }
        }
        raw_write(store, cf, "M:index", "3");
        check(!damaged.empty(), "select second migration batch failure");
        raw_write(store, cf, damaged, "broken object");
        fails(
            NGX_HTTP_CACHE_ROCKSDB_CORRUPT, [&] { Index interrupted(store); });
        check(ngx_http_cache_purge_test_raw(store, "M:index") == "9"
                  && ngx_http_cache_purge_test_raw(store, "M:host-cursor")
                         == "O"
                                + std::string(reinterpret_cast<const char *>(
                                                  id(531).data()),
                                    16),
            "migration fences old readers and checkpoints only committed "
            "batches");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close interrupted migration");
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        "reopen interrupted migration");
    raw_write(store, cf, damaged, original);
    {
        Index index(store);
        for (const auto &host : { "first.test", "second.test" }) {
            check(selected(index, { SelectorKind::host, host, "" }) == all
                      && selected(
                             index, { SelectorKind::host_tag, "photo", host })
                             == all,
                "legacy per-file postings upgrade to shared host membership");
        }
        check(ngx_http_cache_purge_test_raw(store, damaged) == original,
            "migration preserves file revision, fingerprint and metadata");
        for (const auto &key : raw_keys(store, cf)) {
            check(key[0] != 'H' && key != "M:host-cursor",
                "migration removes old host postings and cursor");
        }
        for (const auto &key : raw_keys(store, tag_cf)) {
            check(key[0] != 'H', "migration removes old host-tag postings");
        }
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        ngx_http_cache_rocksdb_stats(store, &before, &error);
        Index reopened(store);
        ngx_http_cache_rocksdb_stats(store, &after, &error);
        check(before.writes == after.writes,
            "completed schema migration is not replayed on reopen");
        for (unsigned n : all) {
            index.retire(id(n), 2);
            index.collect_file(id(n), 3);
        }
        while (index.collect_keys(2)) {
        }
        check(raw_keys(store, cf).size() == 4 && raw_keys(store, tag_cf).empty()
                  && ngx_http_cache_purge_test_raw(store, "M:native-cursor")
                         == "O",
            "migrated dictionary references remain balanced after GC");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close migrated host fixture");
}

static void
ngx_http_cache_purge_test_validation_policy(const std::string &path)
{
    check(mkdir(path.c_str(), 0700) == 0, "validation policy directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error{};
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        "validation policy database");
    auto original = file(301), replacement = file(302);
    replacement.path = original.path;
    ngx_http_cache_purge_index_expected_t expected{};
    std::memcpy(expected.id, original.id.data(), 16);
    expected.revision = 2;
    ngx_http_cache_purge_watermark_t policy{}, found{};
    std::memcpy(policy.ledger, id(99).data(), 16);
    policy.sequence = 7;
    const std::string row =
        "v"
        + std::string(reinterpret_cast<const char *>(original.id.data()), 16);
    {
        Index index(store);
        install(index, original);
        check(ngx_http_cache_purge_index_validation_requirement(
                  store, &expected, &found, &error)
                      == 0
                  && found.sequence == 0,
            "ordinary objects have no persistent validation requirement");
        check(ngx_http_cache_purge_index_require_validation(
                  store, &expected, &policy, &error)
                  == 0,
            "persist exact-instance invalidation");
        policy.sequence = 3;
        check(ngx_http_cache_purge_index_require_validation(
                  store, &expected, &policy, &error)
                      == 0
                  && ngx_http_cache_purge_index_validation_requirement(
                         store, &expected, &found, &error)
                         == 0
                  && found.sequence == 7,
            "late replay cannot lower a newer invalidation");
        policy.sequence = 9;
        check(ngx_http_cache_purge_index_require_validation(
                  store, &expected, &policy, &error)
                  == 0,
            "later invalidation advances the requirement");
        check(index.get(original.id).revision == 2
                  && index.get(original.id).metadata == original.metadata,
            "policy leaves native publication revision and proof unchanged");
        policy.ledger[0] = 1;
        check(ngx_http_cache_purge_index_require_validation(
                  store, &expected, &policy, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "unrelated ledger cannot overwrite a requirement");
        policy.ledger[0] = 0;
        expected.revision = 1;
        check(ngx_http_cache_purge_index_require_validation(
                  store, &expected, &policy, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "changed file revision rejects materialization");
        expected.revision = 2;
        index.prepare(replacement, original.id);
        check(ngx_http_cache_purge_index_require_validation(
                  store, &expected, &policy, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "pending replacement excludes policy mutation");
        check(ngx_http_cache_purge_index_require_delete(
                  store, &expected, &policy, &error)
                  == 0,
            "pending replacement retains a deferred Delete requirement");
        policy.sequence = 11;
        check(ngx_http_cache_purge_index_require_delete(
                  store, &expected, &policy, &error)
                  == 0,
            "a later Delete advances the sparse queue");
        policy.sequence = 9;
        check(ngx_http_cache_purge_index_require_delete(
                  store, &expected, &policy, &error)
                      == 0
                  && ngx_http_cache_purge_index_delete_requirement(
                         store, &expected, &found, &error)
                         == 0
                  && found.sequence == 11,
            "old admission cannot lower a queued Delete");
        check(ngx_http_cache_purge_index_clear_delete(
                  store, &expected, &policy, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "stale cancellation cannot erase a newer Delete");
        check(ngx_http_cache_purge_index_clear_delete(
                  store, &expected, &found, &error)
                  == 0,
            "cancel only the exact queued requirement");
        check(ngx_http_cache_purge_index_require_delete(
                  store, &expected, &policy, &error)
                  == 0,
            "requeue deferred Delete for restart");
        ngx_http_cache_purge_query_t *queued = nullptr;
        ngx_http_cache_purge_index_match_t item{};
        check(ngx_http_cache_purge_query_open(store, nullptr,
                  NGX_HTTP_CACHE_PURGE_QUERY_DELETE, {}, &queued, &error)
                      == 0
                  && ngx_http_cache_purge_query_step(queued, 32, &item, &error)
                         == 0
                  && std::memcmp(item.id, original.id.data(), 16) == 0
                  && item.revision == 2
                  && ngx_http_cache_purge_query_step(queued, 32, &item, &error)
                         == NGX_HTTP_CACHE_ROCKSDB_DONE,
            "bounded Delete query visits only the queued file");
        check(ngx_http_cache_purge_query_close(&queued, &error) == 0,
            "close Delete queue cursor");
        index.abort_prepare(replacement.id, 1);
        index.collect_file(replacement.id, 2);
        expected.revision = index
                                .update_metadata(original.id, 2,
                                    "validated-proof", { "new-tag" })
                                .revision;
        check(ngx_http_cache_purge_index_validation_requirement(
                  store, &expected, &found, &error)
                      == 0
                  && found.sequence == 9,
            "metadata and TAG changes do not discard unmet invalidation");
        raw_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "M:index", "2");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close policy database");
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        "reopen policy database");
    {
        Index index(store);
        check(ngx_http_cache_purge_index_validation_requirement(
                  store, &expected, &found, &error)
                      == 0
                  && found.sequence == 9
                  && std::memcmp(found.ledger, policy.ledger, 16) == 0,
            "schema v2 upgrades and persistent policy survives reopen");
        check(ngx_http_cache_purge_index_delete_requirement(
                  store, &expected, &found, &error)
                      == 0
                  && found.sequence == 9,
            "deferred Delete survives metadata changes and reopen");
        raw_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, row, "bad");
        check(ngx_http_cache_purge_index_validation_requirement(
                  store, &expected, &found, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CORRUPT
                  && found.sequence == 0,
            "malformed policy fails closed");
        raw_write(
            store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, row, std::string(24, '\0'));
        check(ngx_http_cache_purge_index_validation_requirement(
                  store, &expected, &found, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "zero ledger and sequence are rejected");
        auto retired = index.retire(original.id, expected.revision);
        index.collect_file(original.id, retired.revision);
        auto keys = raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT);
        check(std::find(keys.begin(), keys.end(), row) == keys.end(),
            "file collection also removes its sparse policy row");
        check(std::find(keys.begin(), keys.end(), "d" + row.substr(1))
                  == keys.end(),
            "file collection removes the deferred Delete row");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close recovered policy database");
}

static void
bounded_scan(const std::string &path)
{
    check(mkdir(path.c_str(), 0700) == 0, "bounded query directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error;
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        "bounded query database");
    {
        Index index(store);
        std::string shared_key;
        for (unsigned i = 0; i < 100; i++) {
            auto record = file(1000 + i,
                i % 2 ? "/budget/" + std::to_string(i) : "/budget/shared", "",
                "budget.example");
            install(index, record);
            if (i == 0) {
                shared_key = record.key;
            }
            if (i < 98) {
                auto retired = index.retire(record.id, 2);
                if (i % 3 == 0) {
                    index.collect_file(record.id, retired.revision);
                }
            }
        }
        std::vector<Selector> selectors{
            { SelectorKind::url_prefix, "https://shared.cache.test/budget/",
                "" },
            { SelectorKind::key_prefix, "", "" },
            { SelectorKind::url, "https://shared.cache.test/budget/shared",
                "" },
            { SelectorKind::key, shared_key, "" },
            { SelectorKind::host, "budget.example", "" },
            { SelectorKind::namespace_tag, "common", "team" },
            { SelectorKind::host_tag, "common", "budget.example" }
        };
        unsigned yielded = 0;
        for (const auto &selector : selectors) {
            auto expected = scan(index, selector);
            ngx_http_cache_purge_selector_t input{ static_cast<uint32_t>(
                                                       selector.kind),
                bytes(selector.value), bytes(selector.scope) };
            std::string cursor;
            std::vector<unsigned> actual;
            unsigned pages = 0;
            for (;;) {
                ngx_http_cache_purge_query_t *query = nullptr;
                ngx_http_cache_purge_index_match_t match{};
                check(ngx_http_cache_purge_query_open(
                          store, &input, 0, bytes(cursor), &query, &error)
                          == 0,
                    "open bounded query page");
                check(ngx_http_cache_purge_query_step(query, 3, &match, &error)
                          == NGX_HTTP_CACHE_ROCKSDB_INVALID,
                    "reject insufficient traversal budget");
                auto rc =
                    ngx_http_cache_purge_query_step(query, 4, &match, &error);
                auto token = ngx_http_cache_purge_query_bookmark(query);
                cursor.assign(
                    reinterpret_cast<const char *>(token.data), token.len);
                check(ngx_http_cache_purge_query_close(&query, &error) == 0,
                    "close bounded snapshot before yielding executor");
                ngx_http_cache_rocksdb_stats_t stats{};
                check(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0
                          && stats.cursor_handles == 0,
                    "bounded pages retain no cursors");
                check(
                    ++pages < 500, "bounded empty/retired scans make progress");
                if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
                    break;
                }
                if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
                    yielded++;
                    check(!cursor.empty(), "yielded scan has durable progress");
                } else {
                    check(rc == 0, "bounded query result");
                    FileId found{};
                    std::memcpy(found.data(), match.id, 16);
                    actual.push_back(numeric(found));
                }
            }
            check(actual == expected,
                "bounded scans preserve all selector results");
        }
        check(
            yielded > 10, "sparse scans yield repeatedly without starving IPC");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close bounded query database");
}

static int
process_test(const std::string &mode, const char *path)
{
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error;
    check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0,
        error.message);
    {
        Index index(store);
        auto a = file(1), b = file(2, "/dir/a", "", "b.example", "/cache/b"),
             c = file(3, "/dir/a", "fr");
        if (mode == "--crash-prepare") {
            index.prepare_batch({ { a, {} }, { b, {} }, { c, {} } });
        } else if (mode == "--crash-commit") {
            auto pending = index.pending_fills();
            Match m;
            std::vector<ExpectedFile> files;
            while (pending->next(m)) {
                files.push_back({ m.id, m.revision });
            }
            pending.reset();
            check(
                files.size() == 3, "all prepared intents survive abrupt exit");
            check(selected(index, { SelectorKind::url, a.url, "" })
                      == std::set<unsigned>({ 1, 2, 3 }),
                "prepared cross-zone URL links survive");
            index.commit_batch(files);
            index.add_url(a.key, "https://alias.test/a");
            index.retire(c.id, 2);
            ngx_http_cache_purge_index_expected_t expected{};
            std::memcpy(expected.id, a.id.data(), 16);
            expected.revision = 2;
            ngx_http_cache_purge_watermark_t required{};
            std::memcpy(required.ledger, id(99).data(), 16);
            required.sequence = 7;
            check(ngx_http_cache_purge_index_require_validation(
                      store, &expected, &required, &error)
                      == 0,
                "persist invalidation before abrupt process exit");
        } else if (mode == "--recover") {
            check(index.get(a.id).state == FileState::committed,
                "committed state survives abrupt exit");
            ngx_http_cache_purge_index_expected_t expected{};
            std::memcpy(expected.id, a.id.data(), 16);
            expected.revision = 2;
            ngx_http_cache_purge_watermark_t required{};
            check(ngx_http_cache_purge_index_validation_requirement(
                      store, &expected, &required, &error)
                          == 0
                      && required.sequence == 7
                      && std::memcmp(required.ledger, id(99).data(), 16) == 0,
                "sparse invalidation policy survives abrupt process exit");
            auto pending = index.pending_fills();
            Match m;
            check(!pending->next(m), "committed intents removed atomically");
            pending.reset();
            auto retired = index.retired_files();
            check(retired->next(m) && m.id == c.id && m.revision == 3
                      && !retired->next(m),
                "retired queue survives abrupt exit");
            retired.reset();
            check(selected(index, { SelectorKind::url, a.url, "" })
                      == std::set<unsigned>({ 1, 2 }),
                "URL scan survives without rebuild");
            check(selected(
                      index, { SelectorKind::url, "https://alias.test/a", "" })
                      == std::set<unsigned>({ 1, 2 }),
                "alias survives without rebuild");
            check(selected(
                      index, { SelectorKind::namespace_tag, "photo", "team" })
                      == std::set<unsigned>({ 1, 2 }),
                "TAG scan survives without rebuild");
            index.collect_file(c.id, 3);
        } else if (mode == "--crash-unpublished") {
            index.prepare(a);
            index.commit_pending(a.id, 1);
        } else if (mode == "--recover-unpublished") {
            auto pending = index.pending_fills();
            Match m;
            check(pending->next(m) && m.id == a.id
                      && m.state == FileState::committed && m.revision == 2
                      && !pending->next(m),
                "unpublished commit survives abrupt exit in the recovery "
                "queue");
            pending.reset();
            check(index.current(a.root, a.path)->id == a.id,
                "unpublished commit retains its current path");
            check(index.publish(a.id, 2).revision == 2,
                "recovered publication completes without changing object "
                "revision");
            check(!index.pending_fills()->next(m),
                "recovered publication removes only its intent");
        } else {
            throw std::runtime_error("unknown process test mode");
        }
        if (mode != "--recover" && mode != "--recover-unpublished") {
            std::cout << "PASS " << mode << " " << assertions << " assertions"
                      << std::endl;
            _exit(0);
            /*
                 * Deliberately bypass index/DB cleanup after acknowledged
                 * writes.
                 */
        }
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
    std::cout << "PASS recovery " << assertions << " assertions" << std::endl;
    return 0;
}

static void
ngx_http_cache_purge_test_tag_groups()
{
    char path[] = "/tmp/cache-index-tag-groups-XXXXXX";
    check(mkdtemp(path) != nullptr, "TAG group directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error{};
    check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0,
        "TAG group store open");
    {
        Index index(store);
        auto a = file(301, "/tags/a"), b = file(302, "/tags/b"),
             c = file(303, "/tags/c"), d = file(304, "/tags/d"),
             e = file(305, "/tags/e", "", "other.test"),
             f = file(306, "/tags/f");
        a.tags = { "alpha", "beta" };
        b.tags = d.tags = { "beta" };
        c.tags = { "gamma" };
        d.tag_namespace = "other";
        e.tags = { "alpha" };
        f.tags = { std::string("\0a", 2) };
        b.root = "/cache/b";
        b.zone = "zone-b";
        for (const auto &item : { a, b, c, d, e, f }) {
            install(index, item);
        }
        const auto group = tag_group({ "absent", "alpha", "beta" });
        const Selector ns{ SelectorKind::namespace_tags, group, "team" },
            host{ SelectorKind::host_tags, group, "a.example" };
        check(selected(index, ns) == std::set<unsigned>({ 301, 302, 305 })
                  && scan(index, ns).size() == 3,
            "namespace TAG OR spans roots and deduplicates intersecting files");
        check(selected(index, host) == std::set<unsigned>({ 301, 302, 304 })
                  && scan(index, host).size() == 3,
            "host TAG OR isolates business hosts across namespaces");
        check(selected(index,
                  { SelectorKind::namespace_tags,
                      tag_group({ std::string("\0a", 2), "alpha" }), "team" })
                  == std::set<unsigned>({ 301, 305, 306 }),
            "TAG group encoding preserves binary members");
        pages(index, ns);
        pages(index, host);

        std::vector<unsigned> emitted;
        std::string bookmark;
        bool done = false, yielded = false;
        for (unsigned step = 0; step < 256 && !done; step++) {
            ngx_http_cache_purge_selector_t selector{
                NGX_HTTP_CACHE_PURGE_NAMESPACE_TAGS, bytes(group),
                bytes(ns.scope)
            };
            ngx_http_cache_purge_query_t *query = nullptr;
            ngx_http_cache_purge_index_match_t match{};
            check(ngx_http_cache_purge_query_open(
                      store, &selector, 0, bytes(bookmark), &query, &error)
                      == 0,
                "bounded group resume open");
            auto rc = ngx_http_cache_purge_query_step(query, 4, &match, &error);
            check(rc == 0 || rc == NGX_HTTP_CACHE_ROCKSDB_BUSY
                      || rc == NGX_HTTP_CACHE_ROCKSDB_DONE,
                "bounded TAG scan result");
            if (rc == 0) {
                emitted.push_back((match.id[14] << 8) | match.id[15]);
            }
            yielded |= rc == NGX_HTTP_CACHE_ROCKSDB_BUSY;
            done = rc == NGX_HTTP_CACHE_ROCKSDB_DONE;
            auto saved = ngx_http_cache_purge_query_bookmark(query);
            bookmark.assign(
                reinterpret_cast<const char *>(saved.data), saved.len);
            check(ngx_http_cache_purge_query_close(&query, &error) == 0,
                "group closes its member snapshot");
        }
        check(done && yielded && emitted.size() == 3
                  && std::set<unsigned>(emitted.begin(), emitted.end())
                         == std::set<unsigned>({ 301, 302, 305 }),
            "bounded continuation neither skips nor duplicates group members");
        fails(NGX_HTTP_CACHE_ROCKSDB_INVALID, [&] {
            index.query(
                { SelectorKind::namespace_tags, group, "other" }, bookmark);
        });
        for (const auto &bad : { tag_group({ "beta", "alpha" }),
                 tag_group({ "alpha", "alpha" }), tag_group({ "alpha" }),
                 group + "trailing", group.substr(0, group.size() - 1) })
        {
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID, [&] {
                index.query({ SelectorKind::namespace_tags, bad, "team" });
            });
        }
        std::vector<std::string> absent;
        for (unsigned i = 0; i < 64; i++) {
            absent.push_back("missing-" + std::to_string(100 + i));
        }
        check(scan(index,
                  { SelectorKind::namespace_tags, tag_group(absent), "team" })
                  .empty(),
            "64 absent TAG members terminate without scanning other indexes");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "TAG group store close");
}

static void
ngx_http_cache_purge_test_native_migration(const std::string &path)
{
    check(mkdir(path.c_str(), 0700) == 0, "native migration directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error{};
    std::vector<File> files;
    auto open = [&] {
        check(
            ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
                == 0,
            error.message);
    };
    struct Resolution {
        unsigned calls = 0;
        bool fail = true;
        FileId recovered{};
    } resolution;
    auto resolve = [](void *data, const ngx_http_cache_purge_index_file_t *f,
                       unsigned char *main, unsigned char *variant,
                       unsigned *state,
                       ngx_http_cache_rocksdb_error_t *) -> int {
        auto &r = *static_cast<Resolution *>(data);
        r.calls++;
        auto metadata = ngx_http_cache_purge_cpp_bytes(f->metadata);
        if (metadata == "failure" && r.fail) {
            return NGX_HTTP_CACHE_ROCKSDB_IO;
        }
        *state = metadata == "skip"    ? NGX_HTTP_CACHE_PURGE_NATIVE_SKIP :
                 metadata == "missing" ? NGX_HTTP_CACHE_PURGE_NATIVE_MISSING :
                                         NGX_HTTP_CACHE_PURGE_NATIVE_BIND;
        if (std::memcmp(r.recovered.data(), f->id, 16) == 0) {
            *state = NGX_HTTP_CACHE_PURGE_NATIVE_BIND;
        }
        std::memcpy(main, f->id, 16);
        std::memset(variant, 0, 16);
        return 0;
    };
    auto ready = [&](const File &file) {
        int available = 99;
        check(
            ngx_http_cache_purge_index_native_ready(store,
                ngx_http_cache_purge_native_bytes(file.key), &available, &error)
                == 0,
            error.message);
        return available != 0;
    };
    open();
    std::string cursor;
    {
        Index index(store);
        for (unsigned i = 0; i < 5; i++) {
            auto f = file(901 + i, "/migration-" + std::to_string(i));
            f.metadata = i == 1 || i == 4 ? "missing" :
                         i == 2           ? "skip" :
                         i == 3           ? "failure" :
                                            "known";
            install(index, f);
            files.push_back(f);
        }
        raw_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "M:index", "8");
        Index upgraded(store);
        check(!ready(files[0]), "unfinished migration is not an empty catalog");
        int done = 99;
        check(ngx_http_cache_purge_index_migrate_native(
                  store, 1, resolve, &resolution, &done, &error)
                      == 0
                  && !done && resolution.calls == 1,
            "migration limits each committed batch");
        cursor = ngx_http_cache_purge_test_raw(store, "M:native-cursor");
        check(cursor
                  == "O"
                         + std::string(
                             reinterpret_cast<const char *>(files[0].id.data()),
                             16),
            "migration records its last committed object");
        check(ngx_http_cache_purge_index_migrate_native(
                  store, 32, resolve, &resolution, &done, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_IO
                  && !done
                  && ngx_http_cache_purge_test_raw(store, "M:native-cursor")
                         == cursor,
            "resolver failure rolls back bindings and the migration cursor");
        for (const auto &key : raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT))
        {
            check(key[0] != 'z', "failed batch creates no unresolved markers");
        }
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close partially migrated database");
    open();
    {
        Index index(store);
        check(ngx_http_cache_purge_test_raw(store, "M:native-cursor") == cursor,
            "restart resumes rather than resetting native migration");
        resolution.fail = false;
        int done = 0;
        unsigned steps = 0;
        while (!done) {
            check(ngx_http_cache_purge_index_migrate_native(
                      store, 1, resolve, &resolution, &done, &error)
                          == 0
                      && ++steps <= 4,
                error.message);
        }
        check(ready(files[0]) && !ready(files[1]) && ready(files[2])
                  && ready(files[3]) && !ready(files[4]),
            "unresolved old identities block only their key after the scan");
        unsigned char main[16], variant[16];
        auto expected = ngx_http_cache_purge_native_expected(files[3].id, 2);
        check(ngx_http_cache_purge_index_native_identity(
                  store, &expected, main, variant, &error)
                      == 0
                  && std::memcmp(main, files[3].id.data(), 16) == 0,
            "backfilled identity is available at its exact revision");
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
            "completed migration statistics");
        unsigned calls = resolution.calls;
        check(ngx_http_cache_purge_index_migrate_native(
                  store, 8, resolve, &resolution, &done, &error)
                      == 0
                  && done && calls == resolution.calls
                  && ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && before.writes == after.writes,
            "completed migration does not scan or write on another call");
        unsigned char bookmark[25]{};
        size_t length = 0;
        calls = resolution.calls;
        for (unsigned i = 0; i < 2; i++) {
            check(ngx_http_cache_purge_index_retry_native(
                      store, 1, resolve, &resolution, bookmark, &length, &error)
                          == 0
                      && length == (i == 0 ? 25 : 0),
                "unresolved retry respects its budget and wraps at pass end");
        }
        check(resolution.calls == calls + 2
                  && ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && before.writes == after.writes,
            "unchanged retries visit only missing rows and never write WAL");
        resolution.recovered = files[1].id;
        check(ngx_http_cache_purge_index_retry_native(
                  store, 1, resolve, &resolution, bookmark, &length, &error)
                      == 0
                  && length == 25 && ready(files[1]) && !ready(files[4]),
            "later retry binds the resolved row without clearing other keys");
        expected = ngx_http_cache_purge_native_expected(files[1].id, 2);
        std::memcpy(main, files[1].id.data(), 16);
        check(ngx_http_cache_purge_index_bind_native(
                  store, &expected, main, variant, &error)
                      == 0
                  && ready(files[1]),
            "resolved native identity atomically removes the missing marker");
        for (const auto &f : files) {
            auto retired = index.retire(f.id, 2);
            index.collect_file(f.id, retired.revision);
        }
        while (index.collect_keys(8)) {
        }
        check(raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT).size() == 3
                  && ready(files[4]),
            "collection releases unresolved markers and key references");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close migrated database");
}

static void
ngx_http_cache_purge_test_native_bindings(const std::string &path)
{
    check(mkdir(path.c_str(), 0700) == 0, "native binding directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error{};
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        error.message);
    {
        Index index(store);
        auto source = file(801, "/native-binding");
        auto other = file(802, "/native-binding", "", "b.test", "/cache/b");
        unsigned char main[16] = { 1 }, variant[16] = { 2 };
        ngx_http_cache_purge_index_match_t prepared{};
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        auto prepare = [&](const File &file, int expected = 0) {
            auto raw = ngx_http_cache_purge_native_file(file);
            check(ngx_http_cache_purge_index_prepare_native(
                      store, &raw, nullptr, main, variant, &prepared, &error)
                      == expected,
                error.message);
        };
        check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
            "native preparation write count");
        prepare(source);
        check(prepared.state == 1 && prepared.revision == 1,
            "native binding is prepared with its file intent");
        check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && after.writes == before.writes + 1,
            "binding and file preparation use one WAL write");
        before = after;
        prepare(source);
        main[0] = 3;
        prepare(source, NGX_HTTP_CACHE_ROCKSDB_CONFLICT);
        main[0] = 1;
        check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && after.writes == before.writes,
            "binding replay and rejected identity do not write");
        prepare(other);
        index.commit(source.id, 1);
        index.commit(other.id, 1);
        std::vector<std::string> postings;
        auto cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
        for (const auto &key : raw_keys(store, cf)) {
            if (key[0] == 'n') {
                postings.push_back(key);
                check(key.size() == 65 && key[9] == 1 && key[33] == 2,
                    "native posting carries main and Vary identity");
            }
        }
        check(postings.size() == 2
                  && postings[0].substr(0, 49) == postings[1].substr(0, 49),
            "same representation covers distinct zone files");
        auto expected = ngx_http_cache_purge_native_expected(source.id, 2);
        variant[0] = 4;
        check(ngx_http_cache_purge_index_bind_native(
                  store, &expected, main, variant, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "published native identity cannot be rebound");
        variant[0] = 2;
        auto updated = index.update_metadata(source.id, 2, "304", { "new" });
        auto retired = index.retire(source.id, updated.revision);
        std::string reverse =
            "j"
            + std::string(reinterpret_cast<const char *>(source.id.data()), 16);
        auto saved = ngx_http_cache_purge_test_raw(store, reverse);
        check(saved.size() == 32, "reverse native binding has fixed size");
        raw_write(store, cf, reverse, "bad");
        expected =
            ngx_http_cache_purge_native_expected(source.id, retired.revision);
        check(ngx_http_cache_purge_index_collect(store, &expected, 1, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "corrupt reverse binding prevents partial collection");
        check(index.get(source.id).revision == retired.revision,
            "failed native collection leaves its file intact");
        raw_write(store, cf, reverse, saved);
        index.collect_file(source.id, retired.revision);
        retired = index.retire(other.id, 2);
        index.collect_file(other.id, retired.revision);
        auto legacy = file(803, "/native-backfill");
        auto committed = install(index, legacy);
        expected =
            ngx_http_cache_purge_native_expected(legacy.id, committed.revision);
        check(ngx_http_cache_purge_index_bind_native(
                  store, &expected, main, variant, &error)
                  == 0,
            "backfill an exact existing file revision");
        check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
            "backfill replay statistics");
        check(ngx_http_cache_purge_index_bind_native(
                  store, &expected, main, variant, &error)
                      == 0
                  && ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && before.writes == after.writes,
            "backfill replay performs no WAL write");
        retired = index.retire(legacy.id, committed.revision);
        index.collect_file(legacy.id, retired.revision);
        while (index.collect_keys(8)) {
        }
        check(raw_keys(store, cf).size() == 3,
            "collection releases native postings and reverse bindings");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "native binding database close");
}

static void
ngx_http_cache_purge_test_retention(const std::string &path)
{
    check(mkdir(path.c_str(), 0700) == 0, "retention directory");
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    options.cursor_lifetime_ms = 5000;
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error{};
    const auto cf = NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
    unsigned char main[16] = { 1 }, english[16] = { 2 }, german[16] = { 3 };
    auto first = file(701, "/retained");
    auto other_zone = file(702, "/retained", "", "b.example", "/cache/b");
    auto other_variant = file(703, "/retained");
    auto open = [&] {
        check(
            ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
                == 0,
            "open retention database");
    };
    auto keep = [&](const File &file, const unsigned char *variant,
                    ngx_http_cache_purge_retention_t value, int code = 0,
                    uint64_t revision = 2) {
        auto expected = ngx_http_cache_purge_native_expected(file.id, revision);
        check(ngx_http_cache_purge_index_keep_propagation(
                  store, &expected, main, variant, 100, &value, &error)
                  == code,
            error.message);
    };
    auto get = [&](const unsigned char *variant, uint64_t now, int code = 0) {
        ngx_http_cache_purge_fence_t result{ 99, 99 };
        check(ngx_http_cache_purge_index_get_propagation(store,
                  ngx_http_cache_purge_native_bytes(first.key), main,
                  ngx_http_cache_purge_native_bytes(first.vary), variant, now,
                  &result, &error)
                  == code,
            error.message);
        if (code) {
            check(!result.invalidate_ms && !result.delete_ms,
                "failed retained lookup exposes no partial result");
        }
        return result;
    };
    open();
    {
        Index index(store);
        install(index, first);
        install(index, other_zone);
        install(index, other_variant);
        auto original = ngx_http_cache_purge_test_raw(store,
            "O"
                + std::string(
                    reinterpret_cast<const char *>(first.id.data()), 16));
        raw_write(store, cf, "M:index", "6");
        Index upgraded(store);
        check(ngx_http_cache_purge_test_raw(store, "M:index") == "9"
                  && ngx_http_cache_purge_test_raw(store,
                         "O"
                             + std::string(reinterpret_cast<const char *>(
                                               first.id.data()),
                                 16))
                         == original,
            "schema 6 upgrades without rewriting file records");
        ngx_http_cache_purge_retention_t required{ 10, 500, 20, 700 };
        keep(first, english, required);
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
            "retention write statistics");
        keep(first, english, required);
        auto result = get(english, 200);
        check(result.invalidate_ms == 10 && result.delete_ms == 20,
            "retained independent modes");
        get(german, 200, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        get(english, 0, NGX_HTTP_CACHE_ROCKSDB_INVALID);
        main[0] = 9;
        get(english, 200, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        main[0] = 1;
        keep(first, english, required, NGX_HTTP_CACHE_ROCKSDB_CONFLICT, 1);
        keep(first, english, { 10, 0, 0, 0 }, NGX_HTTP_CACHE_ROCKSDB_INVALID);
        keep(first, english, { 10, 100, 0, 0 }, NGX_HTTP_CACHE_ROCKSDB_INVALID);
        check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && before.writes == after.writes,
            "reads, replay and rejected retention do not write WAL");
        keep(other_zone, english, { 30, 400, 0, 0 });
        result = get(english, 499);
        check(result.invalidate_ms == 30 && result.delete_ms == 20,
            "same representation across zones merges times and local windows");
        result = get(english, 500);
        check(result.invalidate_ms == 0 && result.delete_ms == 20,
            "each mode expires independently");
        keep(other_variant, german, { 40, 900, 0, 0 });
        check(get(german, 200).invalidate_ms == 40,
            "different Vary identity does not inherit another representation");
        auto changed =
            index.update_metadata(first.id, 2, "new proof", { "blue" });
        keep(first, english, required, 0, changed.revision);
        check(changed.revision == 3 && get(english, 200).invalidate_ms == 30,
            "TAG replacement does not erase retained matched requirements");
        for (auto file : { first, other_zone, other_variant }) {
            auto current = index.get(file.id);
            auto retired = index.retire(file.id, current.revision);
            index.collect_file(file.id, retired.revision);
        }
        while (index.collect_keys(1)) {
        }
        check(!raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_URL).empty()
                  && raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_TAG).empty(),
            "history keeps URL aliases without retaining file TAG postings");
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close retained database");
    open();
    {
        Index index(store);
        auto result = get(english, 499);
        check(result.invalidate_ms == 30 && result.delete_ms == 20,
            "retained identity survives last-file collection and restart");
        size_t removed = 99;
        check(ngx_http_cache_purge_index_expire_propagation(
                  store, 600, 1, &removed, &error)
                      == 0
                  && removed == 0,
            "expiry scan stops at first future deadline");
        std::string damaged, saved;
        for (const auto &key : raw_keys(store, cf)) {
            if (key.size() == 81 && key[0] == 'q'
                && static_cast<unsigned char>(key[33]) == 3)
            {
                damaged = key;
                saved = ngx_http_cache_purge_test_raw(store, key);
            }
        }
        check(!damaged.empty(), "select later expiry for rollback test");
        raw_write(store, cf, damaged, "broken");
        check(ngx_http_cache_purge_index_expire_propagation(
                  store, 900, 3, &removed, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CORRUPT
                  && removed == 0,
            "corruption rolls back the whole bounded expiry batch");
        check(get(english, 200).invalidate_ms == 30,
            "earlier retained record survives a failed batch");
        raw_write(store, cf, damaged, saved);
        check(ngx_http_cache_purge_index_expire_propagation(
                  store, 700, 1, &removed, &error)
                      == 0
                  && removed == 1,
            "one-item expiry removes only the due representation");
        check(get(english, 700).invalidate_ms == 0,
            "another expired membership cannot extend propagation");
        check(ngx_http_cache_purge_index_expire_propagation(
                  store, 700, 1, &removed, &error)
                      == 0
                  && removed == 1,
            "remaining membership is collected in a separate bounded step");
        get(english, 700, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        check(
            get(german, 700).invalidate_ms == 40 && index.collect_keys(1) == 0,
            "another representation keeps the same key and aliases alive");
        check(get(german, 900).invalidate_ms == 0,
            "expired rows cannot propagate before physical GC");
        check(ngx_http_cache_purge_index_expire_propagation(
                  store, 900, 1, &removed, &error)
                      == 0
                  && removed == 1,
            "last expiry releases the key for ordinary collection");
        unsigned rounds = 0;
        while (index.collect_keys(1)) {
            check(++rounds < 100, "retained key collection remains bounded");
        }
        check(raw_keys(store, cf).size() == 4
                  && raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_URL).empty()
                  && raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_TAG).empty(),
            "expiry releases every key, Vary value and identity association");
        get(german, 901, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        auto bounded = file(704, "/bounded-retention");
        bounded.tags = { "retained-0" };
        auto current = install(index, bounded);
        for (unsigned n = 0; n < 65; n++) {
            if (n) {
                current = index.update_metadata(bounded.id, current.revision,
                    "proof", { "retained-" + std::to_string(n) });
            }
            keep(bounded, english, { 50 + n, 1000, 0, 0 }, 0, current.revision);
        }
        ngx_http_cache_purge_fence_t limited{ 99, 99 };
        check(ngx_http_cache_purge_index_get_propagation(store,
                  ngx_http_cache_purge_native_bytes(bounded.key), main,
                  ngx_http_cache_purge_native_bytes(bounded.vary), english, 200,
                  &limited, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_LIMIT
                  && !limited.invalidate_ms && !limited.delete_ms,
            "membership limit rejects partial propagation output");
        ngx_http_cache_purge_retained_query_t *history = nullptr;
        auto history_open = [&](const File &file, int code = 0) {
            check(ngx_http_cache_purge_index_propagation_open(store,
                      ngx_http_cache_purge_native_bytes(file.key), main,
                      ngx_http_cache_purge_native_bytes(file.vary), english,
                      &history, &error)
                      == code,
                error.message);
        };
        ngx_http_cache_purge_retained_context_t context{};
        check(ngx_http_cache_purge_index_propagation_next(
                  nullptr, &context, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "history iterator rejects missing handle");
        auto missing = bounded;
        missing.key = "unrecorded";
        history_open(missing, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        check(history == nullptr, "failed history open clears its output");
        main[0] = 9;
        history_open(bounded);
        check(ngx_http_cache_purge_index_propagation_next(
                  history, &context, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_DONE
                  && !context.key.len && !context.tag_count,
            "different native main has no historical membership");
        check(
            ngx_http_cache_purge_index_propagation_close(&history, &error) == 0,
            "close empty history");
        main[0] = 1;
        history_open(bounded);
        check(ngx_http_cache_purge_index_propagation_next(
                  history, &context, &error)
                      == 0
                  && context.tag_count == 1,
            "read one historical membership");
        std::set<std::string> historical_tags;
        historical_tags.insert(ngx_http_cache_purge_cpp_bytes(context.tags[0]));
        current = index.update_metadata(bounded.id, current.revision,
            "after snapshot", { "retained-after-snapshot" });
        keep(bounded, english, { 999, 1000, 0, 0 }, 0, current.revision);
        current = index.retire(bounded.id, current.revision);
        index.collect_file(bounded.id, current.revision);
        size_t total = 0;
        do {
            check(ngx_http_cache_purge_index_expire_propagation(
                      store, 1000, 32, &removed, &error)
                          == 0
                      && removed <= 32,
                "many memberships expire in bounded batches");
            total += removed;
        } while (removed);
        check(
            total == 66, "all historical TAG sets were independently retained");
        while (index.collect_keys(1)) {
        }
        check(raw_keys(store, cf).size() == 4,
            "large history releases every dictionary reference");
        ngx_http_cache_rocksdb_stats_t before{}, after{};
        check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
            "history iterator statistics");
        unsigned steps = 1;
        int rc;
        while ((rc = ngx_http_cache_purge_index_propagation_next(
                    history, &context, &error))
               == 0)
        {
            check(++steps <= 65 && context.tag_count == 1
                      && ngx_http_cache_purge_cpp_bytes(context.key)
                             == bounded.key
                      && ngx_http_cache_purge_cpp_bytes(context.tag_namespace)
                             == bounded.tag_namespace,
                "history dictionaries share the original snapshot");
            historical_tags.insert(
                ngx_http_cache_purge_cpp_bytes(context.tags[0]));
            check(context.times.invalidate_ms >= 50
                      && context.times.invalidate_ms <= 114
                      && context.times.invalidate_until_ms == 1000,
                "history excludes memberships inserted after open");
        }
        check(rc == NGX_HTTP_CACHE_ROCKSDB_DONE && steps == 65
                  && historical_tags.size() == 65 && !context.key.len
                  && !context.tag_count,
            "incremental history exceeds 64 sets without truncating");
        check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && before.writes == after.writes,
            "incremental history reads never write WAL");
        check(
            ngx_http_cache_purge_index_propagation_close(&history, &error) == 0
                && history == nullptr,
            "history iterator releases its snapshot");
        check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                  && !after.active_cursors && !after.cursor_handles,
            "history close leaves no cursor resources");
        history_open(bounded, NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND);
        current = install(index, bounded);
        keep(bounded, english, { 200, 2000, 0, 0 }, 0, current.revision);
        std::vector<ngx_http_cache_purge_retained_query_t *> held;
        for (unsigned i = 0; i < options.max_cursors; i++) {
            history_open(bounded);
            held.push_back(history);
        }
        history_open(bounded, NGX_HTTP_CACHE_ROCKSDB_LIMIT);
        check(history == nullptr, "cursor capacity applies to history scans");
        for (auto &cursor : held) {
            check(ngx_http_cache_purge_index_propagation_close(&cursor, &error)
                      == 0,
                "release bounded history cursor slot");
        }
        history_open(bounded);
        std::string row;
        for (const auto &key : raw_keys(store, cf)) {
            if (key.size() == 81 && key[0] == 'q') {
                row = key;
            }
        }
        check(!row.empty(), "history row for corruption check");
        saved = ngx_http_cache_purge_test_raw(store, row);
        raw_write(store, cf, row, "corrupt");
        check(ngx_http_cache_purge_index_propagation_next(
                  history, &context, &error)
                      == 0
                  && context.times.invalidate_ms == 200,
            "already-open snapshot does not observe later row corruption");
        check(
            ngx_http_cache_purge_index_propagation_close(&history, &error) == 0,
            "close original snapshot");
        history_open(bounded);
        check(ngx_http_cache_purge_index_propagation_next(
                  history, &context, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CORRUPT
                  && !context.key.len && !context.tag_count
                  && !context.times.invalidate_ms,
            "corrupt history exposes no borrowed partial identity");
        check(
            ngx_http_cache_purge_index_propagation_close(&history, &error) == 0,
            "close failed history");
        raw_write(store, cf, row, saved);
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close expired database");
    options.cursor_lifetime_ms = 20;
    open();
    auto bounded = file(704, "/bounded-retention");
    ngx_http_cache_purge_retained_query_t *history = nullptr;
    ngx_http_cache_purge_retained_context_t context{};
    check(ngx_http_cache_purge_index_propagation_open(store,
              ngx_http_cache_purge_native_bytes(bounded.key), main,
              ngx_http_cache_purge_native_bytes(bounded.vary), english,
              &history, &error)
              == 0,
        "open short-lived history snapshot");
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    check(ngx_http_cache_purge_index_propagation_next(history, &context, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_EXPIRED
              && !context.key.len && !context.tag_count,
        "expired history cannot claim an empty complete result");
    check(ngx_http_cache_purge_index_propagation_close(&history, &error) == 0,
        "expired snapshot still requires and permits close");
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "expired history releases database ownership");
}

int
main(int argc, char **argv)
{
    try {
        if (argc == 3) {
            return process_test(argv[1], argv[2]);
        }
        char path[] = "/tmp/cache-purge-index-test-XXXXXX";
        check(mkdtemp(path) != nullptr, "test directory");
        ngx_http_cache_purge_test_tag_groups();
        ngx_http_cache_purge_test_native_migration(
            std::string(path) + "-migration");
        ngx_http_cache_purge_test_native_bindings(
            std::string(path) + "-native");
        ngx_http_cache_rocksdb_options_t options;
        ngx_http_cache_rocksdb_options(&options);
        options.cursor_lifetime_ms = 5000;
        ngx_http_cache_rocksdb_t *store = nullptr;
        ngx_http_cache_rocksdb_error_t error;
        check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0,
            error.message);
        {
            Index index(store);
            auto publication = file(200, "/publication");
            check(!index.current(publication.root, publication.path),
                "new physical path has no committed instance");
            check(!index.current("/unknown-root", publication.path),
                "unknown root lookup does not create dictionary entries");
            index.prepare(publication);
            check(!index.current(publication.root, publication.path),
                "prepare alone does not publish the path mapping");
            auto unpublished = index.commit_pending(publication.id, 1);
            check(unpublished.state == FileState::committed
                      && unpublished.revision == 2,
                "DB commit can retain a publication intent");
            {
                auto pending = index.pending_fills();
                Match m;
                check(pending->next(m) && m.id == publication.id
                          && m.state == FileState::committed
                          && !pending->next(m),
                    "pending cursor includes committed-but-unpublished files");
            }
            auto pub_replacement = file(201, "/publication");
            pub_replacement.path = publication.path;
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { index.prepare(pub_replacement, publication.id); });
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { index.retire(publication.id, 2); });
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { index.update_metadata(publication.id, 2, "new", {}); });
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { index.publish(publication.id, 1); });
            check(index.current(publication.root, publication.path)->id
                      == publication.id,
                "current path returns the exact committed instance");
            check(index.publish(publication.id, 2).revision == 2
                      && index.publish(publication.id, 2).revision == 2,
                "publication cleanup is idempotent");
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { index.discard_publication(publication.id, 2); });
            index.prepare(pub_replacement, publication.id);
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { index.publish(publication.id, 2); });
            index.commit_pending(pub_replacement.id, 1);
            check(index.get(publication.id).state == FileState::retired,
                "replacement retires old instance while retaining new intent");
            auto discarded = index.discard_publication(pub_replacement.id, 2);
            check(discarded.state == FileState::retired
                      && !index.current(
                          pub_replacement.root, pub_replacement.path),
                "failed publication can retire its exact instance and clear "
                "its path");
            index.collect_file(publication.id, 3);
            index.collect_file(pub_replacement.id, 3);
            auto retained = file(202, "/schema-retained");
            install(index, retained);
            raw_write(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, "M:index", "1");
            Index upgraded(store);
            check(!upgraded.current(publication.root, publication.path),
                "v1 schema upgrades without rebuilding index records");
            check(upgraded.current(retained.root, retained.path)->id
                          == retained.id
                      && upgraded.get(retained.id).metadata == retained.metadata
                      && selected(
                             upgraded, { SelectorKind::url, retained.url, "" })
                             == std::set<unsigned>{ 202 },
                "schema migration preserves existing files, metadata and URL "
                "relationships");
            upgraded.retire(retained.id, 2);
            upgraded.collect_file(retained.id, 3);
            auto a = file(1),
                 b = file(2, "/dir/a", "", "b.example", "/cache/b");
            b.range = b.components[5] = "bytes=100-199";
            auto c = file(3, "/dir/a", "fr"),
                 d = file(4, "/dir/a", "", "a.example", "/cache/c");
            d.tag_namespace = "other";
            d.vary = "Accept-Encoding";
            auto e = file(5, "/dir/ab"), f = file(6, "/dir/a/child");
            auto prepared = index.prepare(a);
            check(
                prepared.revision == 1 && prepared.state == FileState::prepared,
                "initial prepared row");
            check(index.prepare(a).revision == 1, "idempotent prepare");
            check(selected(index, { SelectorKind::url, a.url, "" })
                      == std::set<unsigned>{ 1 },
                "prepared file enumerated for writer handling");
            auto different = a;
            different.metadata = "other proof";
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { index.prepare(different); });
            check(index.commit(a.id, 1).revision == 2,
                "commit increments revision");
            assertions += ngx_http_cache_purge_test_query_api(store,
                reinterpret_cast<const unsigned char *>(a.key.data()),
                a.key.size(), a.id.data());
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { index.commit(a.id, 1); });
            for (const auto &record : { b, c, d, e, f }) {
                install(index, record);
            }
            auto corrupt = file(7, "/bad-projection");
            for (bool url : { false, true }) {
                auto cf = url ? NGX_HTTP_CACHE_ROCKSDB_URL :
                                NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
                auto projection = std::string(1, url ? 'U' : 'K')
                                  + (url ? corrupt.url : corrupt.key);
                const std::string foreign_id(8, '\x7f');
                raw_write(store, cf, projection, foreign_id);
                ngx_http_cache_rocksdb_stats_t before, after;
                check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
                    "corruption initial stats");
                fails(NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                    [&] { index.prepare(corrupt); });
                check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                          && after.writes == before.writes,
                    "inconsistent projection is not overwritten or partially "
                    "committed");
                raw_write(store, cf, projection, "", true);
            }
            check(selected(index, { SelectorKind::key, a.key, "" })
                      == std::set<unsigned>({ 1, 2, 4 }),
                "key expands all zones/slice/Vary");
            check(selected(index, { SelectorKind::url, a.url, "" })
                      == std::set<unsigned>({ 1, 2, 3, 4 }),
                "URL expands key variants");
            check(selected(index, { SelectorKind::url_prefix, a.url + '/', "" })
                      == std::set<unsigned>{ 6 },
                "directory prefix boundary");
            check(selected(index, { SelectorKind::url_prefix, a.url, "" })
                      == std::set<unsigned>({ 1, 2, 3, 4, 5, 6 }),
                "byte prefix includes ab");
            check(selected(index, { SelectorKind::host, "b.example", "" })
                      == std::set<unsigned>({ 1, 2, 4 }),
                "business host covers all files of its shared cache key");
            check(selected(
                      index, { SelectorKind::namespace_tag, "photo", "other" })
                      == std::set<unsigned>{ 4 },
                "namespace-tag isolation");
            check(selected(
                      index, { SelectorKind::host_tag, "photo", "b.example" })
                      == std::set<unsigned>({ 1, 2, 4 }),
                "host-tag covers shared key files across zones");
            check(index.get(a.id).tags
                      == std::vector<std::string>({ "common", "photo" }),
                "deduplicated canonical TAGs");
            check(index.get(b.id).range == "bytes=100-199"
                      && index.get(b.id).root == "/cache/b",
                "physical locator and range preserved");
            auto bad = file(7);
            bad.path = "../escape";
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID, [&] { index.prepare(bad); });
            bad = file(7);
            bad.key += "wrong";
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID, [&] { index.prepare(bad); });
            bad = file(7);
            bad.url += "wrong";
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID, [&] { index.prepare(bad); });
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { index.collect_file(a.id, 2); });

            auto replacement = a;
            replacement.id = id(10);
            replacement.metadata = "new proof";
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { index.prepare(replacement); });
            index.prepare(replacement, a.id);
            auto competing = replacement;
            competing.id = id(11);
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { index.prepare(competing, a.id); });
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY, [&] { index.retire(a.id, 2); });
            fails(NGX_HTTP_CACHE_ROCKSDB_BUSY,
                [&] { index.update_metadata(a.id, 2, "304", a.tags); });
            auto aborted = index.abort_prepare(replacement.id, 1);
            check(aborted.state == FileState::retired, "abort prepared file");
            index.collect_file(replacement.id, aborted.revision);
            install(index, competing, a.id);
            check(index.get(a.id).state == FileState::retired
                      && index.get(a.id).revision == 3,
                "replace retires old instance");
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { index.collect_file(a.id, 2); });
            index.collect_file(a.id, 3);
            check(index.get(competing.id).state == FileState::committed,
                "old cleanup preserves new path instance");
            auto changed = index.update_metadata(
                competing.id, 2, "304 success", { "updated" });
            check(changed.revision == 3, "metadata revision");
            check(selected(
                      index, { SelectorKind::namespace_tag, "updated", "team" })
                      == std::set<unsigned>{ 11 },
                "TAG replacement added");
            check(!selected(
                      index, { SelectorKind::namespace_tag, "photo", "team" })
                      .count(11),
                "TAG replacement removed old relation");
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, [&] {
                index.update_metadata(competing.id, 2, "stale", { "photo" });
            });
            check(index.get(competing.id).metadata == "304 success",
                "stale metadata update rejected");

            const std::string alias = "https://aliases.test/";
            for (unsigned i = 0; i != 20; ++i) {
                index.add_url(a.key, alias + std::to_string(i));
            }
            index.add_url(a.key, alias + "0");
            check(selected(index, { SelectorKind::url, alias + "0", "" })
                      == std::set<unsigned>({ 2, 4, 11 }),
                "URL alias expands all matching key files");
            for (const auto &selector :
                std::vector<Selector>{ { SelectorKind::key, a.key, "" },
                    { SelectorKind::url, a.url, "" },
                    { SelectorKind::key_prefix, "", "" },
                    { SelectorKind::url_prefix,
                        "https://shared.cache.test/dir/", "" },
                    { SelectorKind::url_prefix, alias, "" },
                    { SelectorKind::host, "a.example", "" },
                    { SelectorKind::namespace_tag, "photo", "team" },
                    { SelectorKind::host_tag, "photo", "a.example" } })
            {
                pages(index, selector);
            }
            auto wrong = index.query({ SelectorKind::url, a.url, "" });
            Match m;
            check(wrong->next(m), "bookmark source");
            auto token = wrong->bookmark();
            wrong.reset();
            fails(NGX_HTTP_CACHE_ROCKSDB_INVALID,
                [&] { index.query({ SelectorKind::url, e.url, "" }, token); });

            auto expected_snapshot =
                selected(index, { SelectorKind::url, a.url, "" });
            auto snapshot = index.query({ SelectorKind::url, a.url, "" });
            check(snapshot->next(m) && m.id != c.id,
                "snapshot first key precedes second key");
            std::set<unsigned> actual_snapshot{ numeric(m.id) };
            auto retired = index.retire(c.id, 2);
            index.collect_file(c.id, retired.revision);
            while (index.collect_keys(3)) {
            }
            while (snapshot->next(m)) {
                actual_snapshot.insert(numeric(m.id));
            }
            snapshot.reset();
            check(actual_snapshot == expected_snapshot,
                "nested cursors retain exact shared snapshot");
            check(!selected(index, { SelectorKind::url, a.url, "" }).count(3),
                "new query sees collected instance");

            auto long_file =
                file(30, "/long/" + std::string(6000, 'x'), "a\\b\x1f");
            install(index, long_file);
            check(index.get(long_file.id).key == long_file.key,
                "component key reconstructs escapes");
            check(selected(index, { SelectorKind::url_prefix,
                                      "https://shared.cache.test/long/"
                                          + std::string(5000, 'x'),
                                      "" })
                      == std::set<unsigned>{ 30 },
                "full long prefix traversal");
            auto leaf = file(31, "/leaf");
            leaf.tags = { a.url };
            install(index, leaf);
            check(index.get(b.id).url == a.url,
                "derived URL becomes a raw component without changing "
                "identity");
            check(
                selected(index, { SelectorKind::namespace_tag, a.url, "team" })
                    == std::set<unsigned>{ 31 },
                "shared raw/derived value identity");
            auto nested_key = file(32, "/key-as-component", a.key);
            install(index, nested_key);
            check(index.get(b.id).key == a.key
                      && index.get(nested_key.id).key == nested_key.key,
                "derived key reused as a component stays flat and preserves "
                "both identities");
            auto old_root = file(40, "/transient-root");
            old_root.structured_url = false;
            install(index, old_root);
            auto root_alias =
                file(41, "/new", old_root.key, "a.example", "/transient-root");
            install(index, root_alias);
            check(index.get(root_alias.id).root == "/transient-root",
                "root reference retained before flattening its former parent");
            auto old_key = file(42, "/key-leaf");
            old_key.structured_url = false;
            install(index, old_key);
            auto key_alias = file(43, old_key.key);
            key_alias.key = "/key-leaf";
            key_alias.structured_key = false;
            install(index, key_alias);
            check(index.get(key_alias.id).key == "/key-leaf",
                "key role retained before URL flattens its former parent");

            std::vector<Fill> fills;
            for (unsigned n = 100; n != 132; ++n) {
                fills.push_back({ file(n, "/batch/" + std::to_string(n)), {} });
            }
            ngx_http_cache_rocksdb_stats_t before, after;
            check(ngx_http_cache_rocksdb_stats(store, &before, &error) == 0,
                "batch initial stats");
            fails(NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                [&] { index.prepare_batch(fills); });
            check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                      && after.writes == before.writes,
                "oversized relationship batch is entirely rolled back");
            auto batched = index.prepare_batch(
                std::vector<Fill>(fills.begin(), fills.begin() + 16));
            auto tail = index.prepare_batch(
                std::vector<Fill>(fills.begin() + 16, fills.end()));
            batched.insert(batched.end(), tail.begin(), tail.end());
            check(ngx_http_cache_rocksdb_stats(store, &after, &error) == 0
                      && after.writes == before.writes + 2,
                "smaller batches preserve the configured entry budget");
            std::vector<ExpectedFile> revisions;
            for (const auto &entry : batched) {
                revisions.push_back({ entry.id, entry.revision });
            }
            auto invalid_batch = revisions;
            invalid_batch.back().revision = 999;
            fails(NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                [&] { index.commit_batch(invalid_batch); });
            check(index.get(revisions.front().id).state == FileState::prepared,
                "failed commit batch is entirely rolled back");
            auto committed = index.commit_batch(revisions);
            revisions.clear();
            for (const auto &entry : committed) {
                revisions.push_back({ entry.id, entry.revision });
            }
            auto retired_batch = index.retire_batch(revisions);
            revisions.clear();
            for (const auto &entry : retired_batch) {
                revisions.push_back({ entry.id, entry.revision });
            }
            index.collect_files(revisions);
            check(selected(index, { SelectorKind::url_prefix,
                                      "https://shared.cache.test/batch/", "" })
                      .empty(),
                "batched lifecycle removes all file candidates");

            auto pending = file(99, "/unfinished");
            index.prepare(pending);
            auto queued = file(98, "/retired");
            install(index, queued);
            index.retire(queued.id, 2);
        }
        check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
        options.cursor_lifetime_ms = 250;
        check(ngx_http_cache_rocksdb_open(path, &options, &store, &error) == 0,
            error.message);
        {
            Index index(store);
            Selector selector{ SelectorKind::url,
                "https://shared.cache.test/dir/a", "" };
            auto expected = scan(index, selector);
            auto expiring = index.query(selector);
            Match first;
            check(expiring->next(first), "expiration first result");
            auto resume = expiring->bookmark();
            std::vector<unsigned> actual{ numeric(first.id) };
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            fails(
                NGX_HTTP_CACHE_ROCKSDB_EXPIRED, [&] { expiring->next(first); });
            expiring.reset();
            auto continued = index.query(selector, resume);
            while (continued->next(first)) {
                actual.push_back(numeric(first.id));
            }
            continued.reset();
            check(actual == expected,
                "expired snapshot resumes without losing existing candidates");
            check(index.get(id(99)).state == FileState::prepared,
                "unfinished fill persists across reopen");
            Match pending;
            auto unfinished = index.pending_fills();
            check(unfinished->next(pending) && pending.id == id(99)
                      && !unfinished->next(pending),
                "recovery enumerates only durable unfinished fills");
            unfinished.reset();
            auto retired_queue = index.retired_files();
            check(retired_queue->next(pending) && pending.id == id(98)
                      && pending.state == FileState::retired,
                "retired files have a durable cleanup queue");
            retired_queue.reset();
            index.collect_file(id(98), pending.revision);
            check(selected(index,
                      { SelectorKind::url, "https://aliases.test/0", "" })
                      == std::set<unsigned>({ 2, 4, 11 }),
                "aliases persist without rebuilding");
            auto aborted = index.abort_prepare(id(99), 1);
            index.collect_file(id(99), aborted.revision);
            auto all = scan(index, { SelectorKind::key_prefix, "", "" });
            for (unsigned n : all) {
                auto record = index.get(id(n));
                auto retired = index.retire(record.id, record.revision);
                index.collect_file(retired.id, retired.revision);
            }
            unsigned passes = 0;
            while (index.collect_keys(3)) {
                check(++passes < 100, "bounded GC makes progress");
            }
            check(raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_URL).empty(),
                "URL CF reclaimed");
            check(raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_TAG).empty(),
                "TAG CF reclaimed");
            auto keys = raw_keys(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT);
            check(keys.size() == 4
                      && ngx_http_cache_purge_test_raw(store, "M:native-cursor")
                             == "O"
                      && std::find(keys.begin(), keys.end(), "M:index")
                             != keys.end()
                      && std::find(keys.begin(), keys.end(), "M:next")
                             != keys.end(),
                "all file/value/component/alias records reclaimed");
        }
        check(ngx_http_cache_rocksdb_close(&store, &error) == 0, error.message);
        bounded_scan(std::string(path) + "-bounded");
        ngx_http_cache_purge_test_hosts(std::string(path) + "-hosts");
        ngx_http_cache_purge_test_host_migration(
            std::string(path) + "-host-migration");
        ngx_http_cache_purge_test_validation_policy(
            std::string(path) + "-validation");
        ngx_http_cache_purge_test_retention(std::string(path) + "-retention");
        std::cout << "PASS " << assertions << " index assertions; database "
                  << path << std::endl;
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "FAIL: " << error.what() << std::endl;
        _exit(1);
    }
}
