#include "ngx_cache_purge_index.hpp"
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <set>
#include <thread>

using namespace ngx_cache_purge;
static unsigned assertions;
static void check(bool value, const char *message) {
    if (!value) throw std::runtime_error(message);
    ++assertions;
}
template<class Function> static void fails(int code, Function function) {
    try { function(); } catch (const IndexError &error) {
        check(error.code == code, error.what()); return;
    }
    throw std::runtime_error("expected an index failure");
}
static FileId id(unsigned n) { FileId value{}; value[14] = n >> 8; value[15] = n; return value; }
static unsigned numeric(const FileId &value) { return (value[14] << 8) | value[15]; }
static std::string key(const std::array<std::string, 6> &parts) {
    std::string result;
    for (unsigned i = 0; i != 5; ++i) {
        if (i) result += '\x1f';
        for (unsigned char c : parts[i]) {
            if (c == '\\') result += "\\\\";
            else if (c == 31) result += "\\x1F";
            else result += static_cast<char>(c);
        }
    }
    return result;
}
static File file(unsigned n, std::string uri = "/dir/a", std::string misc = "",
                  std::string host = "a.example", std::string root = "/cache/a") {
    File result; result.id = id(n); result.host = host; result.tag_namespace = "team";
    result.root = root; result.zone = root == "/cache/a" ? "zone-a" : "zone-b";
    result.path = "a/00/file-" + std::to_string(n);
    result.components = {"team", "https", "shared.cache.test", uri, misc, "bytes=0-99"};
    result.key = key(result.components); result.url = "https://shared.cache.test" + uri;
    result.range = result.components[5]; result.vary = "Accept-Language";
    result.tags = {"photo", "common", "photo"}; result.metadata = "proof:v1";
    result.structured_key = result.structured_url = true;
    return result;
}
static Match install(Index &index, const File &record, const FileId &previous = {}) {
    auto prepared = index.prepare(record, previous);
    check(prepared.state == FileState::prepared && prepared.revision == 1, "prepare state");
    return index.commit(record.id, prepared.revision);
}
static std::vector<unsigned> scan(Index &index, const Selector &selector) {
    auto query = index.query(selector); Match match; std::vector<unsigned> result;
    while (query->next(match)) result.push_back(numeric(match.id));
    return result;
}
static std::set<unsigned> selected(Index &index, const Selector &selector) {
    auto values = scan(index, selector); return {values.begin(), values.end()};
}
static void pages(Index &index, const Selector &selector) {
    auto expected = scan(index, selector); std::vector<unsigned> actual; std::string bookmark;
    for (unsigned i = 0; i != 512; ++i) {
        auto query = index.query(selector, bookmark); Match match;
        if (!query->next(match)) break;
        actual.push_back(numeric(match.id)); bookmark = query->bookmark();
        check(!bookmark.empty(), "nonempty continuation");
        check(i != 511, "pagination terminates");
    }
    check(actual == expected, "paged traversal matches one-snapshot traversal");
}
static std::vector<std::string> raw_keys(ngx_cache_purge_store_t *store, ngx_cache_purge_cf_t cf) {
    ngx_cache_purge_cursor_t *cursor = nullptr; ngx_cache_purge_store_error_t error;
    ngx_cache_purge_bytes_t empty{nullptr, 0};
    check(ngx_cache_purge_cursor_open(store, cf, empty, empty, &cursor, &error) == 0, "raw cursor open");
    std::vector<unsigned char> k(65536), v(262144); std::vector<std::string> keys;
    for (;;) {
        ngx_cache_purge_buffer_t kb{k.data(), k.size(), 0}, vb{v.data(), v.size(), 0};
        int rc = ngx_cache_purge_cursor_next(cursor, &kb, &vb, &error);
        if (rc == NGX_PURGE_STORE_DONE) break;
        check(rc == NGX_PURGE_STORE_OK, "raw cursor next");
        keys.emplace_back(reinterpret_cast<char *>(k.data()), kb.length);
    }
    check(ngx_cache_purge_cursor_close(&cursor, &error) == 0, "raw cursor close");
    return keys;
}

static ngx_cache_purge_bytes_t bytes(const std::string &value) {
    return {reinterpret_cast<const unsigned char *>(value.data()), value.size()};
}
static void raw_write(ngx_cache_purge_store_t *store, ngx_cache_purge_cf_t cf,
                      const std::string &key, const std::string &value, bool remove = false) {
    ngx_cache_purge_mutation_t change{cf, remove ? NGX_PURGE_STORE_DELETE : NGX_PURGE_STORE_PUT, bytes(key), bytes(value)};
    ngx_cache_purge_store_error_t error;
    check(ngx_cache_purge_store_write(store, &change, 1, nullptr, 0, &error) == 0, "raw test mutation");
}

static int process_test(const std::string &mode, const char *path) {
    ngx_cache_purge_store_options_t options; ngx_cache_purge_store_options(&options);
    ngx_cache_purge_store_t *store = nullptr; ngx_cache_purge_store_error_t error;
    check(ngx_cache_purge_store_open(path, &options, &store, &error) == 0, error.message);
    {
        Index index(store);
        auto a = file(1), b = file(2, "/dir/a", "", "b.example", "/cache/b"), c = file(3, "/dir/a", "fr");
        if (mode == "--crash-prepare") {
            index.prepare_batch({{a, {}}, {b, {}}, {c, {}}});
        } else if (mode == "--crash-commit") {
            auto pending = index.pending_fills(); Match m; std::vector<ExpectedFile> files;
            while (pending->next(m)) files.push_back({m.id, m.revision});
            pending.reset(); check(files.size() == 3, "all prepared intents survive abrupt exit");
            check(selected(index, {SelectorKind::url, a.url, ""}) == std::set<unsigned>({1, 2, 3}), "prepared cross-zone URL links survive");
            index.commit_batch(files); index.add_url(a.key, "https://alias.test/a");
            index.retire(c.id, 2);
        } else if (mode == "--recover") {
            check(index.get(a.id).state == FileState::committed, "committed state survives abrupt exit");
            auto pending = index.pending_fills(); Match m;
            check(!pending->next(m), "committed intents removed atomically"); pending.reset();
            auto retired = index.retired_files();
            check(retired->next(m) && m.id == c.id && m.revision == 3 && !retired->next(m), "retired queue survives abrupt exit"); retired.reset();
            check(selected(index, {SelectorKind::url, a.url, ""}) == std::set<unsigned>({1, 2}), "URL scan survives without rebuild");
            check(selected(index, {SelectorKind::url, "https://alias.test/a", ""}) == std::set<unsigned>({1, 2}), "alias survives without rebuild");
            check(selected(index, {SelectorKind::namespace_tag, "photo", "team"}) == std::set<unsigned>({1, 2}), "TAG scan survives without rebuild");
            index.collect_file(c.id, 3);
        } else throw std::runtime_error("unknown process test mode");
        if (mode != "--recover") {
            std::cout << "PASS " << mode << " " << assertions << " assertions" << std::endl;
            _exit(0); // Deliberately bypass index/DB cleanup after acknowledged writes.
        }
    }
    check(ngx_cache_purge_store_close(&store, &error) == 0, error.message);
    std::cout << "PASS recovery " << assertions << " assertions" << std::endl;
    return 0;
}

int main(int argc, char **argv) {
    try {
        if (argc == 3) return process_test(argv[1], argv[2]);
        char path[] = "/tmp/cache-purge-index-test-XXXXXX";
        check(mkdtemp(path) != nullptr, "test directory");
        ngx_cache_purge_store_options_t options; ngx_cache_purge_store_options(&options);
        options.cursor_lifetime_ms = 5000;
        ngx_cache_purge_store_t *store = nullptr; ngx_cache_purge_store_error_t error;
        check(ngx_cache_purge_store_open(path, &options, &store, &error) == 0, error.message);
        {
            Index index(store);
            auto a = file(1), b = file(2, "/dir/a", "", "b.example", "/cache/b");
            b.range = b.components[5] = "bytes=100-199";
            auto c = file(3, "/dir/a", "fr"), d = file(4, "/dir/a", "", "a.example", "/cache/c");
            d.tag_namespace = "other"; d.vary = "Accept-Encoding";
            auto e = file(5, "/dir/ab"), f = file(6, "/dir/a/child");
            auto prepared = index.prepare(a);
            check(prepared.revision == 1 && prepared.state == FileState::prepared, "initial prepared row");
            check(index.prepare(a).revision == 1, "idempotent prepare");
            check(selected(index, {SelectorKind::url, a.url, ""}) == std::set<unsigned>{1}, "prepared file enumerated for writer handling");
            auto different = a; different.metadata = "other proof";
            fails(NGX_PURGE_STORE_CONFLICT, [&] { index.prepare(different); });
            check(index.commit(a.id, 1).revision == 2, "commit increments revision");
            fails(NGX_PURGE_STORE_CONFLICT, [&] { index.commit(a.id, 1); });
            for (const auto &record : {b, c, d, e, f}) install(index, record);
            auto corrupt = file(7, "/bad-projection");
            for (bool url : {false, true}) {
                auto cf = url ? NGX_PURGE_STORE_URL : NGX_PURGE_STORE_DEFAULT;
                auto projection = std::string(1, url ? 'U' : 'K') + (url ? corrupt.url : corrupt.key);
                const std::string foreign_id(8, '\x7f');
                raw_write(store, cf, projection, foreign_id);
                ngx_cache_purge_store_stats_t before, after;
                check(ngx_cache_purge_store_stats(store, &before, &error) == 0, "corruption initial stats");
                fails(NGX_PURGE_STORE_CORRUPT, [&] { index.prepare(corrupt); });
                check(ngx_cache_purge_store_stats(store, &after, &error) == 0 && after.writes == before.writes,
                      "inconsistent projection is not overwritten or partially committed");
                raw_write(store, cf, projection, "", true);
            }
            check(selected(index, {SelectorKind::key, a.key, ""}) == std::set<unsigned>({1, 2, 4}), "key expands all zones/slice/Vary");
            check(selected(index, {SelectorKind::url, a.url, ""}) == std::set<unsigned>({1, 2, 3, 4}), "URL expands key variants");
            check(selected(index, {SelectorKind::url_prefix, a.url + '/', ""}) == std::set<unsigned>{6}, "directory prefix boundary");
            check(selected(index, {SelectorKind::url_prefix, a.url, ""}) == std::set<unsigned>({1, 2, 3, 4, 5, 6}), "byte prefix includes ab");
            check(selected(index, {SelectorKind::host, "b.example", ""}) == std::set<unsigned>{2}, "business host differs from cache host");
            check(selected(index, {SelectorKind::namespace_tag, "photo", "other"}) == std::set<unsigned>{4}, "namespace-tag isolation");
            check(selected(index, {SelectorKind::host_tag, "photo", "b.example"}) == std::set<unsigned>{2}, "host-tag isolation");
            check(index.get(a.id).tags == std::vector<std::string>({"common", "photo"}), "deduplicated canonical TAGs");
            check(index.get(b.id).range == "bytes=100-199" && index.get(b.id).root == "/cache/b", "physical locator and range preserved");
            auto bad = file(7); bad.path = "../escape";
            fails(NGX_PURGE_STORE_INVALID, [&] { index.prepare(bad); });
            bad = file(7); bad.key += "wrong";
            fails(NGX_PURGE_STORE_INVALID, [&] { index.prepare(bad); });
            bad = file(7); bad.url += "wrong";
            fails(NGX_PURGE_STORE_INVALID, [&] { index.prepare(bad); });
            fails(NGX_PURGE_STORE_CONFLICT, [&] { index.collect_file(a.id, 2); });

            auto replacement = a; replacement.id = id(10); replacement.metadata = "new proof";
            fails(NGX_PURGE_STORE_CONFLICT, [&] { index.prepare(replacement); });
            index.prepare(replacement, a.id);
            auto competing = replacement; competing.id = id(11);
            fails(NGX_PURGE_STORE_BUSY, [&] { index.prepare(competing, a.id); });
            fails(NGX_PURGE_STORE_BUSY, [&] { index.retire(a.id, 2); });
            fails(NGX_PURGE_STORE_BUSY, [&] { index.update_metadata(a.id, 2, "304", a.tags); });
            auto aborted = index.abort_prepare(replacement.id, 1);
            check(aborted.state == FileState::retired, "abort prepared file");
            index.collect_file(replacement.id, aborted.revision);
            install(index, competing, a.id);
            check(index.get(a.id).state == FileState::retired && index.get(a.id).revision == 3, "replace retires old instance");
            fails(NGX_PURGE_STORE_CONFLICT, [&] { index.collect_file(a.id, 2); });
            index.collect_file(a.id, 3);
            check(index.get(competing.id).state == FileState::committed, "old cleanup preserves new path instance");
            auto changed = index.update_metadata(competing.id, 2, "304 success", {"updated"});
            check(changed.revision == 3, "metadata revision");
            check(selected(index, {SelectorKind::namespace_tag, "updated", "team"}) == std::set<unsigned>{11}, "TAG replacement added");
            check(!selected(index, {SelectorKind::namespace_tag, "photo", "team"}).count(11), "TAG replacement removed old relation");
            fails(NGX_PURGE_STORE_CONFLICT, [&] { index.update_metadata(competing.id, 2, "stale", {"photo"}); });
            check(index.get(competing.id).metadata == "304 success", "stale metadata update rejected");

            const std::string alias = "https://aliases.test/";
            for (unsigned i = 0; i != 20; ++i) index.add_url(a.key, alias + std::to_string(i));
            index.add_url(a.key, alias + "0");
            check(selected(index, {SelectorKind::url, alias + "0", ""}) == std::set<unsigned>({2, 4, 11}), "URL alias expands all matching key files");
            for (const auto &selector : std::vector<Selector>{
                     {SelectorKind::key, a.key, ""}, {SelectorKind::url, a.url, ""},
                     {SelectorKind::key_prefix, "", ""}, {SelectorKind::url_prefix, "https://shared.cache.test/dir/", ""},
                     {SelectorKind::url_prefix, alias, ""}, {SelectorKind::host, "a.example", ""},
                     {SelectorKind::namespace_tag, "photo", "team"}, {SelectorKind::host_tag, "photo", "a.example"}})
                pages(index, selector);
            auto wrong = index.query({SelectorKind::url, a.url, ""}); Match m;
            check(wrong->next(m), "bookmark source"); auto token = wrong->bookmark(); wrong.reset();
            fails(NGX_PURGE_STORE_INVALID, [&] { index.query({SelectorKind::url, e.url, ""}, token); });

            auto expected_snapshot = selected(index, {SelectorKind::url, a.url, ""});
            auto snapshot = index.query({SelectorKind::url, a.url, ""});
            check(snapshot->next(m) && m.id != c.id, "snapshot first key precedes second key");
            std::set<unsigned> actual_snapshot{numeric(m.id)};
            auto retired = index.retire(c.id, 2); index.collect_file(c.id, retired.revision);
            while (index.collect_keys(3)) {}
            while (snapshot->next(m)) actual_snapshot.insert(numeric(m.id));
            snapshot.reset();
            check(actual_snapshot == expected_snapshot, "nested cursors retain exact shared snapshot");
            check(!selected(index, {SelectorKind::url, a.url, ""}).count(3), "new query sees collected instance");

            auto long_file = file(30, "/long/" + std::string(6000, 'x'), "a\\b\x1f");
            install(index, long_file);
            check(index.get(long_file.id).key == long_file.key, "component key reconstructs escapes");
            check(selected(index, {SelectorKind::url_prefix, "https://shared.cache.test/long/" + std::string(5000, 'x'), ""}) == std::set<unsigned>{30}, "full long prefix traversal");
            auto leaf = file(31, "/leaf"); leaf.tags = {a.url};
            install(index, leaf);
            check(index.get(b.id).url == a.url, "derived URL becomes a raw component without changing identity");
            check(selected(index, {SelectorKind::namespace_tag, a.url, "team"}) == std::set<unsigned>{31}, "shared raw/derived value identity");
            auto nested_key = file(32, "/key-as-component", a.key);
            install(index, nested_key);
            check(index.get(b.id).key == a.key && index.get(nested_key.id).key == nested_key.key,
                  "derived key reused as a component stays flat and preserves both identities");
            auto old_root = file(40, "/transient-root"); old_root.structured_url = false;
            install(index, old_root);
            auto root_alias = file(41, "/new", old_root.key, "a.example", "/transient-root");
            install(index, root_alias);
            check(index.get(root_alias.id).root == "/transient-root", "root reference retained before flattening its former parent");
            auto old_key = file(42, "/key-leaf"); old_key.structured_url = false; install(index, old_key);
            auto key_alias = file(43, old_key.key); key_alias.key = "/key-leaf"; key_alias.structured_key = false;
            install(index, key_alias);
            check(index.get(key_alias.id).key == "/key-leaf", "key role retained before URL flattens its former parent");

            std::vector<Fill> fills;
            for (unsigned n = 100; n != 132; ++n) fills.push_back({file(n, "/batch/" + std::to_string(n)), {}});
            ngx_cache_purge_store_stats_t before, after;
            check(ngx_cache_purge_store_stats(store, &before, &error) == 0, "batch initial stats");
            auto batched = index.prepare_batch(fills);
            check(ngx_cache_purge_store_stats(store, &after, &error) == 0 && after.writes == before.writes + 1,
                  "32 prepares share one durable write");
            std::vector<ExpectedFile> revisions;
            for (const auto &entry : batched) revisions.push_back({entry.id, entry.revision});
            auto invalid_batch = revisions; invalid_batch.back().revision = 999;
            fails(NGX_PURGE_STORE_CONFLICT, [&] { index.commit_batch(invalid_batch); });
            check(index.get(revisions.front().id).state == FileState::prepared, "failed commit batch is entirely rolled back");
            auto committed = index.commit_batch(revisions);
            revisions.clear(); for (const auto &entry : committed) revisions.push_back({entry.id, entry.revision});
            auto retired_batch = index.retire_batch(revisions);
            revisions.clear(); for (const auto &entry : retired_batch) revisions.push_back({entry.id, entry.revision});
            index.collect_files(revisions);
            check(selected(index, {SelectorKind::url_prefix, "https://shared.cache.test/batch/", ""}).empty(), "batched lifecycle removes all file candidates");

            auto pending = file(99, "/unfinished"); index.prepare(pending);
            auto queued = file(98, "/retired"); install(index, queued); index.retire(queued.id, 2);
        }
        check(ngx_cache_purge_store_close(&store, &error) == 0, error.message);
        options.cursor_lifetime_ms = 250;
        check(ngx_cache_purge_store_open(path, &options, &store, &error) == 0, error.message);
        {
            Index index(store);
            Selector selector{SelectorKind::url, "https://shared.cache.test/dir/a", ""};
            auto expected = scan(index, selector); auto expiring = index.query(selector); Match first;
            check(expiring->next(first), "expiration first result");
            auto resume = expiring->bookmark(); std::vector<unsigned> actual{numeric(first.id)};
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            fails(NGX_PURGE_STORE_EXPIRED, [&] { expiring->next(first); }); expiring.reset();
            auto continued = index.query(selector, resume);
            while (continued->next(first)) actual.push_back(numeric(first.id));
            continued.reset();
            check(actual == expected, "expired snapshot resumes without losing existing candidates");
            check(index.get(id(99)).state == FileState::prepared, "unfinished fill persists across reopen");
            Match pending;
            auto unfinished = index.pending_fills();
            check(unfinished->next(pending) && pending.id == id(99) && !unfinished->next(pending),
                  "recovery enumerates only durable unfinished fills");
            unfinished.reset();
            auto retired_queue = index.retired_files();
            check(retired_queue->next(pending) && pending.id == id(98) && pending.state == FileState::retired,
                  "retired files have a durable cleanup queue");
            retired_queue.reset(); index.collect_file(id(98), pending.revision);
            check(selected(index, {SelectorKind::url, "https://aliases.test/0", ""}) == std::set<unsigned>({2, 4, 11}), "aliases persist without rebuilding");
            auto aborted = index.abort_prepare(id(99), 1); index.collect_file(id(99), aborted.revision);
            auto all = scan(index, {SelectorKind::key_prefix, "", ""});
            for (unsigned n : all) {
                auto record = index.get(id(n)); auto retired = index.retire(record.id, record.revision);
                index.collect_file(retired.id, retired.revision);
            }
            unsigned passes = 0;
            while (index.collect_keys(3)) check(++passes < 100, "bounded GC makes progress");
            check(raw_keys(store, NGX_PURGE_STORE_URL).empty(), "URL CF reclaimed");
            check(raw_keys(store, NGX_PURGE_STORE_TAG).empty(), "TAG CF reclaimed");
            auto keys = raw_keys(store, NGX_PURGE_STORE_DEFAULT);
            check(keys.size() == 3 && std::find(keys.begin(), keys.end(), "M:index") != keys.end()
                  && std::find(keys.begin(), keys.end(), "M:next") != keys.end(), "all file/value/component/alias records reclaimed");
        }
        check(ngx_cache_purge_store_close(&store, &error) == 0, error.message);
        std::cout << "PASS " << assertions << " index assertions; database " << path << std::endl;
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "FAIL: " << error.what() << std::endl;
        _exit(1);
    }
}
