/*
 * Real filesystem identity and index-publication checks; not a native
 * consumer.
 */
#include "ngx_http_cache_purge_internal.h"
#include "ngx_http_cache_purge_test.hpp"
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>

using namespace ngx_http_cache_purge;
static unsigned assertions;
static ngx_http_cache_rocksdb_error_t error;
static void
check(bool value, const char *message)
{
    ++assertions;
    if (!value) {
        throw std::runtime_error(std::string(message) + ": " + error.message);
    }
}
struct Descriptor {
    int fd = -1;
    explicit Descriptor(int value) : fd(value)
    {
        check(fd >= 0, "open descriptor");
    }
    ~Descriptor()
    {
        if (fd >= 0) {
            close(fd);
        }
    }
};
static FileId
id(unsigned value)
{
    FileId result{};
    result[15] = value;
    return result;
}
static ngx_http_cache_rocksdb_bytes_t
bytes(const std::string &value)
{
    return { reinterpret_cast<const unsigned char *>(value.data()),
        value.size() };
}
static void
body(int fd)
{
    const char value[] = "cache entity bytes";
    check(
        write(fd, value, sizeof(value)) == sizeof(value), "write cache entity");
}
static void
intact(int fd)
{
    char value[sizeof("cache entity bytes")];
    check(pread(fd, value, sizeof(value), 0) == sizeof(value)
              && std::memcmp(value, "cache entity bytes", sizeof(value)) == 0,
        "cache entity bytes stay unchanged");
}

static int
publication(const std::string &mode, const std::string &path)
{
    std::filesystem::create_directories(path + "/cache");
    std::filesystem::create_directories(path + "/db");
    int lock_fd;
    check(ngx_http_cache_purge_file_locks_open(
              (path + "/locks").c_str(), getuid(), 1, &lock_fd, &error)
              == 0,
        "open publication lock file");
    Descriptor anchor(lock_fd);
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    check(ngx_http_cache_rocksdb_open(
              (path + "/db").c_str(), &options, &store, &error)
              == 0,
        "open publication DB");
    Index index(store);
    Publications publications(
        { { path + "/cache", "cache", NGX_HTTP_CACHE_PURGE_ROOT_CACHE } },
        path + "/db");
    File file;
    file.id = id(1);
    file.key = "base";
    file.url = "https://example.test/file";
    file.host = "example.test";
    file.tag_namespace = "tenant";
    file.root = path + "/cache";
    file.zone = "cache";
    file.path = "file";
    auto target = file.root + '/' + file.path;
    unsigned char address[16];
    uint64_t offset;
    check(ngx_http_cache_purge_file_address(
              bytes(target), address, &offset, &error)
              == 0,
        "file address identity");
    int lease_fd;
    check(ngx_http_cache_purge_file_lease_acquire(
              (path + "/locks").c_str(), anchor.fd, offset, &lease_fd, &error)
              == 0,
        "hold publication/recovery lease");
    Descriptor lease(lease_fd);
    if (mode != "--recover") {
        Descriptor fd(open((file.root + "/temporary").c_str(),
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
        body(fd.fd);
        check(publications
                      .prepare(index, { file, {} }, file.root + "/temporary",
                          anchor.fd, lease.fd, fd.fd, getuid())
                      .state
                  == FileState::prepared,
            "durable intent and temporary identity");
        if (mode == "--crash-prepared") {
            _exit(0);
        }
        check(rename((file.root + "/temporary").c_str(), target.c_str()) == 0,
            "rename cached file");
        Descriptor directory(
            open(file.root.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        check(fsync(directory.fd) == 0, "sync target directory");
        if (mode == "--crash-rename") {
            _exit(0);
        }
        index.commit_pending(file.id, 1);
        if (mode == "--crash-marker") {
            check(ngx_http_cache_purge_file_marker_publish(
                      fd.fd, file.id.data(), address, 1, &error)
                      == 0,
                "durable readable marker");
        } else {
            check(mode == "--crash-index", "known crash cut");
        }
        std::cout << "PASS " << mode << " " << assertions << " assertions"
                  << std::endl;
        _exit(0);
    }
    auto pending = index.pending_fills();
    Match match;
    if (pending->next(match)) {
        check(match.id == file.id
                  && (match.state == FileState::prepared
                      || match.state == FileState::committed)
                  && !pending->next(match),
            "only unfinished publication recovered");
        pending.reset();
        auto finished = publications.finish(
            index, file.id, 1, anchor.fd, lease.fd, getuid());
        if (finished.state == FileState::retired) {
            check(!index.current(file.root, file.path),
                "missing publication cannot be admitted");
        } else {
            int fd = open(target.c_str(), O_RDWR | O_NOFOLLOW | O_CLOEXEC);
            Descriptor descriptor(fd);
            ngx_http_cache_purge_file_marker_t marker;
            check(
                ngx_http_cache_purge_file_marker_read(fd, &marker, &error) == 0
                    && std::memcmp(marker.id, file.id.data(), 16) == 0
                    && std::memcmp(marker.address, address, 16) == 0
                    && marker.state == NGX_HTTP_CACHE_PURGE_FILE_READY,
                "recover and publish exact file instance");
            check(finished.state == FileState::committed
                      && !index.has_fill_intent(file.id),
                "finish cleared durable intent");
            intact(fd);
        }
    }
    pending.reset();
    check(!index.pending_fills()->next(match),
        "publication recovery queue is empty");
    check(publications.finish(index, file.id, 1, anchor.fd, lease.fd, getuid())
                  .state
              == index.get(file.id).state,
        "finish retry preserves the durable result");
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close publication DB");
    std::cout << "PASS recovery " << assertions << " assertions" << std::endl;
    return 0;
}

static int
validation_guard(void *data, const ngx_http_cache_purge_index_file_t *file,
    ngx_http_cache_rocksdb_error_t *)
{
    check(file->state == NGX_HTTP_CACHE_PURGE_INDEX_COMMITTED,
        "guard sees the old committed object");
    return data ? NGX_HTTP_CACHE_ROCKSDB_CONFLICT : 0;
}

static int
validation(
    const std::string &mode, const std::string &path, bool with_origin = false)
{
    bool recovering = mode.find("--validate-recover") == 0;
    bool race = mode == "--validate-race-intent";
    with_origin = with_origin || race;
    std::filesystem::create_directories(path + "/cache");
    std::filesystem::create_directories(path + "/db");
    int lock_fd, lease_fd;
    check(ngx_http_cache_purge_file_locks_open(
              (path + "/db/file.leases").c_str(), getuid(), 1, &lock_fd, &error)
              == 0,
        "validation lock anchor");
    Descriptor anchor(lock_fd);
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    check(ngx_http_cache_rocksdb_open(
              (path + "/db").c_str(), &options, &store, &error)
              == 0,
        "validation database");
    Index index(store);
    Publications publications(
        { { path + "/cache", "cache", NGX_HTTP_CACHE_PURGE_ROOT_CACHE } },
        path + "/db");
    File file;
    file.id = id(42);
    file.key = "key";
    file.url = "https://example.test/value";
    file.host = "example.test";
    file.tag_namespace = "tenant";
    file.tags = { "red" };
    file.root = path + "/cache";
    file.zone = "cache";
    file.path = "file";
    file.metadata = "original-metadata";
    ngx_http_cache_purge_watermark_t origin{};
    if (with_origin) {
        if (race) {
            ngx_http_cache_purge_operations_t operations{};
            ngx_http_cache_purge_operation_limits_t limits{ 4096, 4194304 };
            check(ngx_http_cache_purge_operations_init(
                      &operations, store, &limits, &error)
                      == 0,
                "validation race ledger");
            std::memcpy(origin.ledger, operations.ledger, 16);
        } else {
            std::memset(origin.ledger, 0x17, 16);
            origin.sequence = 3;
        }
        unsigned char encoded[NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE];
        ngx_http_cache_purge_watermark_encode(&origin, encoded);
        file.metadata.assign(
            reinterpret_cast<char *>(encoded), sizeof(encoded));
        origin.sequence = race ? 0 : 7;
    }
    std::string target = file.root + "/file", before = "old-head",
                after = "new-head", tail(65536, 'B');
    unsigned char address[16];
    uint64_t offset;
    check(ngx_http_cache_purge_file_address(
              bytes(target), address, &offset, &error)
              == 0,
        "validation address");
    check(ngx_http_cache_purge_file_lease_acquire(
              (path + "/db/file.leases").c_str(), anchor.fd, offset, &lease_fd,
              &error)
              == 0,
        "validation path lease");
    Descriptor lease(lease_fd);
    if (!recovering) {
        Descriptor temporary(open((file.root + "/temporary").c_str(),
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
        std::string data = before + tail;
        check(write(temporary.fd, data.data(), data.size())
                  == (ssize_t) data.size(),
            "initial validation entity");
        publications.prepare(index, { file, {} }, file.root + "/temporary",
            anchor.fd, lease.fd, temporary.fd, getuid());
        check(rename((file.root + "/temporary").c_str(), target.c_str()) == 0,
            "publish validation fixture");
        publications.finish(index, file.id, 1, anchor.fd, lease.fd, getuid());
    }
    std::string blue = "blue", green = "green";
    ngx_http_cache_rocksdb_bytes_t tags[] = { bytes(green), bytes(blue),
        bytes(green) };
    ngx_http_cache_purge_validation_t input{};
    std::memcpy(input.file.id, file.id.data(), 16);
    input.file.revision = 2;
    input.transaction[15] = 1;
    input.before = bytes(before);
    input.after = bytes(after);
    input.tags = tags;
    input.tag_count = 3;
    input.replace_tags = 1;
    input.origin = with_origin ? &origin : nullptr;
    ngx_http_cache_purge_index_match_t result{};
    if (!recovering) {
        Descriptor selected(open(target.c_str(), O_RDONLY | O_CLOEXEC));
        if (mode == "--validate-basic") {
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &input, anchor.fd, lease.fd,
                      selected.fd, getuid(), validation_guard, &input, &result,
                      &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "guard veto precedes all mutation");
            check(index.get(file.id).revision == 2
                      && !index.has_fill_intent(file.id),
                "veto has no durable effect");
            auto wrong = input;
            wrong.file.revision++;
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &wrong, anchor.fd, lease.fd,
                      selected.fd, getuid(), validation_guard, nullptr, &result,
                      &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "validation CAS rejects stale revision");
            wrong = input;
            std::string mismatch = "bad-head";
            wrong.before = bytes(mismatch);
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &wrong, anchor.fd, lease.fd,
                      selected.fd, getuid(), validation_guard, nullptr, &result,
                      &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "wrong header cannot start validation");
            Descriptor unrelated(open((file.root + "/other").c_str(),
                O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &input, anchor.fd, lease.fd,
                      unrelated.fd, getuid(), validation_guard, nullptr,
                      &result, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "selected descriptor must belong to the indexed instance");
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &input, anchor.fd,
                      anchor.fd, selected.fd, getuid(), validation_guard,
                      nullptr, &result, &error)
                      != 0,
                "independent lock anchor cannot impersonate an OFD lease");
            wrong = input;
            std::string excessive(4097, 'x');
            wrong.before = wrong.after = bytes(excessive);
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &wrong, anchor.fd, lease.fd,
                      selected.fd, getuid(), validation_guard, nullptr, &result,
                      &error)
                      == NGX_HTTP_CACHE_ROCKSDB_INVALID,
                "prefix bound checked before mutation");
            check(index.get(file.id).revision == 2
                      && index.get(file.id).tags == file.tags
                      && !index.has_fill_intent(file.id),
                "rejected validation leaves catalogue unchanged");
        }
        check(ngx_http_cache_purge_publications_validate_prepare(
                  publications.native(), store, &input, anchor.fd, lease.fd,
                  selected.fd, getuid(), validation_guard, nullptr, &result,
                  &error)
                  == 0,
            "persist validation plan with proposed TAGs");
        check(result.state == 2 && result.revision == 3
                  && index.has_fill_intent(file.id),
            "validation owns committed path intent");
        check(index.get(file.id).tags == file.tags,
            "pending validation retains committed TAG membership");
        ngx_http_cache_purge_index_file_t inspected{};
        check(ngx_http_cache_purge_publications_inspect(publications.native(),
                  store, bytes(file.root), bytes(file.zone), bytes(file.path),
                  selected.fd, getuid(), &inspected, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_BUSY,
            "READY marker alone cannot bypass a validation intent");
        check(ngx_http_cache_purge_publications_validate_prepare(
                  publications.native(), store, &input, anchor.fd, lease.fd,
                  selected.fd, getuid(), validation_guard, nullptr, &result,
                  &error)
                      == 0
                  && result.revision == 3,
            "prepared validation retry is stable");
        auto wrong = input;
        std::string conflicting = "bad-head";
        wrong.after = bytes(conflicting);
        check(ngx_http_cache_purge_publications_validate_prepare(
                  publications.native(), store, &wrong, anchor.fd, lease.fd,
                  selected.fd, getuid(), validation_guard, nullptr, &result,
                  &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "validation identity cannot change its bound header");
        if (mode == "--validate-crash-intent" || race) {
            _exit(0);
        }
        if (mode == "--validate-crash-prefix"
            || mode == "--validate-crash-torn")
        {
            Descriptor writing(open(target.c_str(), O_RDWR | O_CLOEXEC));
            size_t length =
                mode == "--validate-crash-prefix" ? after.size() : 2;
            check(
                pwrite(writing.fd, after.data(), length, 0) == (ssize_t) length
                    && fsync(writing.fd) == 0,
                "simulate durable interrupted prefix write");
            _exit(0);
        }
    }
    if (mode == "--validate-recover-reject") {
        check(ngx_http_cache_purge_publications_validate_finish(
                  publications.native(), store, file.id.data(), nullptr, 0,
                  anchor.fd, lease.fd, getuid(), &result, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "changed prefix or object remains pending without file mutation");
        check(
            index.get(file.id).revision == 3 && index.has_fill_intent(file.id),
            "conflict retains recovery intent");
        check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
            "close conflicted validation database");
        std::cout << "PASS rejected validation recovery " << assertions
                  << " assertions" << std::endl;
        return 0;
    }
    check(ngx_http_cache_purge_publications_validate_finish(
              publications.native(), store, file.id.data(), nullptr, 0,
              anchor.fd, lease.fd, getuid(), &result, &error)
              == 0,
        "finish or recover validation");
    if (mode == "--validate-crash-done") {
        _exit(0);
    }
    check(!index.has_fill_intent(file.id),
        "validation completion releases path reservation");
    if (result.state == 3) {
        check(!index.current(file.root, file.path),
            "lost or replaced file is retired without adoption");
        auto wrong = input;
        wrong.transaction[15]++;
        check(
            ngx_http_cache_purge_publications_validate_finish(
                publications.native(), store, file.id.data(), wrong.transaction,
                2, anchor.fd, lease.fd, getuid(), &result, &error)
                == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "retirement does not authorize a different transaction");
        check(
            ngx_http_cache_purge_publications_validate_finish(
                publications.native(), store, file.id.data(), input.transaction,
                2, anchor.fd, lease.fd, getuid(), &result, &error)
                    == 0
                && result.state == 3,
            "matching retired retry preserves its result");
    } else {
        check(result.state == 2 && result.revision == 4,
            "validation advances exactly two revisions");
        check(index.get(file.id).tags
                  == std::vector<std::string>({ "blue", "green" }),
            "validation commit atomically publishes canonical TAGs");
        Descriptor selected(open(target.c_str(), O_RDONLY | O_CLOEXEC));
        std::string contents(8 + tail.size(), '\0');
        check(pread(selected.fd, contents.data(), contents.size(), 0)
                      == (ssize_t) contents.size()
                  && contents == after + tail,
            "only the bounded header prefix changed");
        ngx_http_cache_purge_index_file_t inspected{};
        check(ngx_http_cache_purge_publications_inspect(publications.native(),
                  store, bytes(file.root), bytes(file.zone), bytes(file.path),
                  selected.fd, getuid(), &inspected, &error)
                      == 0
                  && inspected.revision == 4 && inspected.metadata.len > 64
                  && std::memcmp(inspected.metadata.data, "NFR1", 4) == 0,
            "updated physical proof passes descriptor inspection");
        if (with_origin) {
            ngx_http_cache_purge_watermark_t full{}, validated{};
            check(ngx_http_cache_purge_publication_watermarks(
                      inspected.metadata, &full, &validated, &error)
                          == 0
                      && full.sequence == 3 && validated.sequence == 7
                      && std::memcmp(full.ledger, origin.ledger, 16) == 0,
                "recovery preserves distinct full and conditional coverage");
            std::string malformed(
                reinterpret_cast<const char *>(inspected.metadata.data),
                inspected.metadata.len);
            auto at = malformed.find("NFW2");
            check(at != std::string::npos, "conditional proof exists");
            malformed[at + 27] = 2;
            check(ngx_http_cache_purge_publication_watermarks(
                      bytes(malformed), &full, &validated, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                "conditional proof cannot regress below the full origin");
        }
        ngx_http_cache_purge_index_file_free(&inspected);
        ngx_http_cache_rocksdb_stats_t stats{}, retried{};
        check(ngx_http_cache_rocksdb_stats(store, &stats, &error) == 0,
            "read validation write count");
        check(ngx_http_cache_purge_publications_validate_prepare(
                  publications.native(), store, &input, anchor.fd, lease.fd,
                  selected.fd, getuid(), validation_guard, nullptr, &result,
                  &error)
                      == 0
                  && result.revision == 4,
            "lost success reply retries without another update");
        check(
            ngx_http_cache_purge_publications_validate_finish(
                publications.native(), store, file.id.data(), input.transaction,
                2, anchor.fd, lease.fd, getuid(), &result, &error)
                    == 0
                && result.revision == 4,
            "finish retry preserves validation result");
        check(ngx_http_cache_rocksdb_stats(store, &retried, &error) == 0
                  && stats.writes == retried.writes,
            "completed retries perform no database writes");
        if (mode == "--validate-basic") {
            auto previous_size = index.get(file.id).metadata.size();
            if (with_origin) {
                auto older = origin;
                older.sequence = 6;
                auto regressed = input;
                regressed.file.revision = 4;
                regressed.transaction[15] = 2;
                regressed.before = bytes(after);
                regressed.origin = &older;
                check(ngx_http_cache_purge_publications_validate_prepare(
                          publications.native(), store, &regressed, anchor.fd,
                          lease.fd, selected.fd, getuid(), validation_guard,
                          nullptr, &result, &error)
                          == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                    "older conditional origin cannot regress a completed "
                    "validation");
            }
            auto previous = input;
            input.file.revision = 4;
            input.transaction[15] = 2;
            input.before = bytes(after);
            input.after = bytes(after);
            input.replace_tags = 0;
            input.tags = nullptr;
            input.tag_count = 0;
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &input, anchor.fd, lease.fd,
                      selected.fd, getuid(), validation_guard, nullptr, &result,
                      &error)
                      == 0,
                "metadata-only validation can retain TAGs");
            check(ngx_http_cache_purge_publications_validate_finish(
                      publications.native(), store, file.id.data(),
                      input.transaction, 4, anchor.fd, lease.fd, getuid(),
                      &result, &error)
                          == 0
                      && result.revision == 6,
                "metadata-only validation completes");
            check(index.get(file.id).tags
                          == std::vector<std::string>({ "blue", "green" })
                      && index.get(file.id).metadata.size() == previous_size,
                "receipts remain bounded and retain TAGs");
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &previous, anchor.fd,
                      lease.fd, selected.fd, getuid(), validation_guard,
                      nullptr, &result, &error)
                      == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
                "older validation cannot replay after a newer revision");
            input.file.revision = 6;
            input.transaction[15] = 3;
            input.replace_tags = 1;
            check(ngx_http_cache_purge_publications_validate_prepare(
                      publications.native(), store, &input, anchor.fd, lease.fd,
                      selected.fd, getuid(), validation_guard, nullptr, &result,
                      &error)
                      == 0,
                "explicit empty TAG update prepares");
            check(ngx_http_cache_purge_publications_validate_finish(
                      publications.native(), store, file.id.data(),
                      input.transaction, 6, anchor.fd, lease.fd, getuid(),
                      &result, &error)
                          == 0
                      && index.get(file.id).tags.empty(),
                "explicit empty TAG update clears membership");
        }
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close validation database");
    std::cout << "PASS validation " << assertions << " assertions" << std::endl;
    return 0;
}

static int
activate_background(const std::string &path, const std::string &name,
    bool materialize = false, bool empty = false)
{
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    check(ngx_http_cache_rocksdb_open(path.c_str(), &options, &store, &error)
              == 0,
        "offline activation database");
    ngx_http_cache_purge_operations_t operations{};
    ngx_http_cache_purge_operation_limits_t limits{ 4096, 4194304 };
    check(ngx_http_cache_purge_operations_init(
              &operations, store, &limits, &error)
              == 0,
        "offline activation ledger");
    ngx_http_cache_purge_operation_t accepted{}, active{};
    struct Cleanup {
        ngx_http_cache_rocksdb_t **store;
        ngx_http_cache_purge_operation_t *accepted, *active;
        ~Cleanup()
        {
            ngx_http_cache_purge_operation_free(accepted);
            ngx_http_cache_purge_operation_free(active);
            if (*store != nullptr) {
                ngx_http_cache_rocksdb_close(store, nullptr);
            }
        }
    } cleanup{ &store, &accepted, &active };
    check(ngx_http_cache_purge_operations_get(
              &operations, bytes(name), &accepted, &error)
              == 0,
        "offline fixture requires an existing operation");
    if (materialize) {
        check((accepted.input.mode == NGX_HTTP_CACHE_PURGE_INVALIDATE || empty)
                  && accepted.scan_done && !accepted.pending
                  && !accepted.failed,
            "offline retirement requires completed Invalidate enumeration");
        Index index(store);
        auto text = [](ngx_http_cache_rocksdb_bytes_t value) {
            return value.len ?
                       std::string(reinterpret_cast<const char *>(value.data),
                           value.len) :
                       std::string();
        };
        Selector selector{ static_cast<SelectorKind>(
                               accepted.input.selector.kind),
            text(accepted.input.selector.value),
            text(accepted.input.selector.scope) };
        auto query = index.query(selector);
        Match match;
        unsigned count = 0;
        while (query->next(match)) {
            ngx_http_cache_purge_index_expected_t expected{};
            std::memcpy(expected.id, match.id.data(), 16);
            expected.revision = match.revision;
            ngx_http_cache_purge_watermark_t required{};
            check(
                ngx_http_cache_purge_index_validation_requirement(
                    store, &expected, &required, &error)
                        == 0
                    && required.sequence >= accepted.sequence
                    && std::memcmp(required.ledger, operations.ledger, 16) == 0,
                "fixture files have durable Invalidate requirements");
            check(++count <= 16, "bounded offline retirement fixture");
        }
        query.reset();
        check(empty ? count == 0 : count != 0,
            "offline fixture has the expected indexed membership");
        /* All native workers are stopped by the test. This does not implement
         * production retirement, late membership or native node cleanup. */
        check(ngx_http_cache_purge_operations_materialize(&operations,
                  bytes(name), accepted.revision, accepted.sequence, &active,
                  &error)
                  == 0,
            "offline controlled retirement fixture");
        size_t expired;
        check(ngx_http_cache_purge_operations_expire(&operations,
                  accepted.propagation_until_ms + 1, 32, &expired, &error)
                      == 0
                  && (!accepted.propagate || expired != 0),
            "offline fixture removes the remaining propagation record");
        ngx_http_cache_purge_operation_free(&active);
        check(ngx_http_cache_purge_operations_get(
                  &operations, bytes(name), &active, &error)
                      == 0
                  && active.stage == NGX_HTTP_CACHE_PURGE_MATERIALIZED
                  && !active.propagate,
            "offline fixture leaves neither resident role active");
    } else {
        check(ngx_http_cache_purge_operations_activate(&operations, bytes(name),
                  accepted.revision,
                  static_cast<uint64_t>(std::time(nullptr)) * 1000, &active,
                  &error)
                  == 0,
            "offline activation for background integration only");
    }
    ngx_http_cache_purge_operation_free(&accepted);
    ngx_http_cache_purge_operation_free(&active);
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close offline activation database");
    return 0;
}

static int
background_delete(const std::string &mode, const std::string &path)
{
    std::filesystem::create_directories(path + "/cache/a");
    std::filesystem::create_directories(path + "/cache-b/a");
    std::filesystem::create_directories(path + "/db");
    int anchor_fd;
    auto locks = path + "/db/file.leases";
    check(ngx_http_cache_purge_file_locks_open(
              locks.c_str(), getuid(), 1, &anchor_fd, &error)
              == 0,
        "background lease anchor");
    Descriptor anchor(anchor_fd);
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    check(ngx_http_cache_rocksdb_open(
              (path + "/db").c_str(), &options, &store, &error)
              == 0,
        "background database");
    {
        Index index(store);
        Publications publications(
            { { path + "/cache", "cache", NGX_HTTP_CACHE_PURGE_ROOT_CACHE },
                { path + "/cache-b", "cache-b",
                    NGX_HTTP_CACHE_PURGE_ROOT_CACHE } },
            path + "/db");
        ngx_http_cache_purge_operations_t operations{};
        ngx_http_cache_purge_operation_limits_t limits{ 4096, 4194304 };
        check(ngx_http_cache_purge_operations_init(
                  &operations, store, &limits, &error)
                  == 0,
            "background operation ledger");
        auto create = [&](unsigned number, const char *zone, const char *name,
                          const char *url, uint64_t sequence, FileId previous) {
            File file;
            file.id = id(number);
            file.key = std::string("background:") + name;
            file.url = url;
            file.host = "example.test";
            file.tag_namespace = "tenant";
            file.root = path + '/' + zone;
            file.zone = zone;
            file.path = std::string("a/") + name;
            ngx_http_cache_purge_watermark_t watermark{};
            std::memcpy(watermark.ledger, operations.ledger, 16);
            watermark.sequence = sequence;
            unsigned char encoded[NGX_HTTP_CACHE_PURGE_WATERMARK_SIZE];
            ngx_http_cache_purge_watermark_encode(&watermark, encoded);
            file.metadata.assign(
                reinterpret_cast<const char *>(encoded), sizeof(encoded));
            auto target = file.root + '/' + file.path;
            unsigned char digest[16];
            uint64_t offset;
            int lease_fd;
            check(ngx_http_cache_purge_file_address(
                      bytes(target), digest, &offset, &error)
                      == 0,
                "background address");
            check(ngx_http_cache_purge_file_lease_acquire(
                      locks.c_str(), anchor.fd, offset, &lease_fd, &error)
                      == 0,
                "background path lease");
            Descriptor lease(lease_fd);
            auto temporary = file.root + "/temporary";
            Descriptor fd(open(temporary.c_str(),
                O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
            body(fd.fd);
            publications.prepare(index, { file, previous }, temporary,
                anchor.fd, lease.fd, fd.fd, getuid());
            check(rename(temporary.c_str(), target.c_str()) == 0,
                "background rename");
            check(publications
                          .finish(
                              index, file.id, 1, anchor.fd, lease.fd, getuid())
                          .state
                      == FileState::committed,
                "background committed file");
        };
        if (mode == "--background-replace") {
            create(8, "cache", "first", "https://example.test/work/first", 2,
                id(1));
        } else {
            check(mode == "--background-seed"
                      || mode == "--background-seed-invalidate",
                "background fixture mode");
            create(
                1, "cache", "first", "https://example.test/work/first", 0, {});
            create(2, "cache-b", "second", "https://example.test/work/second",
                0, {});
            create(
                4, "cache", "outside", "https://example.test/outside", 0, {});
            create(6, "cache-b", "other", "https://example.test/other", 0, {});
            for (const std::string name : { "background", "independent" }) {
                std::string value = name == "background" ?
                                        "https://example.test/work/" :
                                        "https://example.test/other";
                ngx_http_cache_purge_operation_input_t input{};
                input.id = bytes(name);
                input.mode = mode == "--background-seed-invalidate" ?
                                 NGX_HTTP_CACHE_PURGE_INVALIDATE :
                                 NGX_HTTP_CACHE_PURGE_DELETE;
                input.issued_at_ms = 1;
                input.selector.kind = name == "background" ?
                                          NGX_HTTP_CACHE_PURGE_URL_PREFIX :
                                          NGX_HTTP_CACHE_PURGE_URL;
                input.selector.value = bytes(value);
                ngx_http_cache_purge_operation_t accepted{}, active{};
                uint64_t now =
                    mode == "--background-seed-invalidate" ?
                        static_cast<uint64_t>(std::time(nullptr)) * 1000 :
                        1;
                check(ngx_http_cache_purge_operations_accept(
                          &operations, &input, now, 300000, &accepted, &error)
                          == 0,
                    "seed durable operation");
                /* Offline setup does not claim native HTTP acceptance. */
                check(ngx_http_cache_purge_operations_activate(&operations,
                          input.id, accepted.revision, now, &active, &error)
                          == 0,
                    "seed active background task");
                ngx_http_cache_purge_operation_free(&accepted);
                ngx_http_cache_purge_operation_free(&active);
            }
            create(
                3, "cache", "fresh", "https://example.test/work/fresh", 2, {});
        }
    }
    check(ngx_http_cache_rocksdb_close(&store, &error) == 0,
        "close background fixture database");
    std::cout << "PASS background fixture " << assertions << std::endl;
    return 0;
}

static int
removal(const std::string &mode, const std::string &path)
{
    std::filesystem::create_directories(path + "/cache/a/00");
    std::filesystem::create_directories(path + "/db");
    int lock_fd, lease_fd;
    check(ngx_http_cache_purge_file_locks_open(
              (path + "/locks").c_str(), getuid(), 1, &lock_fd, &error)
              == 0,
        "open removal lock file");
    Descriptor anchor(lock_fd);
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    check(ngx_http_cache_rocksdb_open(
              (path + "/db").c_str(), &options, &store, &error)
              == 0,
        "open removal DB");
    {
        Index index(store);
        Publications publications(
            { { path + "/cache", "cache", NGX_HTTP_CACHE_PURGE_ROOT_CACHE } },
            path + "/db");
        File file;
        file.id = id(43);
        file.key = "remove-key";
        file.url = "https://example.test/remove";
        file.host = "example.test";
        file.tag_namespace = "tenant";
        file.tags = { "tag" };
        file.root = path + "/cache";
        file.zone = "cache";
        file.path = "a/00/file";
        auto target = file.root + '/' + file.path;
        unsigned char address[16];
        uint64_t offset;
        check(ngx_http_cache_purge_file_address(
                  bytes(target), address, &offset, &error)
                  == 0,
            "removal address");
        check(ngx_http_cache_purge_file_lease_acquire((path + "/locks").c_str(),
                  anchor.fd, offset, &lease_fd, &error)
                  == 0,
            "hold removal lease");
        Descriptor lease(lease_fd);
        if (mode != "--remove-recover") {
            Descriptor fd(open((file.root + "/temp").c_str(),
                O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
            body(fd.fd);
            publications.prepare(index, { file, {} }, file.root + "/temp",
                anchor.fd, lease.fd, fd.fd, getuid());
            check(rename((file.root + "/temp").c_str(), target.c_str()) == 0,
                "rename removal fixture");
            check(publications
                          .finish(
                              index, file.id, 1, anchor.fd, lease.fd, getuid())
                          .state
                      == FileState::committed,
                "publish removal fixture");
            if (mode != "--remove-basic") {
                check(index.retire(file.id, 2).state == FileState::retired,
                    "persist retirement before unlink");
                if (mode == "--remove-crash-unlinked") {
                    check(unlink(target.c_str()) == 0, "unlink before crash");
                } else {
                    check(mode == "--remove-crash-retired",
                        "known removal crash cut");
                }
                std::cout << "PASS " << mode << std::endl;
                _exit(0);
            }
        }
        auto saved = index.get(file.id);
        ngx_http_cache_purge_index_expected_t expected{};
        std::memcpy(expected.id, file.id.data(), 16);
        expected.revision = saved.revision;
        ngx_http_cache_purge_index_match_t removed{};
        uint32_t outcome = 0;
        check(ngx_http_cache_purge_publications_remove(publications.native(),
                  store, &expected, anchor.fd, lease.fd, getuid(), &outcome,
                  &removed, &error)
                  == 0,
            "remove exact instance after durable retirement");
        check((outcome == NGX_HTTP_CACHE_PURGE_REMOVED
                  || outcome == NGX_HTTP_CACHE_PURGE_MISSING)
                  && removed.state == NGX_HTTP_CACHE_PURGE_INDEX_RETIRED
                  && removed.revision == 3 && !std::filesystem::exists(target),
            "removed result and absent file");
        check(!index.current(file.root, file.path),
            "removed instance has no current mapping");
        Match pending;
        check(index.retired_files()->next(pending) && pending.id == file.id,
            "retired record remains available for node/task cleanup");
        expected.revision = removed.revision;
        check(ngx_http_cache_purge_publications_remove(publications.native(),
                  store, &expected, anchor.fd, lease.fd, getuid(), &outcome,
                  &removed, &error)
                      == 0
                  && outcome == NGX_HTTP_CACHE_PURGE_MISSING
                  && removed.revision == 3,
            "repeated removal is idempotent and syncs absence");
    }
    check(
        ngx_http_cache_rocksdb_close(&store, &error) == 0, "close removal DB");
    std::cout << "PASS removal " << assertions << " assertions" << std::endl;
    return 0;
}

int
main(int argc, char **argv)
{
    try {
        if (argc == 4 && std::string(argv[1]) == "--activate-background") {
            return activate_background(argv[2], argv[3]);
        }
        if (argc == 4 && std::string(argv[1]) == "--retire-invalidate-fixture")
        {
            return activate_background(argv[2], argv[3], true);
        }
        if (argc == 4 && std::string(argv[1]) == "--retire-empty-fixture") {
            return activate_background(argv[2], argv[3], true, true);
        }
        if (argc == 3 && std::string(argv[1]).find("--background-") == 0) {
            return background_delete(argv[1], argv[2]);
        }
        if (argc == 3 && std::string(argv[1]).find("--validate-origin-") == 0) {
            return validation(
                "--validate-" + std::string(argv[1]).substr(18), argv[2], true);
        }
        if (argc == 3 && std::string(argv[1]).find("--remove-") == 0) {
            return removal(argv[1], argv[2]);
        }
        if (argc == 3) {
            return std::string(argv[1]).find("--validate-") == 0 ?
                       validation(argv[1], argv[2]) :
                       publication(argv[1], argv[2]);
        }
        char root[] = "/tmp/cache-purge-files-XXXXXX";
        check(mkdtemp(root) != nullptr, "temporary directory");
        std::string path(root), target = path + "/final";
        ngx_http_cache_purge_watermark_t invalid_origin{}, invalid_validation{};
        check(ngx_http_cache_purge_publication_watermarks(
                  { nullptr, 4 }, &invalid_origin, &invalid_validation, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "watermark inspection rejects a missing metadata buffer");
        unsigned char address[16], again[16], different[16];
        uint64_t offset, second;
        check(ngx_http_cache_purge_file_address(
                  bytes(target), address, &offset, &error)
                  == 0,
            "address hash");
        check(ngx_http_cache_purge_file_address(
                  bytes(target), again, &second, &error)
                      == 0
                  && offset == second && std::memcmp(address, again, 16) == 0,
            "address is stable across calls");
        check(ngx_http_cache_purge_file_address(
                  bytes(path + "/other"), different, &second, &error)
                      == 0
                  && std::memcmp(address, different, 16) != 0,
            "physical addresses differ");
        Descriptor fd(open((path + "/temporary").c_str(),
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
        body(fd.fd);
        ngx_http_cache_purge_file_marker_t marker;
        check(ngx_http_cache_purge_file_marker_read(fd.fd, &marker, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
            "unregistered file has no identity");
        auto first = id(1), other = id(2);
        check(ngx_http_cache_purge_file_marker_prepare(
                  fd.fd, first.data(), address, 1, &error)
                  == 0,
            "prepare identity");
        check(ngx_http_cache_purge_file_marker_prepare(
                  fd.fd, first.data(), address, 1, &error)
                  == 0,
            "prepare retry");
        check(ngx_http_cache_purge_file_marker_prepare(
                  fd.fd, other.data(), address, 0, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "do not overwrite another instance");
        check(rename((path + "/temporary").c_str(), target.c_str()) == 0,
            "same-filesystem rename");
        Descriptor reopened(
            open(target.c_str(), O_RDWR | O_NOFOLLOW | O_CLOEXEC));
        check(
            ngx_http_cache_purge_file_marker_read(reopened.fd, &marker, &error)
                    == 0
                && marker.state == NGX_HTTP_CACHE_PURGE_FILE_PREPARED
                && std::memcmp(marker.id, first.data(), 16) == 0,
            "same-filesystem rename preserves prepared identity");
        check(ngx_http_cache_purge_file_marker_publish(
                  reopened.fd, other.data(), address, 0, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "publication compares the instance");
        check(ngx_http_cache_purge_file_marker_publish(
                  reopened.fd, first.data(), different, 0, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "publication compares the address");
        check(ngx_http_cache_purge_file_marker_publish(
                  reopened.fd, first.data(), address, 1, &error)
                  == 0,
            "publish identity");
        check(ngx_http_cache_purge_file_marker_publish(
                  reopened.fd, first.data(), address, 1, &error)
                  == 0,
            "publication retry");
        check(ngx_http_cache_purge_file_marker_prepare(
                  reopened.fd, first.data(), address, 0, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_CONFLICT,
            "cannot downgrade a readable file");
        intact(reopened.fd);
        /* Copy the real source bytes, without copying its xattrs. */
        Descriptor copied_local(open((path + "/copy").c_str(),
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
        char entity[64];
        ssize_t size = pread(reopened.fd, entity, sizeof(entity), 0);
        check(size > 0 && write(copied_local.fd, entity, size) == size,
            "copy source bytes");
        intact(copied_local.fd);
        check(ngx_http_cache_purge_file_marker_read(
                  copied_local.fd, &marker, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
            "byte copy does not copy the source identity");
        check(ngx_http_cache_purge_file_marker_publish(
                  copied_local.fd, first.data(), address, 0, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND,
            "unmarked byte copy cannot be published");
        char copied[] = "/dev/shm/cache-purge-copy-XXXXXX";
        Descriptor copy(mkstemp(copied));
        check(unlink(copied) == 0, "unlink cross-filesystem fixture");
        struct stat source_stat{}, destination_stat{};
        check(fstat(reopened.fd, &source_stat) == 0
                  && fstat(copy.fd, &destination_stat) == 0
                  && source_stat.st_dev != destination_stat.st_dev,
            "copy test uses distinct filesystems");
        check(write(copy.fd, entity, size) == size,
            "copy source bytes across devices");
        intact(copy.fd);
        int copied_status =
            ngx_http_cache_purge_file_marker_read(copy.fd, &marker, &error);
        if (copied_status != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
            char raw[40];
            check(copied_status == NGX_HTTP_CACHE_ROCKSDB_IO
                      && fgetxattr(copy.fd, "user.ngx_cache_purge.id", raw,
                             sizeof(raw))
                             == -1
                      && errno == EOPNOTSUPP,
                "unsupported destination xattrs are explicitly rejected");
        }
        check(
            ngx_http_cache_purge_file_marker_read(reopened.fd, &marker, &error)
                    == 0
                && marker.state == NGX_HTTP_CACHE_PURGE_FILE_READY,
            "copy leaves valid source identity unchanged");
        unsigned char malformed[41]{};
        check(fsetxattr(reopened.fd, "user.ngx_cache_purge.id", malformed,
                  sizeof(malformed), XATTR_REPLACE)
                  == 0,
            "inject oversized marker");
        check(
            ngx_http_cache_purge_file_marker_read(reopened.fd, &marker, &error)
                == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "reject oversized marker");
        check(fsetxattr(reopened.fd, "user.ngx_cache_purge.id", malformed, 40,
                  XATTR_REPLACE)
                  == 0,
            "inject unknown marker");
        check(
            ngx_http_cache_purge_file_marker_read(reopened.fd, &marker, &error)
                == NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
            "reject unknown format");

        std::string locks = path + "/locks";
        int lock_fd;
        check(ngx_http_cache_purge_file_locks_open(
                  locks.c_str(), getuid(), 1, &lock_fd, &error)
                  == 0,
            "create persistent lock file");
        Descriptor anchor(lock_fd);
        int lease_fd;
        check(ngx_http_cache_purge_file_lease_acquire(
                  locks.c_str(), anchor.fd, offset, &lease_fd, &error)
                  == 0,
            "acquire independent OFD lease");
        check(ngx_http_cache_purge_file_lease_release(&lease_fd, &error) == 0
                  && lease_fd == -1,
            "consume lease reference");
        auto alias = path + "/alias";
        check(symlink(locks.c_str(), alias.c_str()) == 0,
            "lease symlink fixture");
        int invalid;
        check(ngx_http_cache_purge_file_locks_open(
                  alias.c_str(), getuid(), 0, &invalid, &error)
                      != 0
                  && invalid == -1,
            "lease opening refuses symlinks without replacing them");
        check(ngx_http_cache_purge_file_locks_open(
                  locks.c_str(), getuid() + 1, 0, &invalid, &error)
                  == NGX_HTTP_CACHE_ROCKSDB_INVALID,
            "foreign owner is rejected rather than chowned");
        std::cout << "PASS " << assertions
                  << " filesystem assertions; directory " << root << std::endl;
        return 0;
    } catch (const std::exception &failure) {
        std::cerr << "FAIL: " << failure.what() << std::endl;
        return 1;
    }
}
