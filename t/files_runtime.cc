/* Real filesystem identity and index-publication checks; not a native consumer. */
#include "ngx_cache_purge_files.h"
#include "ngx_cache_purge_index.hpp"
#include "ngx_cache_purge_publication.hpp"
#include "ngx_cache_purge_component.h"
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>

using namespace ngx_cache_purge;
static unsigned assertions;
static ngx_cache_purge_store_error_t error;
static void check(bool value, const char *message) {
    ++assertions;
    if (!value) throw std::runtime_error(std::string(message) + ": " + error.message);
}
struct Descriptor {
    int fd = -1;
    explicit Descriptor(int value) : fd(value) { check(fd >= 0, "open descriptor"); }
    ~Descriptor() { if (fd >= 0) close(fd); }
};
static FileId id(unsigned value) { FileId result{}; result[15] = value; return result; }
static ngx_cache_purge_bytes_t bytes(const std::string &value) {
    return {reinterpret_cast<const unsigned char *>(value.data()), value.size()};
}
static void body(int fd) {
    const char value[] = "cache entity bytes";
    check(write(fd, value, sizeof(value)) == sizeof(value), "write cache entity");
}
static void intact(int fd) {
    char value[sizeof("cache entity bytes")];
    check(pread(fd, value, sizeof(value), 0) == sizeof(value)
          && std::memcmp(value, "cache entity bytes", sizeof(value)) == 0, "cache entity bytes stay unchanged");
}

static int publication(const std::string &mode, const std::string &path) {
    std::filesystem::create_directories(path + "/cache");
    std::filesystem::create_directories(path + "/db");
    int lock_fd;
    check(ngx_cache_purge_file_locks_open((path + "/locks").c_str(), getuid(), 1, &lock_fd, &error) == 0,
          "open publication lock file");
    Descriptor anchor(lock_fd);
    ngx_cache_purge_store_options_t options; ngx_cache_purge_store_options(&options);
    ngx_cache_purge_store_t *store = nullptr;
    check(ngx_cache_purge_store_open((path + "/db").c_str(), &options, &store, &error) == 0, "open publication DB");
    Index index(store);
    Publications publications({{path + "/cache", "cache", NGX_PURGE_ROOT_CACHE}}, path + "/db");
    File file; file.id = id(1); file.key = "base"; file.url = "https://example.test/file";
    file.host = "example.test"; file.tag_namespace = "tenant";
    file.root = path + "/cache"; file.zone = "cache"; file.path = "file";
    auto target = file.root + '/' + file.path;
    unsigned char address[16]; uint64_t offset;
    check(ngx_cache_purge_file_address(bytes(target), address, &offset, &error) == 0, "file address identity");
    int lease_fd;
    check(ngx_cache_purge_file_lease_acquire((path + "/locks").c_str(), anchor.fd, offset, &lease_fd, &error) == 0,
          "hold publication/recovery lease");
    Descriptor lease(lease_fd);
    if (mode != "--recover") {
        Descriptor fd(open((file.root + "/temporary").c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
        body(fd.fd);
        check(publications.prepare(index, {file, {}}, file.root + "/temporary", anchor.fd, lease.fd, fd.fd, getuid()).state
              == FileState::prepared, "durable intent and temporary identity");
        if (mode == "--crash-prepared") _exit(0);
        check(rename((file.root + "/temporary").c_str(), target.c_str()) == 0, "rename cached file");
        Descriptor directory(open(file.root.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        check(fsync(directory.fd) == 0, "sync target directory");
        if (mode == "--crash-rename") _exit(0);
        index.commit_pending(file.id, 1);
        if (mode == "--crash-marker")
            check(ngx_cache_purge_file_marker_publish(fd.fd, file.id.data(), address, 1, &error) == 0, "durable readable marker");
        else check(mode == "--crash-index", "known crash cut");
        std::cout << "PASS " << mode << " " << assertions << " assertions" << std::endl;
        _exit(0);
    }
    auto pending = index.pending_fills(); Match match;
    if (pending->next(match)) {
        check(match.id == file.id && (match.state == FileState::prepared || match.state == FileState::committed)
              && !pending->next(match), "only unfinished publication recovered");
        pending.reset();
        auto finished = publications.finish(index, file.id, 1, anchor.fd, lease.fd, getuid());
        if (finished.state == FileState::retired) {
            check(!index.current(file.root, file.path), "missing publication cannot be admitted");
        } else {
            int fd = open(target.c_str(), O_RDWR | O_NOFOLLOW | O_CLOEXEC);
            Descriptor descriptor(fd);
            ngx_cache_purge_file_marker_t marker;
            check(ngx_cache_purge_file_marker_read(fd, &marker, &error) == 0
                  && std::memcmp(marker.id, file.id.data(), 16) == 0
                  && std::memcmp(marker.address, address, 16) == 0
                  && marker.state == NGX_CACHE_PURGE_FILE_READY, "recover and publish exact file instance");
            check(finished.state == FileState::committed && !index.has_fill_intent(file.id), "finish cleared durable intent");
            intact(fd);
        }
    }
    pending.reset();
    check(!index.pending_fills()->next(match), "publication recovery queue is empty");
    check(publications.finish(index, file.id, 1, anchor.fd, lease.fd, getuid()).state == index.get(file.id).state,
          "finish retry preserves the durable result");
    check(ngx_cache_purge_store_close(&store, &error) == 0, "close publication DB");
    std::cout << "PASS recovery " << assertions << " assertions" << std::endl;
    return 0;
}

int main(int argc, char **argv) {
    try {
        if (argc == 3) return publication(argv[1], argv[2]);
        char root[] = "/tmp/cache-purge-files-XXXXXX";
        check(mkdtemp(root) != nullptr, "temporary directory");
        std::string path(root), target = path + "/final";
        unsigned char address[16], again[16], different[16]; uint64_t offset, second;
        check(ngx_cache_purge_file_address(bytes(target), address, &offset, &error) == 0, "address hash");
        check(ngx_cache_purge_file_address(bytes(target), again, &second, &error) == 0
              && offset == second && std::memcmp(address, again, 16) == 0, "address is stable across calls");
        check(ngx_cache_purge_file_address(bytes(path + "/other"), different, &second, &error) == 0
              && std::memcmp(address, different, 16) != 0, "physical addresses differ");
        Descriptor fd(open((path + "/temporary").c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600));
        body(fd.fd);
        ngx_cache_purge_file_marker_t marker;
        check(ngx_cache_purge_file_marker_read(fd.fd, &marker, &error) == NGX_PURGE_STORE_NOT_FOUND, "unregistered file has no identity");
        auto first = id(1), other = id(2);
        check(ngx_cache_purge_file_marker_prepare(fd.fd, first.data(), address, 1, &error) == 0, "prepare identity");
        check(ngx_cache_purge_file_marker_prepare(fd.fd, first.data(), address, 1, &error) == 0, "prepare retry");
        check(ngx_cache_purge_file_marker_prepare(fd.fd, other.data(), address, 0, &error) == NGX_PURGE_STORE_CONFLICT, "do not overwrite another instance");
        check(rename((path + "/temporary").c_str(), target.c_str()) == 0, "same-filesystem rename");
        Descriptor reopened(open(target.c_str(), O_RDWR | O_NOFOLLOW | O_CLOEXEC));
        check(ngx_cache_purge_file_marker_read(reopened.fd, &marker, &error) == 0
              && marker.state == NGX_CACHE_PURGE_FILE_PREPARED && std::memcmp(marker.id, first.data(), 16) == 0,
              "same-filesystem rename preserves prepared identity");
        check(ngx_cache_purge_file_marker_publish(reopened.fd, other.data(), address, 0, &error) == NGX_PURGE_STORE_CONFLICT, "publication compares the instance");
        check(ngx_cache_purge_file_marker_publish(reopened.fd, first.data(), different, 0, &error) == NGX_PURGE_STORE_CONFLICT, "publication compares the address");
        check(ngx_cache_purge_file_marker_publish(reopened.fd, first.data(), address, 1, &error) == 0, "publish identity");
        check(ngx_cache_purge_file_marker_publish(reopened.fd, first.data(), address, 1, &error) == 0, "publication retry");
        check(ngx_cache_purge_file_marker_prepare(reopened.fd, first.data(), address, 0, &error) == NGX_PURGE_STORE_CONFLICT, "cannot downgrade a readable file");
        intact(reopened.fd);
        // Copy the real source bytes, without copying its xattrs.
        Descriptor copied_local(open((path + "/copy").c_str(), O_CREAT|O_EXCL|O_RDWR|O_CLOEXEC, 0600));
        char entity[64];
        ssize_t size = pread(reopened.fd, entity, sizeof(entity), 0);
        check(size > 0 && write(copied_local.fd, entity, size) == size, "copy source bytes");
        intact(copied_local.fd);
        check(ngx_cache_purge_file_marker_read(copied_local.fd, &marker, &error) == NGX_PURGE_STORE_NOT_FOUND,
              "byte copy does not copy the source identity");
        check(ngx_cache_purge_file_marker_publish(copied_local.fd, first.data(), address, 0, &error) == NGX_PURGE_STORE_NOT_FOUND,
              "unmarked byte copy cannot be published");
        char copied[] = "/dev/shm/cache-purge-copy-XXXXXX";
        Descriptor copy(mkstemp(copied));
        check(unlink(copied) == 0, "unlink cross-filesystem fixture");
        struct stat source_stat{}, destination_stat{};
        check(fstat(reopened.fd, &source_stat) == 0 && fstat(copy.fd, &destination_stat) == 0
              && source_stat.st_dev != destination_stat.st_dev, "copy test uses distinct filesystems");
        check(write(copy.fd, entity, size) == size, "copy source bytes across devices");
        intact(copy.fd);
        int copied_status = ngx_cache_purge_file_marker_read(copy.fd, &marker, &error);
        if (copied_status != NGX_PURGE_STORE_NOT_FOUND) {
            char raw[40];
            check(copied_status == NGX_PURGE_STORE_IO
                  && fgetxattr(copy.fd, "user.ngx_cache_purge.id", raw, sizeof(raw)) == -1
                  && errno == EOPNOTSUPP, "unsupported destination xattrs are explicitly rejected");
        }
        check(ngx_cache_purge_file_marker_read(reopened.fd, &marker, &error) == 0
              && marker.state == NGX_CACHE_PURGE_FILE_READY, "copy leaves valid source identity unchanged");
        unsigned char malformed[41]{};
        check(fsetxattr(reopened.fd, "user.ngx_cache_purge.id", malformed, sizeof(malformed), XATTR_REPLACE) == 0, "inject oversized marker");
        check(ngx_cache_purge_file_marker_read(reopened.fd, &marker, &error) == NGX_PURGE_STORE_CORRUPT, "reject oversized marker");
        check(fsetxattr(reopened.fd, "user.ngx_cache_purge.id", malformed, 40, XATTR_REPLACE) == 0, "inject unknown marker");
        check(ngx_cache_purge_file_marker_read(reopened.fd, &marker, &error) == NGX_PURGE_STORE_CORRUPT, "reject unknown format");

        std::string locks = path + "/locks";
        int lock_fd;
        check(ngx_cache_purge_file_locks_open(locks.c_str(), getuid(), 1, &lock_fd, &error) == 0, "create persistent lock file");
        Descriptor anchor(lock_fd);
        int lease_fd;
        check(ngx_cache_purge_file_lease_acquire(locks.c_str(), anchor.fd, offset, &lease_fd, &error) == 0,
              "acquire independent OFD lease");
        check(ngx_cache_purge_file_lease_release(&lease_fd, &error) == 0 && lease_fd == -1,
              "consume lease reference");
        auto alias = path + "/alias";
        check(symlink(locks.c_str(), alias.c_str()) == 0, "lease symlink fixture");
        int invalid;
        check(ngx_cache_purge_file_locks_open(alias.c_str(), getuid(), 0, &invalid, &error) != 0 && invalid == -1,
              "lease opening refuses symlinks without replacing them");
        check(ngx_cache_purge_file_locks_open(locks.c_str(), getuid() + 1, 0, &invalid, &error) == NGX_PURGE_STORE_INVALID,
              "foreign owner is rejected rather than chowned");
        std::cout << "PASS " << assertions << " filesystem assertions; directory " << root << std::endl;
        return 0;
    } catch (const std::exception &failure) {
        std::cerr << "FAIL: " << failure.what() << std::endl;
        return 1;
    }
}
