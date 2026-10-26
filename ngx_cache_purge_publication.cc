/* Copyright (C) Hanada */
#include "ngx_cache_purge_publication.hpp"
#include "ngx_cache_purge_component.h"
#include "ngx_cache_purge_files.h"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ngx_cache_purge {
namespace {
struct FD {
    int value;
    explicit FD(int fd = -1) : value(fd) {}
    ~FD() { if (value >= 0) close(value); }
    FD(const FD &) = delete;
    FD &operator=(const FD &) = delete;
};
void require(bool valid, const char *message) {
    if (!valid) throw IndexError(NGX_PURGE_STORE_INVALID, message);
}
void checked(int rc, const ngx_cache_purge_store_error_t &error) {
    if (rc) throw IndexError(rc, error.message);
}
[[noreturn]] void io(const char *message) {
    int saved = errno;
    throw IndexError(saved == EMFILE || saved == ENFILE || saved == ENOMEM ? NGX_PURGE_STORE_LIMIT : NGX_PURGE_STORE_IO,
                     std::string(message) + ": errno " + std::to_string(saved));
}
bool relative(const std::string &path) {
    if (path.empty() || path.front() == '/' || path.back() == '/' || path.find('\0') != std::string::npos) return false;
    size_t at = 0;
    while (at < path.size()) {
        auto end = path.find('/', at); if (end == std::string::npos) end = path.size();
        auto part = path.substr(at, end - at);
        if (part.empty() || part == "." || part == "..") return false;
        at = end + 1;
    }
    return true;
}
bool contains(const std::string &root, const std::string &path) {
    return path.size() > root.size() && path.compare(0, root.size(), root) == 0 && path[root.size()] == '/';
}
std::string canonical(const std::string &path) {
    char *value = realpath(path.c_str(), nullptr);
    if (!value) io("resolve registered directory");
    std::string result;
    try { result = value; } catch (...) { free(value); throw; }
    free(value); return result;
}
/* Resolve relative components from the pinned directory without following
 * symlinks. An absent target is allowed during PREPARE; absent temp is not. */
int beneath(int root, const std::string &path, bool missing) {
    require(relative(path), "unsafe relative file path");
    FD current(fcntl(root, F_DUPFD_CLOEXEC, 0));
    if (current.value < 0) io("duplicate registered directory");
    size_t at = 0;
    for (;;) {
        auto slash = path.find('/', at);
        bool last = slash == std::string::npos;
        auto part = path.substr(at, last ? std::string::npos : slash - at);
        int fd = openat(current.value, part.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK
                       | (last ? 0 : O_DIRECTORY));
        if (fd < 0) {
            if (errno == ENOENT && missing) return -1;
            if (errno == ENOENT || errno == ELOOP || errno == ENOTDIR || errno == EACCES)
                throw IndexError(NGX_PURGE_STORE_INVALID, "file path is missing, inaccessible or contains a symlink");
            io("open registered file path");
        }
        if (last) return fd;
        close(current.value); current.value = fd; at = slash + 1;
    }
}
struct Root {
    RootSpec configured;
    std::string path;
    FD fd;
    explicit Root(const RootSpec &spec) : configured(spec), path(canonical(spec.path)),
        fd(open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)) {
        require(path != "/", "filesystem root cannot be a cache/temp directory");
        if (fd.value < 0) io("open registered directory");
    }
    void verify() const {
        FD named(open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
        require(named.value >= 0, "registered directory disappeared or became inaccessible");
        struct stat pinned{}, current{};
        if (fstat(fd.value, &pinned) || fstat(named.value, &current)) io("inspect registered directory");
        require(pinned.st_dev == current.st_dev && pinned.st_ino == current.st_ino,
                "registered directory was replaced; restart required");
    }
};
}
struct Publications::Impl {
    std::vector<std::unique_ptr<Root>> roots;
    std::string database;
};
Publications::Publications(const std::vector<RootSpec> &roots, const std::string &database) : impl_(std::make_unique<Impl>()) {
    impl_->database = canonical(database);
    for (const auto &root : roots) {
        require(root.path.size() > 1 && root.path.front() == '/' && relative(root.path.substr(1)), "unsafe registered root path");
        auto value = std::make_unique<Root>(root);
        if (root.kind == NGX_PURGE_ROOT_CACHE)
            require(value->path != impl_->database && !contains(value->path, impl_->database)
                    && !contains(impl_->database, value->path), "database and cache roots overlap");
        impl_->roots.push_back(std::move(value));
    }
}
Publications::~Publications() = default;
Match Publications::prepare(Index &index, Fill fill, const std::string &temporary,
                            int anchor, int lease, int temporary_fd, uint32_t peer_uid) {
    auto &file = fill.file;
    Root *target = nullptr;
    for (const auto &root : impl_->roots) {
        if (root->configured.kind == NGX_PURGE_ROOT_CACHE && root->configured.zone == file.zone
            && (root->configured.path == file.root || root->path == file.root)) { target = root.get(); break; }
    }
    require(target, "unregistered cache root/zone");
    target->verify();
    require(relative(file.path), "unsafe target path");
    file.root = target->path;
    std::string address = file.root + '/' + file.path;
    require(temporary != address, "temporary file cannot be the final cache path");
    struct stat received{}, named{}, existing{};
    int flags = fcntl(temporary_fd, F_GETFL);
    require(flags >= 0 && (flags & O_ACCMODE) == O_RDWR, "temporary descriptor is not read/write");
    if (fstat(temporary_fd, &received)) io("inspect temporary descriptor");
    require(S_ISREG(received.st_mode) && received.st_nlink == 1 && received.st_uid == peer_uid,
            "temporary file type, links or owner mismatch");
    Root *temp_root = nullptr; std::string relative_name; size_t longest = 0;
    for (const auto &root : impl_->roots) {
        for (const auto *prefix : {&root->configured.path, &root->path}) {
            if (prefix->size() > longest && contains(*prefix, temporary)) {
                longest = prefix->size(); temp_root = root.get(); relative_name = temporary.substr(longest + 1);
            }
        }
    }
    require(temp_root, "temporary file is outside registered directories");
    temp_root->verify();
    auto temp_address = temp_root->path + '/' + relative_name;
    require(!contains(impl_->database, temp_address) && temp_address != impl_->database,
            "temporary descriptor cannot address database storage");
    FD file_fd(beneath(temp_root->fd.value, relative_name, false));
    if (fstat(file_fd.value, &named)) io("inspect named temporary file");
    require(named.st_dev == received.st_dev && named.st_ino == received.st_ino,
            "temporary path and descriptor name different instances");
    FD current(beneath(target->fd.value, file.path, true));
    if (current.value >= 0) {
        if (fstat(current.value, &existing)) io("inspect current cache file");
        require(S_ISREG(existing.st_mode) && (existing.st_dev != received.st_dev || existing.st_ino != received.st_ino),
                "temporary file aliases the current cache instance");
    }
    unsigned char digest[16]; uint64_t offset;
    ngx_cache_purge_store_error_t error;
    checked(ngx_cache_purge_file_address({reinterpret_cast<const unsigned char *>(address.data()), address.size()},
                                        digest, &offset, &error), error);
    checked(ngx_cache_purge_file_lease_adopt(anchor, lease, offset, &error), error);
    ngx_cache_purge_file_marker_t marker;
    int rc = ngx_cache_purge_file_marker_read(temporary_fd, &marker, &error);
    if (rc != NGX_PURGE_STORE_NOT_FOUND) {
        checked(rc, error);
        require(marker.state == NGX_CACHE_PURGE_FILE_PREPARED
                && !std::memcmp(marker.id, file.id.data(), 16) && !std::memcmp(marker.address, digest, 16),
                "temporary file already belongs to another publication");
    }
    // Bind retries to the observed physical temporary file, not just its path.
    // xattr changes ctime, so use size/mtime with device/inode instead.
    std::string proof = "NFP1";
    auto number = [&](uint64_t value, unsigned width) {
        for (unsigned n = width; n; --n) proof += static_cast<char>(value >> ((n - 1) * 8));
    };
    number(received.st_dev, 8); number(received.st_ino, 8); number(received.st_size, 8);
    number(received.st_mtim.tv_sec, 8); number(received.st_mtim.tv_nsec, 4);
    number(temp_address.size(), 4); proof += temp_address;
    number(file.metadata.size(), 4); proof += file.metadata;
    file.metadata = std::move(proof);
    // Durable intent must precede the marker. A lost reply or marker-sync
    // failure leaves a recoverable PREPARED record, never permission to rename.
    auto match = index.prepare(file, fill.previous);
    require(match.state == FileState::prepared, "file publication already advanced");
    checked(ngx_cache_purge_file_marker_prepare(temporary_fd, file.id.data(), digest, 1, &error), error);
    struct stat after{};
    if (fstat(temporary_fd, &after)) io("recheck prepared temporary file");
    if (after.st_nlink != 1 || after.st_size != received.st_size
        || after.st_mtim.tv_sec != received.st_mtim.tv_sec || after.st_mtim.tv_nsec != received.st_mtim.tv_nsec)
        throw IndexError(NGX_PURGE_STORE_CONFLICT, "temporary file changed during durable preparation");
    return match;
}
}
