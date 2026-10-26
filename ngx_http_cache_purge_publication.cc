/* Copyright (C) Hanada */
#include "ngx_http_cache_purge_backend.hpp"
#include "ngx_http_cache_purge_internal.h"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ngx_http_cache_purge {
namespace {
struct FD {
    int value;
    explicit FD(int fd = -1) : value(fd) {}
    ~FD() { if (value >= 0) close(value); }
    FD(const FD &) = delete;
    FD &operator=(const FD &) = delete;
};
void require(bool valid, const char *message) {
    if (!valid) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, message);
}
void checked(int rc, const ngx_http_cache_rocksdb_error_t &error) {
    if (rc) throw IndexError(rc, error.message);
}
[[noreturn]] void io(const char *message) {
    int saved = errno;
    throw IndexError(saved == EMFILE || saved == ENFILE || saved == ENOMEM ? NGX_HTTP_CACHE_ROCKSDB_LIMIT : NGX_HTTP_CACHE_ROCKSDB_IO,
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
int beneath(int root, const std::string &path, bool missing, bool writable = false) {
    require(relative(path), "unsafe relative file path");
    FD current(fcntl(root, F_DUPFD_CLOEXEC, 0));
    if (current.value < 0) io("duplicate registered directory");
    size_t at = 0;
    for (;;) {
        auto slash = path.find('/', at);
        bool last = slash == std::string::npos;
        auto part = path.substr(at, last ? std::string::npos : slash - at);
        int fd = openat(current.value, part.c_str(), (last && writable ? O_RDWR : O_RDONLY) | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK
                       | (last ? 0 : O_DIRECTORY));
        if (fd < 0) {
            if (errno == ENOENT && missing) return -1;
            if (errno == ENOENT || errno == ELOOP || errno == ENOTDIR || errno == EACCES)
                throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "file path is missing, inaccessible or contains a symlink");
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
struct Proof {
    uint64_t device, inode, size, seconds;
    uint32_t nanoseconds;
    std::string temporary;
};
Proof proof(const std::string &metadata) {
    std::string_view input(metadata); size_t at = 4;
    require(input.size() >= 4 && input.substr(0, 4) == "NFP1", "file has no publication proof");
    auto number = [&](unsigned width) {
        require(width <= input.size() - at, "truncated publication proof");
        uint64_t value = 0;
        while (width--) value = (value << 8) | static_cast<unsigned char>(input[at++]);
        return value;
    };
    Proof result{number(8), number(8), number(8), number(8), static_cast<uint32_t>(number(4)), ""};
    auto length = number(4); require(length <= input.size() - at, "truncated publication temporary path");
    result.temporary = std::string(input.substr(at, length)); at += length;
    length = number(4); require(length == input.size() - at, "invalid publication policy payload");
    require(result.nanoseconds < 1000000000 && !result.temporary.empty() && result.temporary.front() == '/',
            "invalid publication identity");
    return result;
}
bool same(const Proof &proof, const struct stat &st) {
    return proof.device == static_cast<uint64_t>(st.st_dev) && proof.inode == static_cast<uint64_t>(st.st_ino)
        && proof.size == static_cast<uint64_t>(st.st_size) && proof.seconds == static_cast<uint64_t>(st.st_mtim.tv_sec)
        && proof.nanoseconds == static_cast<uint32_t>(st.st_mtim.tv_nsec);
}
void sync_parents(Root &root, std::string relative_name) {
    // Constant FD space; native hashed cache paths have only a few components.
    // Sync bottom-up so newly created path entries precede their parents.
    for (;;) {
        auto slash = relative_name.rfind('/');
        if (slash == std::string::npos) {
            if (fsync(root.fd.value)) io("sync registered cache root");
            return;
        }
        relative_name.resize(slash);
        FD directory(beneath(root.fd.value, relative_name, true));
        if (directory.value >= 0 && fsync(directory.value)) io("sync publication directory");
    }
}
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
        if (root.kind == NGX_HTTP_CACHE_PURGE_ROOT_CACHE)
            require(value->path != impl_->database && !contains(value->path, impl_->database)
                    && !contains(impl_->database, value->path), "database and cache roots overlap");
        impl_->roots.push_back(std::move(value));
    }
}
Publications::~Publications() = default;
PublicationTarget Publications::target(Index &index, const std::string &root,
                                       const std::string &zone, const std::string &path) {
    require(relative(path), "unsafe target path");
    for (const auto &entry : impl_->roots) {
        if (entry->configured.kind != NGX_HTTP_CACHE_PURGE_ROOT_CACHE || entry->configured.zone != zone
            || (entry->configured.path != root && entry->path != root)) continue;
        entry->verify();
        PublicationTarget result{entry->path, entry->path + '/' + path, {}};
        auto current = index.current(result.root, path);
        if (current) result.previous = current->id;
        return result;
    }
    throw IndexError(NGX_HTTP_CACHE_ROCKSDB_INVALID, "unregistered cache root/zone");
}
Match Publications::prepare(Index &index, Fill fill, const std::string &temporary,
                            int anchor, int lease, int temporary_fd, uint32_t peer_uid) {
    auto &file = fill.file;
    Root *target = nullptr;
    for (const auto &root : impl_->roots) {
        if (root->configured.kind == NGX_HTTP_CACHE_PURGE_ROOT_CACHE && root->configured.zone == file.zone
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
    ngx_http_cache_rocksdb_error_t error;
    checked(ngx_http_cache_purge_file_address({reinterpret_cast<const unsigned char *>(address.data()), address.size()},
                                        digest, &offset, &error), error);
    checked(ngx_http_cache_purge_file_lease_adopt(anchor, lease, offset, &error), error);
    ngx_http_cache_purge_file_marker_t marker;
    int rc = ngx_http_cache_purge_file_marker_read(temporary_fd, &marker, &error);
    if (rc != NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        checked(rc, error);
        require(marker.state == NGX_HTTP_CACHE_PURGE_FILE_PREPARED
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
    checked(ngx_http_cache_purge_file_marker_prepare(temporary_fd, file.id.data(), digest, 1, &error), error);
    struct stat after{};
    if (fstat(temporary_fd, &after)) io("recheck prepared temporary file");
    if (after.st_nlink != 1 || after.st_size != received.st_size
        || after.st_mtim.tv_sec != received.st_mtim.tv_sec || after.st_mtim.tv_nsec != received.st_mtim.tv_nsec)
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "temporary file changed during durable preparation");
    return match;
}

Match Publications::finish(Index &index, const FileId &id, uint64_t revision,
                           int anchor, int lease, uint32_t peer_uid) {
    require(revision != 0, "missing prepared revision");
    auto file = index.get(id);
    auto saved = proof(file.metadata);
    if (file.revision < revision) throw IndexError(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "publication revision moved backwards");
    if (!index.has_fill_intent(id)) return {id, file.revision, file.state};
    if (file.revision != revision && !(revision != UINT64_MAX && file.revision == revision + 1
                                      && file.state == FileState::committed))
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "publication revision changed");
    Root *target = nullptr, *source = nullptr; size_t longest = 0;
    for (const auto &root : impl_->roots) {
        if (root->configured.kind == NGX_HTTP_CACHE_PURGE_ROOT_CACHE && root->path == file.root
            && root->configured.zone == file.zone) target = root.get();
        if (root->path.size() > longest && contains(root->path, saved.temporary)) {
            source = root.get(); longest = root->path.size();
        }
    }
    require(target && source, "publication directories are no longer registered");
    target->verify(); source->verify();
    std::string address = target->path + '/' + file.path;
    unsigned char digest[16]; uint64_t offset; ngx_http_cache_rocksdb_error_t error;
    checked(ngx_http_cache_purge_file_address({reinterpret_cast<const unsigned char *>(address.data()), address.size()},
                                        digest, &offset, &error), error);
    checked(ngx_http_cache_purge_file_lease_adopt(anchor, lease, offset, &error), error);
    auto abandon = [&]() {
        return file.state == FileState::prepared ? index.abort_prepare(id, file.revision)
                                                : index.discard_publication(id, file.revision);
    };
    FD observed(beneath(target->fd.value, file.path, true));
    if (observed.value < 0) return abandon();
    struct stat st{};
    if (fstat(observed.value, &st)) io("inspect publication result");
    if (!S_ISREG(st.st_mode)) return abandon();
    if (static_cast<uint64_t>(st.st_dev) != saved.device)
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "cross-filesystem copy needs verified publication evidence");
    ngx_http_cache_purge_file_marker_t marker;
    int rc = ngx_http_cache_purge_file_marker_read(observed.value, &marker, &error);
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) return abandon();
    if (rc == NGX_HTTP_CACHE_ROCKSDB_CORRUPT)
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "publication marker is malformed");
    checked(rc, error);
    if (std::memcmp(marker.id, id.data(), 16) || std::memcmp(marker.address, digest, 16)) return abandon();
    if (!same(saved, st) || st.st_nlink != 1 || st.st_uid != peer_uid)
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "publication file differs from its prepared identity");
    FD fd(beneath(target->fd.value, file.path, false, true));
    struct stat opened{};
    if (fstat(fd.value, &opened)) io("inspect writable publication result");
    if (!same(saved, opened) || opened.st_nlink != 1)
        throw IndexError(NGX_HTTP_CACHE_ROCKSDB_CONFLICT, "publication file changed while opening");
    if (fsync(fd.value)) io("sync final cache file");
    sync_parents(*target, file.path);
    sync_parents(*source, saved.temporary.substr(source->path.size() + 1));
    Match result{id, file.revision, file.state};
    if (file.state == FileState::prepared) result = index.commit_pending(id, file.revision);
    checked(ngx_http_cache_purge_file_marker_publish(fd.value, id.data(), digest, 1, &error), error);
    FD final(beneath(target->fd.value, file.path, true));
    if (final.value < 0) return index.discard_publication(id, result.revision);
    struct stat final_stat{};
    if (fstat(final.value, &final_stat)) io("recheck published file");
    if (!same(saved, final_stat) || final_stat.st_nlink != 1)
        return index.discard_publication(id, result.revision);
    return index.publish(id, result.revision);
}
}
