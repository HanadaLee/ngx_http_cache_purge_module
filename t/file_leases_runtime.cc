/* Kernel-backed leases across threads, descriptor handoff, and process death. */
#include "ngx_cache_purge_files.h"
#include <atomic>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static unsigned assertions;
static ngx_cache_purge_store_error_t error;
static void check(bool value, const char *message) {
    ++assertions;
    if (!value) throw std::runtime_error(std::string(message) + ": " + error.message);
}
struct FD {
    int value = -1;
    ~FD() { if (value >= 0) close(value); }
    FD() = default;
    explicit FD(int fd) : value(fd) {}
    FD(const FD &) = delete;
    FD &operator=(const FD &) = delete;
};
struct Child {
    pid_t pid;
    ~Child() {
        if (pid > 0) { kill(pid, SIGKILL); while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {} }
    }
    void stop() {
        check(kill(pid, SIGKILL) == 0, "kill lease holder");
        int status;
        check(waitpid(pid, &status, 0) == pid && WIFSIGNALED(status), "reap lease holder");
        pid = -1;
    }
};
static void release(FD &fd) {
    check(ngx_cache_purge_file_lease_release(&fd.value, &error) == 0 && fd.value == -1,
          "release one owned reference");
}
static int send_fd(int channel, int fd) {
    char byte = 'L';
    iovec iov{&byte, 1};
    alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int))]{};
    msghdr message{};
    message.msg_iov = &iov; message.msg_iovlen = 1;
    message.msg_control = control; message.msg_controllen = sizeof(control);
    auto header = CMSG_FIRSTHDR(&message);
    header->cmsg_level = SOL_SOCKET; header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(sizeof(int));
    std::memcpy(CMSG_DATA(header), &fd, sizeof(fd));
    return sendmsg(channel, &message, MSG_NOSIGNAL) == 1 ? 0 : -1;
}
static int receive_fd(int channel) {
    char byte;
    iovec iov{&byte, 1};
    alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int))]{};
    msghdr message{};
    message.msg_iov = &iov; message.msg_iovlen = 1;
    message.msg_control = control; message.msg_controllen = sizeof(control);
    check(recvmsg(channel, &message, MSG_CMSG_CLOEXEC) == 1 && byte == 'L'
          && !(message.msg_flags & MSG_CTRUNC), "receive complete descriptor message");
    auto header = CMSG_FIRSTHDR(&message);
    check(header && header->cmsg_level == SOL_SOCKET && header->cmsg_type == SCM_RIGHTS
          && header->cmsg_len == CMSG_LEN(sizeof(int)) && !CMSG_NXTHDR(&message, header),
          "receive exactly one lease descriptor");
    int fd;
    std::memcpy(&fd, CMSG_DATA(header), sizeof(fd));
    return fd;
}
static size_t descriptor_count() {
    return std::distance(std::filesystem::directory_iterator("/proc/self/fd"),
                         std::filesystem::directory_iterator());
}

int main(int argc, char **argv) {
    if (argc == 3 && std::string(argv[1]) == "--closed") {
        errno = 0;
        return fcntl(std::stoi(argv[2]), F_GETFD) < 0 && errno == EBADF ? 0 : 2;
    }
    try {
        char temporary[] = "/tmp/cache-purge-leases-XXXXXX";
        check(mkdtemp(temporary), "temporary lease directory");
        std::string root(temporary), path = root + "/locks";
        FD anchor;
        check(ngx_cache_purge_file_locks_open(path.c_str(), getuid(), 1, &anchor.value, &error) == 0,
              "create persistent anchor");
        FD first, second, distinct;
        check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &first.value, &error) == 0,
              "acquire first lease");
        check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &second.value, &error)
              == NGX_PURGE_STORE_BUSY && second.value == -1, "same-PID acquisition must conflict");
        check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 43, &distinct.value, &error) == 0,
              "unrelated path remains available");
        release(distinct);
        FD shared(fcntl(first.value, F_DUPFD_CLOEXEC, 0));
        check(shared.value >= 0, "duplicate an existing lease reference");
        release(first);
        check(ngx_cache_purge_file_lease_adopt(anchor.value, shared.value, 42, &error) == 0,
              "executor in same PID adopts the same description");
        FD unowned;
        check(ngx_cache_purge_file_locks_open(path.c_str(), getuid(), 0, &unowned.value, &error) == 0,
              "independent unowned descriptor");
        check(ngx_cache_purge_file_lease_adopt(anchor.value, unowned.value, 42, &error) == NGX_PURGE_STORE_BUSY,
              "same inode alone does not grant a held lease");
        release(unowned);
        check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &second.value, &error)
              == NGX_PURGE_STORE_BUSY, "closing unrelated descriptors cannot drop a held lease");
        release(shared);
        check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &second.value, &error) == 0,
              "last reference releases the lease");
        release(second);

        FD other;
        auto other_path = root + "/other";
        check(ngx_cache_purge_file_locks_open(other_path.c_str(), getuid(), 1, &other.value, &error) == 0,
              "second lock file");
        check(ngx_cache_purge_file_lease_adopt(anchor.value, other.value, 42, &error) == NGX_PURGE_STORE_OWNER,
              "reject a foreign lock inode");
        check(ngx_cache_purge_file_lease_acquire(other_path.c_str(), anchor.value, 42, &second.value, &error)
              == NGX_PURGE_STORE_OWNER && second.value == -1, "acquisition checks the pinned anchor inode");
        check(fchmod(other.value, 0644) == 0, "unsafe mode fixture");
        check(ngx_cache_purge_file_lease_adopt(anchor.value, other.value, 42, &error) == NGX_PURGE_STORE_INVALID,
              "reject permissive lease file");
        check(fchmod(other.value, 0600) == 0, "restore mode fixture");
        FD readonly(open(path.c_str(), O_RDONLY | O_CLOEXEC));
        check(ngx_cache_purge_file_lease_adopt(anchor.value, readonly.value, 42, &error) == NGX_PURGE_STORE_INVALID,
              "reject read-only descriptors");
        check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, UINT64_MAX, &second.value, &error)
              == NGX_PURGE_STORE_INVALID && second.value == -1, "reject out-of-range offsets");

        // Fork only before creating any test threads. Queued SCM_RIGHTS
        // references survive the sending worker until received or discarded.
        for (bool transfer : {false, true}) {
            int sockets[2], pipefd[2];
            check(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) == 0, "lease handoff socket");
            FD receiver(sockets[0]), sender(sockets[1]);
            check(pipe2(pipefd, O_CLOEXEC) == 0, "handoff barrier");
            FD ready_read(pipefd[0]), ready_write(pipefd[1]);
            pid_t pid = fork(); check(pid >= 0, "fork lease holder");
            if (!pid) {
                close(receiver.value); close(ready_read.value);
                int fd;
                if (ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &fd, &error)) _exit(2);
                if (transfer && send_fd(sender.value, fd)) _exit(3);
                char ready = 'R';
                if (write(ready_write.value, &ready, 1) != 1) _exit(4);
                for (;;) pause();
            }
            Child child{pid};
            release(sender); release(ready_write);
            char ready;
            check(read(ready_read.value, &ready, 1) == 1 && ready == 'R', "holder owns lease before observation");
            check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &first.value, &error)
                  == NGX_PURGE_STORE_BUSY, "live process holds path exclusively");
            child.stop();
            if (transfer) {
                check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &first.value, &error)
                      == NGX_PURGE_STORE_BUSY, "queued descriptor survives sender SIGKILL");
                FD received(receive_fd(receiver.value));
                check((fcntl(received.value, F_GETFD) & FD_CLOEXEC) != 0, "received lease is close-on-exec");
                check(ngx_cache_purge_file_lease_adopt(anchor.value, received.value, 42, &error) == 0,
                      "receiver safely adopts the transferred lease");
                check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &first.value, &error)
                      == NGX_PURGE_STORE_BUSY, "executor retains exclusive access after sender death");
                release(received);
            }
            check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &first.value, &error) == 0,
                  "recovery acquires only after all descriptions are gone");
            release(first);
        }

        check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &first.value, &error) == 0,
              "lease for exec inheritance check");
        auto fd_argument = std::to_string(first.value);
        pid_t pid = fork(); check(pid >= 0, "fork exec verifier");
        if (!pid) {
            execl("/proc/self/exe", "file-leases", "--closed", fd_argument.c_str(), nullptr);
            _exit(127);
        }
        Child child{pid}; int status;
        check(waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0,
              "lease does not leak through exec");
        child.pid = -1;
        release(first);

        auto before = descriptor_count();
        for (unsigned i = 0; i < 128; ++i) {
            check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &first.value, &error) == 0,
                  "repeat acquisition");
            int sockets[2];
            check(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) == 0, "discarded handoff socket");
            FD receiver(sockets[0]), sender(sockets[1]);
            check(send_fd(sender.value, first.value) == 0, "queue lease reference");
            release(first);
            release(receiver); // Kernel discards the unreceived SCM_RIGHTS reference.
            check(ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &second.value, &error) == 0,
                  "closed receiver releases queued lease");
            release(second);
        }
        check(descriptor_count() == before, "handoff/rejection loops leak no descriptors");

        std::atomic<unsigned> ready{0}, completed{0}, winners{0}, busy{0}, failures{0};
        std::atomic<bool> start{false}, finish{false};
        std::vector<std::thread> threads;
        for (unsigned i = 0; i < 16; ++i) {
            threads.emplace_back([&] {
                ++ready;
                while (!start.load()) std::this_thread::yield();
                int fd = -1; ngx_cache_purge_store_error_t local;
                int rc = ngx_cache_purge_file_lease_acquire(path.c_str(), anchor.value, 42, &fd, &local);
                if (!rc) ++winners;
                else if (rc == NGX_PURGE_STORE_BUSY) ++busy;
                else ++failures;
                ++completed;
                while (!finish.load()) std::this_thread::yield();
                if (fd >= 0 && ngx_cache_purge_file_lease_release(&fd, &local)) ++failures;
            });
        }
        while (ready != 16) std::this_thread::yield();
        start = true;
        while (completed != 16) std::this_thread::yield();
        finish = true;
        for (auto &thread : threads) thread.join();
        check(winners == 1 && busy == 15 && failures == 0, "exactly one same-PID thread can acquire a path");
        check(descriptor_count() == before, "thread competitors leak no descriptors");
        std::cout << "PASS " << assertions << " OFD lease assertions; directory " << root << std::endl;
    } catch (const std::exception &failure) {
        std::cerr << "FAIL: " << failure.what() << std::endl;
        return 1;
    }
}
