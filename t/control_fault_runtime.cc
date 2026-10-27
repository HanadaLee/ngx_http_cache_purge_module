#include "ngx_http_cache_purge_test.hpp"
#include <sys/mman.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <cstdio>

using namespace ngx_http_cache_purge;
extern "C" void ngx_http_cache_purge_control_test_fault(int point, int fd);
static void
check(bool value)
{
    if (!value) {
        _exit(1);
    }
}
static int
visitor(void *, const ngx_http_cache_purge_control_match_t *)
{
    return 0;
}
static void
killed(pid_t pid, int pipe)
{
    char c;
    check(read(pipe, &c, 1) == 1);
    check(kill(pid, SIGKILL) == 0);
    int status;
    check(waitpid(pid, &status, 0) == pid && WIFSIGNALED(status));
}

int
main()
{
    size_t size = ngx_http_cache_purge_control_size(65536, 8);
    void *memory = mmap(nullptr, size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    check(memory != MAP_FAILED);
    ngx_http_cache_rocksdb_error_t error;
    check(
        ngx_http_cache_purge_control_init(memory, size, 65536, 8, &error) == 0);
    ngx_http_cache_purge_control_writer_t *writer = nullptr;
    check(
        ngx_http_cache_purge_control_writer_open(memory, size, &writer, &error)
        == 0);
    publish_controls(writer, {});
    check(ngx_http_cache_purge_control_gate_open(writer, &error) == 0);
    ngx_http_cache_purge_control_reader_t *reader = nullptr,
                                          *recovery_reader = nullptr;
    check(
        ngx_http_cache_purge_control_reader_open(memory, size, &reader, &error)
        == 0);
    int ready[2];
    check(pipe(ready) == 0);
    pid_t child = fork();
    check(child >= 0);
    if (!child) {
        ngx_http_cache_purge_control_test_fault(1, ready[1]);
        ngx_http_cache_purge_control_reader_t *local = nullptr;
        ngx_http_cache_purge_control_reader_open(memory, size, &local, &error);
        _exit(2);
    }
    killed(child, ready[0]);
    check(ngx_http_cache_purge_control_reader_open(
              memory, size, &recovery_reader, &error)
          == 0);
    ngx_http_cache_purge_control_context_t context{};
    uint64_t version;
    check(ngx_http_cache_purge_control_match(
              reader, &context, 10, 4096, visitor, nullptr, &version, &error)
          == NGX_HTTP_CACHE_ROCKSDB_BUSY);
    publish_controls(writer, {});
    check(ngx_http_cache_purge_control_gate_open(writer, &error) == 0);
    check(ngx_http_cache_purge_control_writer_close(&writer, &error) == 0);
    child = fork();
    check(child >= 0);
    if (!child) {
        ngx_http_cache_purge_control_writer_t *local = nullptr;
        check(ngx_http_cache_purge_control_writer_open(
                  memory, size, &local, &error)
              == 0);
        Operation op;
        op.input.id = "partial";
        op.input.issued_at_ms = 1;
        op.input.selector = { SelectorKind::key_prefix, std::string(30000, 'x'),
            "" };
        op.sequence = 1;
        ngx_http_cache_purge_control_test_fault(2, ready[1]);
        publish_controls(local, { op });
        _exit(2);
    }
    killed(child, ready[0]);
    check(
        ngx_http_cache_purge_control_writer_open(memory, size, &writer, &error)
        == 0);
    check(ngx_http_cache_purge_control_gate_open(writer, &error)
          == NGX_HTTP_CACHE_ROCKSDB_BUSY);
    check(ngx_http_cache_purge_control_match(
              reader, &context, 10, 4096, visitor, nullptr, &version, &error)
          == NGX_HTTP_CACHE_ROCKSDB_BUSY);
    publish_controls(writer, {});
    check(ngx_http_cache_purge_control_gate_open(writer, &error) == 0);
    check(ngx_http_cache_purge_control_match(
              reader, &context, 10, 4096, visitor, nullptr, &version, &error)
          == 0);
    check(ngx_http_cache_purge_control_reader_close(&reader, &error) == 0);
    check(ngx_http_cache_purge_control_reader_close(&recovery_reader, &error)
          == 0);
    check(ngx_http_cache_purge_control_writer_close(&writer, &error) == 0);
    close(ready[0]);
    close(ready[1]);
    munmap(memory, size);
    std::puts(
        "PASS robust mutex owner death and partial image publication recovery");
    return 0;
}
