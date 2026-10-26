#include "ngx_cache_purge_component.h"
#include "ngx_cache_purge_control.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <cstring>

static int visit(void *data, const ngx_cache_purge_control_match_t *match) {
    auto *result = static_cast<uint64_t *>(data); ++result[0]; result[1] |= match->flags; return 0;
}

int main(int argc, char **argv) {
    if (argc != 6 && argc != 7) return 2;
    ngx_cache_purge_component_options_t options;
    ngx_cache_purge_component_options(&options);
    options.max_connections = std::strtoul(argv[2], nullptr, 10);
    options.io_timeout_ms = std::strtoul(argv[3], nullptr, 10);
    options.buffer_bytes = std::strtoul(argv[4], nullptr, 10);
    options.max_payload_bytes = std::strtoul(argv[5], nullptr, 10);
    ngx_cache_purge_root_t roots[2]{};
    const char *cache = std::getenv("CACHE_PURGE_TEST_CACHE_ROOT");
    const char *temporary = std::getenv("CACHE_PURGE_TEST_TEMP_ROOT");
    if (cache && temporary) {
        roots[0] = {{reinterpret_cast<const unsigned char *>(cache), std::strlen(cache)},
                    {reinterpret_cast<const unsigned char *>("zone-a"), 6}, NGX_PURGE_ROOT_CACHE};
        roots[1] = {{reinterpret_cast<const unsigned char *>(temporary), std::strlen(temporary)},
                    {nullptr, 0}, NGX_PURGE_ROOT_TEMP};
        options.roots = roots; options.root_count = 2;
    }
    ngx_cache_purge_component_t *component = nullptr;
    ngx_cache_purge_store_error_t error;
    void *shared = nullptr; size_t shared_size = 0;
    ngx_cache_purge_control_reader_t *reader = nullptr;
    bool guarded = false; uint64_t version = 0;
    if (argc == 7) {
        shared_size = ngx_cache_purge_control_size(1024 * 1024, 32);
        int fd = open(argv[6], O_RDWR | O_CREAT | O_EXCL, 0600); bool created = fd >= 0;
        if (fd < 0) fd = open(argv[6], O_RDWR);
        if (fd < 0 || (created && ftruncate(fd, shared_size) < 0)) return 4;
        struct stat st; if (fstat(fd, &st) || static_cast<size_t>(st.st_size) != shared_size) return 4;
        shared = mmap(nullptr, shared_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0); close(fd);
        if (shared == MAP_FAILED) return 4;
        if (created && ngx_cache_purge_control_init(shared, shared_size, 1024 * 1024, 32, &error)) return 4;
        options.control_memory = shared; options.control_bytes = shared_size;
    }
    int rc = ngx_cache_purge_component_start(argv[1], &options, &component, &error);
    if (rc) { std::cerr << error.message << std::endl; return rc; }
    int foreign = 0;
    std::thread probe([&] {
        ngx_cache_purge_component_status_t status;
        ngx_cache_purge_store_error_t local;
        foreign = ngx_cache_purge_component_status(component, &status, &local);
    });
    probe.join();
    if (foreign != NGX_PURGE_STORE_OWNER) return 3;
    std::cout << "STARTED" << std::endl;
    char command;
    while (std::cin >> command) {
        if (command == 'q') break;
        if (command == 'm' || command == 'g' || command == 'r') {
            if (!reader) {
                rc = ngx_cache_purge_control_reader_open(shared, shared_size, &reader, &error);
                if (rc) { std::cout << "MATCH " << rc << " 0 0 0" << std::endl; continue; }
            }
            if (command == 'g') {
                rc = ngx_cache_purge_control_guard_begin(reader, version, &error);
                if (!rc) guarded = true;
                std::cout << "GUARD " << rc << std::endl; continue;
            }
            if (command == 'r') {
                rc = ngx_cache_purge_control_guard_end(reader, &error);
                if (!rc) guarded = false;
                std::cout << "RELEASE " << rc << std::endl; continue;
            }
            const unsigned char url[] = "https://a.test/dir/a";
            ngx_cache_purge_control_context_t context{};
            context.url = {url, sizeof(url) - 1}; context.now_ms = 5000; context.proxy = 1;
            uint64_t found[2]{};
            rc = ngx_cache_purge_control_match(reader, &context, 64, 65536, visit, found, &version, &error);
            std::cout << "MATCH " << rc << ' ' << found[0] << ' ' << found[1] << ' ' << version << std::endl; continue;
        }
        ngx_cache_purge_component_status_t status;
        rc = ngx_cache_purge_component_status(component, &status, &error);
        if (rc) return rc;
        std::cout << status.state << ' ' << status.error_code << ' ' << status.connections << ' '
                  << status.requests << ' ' << status.rejected << ' ' << status.buffered_bytes
                  << ' ' << status.peak_buffered_bytes << std::endl;
    }
    if (guarded) ngx_cache_purge_control_guard_end(reader, &error);
    if (reader) {
        do { rc = ngx_cache_purge_control_reader_close(&reader, &error); if (rc == NGX_PURGE_STORE_BUSY) std::this_thread::yield(); } while (rc == NGX_PURGE_STORE_BUSY);
        if (rc) return rc;
    }
    rc = ngx_cache_purge_component_stop(&component, &error);
    std::cout << "STOP " << rc << ' ' << (component == nullptr) << std::endl;
    if (rc) std::cerr << error.message << std::endl;
    if (shared) munmap(shared, shared_size);
    return rc;
}
