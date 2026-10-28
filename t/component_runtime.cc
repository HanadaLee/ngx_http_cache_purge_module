#include "ngx_http_cache_purge_internal.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <cstring>
#include <string>

static int
ngx_http_cache_purge_test_seed(const char *path)
{
    ngx_http_cache_rocksdb_options_t options;
    ngx_http_cache_rocksdb_options(&options);
    ngx_http_cache_rocksdb_t *store = nullptr;
    ngx_http_cache_rocksdb_error_t error{};
    int rc = ngx_http_cache_rocksdb_open(path, &options, &store, &error);
    if (rc != 0) {
        return rc;
    }
    rc = ngx_http_cache_purge_index_init(store, &error);
    auto bytes = [](const char *value) {
        return ngx_http_cache_rocksdb_bytes_t{
            reinterpret_cast<const unsigned char *>(value), std::strlen(value)
        };
    };
    ngx_http_cache_purge_index_file_t file{};
    file.id[15] = 1;
    file.state = NGX_HTTP_CACHE_PURGE_INDEX_PREPARED;
    file.key = bytes("object-key");
    file.url = bytes("https://object.test/item");
    file.host = bytes("object.test");
    file.tag_namespace = bytes("team");
    file.root = bytes("/unregistered-test-cache");
    file.zone = bytes("test");
    file.path = bytes("object");
    file.tags[0] = bytes("red");
    file.tag_count = 1;
    unsigned char main[16] = { 1 }, variant[16] = {};
    ngx_http_cache_purge_index_match_t prepared{};
    if (rc == 0) {
        rc = ngx_http_cache_purge_index_prepare_native(
            store, &file, nullptr, main, variant, &prepared, &error);
    }
    ngx_http_cache_purge_index_expected_t expected{};
    std::memcpy(expected.id, file.id, 16);
    expected.revision = 1;
    if (rc == 0) {
        rc = ngx_http_cache_purge_index_apply(store,
            NGX_HTTP_CACHE_PURGE_INDEX_COMMIT, &expected, 1, &prepared, &error);
    }
    for (unsigned i = 0; i < 2048 && rc == 0; i++) {
        auto url = "https://object-alias.test/" + std::to_string(i);
        rc = ngx_http_cache_purge_index_add_url(
            store, file.key, bytes(url.c_str()), &error);
    }
    int closed = ngx_http_cache_rocksdb_close(&store, nullptr);
    if (rc != 0) {
        std::cerr << error.message << std::endl;
    }
    return rc != 0 ? rc : closed;
}

static int
visit(void *data, const ngx_http_cache_purge_control_match_t *match)
{
    auto *result = static_cast<uint64_t *>(data);
    ++result[0];
    result[1] |= match->flags;
    return 0;
}

int
main(int argc, char **argv)
{
    if (argc != 6 && argc != 7) {
        return 2;
    }
    ngx_http_cache_purge_component_options_t options;
    ngx_http_cache_purge_component_options(&options);
    options.max_connections = std::strtoul(argv[2], nullptr, 10);
    options.io_timeout_ms = std::strtoul(argv[3], nullptr, 10);
    options.buffer_bytes = std::strtoul(argv[4], nullptr, 10);
    options.max_payload_bytes = std::strtoul(argv[5], nullptr, 10);
    ngx_http_cache_purge_root_t roots[3]{};
    const char *cache = std::getenv("CACHE_PURGE_TEST_CACHE_ROOT");
    const char *temporary = std::getenv("CACHE_PURGE_TEST_TEMP_ROOT");
    const char *zone = std::getenv("CACHE_PURGE_TEST_CACHE_ZONE");
    const char *second = std::getenv("CACHE_PURGE_TEST_SECOND_ROOT");
    if (!zone) {
        zone = "zone-a";
    }
    if (cache && temporary) {
        roots[0] = { { reinterpret_cast<const unsigned char *>(cache),
                         std::strlen(cache) },
            { reinterpret_cast<const unsigned char *>(zone),
                std::strlen(zone) },
            NGX_HTTP_CACHE_PURGE_ROOT_CACHE };
        roots[1] = { { reinterpret_cast<const unsigned char *>(temporary),
                         std::strlen(temporary) },
            { nullptr, 0 }, NGX_HTTP_CACHE_PURGE_ROOT_TEMP };
        options.roots = roots;
        options.root_count = 2;
        if (second) {
            static const unsigned char second_zone[] = "cache-b";
            roots[2] = { { reinterpret_cast<const unsigned char *>(second),
                             std::strlen(second) },
                { second_zone, sizeof(second_zone) - 1 },
                NGX_HTTP_CACHE_PURGE_ROOT_CACHE };
            options.root_count = 3;
        }
        if (std::getenv("CACHE_PURGE_TEST_NATIVE_ROOT")) {
            roots[0].kind = NGX_HTTP_CACHE_PURGE_ROOT_NATIVE;
            if (second) {
                roots[2].kind = NGX_HTTP_CACHE_PURGE_ROOT_NATIVE;
            }
        }
    }
    ngx_http_cache_purge_component_t *component = nullptr;
    ngx_http_cache_rocksdb_error_t error;
    void *shared = nullptr;
    size_t shared_size = 0;
    ngx_http_cache_purge_control_reader_t *reader = nullptr;
    bool guarded = false, origin = false;
    uint64_t version = 0;
    ngx_http_cache_purge_cleanup_t cleanup{};
    if (argc == 7) {
        shared_size = ngx_http_cache_purge_control_size(1024 * 1024, 32);
        int fd = open(argv[6], O_RDWR | O_CREAT | O_EXCL, 0600);
        bool created = fd >= 0;
        if (fd < 0) {
            fd = open(argv[6], O_RDWR);
        }
        if (fd < 0 || (created && ftruncate(fd, shared_size) < 0)) {
            return 4;
        }
        struct stat st;
        if (fstat(fd, &st) || static_cast<size_t>(st.st_size) != shared_size) {
            return 4;
        }
        shared = mmap(
            nullptr, shared_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
        if (shared == MAP_FAILED) {
            return 4;
        }
        if (created
            && ngx_http_cache_purge_control_init(
                shared, shared_size, 1024 * 1024, 32, &error))
        {
            return 4;
        }
        options.control_memory = shared;
        options.control_bytes = shared_size;
        if (std::getenv("CACHE_PURGE_TEST_HOLD_ORIGIN")) {
            if (ngx_http_cache_purge_control_reader_open(
                    shared, shared_size, &reader, &error)
                || ngx_http_cache_purge_control_origins(
                    reader, nullptr, 1, &error))
            {
                return 4;
            }
            origin = true;
        }
    }
    if (std::getenv("CACHE_PURGE_TEST_OBJECT_SEED")) {
        int rc = ngx_http_cache_purge_test_seed(argv[1]);
        if (rc != 0) {
            return rc;
        }
    }
    int rc = ngx_http_cache_purge_component_start(
        argv[1], &options, &component, &error);
    if (rc) {
        std::cerr << error.message << std::endl;
        return rc;
    }
    int foreign = 0;
    std::thread probe([&] {
        ngx_http_cache_purge_component_status_t status;
        ngx_http_cache_rocksdb_error_t local;
        foreign =
            ngx_http_cache_purge_component_status(component, &status, &local);
    });
    probe.join();
    if (foreign != NGX_HTTP_CACHE_ROCKSDB_OWNER) {
        return 3;
    }
    std::cout << "STARTED" << std::endl;
    char command;
    while (std::cin >> command) {
        if (command == 'q') {
            break;
        }
        if (command == 'h') {
            rc = ngx_http_cache_purge_component_request_stop(component, &error);
            std::cout << "REQUEST_STOP " << rc << std::endl;
            continue;
        }
        if (command == 'n') {
            rc = ngx_http_cache_purge_component_cleanup_next(
                component, &cleanup, &error);
            std::cout << "NATIVE " << rc << ' ' << cleanup.token << ' '
                      << cleanup.inode << ' ' << cleanup.outcome << ' ';
            if (rc == 0) {
                std::cout.write(
                    reinterpret_cast<const char *>(cleanup.path.data),
                    cleanup.path.len);
            }
            std::cout << std::endl;
            continue;
        }
        if (command == 'y' || command == 'x' || command == 'z') {
            rc = ngx_http_cache_purge_component_cleanup_complete(component,
                cleanup.token + (command == 'z'),
                command == 'x' ? NGX_HTTP_CACHE_ROCKSDB_BUSY : 0, &error);
            std::cout << "ACK " << rc << std::endl;
            continue;
        }
        if (command == 'm' || command == 'g' || command == 'r' || command == 'w'
            || command == 'b' || command == 'e')
        {
            if (!reader) {
                rc = ngx_http_cache_purge_control_reader_open(
                    shared, shared_size, &reader, &error);
                if (rc) {
                    std::cout << "MATCH " << rc << " 0 0 0" << std::endl;
                    continue;
                }
            }
            if (command == 'g') {
                rc = ngx_http_cache_purge_control_guard_begin(
                    reader, version, &error);
                if (!rc) {
                    guarded = true;
                }
                std::cout << "GUARD " << rc << std::endl;
                continue;
            }
            if (command == 'b' || command == 'e') {
                ngx_http_cache_purge_watermark_t watermark{};
                uint64_t publication = 0;
                if (command == 'e') {
                    rc = ngx_http_cache_purge_control_origins(
                        reader, nullptr, 0, &error);
                    if (!rc) {
                        origin = false;
                    }
                } else {
                    rc = ngx_http_cache_purge_control_origins(
                        reader, nullptr, 1, &error);
                    if (!rc) {
                        origin = true;
                    }
                    if (!rc) {
                        rc = ngx_http_cache_purge_control_watermark(
                            reader, &watermark, &publication, &error);
                    }
                    if (!rc) {
                        rc = ngx_http_cache_purge_control_origins(
                            reader, &watermark, 1, &error);
                    }
                }
                std::cout << "ORIGIN " << rc << ' ' << watermark.sequence
                          << std::endl;
                continue;
            }
            if (command == 'w') {
                ngx_http_cache_purge_watermark_t watermark{};
                uint64_t publication = 0;
                rc = ngx_http_cache_purge_control_watermark(
                    reader, &watermark, &publication, &error);
                const char hex[] = "0123456789abcdef";
                std::string ledger;
                for (auto byte : watermark.ledger) {
                    ledger += hex[byte >> 4];
                    ledger += hex[byte & 15];
                }
                std::cout << "WATERMARK " << rc << ' ' << watermark.sequence
                          << ' ' << ledger << std::endl;
                continue;
            }
            if (command == 'r') {
                rc = ngx_http_cache_purge_control_guard_end(reader, &error);
                if (!rc) {
                    guarded = false;
                }
                std::cout << "RELEASE " << rc << std::endl;
                continue;
            }
            const char *url = std::getenv("CACHE_PURGE_TEST_MATCH_URL");
            if (!url) {
                url = "https://a.test/dir/a";
            }
            ngx_http_cache_purge_control_context_t context{};
            context.url = { reinterpret_cast<const unsigned char *>(url),
                std::strlen(url) };
            context.now_ms = 5000;
            context.proxy = 1;
            uint64_t found[2]{};
            rc = ngx_http_cache_purge_control_match(
                reader, &context, 64, 65536, visit, found, &version, &error);
            std::cout << "MATCH " << rc << ' ' << found[0] << ' ' << found[1]
                      << ' ' << version << std::endl;
            continue;
        }
        ngx_http_cache_purge_component_status_t status;
        rc = ngx_http_cache_purge_component_status(component, &status, &error);
        if (rc) {
            return rc;
        }
        std::cout << status.state << ' ' << status.error_code << ' '
                  << status.connections << ' ' << status.requests << ' '
                  << status.rejected << ' ' << status.buffered_bytes << ' '
                  << status.peak_buffered_bytes << std::endl;
    }
    if (guarded) {
        ngx_http_cache_purge_control_guard_end(reader, &error);
    }
    if (origin) {
        ngx_http_cache_purge_control_origins(reader, nullptr, 0, &error);
    }
    if (reader) {
        do {
            rc = ngx_http_cache_purge_control_reader_close(&reader, &error);
            if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
                std::this_thread::yield();
            }
        } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
        if (rc) {
            return rc;
        }
    }
    rc = ngx_http_cache_purge_component_stop(&component, &error);
    std::cout << "STOP " << rc << ' ' << (component == nullptr) << std::endl;
    if (rc) {
        std::cerr << error.message << std::endl;
    }
    if (shared) {
        munmap(shared, shared_size);
    }
    return rc;
}
