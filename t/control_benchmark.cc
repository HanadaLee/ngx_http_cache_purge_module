#include "ngx_http_cache_purge_test.hpp"
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace ngx_http_cache_purge;
using Clock = std::chrono::steady_clock;
static ngx_http_cache_rocksdb_bytes_t
bytes(const std::string &s)
{
    return { reinterpret_cast<const unsigned char *>(s.data()), s.size() };
}
static void
check(bool ok)
{
    if (!ok) {
        _exit(1);
    }
}
struct Request {
    std::string key, url, host, ns = "team", tag;
    ngx_http_cache_rocksdb_bytes_t tags[1];
    ngx_http_cache_purge_tag_digest_t digest;
    ngx_http_cache_purge_control_context_t context;
    explicit Request(unsigned n)
    {
        host = "h-" + std::to_string(n) + ".test";
        tag = "tag-" + std::to_string(n);
        url = "https://" + host + "/assets/" + std::to_string(n) + "/file";
        key = "team\x1f" + url;
    }
    void
    bind(bool digests)
    {
        tags[0] = bytes(tag);
        if (digests) {
            ngx_http_cache_rocksdb_error_t error;
            check(ngx_http_cache_purge_tag_digest(tags[0], digest, &error)
                  == 0);
            tags[0] = { digest, sizeof(digest) };
        }
        context = { bytes(key), bytes(url), bytes(host), bytes(ns), tags, 1,
            5000, 1 };
    }
};
static int
visit(void *value, const ngx_http_cache_purge_control_match_t *)
{
    ++*static_cast<uint64_t *>(value);
    return 0;
}
struct Result {
    double mean_ns, p50_ns, p99_ns;
    uint64_t matches;
};
static Result
workload(void *memory, size_t size, std::vector<Request> &requests,
    unsigned iterations, bool digests)
{
    ngx_http_cache_purge_control_reader_t *reader = nullptr;
    ngx_http_cache_rocksdb_error_t error;
    int rc;
    do {
        rc = ngx_http_cache_purge_control_reader_open(
            memory, size, &reader, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            std::this_thread::yield();
        }
    } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
    check(rc == 0);
    uint64_t count = 0, version;
    auto match = digests ? ngx_http_cache_purge_control_match_digests :
                          ngx_http_cache_purge_control_match;
    auto begin = Clock::now();
    for (unsigned i = 0; i < iterations; ++i) {
        check(match(reader,
                  &requests[i % requests.size()].context, 8, 4096, visit,
                  &count, &version, &error)
              == 0);
    }
    auto end = Clock::now();
    std::vector<double> samples;
    samples.reserve(2000);
    for (unsigned i = 0; i != 2000; ++i) {
        auto before = Clock::now();
        check(match(reader,
                  &requests[i % requests.size()].context, 8, 4096, visit,
                  &count, &version, &error)
              == 0);
        samples.push_back(
            std::chrono::duration<double, std::nano>(Clock::now() - before)
                .count());
    }
    std::sort(samples.begin(), samples.end());
    do {
        rc = ngx_http_cache_purge_control_reader_close(&reader, &error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY) {
            std::this_thread::yield();
        }
    } while (rc == NGX_HTTP_CACHE_ROCKSDB_BUSY);
    check(rc == 0);
    return { std::chrono::duration<double, std::nano>(end - begin).count()
                 / iterations,
        samples[1000], samples[1980], count };
}

int
main(int argc, char **argv)
{
    bool groups = false, digests = false;
    unsigned members = 2;
    for (int i = 1; i < argc; i++) {
        std::string option(argv[i]);
        if (option == "--digests") {
            digests = true;
        } else if (option == "--tag-groups" || option == "--tag-groups-64") {
            groups = true;
            members = option == "--tag-groups-64" ? 64 : 2;
        } else {
            check(false);
        }
    }
    size_t size = ngx_http_cache_purge_control_size(8 * 1024 * 1024, 32);
    void *memory = mmap(nullptr, size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    check(memory != MAP_FAILED);
    ngx_http_cache_rocksdb_error_t error;
    ngx_http_cache_purge_control_writer_t *writer = nullptr;
    check(ngx_http_cache_purge_control_init(
              memory, size, 8 * 1024 * 1024, 32, &error)
          == 0);
    check(
        ngx_http_cache_purge_control_writer_open(memory, size, &writer, &error)
        == 0);
    std::printf("{\"tag_groups\":%s,\"tag_digests\":%s,"
                "\"members_per_group\":%u,"
                "\"reserved_shm_bytes\":%zu,\"iterations_"
                "per_worker\":200000,"
                "\"scenarios\":[",
        groups ? "true" : "false", digests ? "true" : "false",
        groups ? members : 0, size);
    bool first = true;
    for (unsigned count : { 0, 16, 256, 4096 }) {
        std::vector<Operation> operations;
        for (unsigned i = 0; i < count; ++i) {
            Request r(i);
            Operation op;
            op.sequence = i + 1;
            op.input.id = "op-" + std::to_string(i);
            op.input.issued_at_ms = 1000;
            op.stage = OperationStage::active;
            op.propagate = true;
            op.propagation_until_ms = 305000;
            op.input.selector.kind =
                static_cast<SelectorKind>(i % (groups ? 9 : 7));
            switch (op.input.selector.kind) {
            case SelectorKind::key:
                op.input.selector.value = r.key;
                break;
            case SelectorKind::url:
                op.input.selector.value = r.url;
                break;
            case SelectorKind::key_prefix:
                op.input.selector.value = r.key.substr(0, r.key.size() - 4);
                break;
            case SelectorKind::url_prefix:
                op.input.selector.value = r.url.substr(0, r.url.size() - 4);
                break;
            case SelectorKind::host:
                op.input.selector.value = r.host;
                break;
            case SelectorKind::namespace_tag:
                op.input.selector.value = r.tag;
                op.input.selector.scope = r.ns;
                break;
            case SelectorKind::host_tag:
                op.input.selector.value = r.tag;
                op.input.selector.scope = r.host;
                break;
            case SelectorKind::namespace_tags:
            case SelectorKind::host_tags: {
                std::vector<std::string> values{ r.tag };
                for (unsigned n = 1; n < members; n++) {
                    values.push_back("zz-" + std::to_string(100 + n));
                }
                op.input.selector.value = tag_group(values);
                op.input.selector.scope =
                    op.input.selector.kind == SelectorKind::namespace_tags ?
                        r.ns :
                        r.host;
                break;
            }
            }
            operations.push_back(std::move(op));
        }
        std::vector<Request> requests;
        for (unsigned i = 0; i < 32; ++i) {
            requests.emplace_back(count ? (i * 997) % count : 0);
        }
        for (auto &r : requests) {
            r.bind(digests);
        }
        auto before = Clock::now();
        auto prepared = prepare_controls(writer, operations);
        double build_us =
            std::chrono::duration<double, std::micro>(Clock::now() - before)
                .count();
        check(ngx_http_cache_purge_control_gate_close(writer, &error) == 0);
        before = Clock::now();
        publish_controls(writer, prepared);
        double publish_us =
            std::chrono::duration<double, std::micro>(Clock::now() - before)
                .count();
        check(ngx_http_cache_purge_control_gate_open(writer, &error) == 0);
        Result single = workload(memory, size, requests, 200000, digests);
        check(single.matches == (count ? 202000U : 0U));
        int channel[2];
        check(pipe(channel) == 0);
        std::vector<pid_t> children;
        before = Clock::now();
        for (unsigned i = 0; i != 4; ++i) {
            pid_t child = fork();
            check(child >= 0);
            if (!child) {
                auto result = workload(memory, size, requests, 200000, digests);
                check(write(channel[1], &result, sizeof(result))
                      == sizeof(result));
                _exit(0);
            }
            children.push_back(child);
        }
        for (unsigned i = 0; i != 4; ++i) {
            Result result;
            check(read(channel[0], &result, sizeof(result)) == sizeof(result));
            check(result.matches == (count ? 202000U : 0U));
        }
        for (auto child : children) {
            int status;
            check(waitpid(child, &status, 0) == child && status == 0);
        }
        double elapsed =
            std::chrono::duration<double>(Clock::now() - before).count();
        close(channel[0]);
        close(channel[1]);
        /*
         * Touch both banks at this scale before measuring resident SHM pages.
         */
        auto second = prepare_controls(writer, operations);
        check(ngx_http_cache_purge_control_gate_close(writer, &error) == 0);
        publish_controls(writer, second);
        check(ngx_http_cache_purge_control_gate_open(writer, &error) == 0);
        size_t page_size = sysconf(_SC_PAGESIZE);
        std::vector<unsigned char> pages((size + page_size - 1) / page_size);
        check(mincore(memory, size, pages.data()) == 0);
        size_t resident = 0;
        for (auto page : pages) {
            if (page & 1) {
                resident += page_size;
            }
        }
        std::printf("%s{\"controls\":%u,\"prepare_us\":%.3f,\"publish_us\":%."
                    "3f,\"lookup_mean_ns\":%.3f,\"sample_p50_ns\":%.3f,"
                    "\"sample_p99_ns\":%.3f,\"four_process_lookups_per_"
                    "second\":%.3f,\"resident_shm_bytes_both_banks\":%zu}",
            first ? "" : ",", count, build_us, publish_us, single.mean_ns,
            single.p50_ns, single.p99_ns, 808000 / elapsed, resident);
        first = false;
    }
    std::puts(
        "],\"scope\":\"synthetic control lookup only; no nginx request, DB or "
        "native file hooks; sample quantiles include clock overhead\"}");
    check(ngx_http_cache_purge_control_writer_close(&writer, &error) == 0);
    munmap(memory, size);
    return 0;
}
