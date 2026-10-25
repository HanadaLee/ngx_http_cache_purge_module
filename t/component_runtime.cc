#include "ngx_cache_purge_component.h"
#include <cstdlib>
#include <iostream>
#include <thread>

int main(int argc, char **argv) {
    if (argc != 6) return 2;
    ngx_cache_purge_component_options_t options;
    ngx_cache_purge_component_options(&options);
    options.max_connections = std::strtoul(argv[2], nullptr, 10);
    options.io_timeout_ms = std::strtoul(argv[3], nullptr, 10);
    options.buffer_bytes = std::strtoul(argv[4], nullptr, 10);
    options.max_payload_bytes = std::strtoul(argv[5], nullptr, 10);
    ngx_cache_purge_component_t *component = nullptr;
    ngx_cache_purge_store_error_t error;
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
        ngx_cache_purge_component_status_t status;
        rc = ngx_cache_purge_component_status(component, &status, &error);
        if (rc) return rc;
        std::cout << status.state << ' ' << status.error_code << ' ' << status.connections << ' '
                  << status.requests << ' ' << status.rejected << ' ' << status.buffered_bytes
                  << ' ' << status.peak_buffered_bytes << std::endl;
    }
    rc = ngx_cache_purge_component_stop(&component, &error);
    std::cout << "STOP " << rc << ' ' << (component == nullptr) << std::endl;
    if (rc) std::cerr << error.message << std::endl;
    return rc;
}
