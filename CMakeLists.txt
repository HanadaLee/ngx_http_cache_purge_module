cmake_minimum_required(VERSION 3.16)
project(ngx_http_cache_modules LANGUAGES C CXX)

find_package(RocksDB CONFIG REQUIRED)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

add_library(ngx_http_cache_rocksdb STATIC rocksdb/ngx_http_cache_rocksdb.cc)
target_compile_features(ngx_http_cache_rocksdb PRIVATE cxx_std_17)
set_target_properties(ngx_http_cache_rocksdb PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(ngx_http_cache_rocksdb PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(ngx_http_cache_rocksdb PUBLIC RocksDB::rocksdb Threads::Threads)
if(UNIX)
  target_link_libraries(ngx_http_cache_rocksdb PUBLIC m)
endif()
target_compile_options(ngx_http_cache_rocksdb PRIVATE -Wall -Wextra -Werror)

add_library(ngx_http_cache_purge_files STATIC ngx_http_cache_purge_files.c)
target_compile_features(ngx_http_cache_purge_files PUBLIC c_std_99)
target_compile_definitions(ngx_http_cache_purge_files PRIVATE _GNU_SOURCE)
target_include_directories(ngx_http_cache_purge_files PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
set_target_properties(ngx_http_cache_purge_files PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(ngx_http_cache_purge_files PUBLIC ngx_http_cache_purge_index OpenSSL::Crypto)
target_compile_options(ngx_http_cache_purge_files PRIVATE -Wall -Wextra -Werror -Wdeclaration-after-statement)

add_library(ngx_http_cache_purge_index STATIC ngx_http_cache_purge_index.c)
target_compile_features(ngx_http_cache_purge_index PUBLIC c_std_99)
set_target_properties(ngx_http_cache_purge_index PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(ngx_http_cache_purge_index PUBLIC ngx_http_cache_rocksdb OpenSSL::Crypto)
target_compile_options(ngx_http_cache_purge_index PRIVATE -Wall -Wextra -Werror -Wdeclaration-after-statement)

add_library(ngx_http_cache_purge_control STATIC ngx_http_cache_purge_control.c)
target_compile_features(ngx_http_cache_purge_control PUBLIC c_std_99)
target_compile_definitions(ngx_http_cache_purge_control PRIVATE _GNU_SOURCE)
set_target_properties(ngx_http_cache_purge_control PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(ngx_http_cache_purge_control PUBLIC ngx_http_cache_purge_index)
target_compile_options(ngx_http_cache_purge_control PRIVATE -Wall -Wextra -Werror -Wdeclaration-after-statement)

add_library(ngx_http_cache_purge_component STATIC ngx_http_cache_purge_component.cc)
target_compile_features(ngx_http_cache_purge_component PRIVATE cxx_std_17)
set_target_properties(ngx_http_cache_purge_component PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(ngx_http_cache_purge_component PUBLIC ngx_http_cache_purge_control ngx_http_cache_purge_index ngx_http_cache_purge_files)
target_compile_options(ngx_http_cache_purge_component PRIVATE -Wall -Wextra -Werror)

option(CACHE_PURGE_STORE_TESTS "Build cache index storage regression tests" OFF)
option(CACHE_PURGE_STORE_SANITIZE "Enable address and undefined behavior sanitizers" OFF)
if(CACHE_PURGE_STORE_SANITIZE)
  target_compile_options(ngx_http_cache_rocksdb PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(ngx_http_cache_rocksdb PUBLIC -fsanitize=address,undefined)
  target_compile_options(ngx_http_cache_purge_files PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(ngx_http_cache_purge_files PUBLIC -fsanitize=address,undefined)
endif()

if(CACHE_PURGE_STORE_TESTS)
  set(CMAKE_CXX_STANDARD 17)
  enable_testing()
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_executable(cache_purge_store_test t/store_runtime.cc t/store_c_api.c)
  target_link_libraries(cache_purge_store_test PRIVATE ngx_http_cache_rocksdb)
  target_compile_options(cache_purge_store_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME store_runtime COMMAND cache_purge_store_test)
  add_executable(cache_purge_index_test t/index_runtime.cc t/ngx_http_cache_purge_index_c_api.c)
  target_link_libraries(cache_purge_index_test PRIVATE ngx_http_cache_purge_index)
  target_compile_options(cache_purge_index_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME index_runtime COMMAND cache_purge_index_test)
  add_test(NAME index_process_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/t/index_process_runtime.py
    --binary $<TARGET_FILE:cache_purge_index_test>
    --output ${CMAKE_CURRENT_BINARY_DIR}/index-process-results.json)
  add_executable(cache_purge_files_test t/files_runtime.cc)
  target_link_libraries(cache_purge_files_test PRIVATE ngx_http_cache_purge_component)
  target_compile_options(cache_purge_files_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME files_runtime COMMAND cache_purge_files_test)
  add_executable(cache_purge_file_leases_test t/file_leases_runtime.cc)
  target_link_libraries(cache_purge_file_leases_test PRIVATE ngx_http_cache_purge_files Threads::Threads)
  target_compile_options(cache_purge_file_leases_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME file_leases_runtime COMMAND cache_purge_file_leases_test)
  add_test(NAME files_process_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/t/files_process_runtime.py
    --binary $<TARGET_FILE:cache_purge_files_test>
    --output ${CMAKE_CURRENT_BINARY_DIR}/files-process-results.json)
  add_executable(cache_purge_component_test t/component_runtime.cc)
  target_link_libraries(cache_purge_component_test PRIVATE ngx_http_cache_purge_component)
  target_compile_options(cache_purge_component_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME component_process_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/t/component_process_runtime.py
    --binary $<TARGET_FILE:cache_purge_component_test>
    --output ${CMAKE_CURRENT_BINARY_DIR}/component-process-results.json)
  add_executable(cache_purge_operations_test t/operations_runtime.cc)
  target_link_libraries(cache_purge_operations_test PRIVATE ngx_http_cache_purge_index)
  target_compile_options(cache_purge_operations_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME operations_runtime COMMAND cache_purge_operations_test)
  add_executable(cache_purge_operations_c_test t/ngx_http_cache_purge_operations_test.c)
  target_compile_definitions(cache_purge_operations_c_test PRIVATE _GNU_SOURCE)
  target_compile_options(cache_purge_operations_c_test PRIVATE -std=gnu89 -Wall -Wextra -Werror -Wdeclaration-after-statement)
  target_link_libraries(cache_purge_operations_c_test PRIVATE ngx_http_cache_purge_control ngx_http_cache_purge_files)
  add_test(NAME operations_c_api COMMAND cache_purge_operations_c_test)
  add_test(NAME operations_process_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/t/operations_process_runtime.py
    --binary $<TARGET_FILE:cache_purge_operations_test>
    --output ${CMAKE_CURRENT_BINARY_DIR}/operations-process-results.json)
  add_executable(cache_purge_control_test t/control_runtime.cc)
  target_link_libraries(cache_purge_control_test PRIVATE ngx_http_cache_purge_control)
  target_compile_options(cache_purge_control_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME control_runtime COMMAND cache_purge_control_test)
  add_library(cache_purge_control_faults STATIC ngx_http_cache_purge_control.c)
  target_link_libraries(cache_purge_control_faults PUBLIC ngx_http_cache_purge_index)
  target_compile_definitions(cache_purge_control_faults PRIVATE _GNU_SOURCE NGX_HTTP_CACHE_PURGE_CONTROL_FAULT_TEST=1)
  target_compile_options(cache_purge_control_faults PRIVATE -Wall -Wextra -Werror)
  add_executable(cache_purge_control_fault_test t/control_fault_runtime.cc)
  target_link_libraries(cache_purge_control_fault_test PRIVATE cache_purge_control_faults)
  target_compile_options(cache_purge_control_fault_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME control_fault_runtime COMMAND cache_purge_control_fault_test)
  add_executable(cache_purge_control_benchmark t/control_benchmark.cc)
  target_link_libraries(cache_purge_control_benchmark PRIVATE ngx_http_cache_purge_control)
  target_compile_options(cache_purge_control_benchmark PRIVATE -Wall -Wextra -Werror)
  add_executable(cache_purge_coordinator_test t/coordinator_runtime.cc)
  target_link_libraries(cache_purge_coordinator_test PRIVATE ngx_http_cache_purge_control ngx_http_cache_purge_index)
  target_compile_options(cache_purge_coordinator_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME coordinator_runtime COMMAND cache_purge_coordinator_test)
  add_test(NAME coordinator_process_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/t/coordinator_process_runtime.py
    --binary $<TARGET_FILE:cache_purge_coordinator_test>
    --output ${CMAKE_CURRENT_BINARY_DIR}/coordinator-process-results.json)
  set_tests_properties(store_runtime index_runtime index_process_runtime files_runtime files_process_runtime file_leases_runtime component_process_runtime
    operations_runtime operations_c_api operations_process_runtime control_runtime control_fault_runtime
    coordinator_runtime coordinator_process_runtime PROPERTIES TIMEOUT 120)
endif()
