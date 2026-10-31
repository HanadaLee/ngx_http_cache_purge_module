cmake_minimum_required(VERSION 3.16)
project(ngx_http_cache_modules LANGUAGES C CXX)

find_package(RocksDB CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_library(ngx_http_cache_rocksdb STATIC rocksdb/ngx_http_cache_rocksdb.cc)
target_compile_features(ngx_http_cache_rocksdb PRIVATE cxx_std_17)
set_target_properties(ngx_http_cache_rocksdb PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(ngx_http_cache_rocksdb PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(ngx_http_cache_rocksdb PUBLIC RocksDB::rocksdb Threads::Threads)
target_compile_options(ngx_http_cache_rocksdb PRIVATE -Wall -Wextra -Werror)

add_library(ngx_http_cache_purge_index STATIC
  ngx_http_cache_purge_index.c ngx_http_cache_purge_component.c)
set_target_properties(ngx_http_cache_purge_index PROPERTIES
  POSITION_INDEPENDENT_CODE ON C_STANDARD 90 C_STANDARD_REQUIRED ON)
target_link_libraries(ngx_http_cache_purge_index PUBLIC ngx_http_cache_rocksdb)
target_compile_options(ngx_http_cache_purge_index PRIVATE
  -Wall -Wextra -Werror -Wdeclaration-after-statement)

option(CACHE_PURGE_STORE_TESTS "Build RocksDB and external-index tests" OFF)
option(CACHE_PURGE_STORE_SANITIZE "Enable address and undefined behavior sanitizers" OFF)
if(CACHE_PURGE_STORE_SANITIZE)
  foreach(target ngx_http_cache_rocksdb ngx_http_cache_purge_index)
    target_compile_options(${target} PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${target} PUBLIC -fsanitize=address,undefined)
  endforeach()
endif()

if(CACHE_PURGE_STORE_TESTS)
  enable_testing()
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_executable(cache_purge_store_test t/store_runtime.cc t/ngx_http_cache_purge_store_c_api.c)
  target_compile_features(cache_purge_store_test PRIVATE cxx_std_17)
  target_link_libraries(cache_purge_store_test PRIVATE ngx_http_cache_rocksdb)
  target_compile_options(cache_purge_store_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME store_runtime COMMAND cache_purge_store_test)
  add_test(NAME store_process_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/t/store_process_runtime.py
    --binary $<TARGET_FILE:cache_purge_store_test>
    --output ${CMAKE_CURRENT_BINARY_DIR}/store-process-results.json)

  add_executable(cache_purge_catalog_test t/ngx_http_cache_purge_catalog_test.c)
  target_compile_definitions(cache_purge_catalog_test PRIVATE
    _GNU_SOURCE ngx_http_cache_purge_test_main=main)
  target_compile_options(cache_purge_catalog_test PRIVATE
    -std=gnu89 -Wall -Wextra -Werror -Wdeclaration-after-statement)
  target_link_libraries(cache_purge_catalog_test PRIVATE ngx_http_cache_purge_index)
  add_test(NAME catalog_runtime COMMAND cache_purge_catalog_test)
  add_executable(cache_purge_jobs_test t/ngx_http_cache_purge_jobs_test.c)
  target_compile_definitions(cache_purge_jobs_test PRIVATE
    _GNU_SOURCE ngx_http_cache_purge_test_main=main)
  target_compile_options(cache_purge_jobs_test PRIVATE
    -std=gnu89 -Wall -Wextra -Werror -Wdeclaration-after-statement)
  target_link_libraries(cache_purge_jobs_test PRIVATE ngx_http_cache_purge_index)
  add_test(NAME jobs_runtime COMMAND cache_purge_jobs_test)
  set_tests_properties(store_runtime store_process_runtime catalog_runtime
    jobs_runtime PROPERTIES TIMEOUT 120)
endif()
