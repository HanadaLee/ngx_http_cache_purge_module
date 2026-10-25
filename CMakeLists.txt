cmake_minimum_required(VERSION 3.16)
project(ngx_cache_purge_store LANGUAGES C CXX)

find_package(RocksDB CONFIG REQUIRED)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

add_library(ngx_cache_purge_store STATIC ngx_cache_purge_store.cc)
target_compile_features(ngx_cache_purge_store PUBLIC cxx_std_17)
set_target_properties(ngx_cache_purge_store PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(ngx_cache_purge_store PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(ngx_cache_purge_store PUBLIC RocksDB::rocksdb Threads::Threads)
if(UNIX)
  target_link_libraries(ngx_cache_purge_store PUBLIC m)
endif()
target_compile_options(ngx_cache_purge_store PRIVATE -Wall -Wextra -Werror)

add_library(ngx_cache_purge_index STATIC ngx_cache_purge_index.cc)
target_compile_features(ngx_cache_purge_index PUBLIC cxx_std_17)
set_target_properties(ngx_cache_purge_index PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(ngx_cache_purge_index PUBLIC ngx_cache_purge_store OpenSSL::Crypto)
target_compile_options(ngx_cache_purge_index PRIVATE -Wall -Wextra -Werror)

add_library(ngx_cache_purge_component STATIC ngx_cache_purge_component.cc)
set_target_properties(ngx_cache_purge_component PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(ngx_cache_purge_component PUBLIC ngx_cache_purge_index)
target_compile_options(ngx_cache_purge_component PRIVATE -Wall -Wextra -Werror)

option(CACHE_PURGE_STORE_TESTS "Build cache index storage regression tests" OFF)
option(CACHE_PURGE_STORE_SANITIZE "Enable address and undefined behavior sanitizers" OFF)
if(CACHE_PURGE_STORE_SANITIZE)
  target_compile_options(ngx_cache_purge_store PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(ngx_cache_purge_store PUBLIC -fsanitize=address,undefined)
endif()

if(CACHE_PURGE_STORE_TESTS)
  enable_testing()
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_executable(cache_purge_store_test t/store_runtime.cc t/store_c_api.c)
  target_link_libraries(cache_purge_store_test PRIVATE ngx_cache_purge_store)
  target_compile_options(cache_purge_store_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME store_runtime COMMAND cache_purge_store_test)
  add_executable(cache_purge_index_test t/index_runtime.cc)
  target_link_libraries(cache_purge_index_test PRIVATE ngx_cache_purge_index)
  target_compile_options(cache_purge_index_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME index_runtime COMMAND cache_purge_index_test)
  add_test(NAME index_process_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/t/index_process_runtime.py
    --binary $<TARGET_FILE:cache_purge_index_test>
    --output ${CMAKE_CURRENT_BINARY_DIR}/index-process-results.json)
  add_executable(cache_purge_component_test t/component_runtime.cc)
  target_link_libraries(cache_purge_component_test PRIVATE ngx_cache_purge_component)
  target_compile_options(cache_purge_component_test PRIVATE -Wall -Wextra -Werror)
  add_test(NAME component_process_runtime COMMAND ${Python3_EXECUTABLE}
    ${CMAKE_CURRENT_SOURCE_DIR}/t/component_process_runtime.py
    --binary $<TARGET_FILE:cache_purge_component_test>
    --output ${CMAKE_CURRENT_BINARY_DIR}/component-process-results.json)
  set_tests_properties(store_runtime index_runtime index_process_runtime component_process_runtime PROPERTIES TIMEOUT 120)
endif()
