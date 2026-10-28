# Native Linux tests; build and reports stay outside the source checkout.
.DEFAULT_GOAL := help
BUILD_DIR ?= /tmp/ngx-http-cache-purge-build
REPORT_DIR ?= /tmp/ngx-http-cache-purge-results
JOBS ?= 2
HTTP_OPTIONS ?= --dynamic
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
.PHONY: help build test test-http test-sanitize

help:
	@echo "build          Build embedded backend libraries and tests"
	@echo "test           Run backend tests (BUILD_DIR, REPORT_DIR, JOBS)"
	@echo "test-http      Run backend, lifecycle, fill and HTTP suites; set NGINX_SOURCE and CACHE_KEY_SOURCE"
	@echo "test-sanitize  Run the same suites with ASan/UBSan"
	@echo "Choose a new REPORT_DIR for each run; stale reports are rejected."

build:
	cmake -S "$(ROOT)" -B "$(BUILD_DIR)" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCACHE_PURGE_STORE_TESTS=ON
	cmake --build "$(BUILD_DIR)" --parallel $(JOBS)

test:
	python3 "$(ROOT)/t/persistent_runtime.py" --backend-only --build "$(BUILD_DIR)" --reports "$(REPORT_DIR)" --jobs $(JOBS)

test-http:
	python3 "$(ROOT)/t/persistent_runtime.py" --nginx "$(NGINX_SOURCE)" --key "$(CACHE_KEY_SOURCE)" --build "$(BUILD_DIR)" --reports "$(REPORT_DIR)" --jobs $(JOBS) $(HTTP_OPTIONS)

test-sanitize:
	$(MAKE) test-http BUILD_DIR="$(BUILD_DIR)-asan" REPORT_DIR="$(REPORT_DIR)-asan" HTTP_OPTIONS="$(HTTP_OPTIONS) --sanitize"
