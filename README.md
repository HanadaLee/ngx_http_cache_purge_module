# ngx_http_cache_purge

This repository builds two HTTP modules in persistent mode:
`ngx_http_cache_rocksdb_module` provides the bounded RocksDB C API, and
`ngx_http_cache_purge_module` implements HTTP cache indexing and purge behavior.
Both modules remain in this repository. The engine bridge is isolated in
`rocksdb/`; the migration of the remaining C++ purge implementation to C is in
progress. File markers, OFD leases, and the durable operation ledger already use
C implementations.

HTTP cache invalidation for nginx proxy, FastCGI, SCGI, and uWSGI caches.

The module is undergoing a storage and lifecycle redesign. The default build
implements the legacy PURGE/REFRESH interfaces. An opt-in development build now
connects RocksDB ownership, durable operations, and shared control publication to
one executor in ordinary nginx worker 0. Real upstream fills now enter the
persistent index through standard module interfaces. Read admission, the new
Invalidate/Delete HTTP API, and multilevel cache propagation are still being implemented.

The repository now contains a bounded RocksDB storage bridge and a persistent
URL/key/file index. They support cross-zone lookup, scoped TAG postings, full
bytewise prefix scans, shared value storage, and batched fill/retirement records.
The component executor hosts that index on one thread and accepts bounded local
IPC independently of nginx's event loops. It supports cancellable owner
handoff, explicit shutdown, and paged queries without retaining server cursors
between requests. Cache-fill capture is connected; invalidation request handling
and the rest of the file lifecycle are still being implemented. Index state changes
do not perform filesystem operations or provide immediate invalidation by themselves.

HTTP workers now have an event-driven Unix IPC client with a reusable connection,
bounded queues and buffers, absolute deadlines, peer UID verification, and explicit
cancellation/shutdown. It preserves application errors and reports uncertain
outcomes after a lost reply without replaying requests automatically. The client
does not retain nginx requests implicitly; its consumers own that lifecycle.
The real worker owner and fault-injection peers are exercised by `t/runtime_nginx.py`.

The standalone filesystem library provides durable file-instance markers and
Linux open-file-description leases. Separate writers conflict even in the same
process; duplicated or transferred lease descriptors keep a transaction protected
until its last reference closes. The file API is version 2 and requires OFD-lock
support. Its tests cover real descriptor handoff, sender death, same-process
contention, and unfinished index publication recovery. The component can acquire
a lease and return its descriptor through the worker IPC client. Successful
lease-acquisition replies may carry one descriptor, attached to the first
frame byte. The client closes its borrowed reference after the callback and on
failure or cancellation. Consumers that retain a duplicate must bound and close
it separately. `PREPARE_FILE` accepts a lease and a complete temporary-file
descriptor. The component validates configured cache roots/zones, the temporary
path and inode, then persists an index intent and a matching file marker.
Retries bind to the same physical temporary file. `FINISH_FILE` accepts the
original prepared revision and a lease descriptor, verifies the final instance,
and completes durable index/READY-marker publication after a same-filesystem
rename. Retries recover interrupted publication without recreating an evicted
file. Raw index mutations cannot bypass these checks for physical records.
`ACQUIRE_PUBLICATION` also resolves the registered root and current file ID, so
the native fill consumer can replace an indexed instance safely. Cross-filesystem
copy verification remains unfinished. Added database, marker, and fsync work
runs on the executor; nginx's existing rename/copy path is unchanged.

A durable operation ledger now stores selectors, idempotency identities,
checkpoints, retry scheduling, and independent materialization/propagation states.
Acceptance remains `preparing` until native cache integration can attest effective
invalidation, even after its conservative shared barrier is published. The internal
IPC can accept and inspect those records; it does not expose a completed HTTP
purge API or automatically execute physical cache operations yet.

The shared control-table library uses bounded immutable images for exact,
prefix, host, and scoped TAG matching. Normal lookups do not acquire a mutex or
query RocksDB. FENCE and proxy-only PROPAGATE duties share records but expire
independently. Native read/FENCE integration is still pending; tests use
a separate precontent consumer to verify the shared barrier against real HITs.

The module-owned fill helper uses an auxiliary header filter and the existing
upstream finalizer to defer publication
until asynchronous preparation has completed. It retains the request and cache
node through cancellation and reload, and works on unmodified nginx without a
custom store hook. FINISH verifies the actual final file because native update
does not return a rename result. `t/native_fill_runtime.py` covers the native
lifecycle separately. `t/capture_runtime.py` exercises real native fills through
the companion key module, worker IPC, durable preparation, rename and READY
publication. Cached-response admission, 304 commits, eviction coordination and
background invalidation remain to be connected.

The persistent build provides `cache_purge_key`, `cache_purge_url`,
`cache_purge_tag`, and `cache_purge_tag_namespace` as independently inherited
complex values. Configuring `cache_purge_key` enables capture for that cached
location and requires `cache_purge_store` and `cache_purge_state_zone`.
For a location using the companion key module:

```nginx
proxy_cache_key $cache_key_full;
cache_purge_key $cache_key_base;
cache_purge_url $cache_key_url;
cache_purge_tag $upstream_http_cache_tag;
cache_purge_tag_namespace $cache_key_namespace;
```

Key, URL and namespace inputs are frozen after native key creation, including
requests that bypass cache reads. Exact companion-variable references use its
committed C snapshot and preserve component boundaries in the index. An omitted
URL expression uses the actual native cache-key bytes. TAG values are comma
separated, case sensitive, trimmed and deduplicated; an empty value clears the
new entity's TAG set. Namespace defaults to `$host`.

Publication is limited to 64 concurrent transactions and a 2 MiB logical buffer
budget per worker, with a three-second overall IPC deadline. The limits apply
after the response body is complete and do not turn normal HITs into origin
requests. Request inputs and each publication payload are separately bounded
to 128 KiB; these limits exclude allocator overhead and native nginx buffers.
The persistent build replaces the legacy generation form of `cache_purge_key`
with its single-expression index meaning and does not register generation
namespace directives or variables. Automatic recovery of unfinished native
publications and URL aliases first seen on HITs are still pending.

## Current features

- Exact-key and trailing-wildcard cache removal.
- Inline PURGE requests or a dedicated purge location.
- Bounded background filesystem cleanup, with optional nginx thread pools.
- Vary-aware removal and per-cache queue/status metrics.
- Legacy generation-key invalidation and conditional proxy REFRESH.

Legacy tasks operate on their selected cache zone/path. They do not provide the
new persistent cross-zone index or its immediate invalidation guarantees.

## Build

```sh
./configure --add-module=/path/to/ngx_http_cache_purge
make
```

For a dynamic module, use `--add-dynamic-module` and build with `make modules`.
Use compatible nginx sources and build options. `--with-compat` does not make a
binary module portable across arbitrary nginx versions or C libraries.

The storage, index, and component libraries can be built separately on Linux
with RocksDB and OpenSSL development packages. RocksDB must provide its CMake
targets. Tests also require Python 3:

```sh
cmake -S . -B /tmp/cache-purge-store -DCACHE_PURGE_STORE_TESTS=ON
cmake --build /tmp/cache-purge-store
ctest --test-dir /tmp/cache-purge-store --output-on-failure
```

Its blocking APIs belong on the index component's execution thread.
They must not run on an HTTP worker's event loop.

To build the development runtime into nginx, supply the directory containing
the CMake archives when configuring the addon:

```sh
CACHE_PURGE_ROCKSDB_LIB_DIR=/tmp/cache-purge-store \
    ./configure --add-module=/path/to/ngx_http_cache_purge
make
```

Dynamic persistent builds produce two module files. Load the provider first:

```nginx
load_module modules/ngx_http_cache_rocksdb_module.so;
load_module modules/ngx_http_cache_purge_module.so;
```

This requires Linux, HTTP cache support, and `master_process on`. The persistent
runtime uses standard worker initialization, timers, and exit callbacks. It does
not require privileged-agent support or an additional nginx core patch. Only
worker 0 starts the component executor; all workers access it through bounded
asynchronous IPC. The executor shares that worker's address space and failure
domain. Database handles and threads are never created before fork.

On graceful reload, the old worker requests asynchronous executor shutdown so
long-lived HTTP responses do not keep the database owner indefinitely. Its
executor finishes the current operation and closes the DB before the replacement
opens it. The old worker can continue sending its response and joins the stopped
executor when it exits. The component C ABI is 5; rebuild its archives together
with the addon. Override `CACHE_PURGE_ROCKSDB_LIBS` to use
a fixed RocksDB archive and its compression/runtime dependencies. The default
links the system shared RocksDB library. Static archives linked into a dynamic
nginx module must be built with PIC.

The implemented resource directives are:

```nginx
http {
    cache_purge_store /var/lib/nginx/cache-index cache=16m write_buffer=4m;
    cache_purge_state_zone purge_control 16m;
    cache_purge_propagation_timeout 300s;
}
```

`cache` is the shared database cache budget, including the write-buffer charge;
it is not a process RSS limit. The database directory must be outside cache-file
directories and be a private directory owned by the configured worker UID.
New storage is created for that UID; existing agent-owned storage must be migrated
while stopped. Existing directories are not recursively chowned. Store and
state-zone identity changes require a full restart.
These directives initialize the component and its resources. They do not yet
install a complete purge handler or replace native cache read/write behavior.

## Usage

```nginx
location / {
    proxy_cache cache_zone;
    proxy_cache_key "$scheme://$host$request_uri";
    proxy_cache_purge PURGE from 127.0.0.1;
    proxy_pass http://origin;
}
```

Restrict purge and refresh endpoints to authorized callers. Do not expose them as
unrestricted public cache-control APIs.

Runtime regression tests and benchmark programs are under `t/`. Design, rollout,
and validation records are maintained outside this repository. The independent
`ngx_http_cache_key_module` supplies the new component-based cache identities.

## License

See [LICENSE](LICENSE). Original authorship notices remain in the source files.
