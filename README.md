# ngx_http_cache_purge

HTTP cache invalidation for nginx proxy, FastCGI, SCGI, and uWSGI caches.

The module is undergoing a storage and lifecycle redesign. The default build
implements the legacy PURGE/REFRESH interfaces. An opt-in development build now
connects RocksDB ownership, durable operations, and shared control publication to
nginx's cache manager. Native cache file hooks, the new Invalidate/Delete HTTP
API, and multilevel cache propagation are still being implemented.

The repository now contains a bounded RocksDB storage bridge and a persistent
URL/key/file index. They support cross-zone lookup, scoped TAG postings, full
bytewise prefix scans, shared value storage, and batched fill/retirement records.
The component executor hosts that index on one thread and accepts bounded local
IPC independently of the cache-manager event loop. It supports cancellable owner
handoff, explicit shutdown, and paged queries without retaining server cursors
between requests. These are internal building blocks; request handling and cache
file hooks are still being implemented. Index state changes
do not perform filesystem operations or provide immediate invalidation by themselves.

A durable operation ledger now stores selectors, idempotency identities,
checkpoints, retry scheduling, and independent materialization/propagation states.
Acceptance remains `preparing` until native cache integration can attest effective
invalidation, even after its conservative shared barrier is published. The internal
IPC can accept and inspect those records; it does not expose a completed HTTP
purge API or automatically execute physical cache operations yet.

The shared control-table library uses bounded immutable images for exact,
prefix, host, and scoped TAG matching. Normal lookups do not acquire a mutex or
query RocksDB. FENCE and proxy-only PROPAGATE duties share records but expire
independently. Native request/cache hook integration is still pending; tests use
a separate precontent consumer to verify the shared barrier against real HITs.

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

Its blocking APIs belong on the cache-manager component's execution thread.
They must not run directly in HTTP workers.

To build the development runtime into nginx, supply the directory containing
the CMake archives when configuring the addon:

```sh
CACHE_PURGE_ROCKSDB_LIB_DIR=/tmp/cache-purge-store \
    ./configure --add-module=/path/to/ngx_http_cache_purge
make
```

This requires Linux, HTTP cache support, and `NGX_HAVE_CACHE_MANAGER_EXIT` from
nginx-ext or an equivalent core patch. Override `CACHE_PURGE_ROCKSDB_LIBS` to use
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
directories. Store and state-zone identity changes require a full restart.
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
