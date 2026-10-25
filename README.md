# ngx_http_cache_purge

HTTP cache invalidation for nginx proxy, FastCGI, SCGI, and uWSGI caches.

The module is undergoing a storage and lifecycle redesign. The current runtime
still implements the legacy PURGE/REFRESH interfaces. Persistent RocksDB indexing,
the new Invalidate/Delete API, and multilevel cache propagation are not yet wired
into this checkout; their configuration must not be treated as available features.

The repository now contains a bounded RocksDB storage bridge with a C API,
atomic cross-column-family writes, conditional updates, and bytewise prefix
cursors. It is an internal building block; the nginx task dispatcher and cache
file/index lifecycle are still being implemented.

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

The storage bridge can be built and tested separately with a RocksDB development
package that provides its CMake targets:

```sh
cmake -S . -B /tmp/cache-purge-store -DCACHE_PURGE_STORE_TESTS=ON
cmake --build /tmp/cache-purge-store
ctest --test-dir /tmp/cache-purge-store --output-on-failure
```

Its blocking APIs belong on the cache-manager component's execution thread.
They must not run directly in HTTP workers. The current nginx addon build does
not yet link this library or enable the new persistent-index API.

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
