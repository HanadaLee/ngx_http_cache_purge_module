# ngx_http_cache_purge

HTTP cache indexing and purge support for nginx and OpenResty. The repository
contains `ngx_http_cache_purge_module` and `ngx_http_cache_rocksdb_module`.
Production code is C; only the RocksDB bridge uses C++.

The simplified implementation builds as an nginx addon. Public PURGE requests
persist a background task and publish a shared rule before returning 202.
The native nginx cache manager owns RocksDB and processes files in bounded
batches. Full feature and performance acceptance is still pending; this working
tree is not a release.

## Behavior

- Find cached objects across cache zones using an external persistent index.
- Select objects by host, URL, URL prefix, URL/misc/namespace key, or scoped TAG.
- Delete matching cache files in the background.
- Invalidate by setting the native cache header's `valid_sec` to zero. Use the
  existing in-memory `purged` flag to require successful origin validation when
  `NGX_RESTY_EXT` is enabled. A 304 can reuse the entity; a 200 replaces it.
- Permit restart to lose `purged`. The expired file then follows ordinary nginx
  stale policy. Do not reconstruct strict invalidation from persistent proofs.
- Keep active purge rules in bounded shared memory while background processing
  is pending. Preserve proxy propagation for its configured window.
- Maintain URL and TAG relationships in RocksDB without loading the entire
  index into memory. RocksDB runs inside the native cache manager, with bounded
  background work; no separate database service or container is required.

No module metadata belongs in nginx cache files, extended attributes, hidden
response headers or sidecar files. Ordinary HITs must not read module file
proofs, query the index on every request or wait for background file maintenance.
The removed transaction and recovery mechanisms are not optional compatibility
modes.

Indexing starts with managed cache fills. Existing unindexed files require an
explicit index migration or a fresh cache-key namespace when first enabling
this mode. The module does not infer historical identities from native keys or
rebuild the index on ordinary HITs. Normal restarts reuse the persistent index.

## Configuration contract

The key module and purge module are independent. Purge identity comes from
explicit expressions, without parsing a particular native cache-key format:

```nginx
cache_purge_key_url       $cache_key_url;
cache_purge_key_misc      $cache_key_misc;
cache_purge_key_namespace $cache_key_namespace;
```

The variables above are optional outputs of the separate cache-key module;
other configured expressions may be used. Both namespace defaults are empty.
An empty namespace is an exact scope and never falls back to Host.

`proxy_cache_purge`, `fastcgi_cache_purge`, `uwsgi_cache_purge` and
`scgi_cache_purge` activate their respective location behavior. PURGE requests
can use the original business location or a dedicated purge location. URL and
misc construction remains configuration-driven. Module authentication is not
provided; use nginx access controls as required.

Control headers have separate configurable directive names and defaults under
`X-Cache-Purge-`. Conditional configuration uses the expr module's `when`
blocks, not an extra directive argument. Regular-expression selectors and
physical MD5 filename selectors are outside the current scope.

PURGE is the submission method. The management location supports JSON capability
and task-status responses; it does not accept JSON POST submissions.
A TAG purge currently accepts one TAG;
cached objects can have multiple TAGs.

## Proxy propagation

Propagation is one-way from an edge to its CDN parent. The default outgoing
window is 120 seconds. The parent compares the request's purge timestamp and
mode with the native cache date, then serves, revalidates or replaces its cache.
There is no response receipt or module signature.

Use `cache_purge_propagation on|off` to distinguish parent traffic from origin
traffic. Extension builds use the existing proxy request-header framework;
other builds expose variables for `proxy_set_header`. FastCGI, uwsgi and scgi do
not propagate purge headers. TAG names are not sent to the parent.

Known TAG associations survive local deletion for the propagation window.
When a proxy MISS needs these associations, a background lookup reads the
deduplicated key-to-TAG relation before request bytes are sent upstream. Cache
HITs do not perform this lookup. An unknown TAG association adds no propagation
requirement; first-MISS compensation for unknown TAGs is outside the scope.

## Focused native tests

The storage libraries and their current tests build independently:

```sh
cmake -S . -B build -DCACHE_PURGE_STORE_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

The index uses one shared dictionary for repeated values and compact file
postings. URL-prefix scans follow ordered URL rows; TAG scans use scoped
postings. Deleting the last reference removes its dictionary rows. A short
retention deadline can preserve known TAG associations after file removal;
expired associations are collected in bounded batches. No file publication or
validation proofs are stored by this index.

Background tasks use a durable admission record and an ordered pending queue.
The nginx executor processes at most 8 files per batch, with at least 50 ms
between successful batches. Each batch closes its snapshot and stores a
continuation bookmark. Ordinary progress uses deferred WAL; admission and
completion are synchronous. File errors preserve the unfinished entry for a
later background retry. The executor must publish its shared rule before replying
202, and apply native-node updates before withdrawing that rule. The owner
and public PURGE handler implement this ordering.

The store registers a normal nginx path-manager callback. nginx starts,
reloads and respawns the cache manager; the module does not add a process
supervisor or change nginx core. HTTP workers neither open RocksDB nor create
database threads. The cache manager handles its queue through the existing pipe
and timer, alongside native cache maintenance. RocksDB's internal threads stay
inside that helper process. With `master_process off`, the existing in-process
thread remains a debugging fallback and has a different performance profile.

The shared state reserves rule space before admission and exposes a successful
completion only after publication. Its empty-rule path does not take the shared
mutex. Completed rules retained only for propagation skip HIT filtering.
When local rules contain only URL and prefix selectors, HIT filtering evaluates
only the URL and searches a sorted array of at most 256 rules, following prefix
ancestors for overlapping matches. The array is rebuilt on rule changes under
the shared mutex. Other selectors use full identity matching; outgoing requests
still include retained propagation rules. Full HTTP latency and contention
acceptance remain outstanding. Queues contain
copied messages, with count and byte limits, rather than request pointers or
an in-memory copy of the cache index.

The following focused checks exercise the native primitives and public addon.
They are not full feature or performance acceptance tests.

```sh
python3 t/native_files_runtime.py \
    --nginx /path/to/configured/nginx --backend /path/to/build --state \
    --output /tmp/native-files.json --sanitize

python3 t/native_cache_runtime.py \
    --nginx /path/to/nginx-ext-git-checkout \
    --extension 1 --backend /path/to/build --public-module --no-thread-pool \
    --output /tmp/native-cache.json
```

The file checks verify that Invalidate changes only the native expiry field,
Delete removes the selected file, already replaced files are preserved, invalid
headers and nonregular files are rejected, and no module attributes or sidecars
are created. The HTTP checks exercise failed-origin stale handling, 304 reuse,
restart degradation and Delete with `open_file_cache`. `--extension 0` checks
the existing native stale fallback without extension behavior. With
`--public-module --backend`, the runner builds the actual addon and exercises
PURGE and task status in a two-worker nginx instance. It covers cross-zone
deletion, background retry, reload handoff, native helper respawn and request
enforcement while a 512-object prefix task is still pending. Omitting
`--public-module` selects a
native-operation test adapter. `--sanitize` enables ASan/UBSan and requires
matching backend archives; leak detection is disabled for nginx's process-exit
allocation model.

Add `--slice` to exercise all native slices with subrequest logging disabled.
Add `--key /path/to/ngx_http_cache_key_module` to test the two modules together
through configuration variables. The same selector checks also run without the
key module, including misc/namespace isolation, scoped TAGs and native Vary.
`--dynamic` builds and loads the RocksDB and purge modules as separate DSOs.
`--protocols` adds real FastCGI, uwsgi and scgi origin checks. `--ssl` builds
TLS support for the parent propagation tests.

`t/native_propagation_runtime.py --binary /path/to/nginx --output /tmp/tiers.json`
runs two nginx cache instances and checks the one-way protocol, known TAG MISS,
request-body preservation and outgoing timeout. Use `--framework --expr` with
a binary built with those modules; otherwise it uses `proxy_set_header`.
Add `--tls --streaming` to verify a TLS parent with certificate/hostname checks
and unbuffered Content-Length and chunked request bodies. This requires a binary
built with `--ssl` and the `openssl` command.

A focused foreground benchmark uses an already built public-module binary:

```sh
python3 t/native_cache_benchmark.py --binary /path/to/nginx \
    --output /tmp/cache-benchmark.json
```

It requires Linux and `wrk`. It compares the same binary with purge disabled,
enabled without rules, deleting unrelated objects in the background, and
retaining a completed rule. It records latency, throughput and process PSS;
`--fills` and `--fill-rate` add continuous fills and check their index coverage.
`--cgroup` uses an existing Linux memory cgroup; `--profile` samples worker main
threads with perf and is for diagnosis, not latency acceptance. These focused
measurements do not establish a production performance guarantee.

The optional `--backend` file check runs persistent tasks through the actual
native file functions. Use a matching sanitizer build when passing `--sanitize`.
Task checks cover durable admission after abrupt owner exit, bounded progress,
failure retries, idempotent operation IDs, large and binary selector values,
and snapshot release between batches.

`--state` also tests shared queues/rules with nginx's actual slab allocator and
process mutex. It checks publication ordering, disconnected callers, capacity
rejection and cross-process visibility. It can run without linking RocksDB;
with `--backend`, it also checks the durable-admission/publication sequence.

Additional safeguards for unusual races or failure conditions are considered
separately. They must not silently add work to every cache HIT or turn cache
maintenance into a general transaction/recovery framework.

## License

See [LICENSE](LICENSE).
