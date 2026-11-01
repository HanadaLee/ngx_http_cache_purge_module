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

For a first MISS with no indexed file, a purge can complete before that
in-flight response is stored. The old response can then become a HIT; strict
prevention of this boundary is not implemented.

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

The native cache manager also sweeps file records for configured cache roots.
It checks at most eight records per batch, at least 250 ms apart, and yields
between records to queued work or a 5 ms time budget. Individual database or
filesystem calls cannot be interrupted by that budget. Only `ENOENT` retires
a record; other filesystem errors preserve it. Missing files keep their TAG
associations for the configured propagation window before normal collection.
The sweep never deletes cache files or adds work to the HIT path.
Later TAG purges extend known retained associations to their own window end,
without reopening missing files. These rows count as skipped physical work.

The capabilities response exposes cumulative `index_cleanup.checked`,
`index_cleanup.retained`, and `index_cleanup.errors` counters. These describe
file checks, successful transitions into retention, and check/retention errors;
they are operational counters, not a durable ledger. Reload retains them;
a full restart resets them. Sweep bookmarks are process-local and may rescan
after a restart. Cleanup yields under load, so completion time is not fixed.
The shared state format is now version 8; upgrading from earlier formats
requires a full restart. The database schema and nginx core ABI are unchanged.

RocksDB's target SST size and base-level capacity follow the write-buffer
budget: 2 MiB and 8 MiB by default. They are compaction targets, not a limit on
the total index size.

The store's `io_rate=SIZE` parameter limits combined RocksDB flush and
compaction reads/writes in bytes per second. It defaults to `16m`; `0` disables
the limit, and nonzero values must be at least `1k`. For example:

```nginx
cache_purge_store /var/cache/nginx/index cache=16m buffer=4m io_rate=16m;
```

The limit uses RocksDB's rate limiter with a 1 ms refill interval. Ordinary
index reads and WAL writes keep their existing behavior. Native cache-file
cleanup still uses the executor's separate batch limit. The bridge options
structure now uses ABI 2; rebuild both addons together. nginx's core ABI and
the persisted index schema are unchanged.

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
New queue messages wake the helper through the pipe. Once it drains the queue,
it uses its idle timer instead of scheduling an extra immediate retry.

The shared state reserves rule space before admission and exposes a successful
completion only after publication. Its empty-rule path does not take the shared
mutex. Completed rules retained only for propagation skip HIT filtering.
Responses at least as new as the shared timestamp bound also skip URL/TAG
evaluation and rule lookup, unless a newer incoming propagation timestamp
requires filtering. Cached responses use the native cache date; origin
responses use the request start time. This gate reads one shared atomic word
without a lock. Writers keep a conservative timestamp bound until the last
local rule finishes, with one extra shared counter and no per-object state.
The existing sorted array of at most 256 published rules groups URL/key/prefix,
host and TAG selectors. URL searches follow prefix ancestors; host searches
compare full values. Larger TAG groups compare precomputed hashes, then full
values and scope, so hash collisions cannot broaden a purge. TAG rules reuse
the ancestor pointer's storage for their hash, so the rule structure does not
grow. TAG groups of at most four rules use direct comparisons to avoid
small-table lookup overhead.
When local rules contain only URL and prefix selectors, HIT filtering still
evaluates only the URL. The array is rebuilt on publication and once after each
collection batch under the shared mutex. Outgoing requests use the same index
including retained propagation rules. Full HTTP latency and contention
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
`--fill-only-control` adds a run that indexes new fills while disabling purge
only for the measured hot URL. Every compared run then uses the same exact
location for that URL. This separates request handling from background indexing;
the fill coverage and deletion assertions remain enabled in the control run.
`--request-delay` adds a delay in milliseconds between requests on each wrk
connection to compare latency below saturation; it is not an open-loop load.
`--seed-store` copies an offline, closed index before each enabled run; copied
files are synchronized before measurement and charged to `--cgroup` when set.
`--url-origin` selects the scheme and host of the configured purge URL identity.
`--io-rate` overrides the store limit for comparisons with the same binary.
`--cgroup` uses an existing Linux memory cgroup; `--profile` samples worker main
threads with perf, while `--profile all` includes the cache manager and database
threads. Profiling is for diagnosis, not latency acceptance. These focused
measurements do not establish a production performance guarantee.

The optional `--backend` file check runs persistent tasks through the actual
native file functions. Use a matching sanitizer build when passing `--sanitize`.
Task checks cover durable admission after abrupt owner exit, bounded progress,
failure retries, idempotent operation IDs, large and binary selector values,
and snapshot release between batches.

The native runtime and benchmark runners accept `--cache-io` to exercise
`sendfile on` with a 10,000-entry open-file cache, a 10-second validity interval,
and `min_uses=2`. This is a separate I/O configuration comparison; keep its
measurements separate from the default benchmark. The runtime runner retains
its existing smaller open-file cache when this option is absent.

The benchmark selects distinct cores from the topology exposed by Linux.
It excludes nginx's SMT siblings from the load generator and controller,
pins wrk to one separate core, and records actual thread affinities.
`--server-cores` counts distinct cores; a separate load-generator core is
required. Virtual-machine topology does not guarantee host CPU isolation.

`--state` also tests shared queues/rules with nginx's actual slab allocator and
process mutex. It checks publication ordering, disconnected callers, capacity
rejection and cross-process visibility. It can run without linking RocksDB;
with `--backend`, it also checks the durable-admission/publication sequence.

Additional safeguards for unusual races or failure conditions are considered
separately. They must not silently add work to every cache HIT or turn cache
maintenance into a general transaction/recovery framework.

## License

See [LICENSE](LICENSE).
