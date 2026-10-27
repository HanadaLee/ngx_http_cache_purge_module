# ngx_http_cache_purge

This repository builds two HTTP modules in persistent mode:
`ngx_http_cache_rocksdb_module` provides the bounded RocksDB C API, and
`ngx_http_cache_purge_module` implements HTTP cache indexing and purge behavior.
Both modules remain in this repository. Production C++ is confined to the
RocksDB bridge in `rocksdb/`; the entire purge implementation uses C. Its
executor, shared controls and operation coordination share one source file.
The persistent index and filesystem publication logic each have their own C
implementation. Test-only C++ adapters and wire encoders live under `t/`.
Production code is organized into 12 source/header files across the two modules;
new behavior is added within these responsibility boundaries.

HTTP cache invalidation for nginx proxy, FastCGI, SCGI, and uWSGI caches.

The module is undergoing a storage and lifecycle redesign. The default build
implements the legacy PURGE/REFRESH interfaces. An opt-in development build now
connects RocksDB ownership, durable operations, and shared control publication to
one executor in ordinary nginx worker 0. Real upstream fills now enter the
persistent index through standard module interfaces. File-response admission now
checks the selected descriptor and shared FENCE table. Special cache paths, the
new Invalidate/Delete HTTP API, and multilevel propagation remain unfinished.

The repository now contains a bounded RocksDB storage bridge and a persistent
URL/key/file index. They support cross-zone lookup, scoped TAG postings, full
bytewise prefix scans, shared value storage, and batched fill/retirement records.
The component executor hosts that index on one thread and accepts bounded local
IPC independently of nginx's event loops. It supports cancellable owner
handoff, explicit shutdown, and paged queries without retaining server cursors
between requests. Cache-fill capture is connected; invalidation request handling
and the rest of the file lifecycle are still being implemented. Index state changes
do not perform filesystem operations or provide immediate invalidation by themselves.

The executor automatically revisits unfinished native file publications in
bounded background steps, including after owner restart or a lost reply. It
first acquires an independent OFD lease, so active writers remain protected.
Verified renamed files become READY; abandoned preparations are retired without
deleting unrelated files. Changed identities and inaccessible roots remain
pending. Recovery and response admission have independent responsibilities.

The executor can physically remove one indexed file under its exclusive path
lease. It durably retires the record before unlinking, checks the instance marker
and inode, and synchronizes parent directories. Retries distinguish removed,
missing and replaced files; a replacement is preserved. Per-file I/O failures
remain retryable without stopping the database owner. Retired records remain for
subsequent task and native-node cleanup. The executor now advances persisted
active Delete tasks automatically, one checkpoint or file attempt per event-loop
pass. It records the selected instance before retirement can remove it from
enumeration, then resumes that exact instance after a crash. Busy leases and
per-file failures retain a durable retry; other tasks and IPC continue. Full
entities fetched after the operation are preserved, including replacements.

For native nginx roots, background Delete now hands node accounting back to the
owning worker through one bounded notification slot. The executor retains the
path lease until the worker acknowledges the exact inode and physical key.
The event handler uses a try-lock and bounded tree lookup, without database or
filesystem I/O. It preserves replacements, retries nodes still in use or being
loaded/deleted, and subtracts the old node's block count once. Task progress is
not acknowledged before this step; a crash resumes the persisted file identity.
Graceful executor shutdown releases the pending lease before DB ownership moves
to a new worker. No nginx request or cache-node pointers cross into the database
thread.

Background scans yield after at most 32 cursor advances, including retired rows
and empty URL/key branches. Every yield releases its snapshot and saves a resume
position; it does not retain all matching files in memory. These are work-unit
bounds, not hard latency bounds for filesystem or database I/O. A file failure
currently pauses that task at its pending instance while other tasks continue.
Active Invalidate and Delete tasks automatically retire their FENCE after file
processing finishes, no old or unknown
origin registrations remain, and the durable file-intent catalog is empty.
The checks and retirement run in one executor step. Retirement atomically raises
the durable write floor, so delayed old publications are rejected, then republishes
the control image through the existing reader gate. Per-file and selector
requirements still enforce validation after FENCE removal. PROPAGATE keeps its
independent deadline. Even a newer unrelated file intent conservatively delays
retirement; retries neither scan all pending metadata nor rewrite checkpoints.
Admission of an old entity under a newly observed URL or host alias persists a
sparse Delete requirement before rejecting reuse. The executor drains that queue
and the retired-file queue in bounded passes, preserving path writers and file
IDs referenced by unfinished tasks. Native cleanup requires worker acknowledgement
before index collection. Busy entries remain durable and do not block other
entries; completed passes back off for one second. A new full entity can satisfy
Delete, while a 304 alone cannot. Retired file IDs are diagnostic handles and
may disappear after collection; operation receipts remain durable. Task completion
covers its enumeration and drain, while subsequently discovered aliases use the
persistent selector requirements and independent deferred cleanup queue.
Diagnostic acceptance still returns PREPARING. Internal IPC can activate a task
only after its FENCE publication is READY; retries preserve the effective time and
propagation deadline. Activation republishes its updated controls, so callers
must also await READY afterwards. Reliable submission instead writes the operation
and its autonomous activation intent in the same durable batch. The executor
activates it after FENCE publication, including after the caller disconnects or
the owner dies. No caller activation request is required. Retries preserve the
original acceptance, first effective time and propagation deadline; completed
operations are never requeued. A bounded runtime snapshot reports current READY
state, publication, persistent ledger identity, operation usage and propagation
policy. An HTTP caller must still wait for activation and READY before returning
202; the public HTTP handler is not implemented yet.

The filesystem backend also supports bounded in-place validation transactions.
It reserves the existing path and persists an NFV2 plan while retaining the old
TAG postings. Proposed TAGs remain in the bounded plan until completion. Both
preparation and recovery check old and proposed TAGs against accepted controls;
a conflicting recovery retires the instance before further header writes.
Completion writes and fsyncs only the supplied prefix, then atomically publishes
the new TAGs, physical proof and completed receipt. Inspection excludes the
pending instance. Retries and automatic recovery
share the same OFD lease protocol; completed receipts remain bounded across
repeated validation. `VALIDATE_FILE` connects this transaction to native upstream
304 handling. It transfers the lease and selected descriptor, verifies accepted
controls before changing old TAG membership, and returns the durable record.
Control publication must be ready; persisted URL aliases are visited within
explicit count and byte budgets, with no complete alias set retained in memory.

`INSPECT_FILE` checks the actual cache descriptor on the executor through bounded
IPC. It requires a READY marker, a committed current index record with no pending
intent, and matching root/zone/path and same-filesystem publication identity.
Unknown, unfinished, changed, replaced and evicted instances are rejected without
repairing files or writing the database. The worker client duplicates and transfers
one descriptor; the caller keeps its original. Inspection alone does not attest
purge freshness. The native file-response consumer also checks key, URL, host,
prefix and scoped TAG FENCEs, then commits its decision under a short shared
control guard. It waits before slice, Range and client-conditional filters run.
No shared guard spans IPC or asynchronous body streaming. Unknown files and
unsatisfied Delete FENCEs cause an unconditional upstream retry using the
original key. An unsatisfied Invalidate found at file admission retains the
entity and validators for a conditional upstream request when configured.

New entity fills persist the durable ledger identity and complete local operation
sequence observed before origin dispatch. File admission can recognize rules
already covered by that entity, allowing HIT under an existing TAG FENCE.
Publication and automatic recovery check both the candidate and its predecessor,
including old TAGs and watermark regression, before committing a replacement.
Conflicting late candidates are retired without being stamped READY. This local
watermark neither changes the cache key nor proves upstream CDN freshness.
Current selectors use the file's proof for Invalidate and Delete; already-covered
entities can HIT while their rules remain resident. Rule retirement is unfinished.
An origin request
started before a push may finish its response, but its old result must not become
a reusable cache entry. Requests started after the push must avoid old entries.

Each worker announces its oldest active origin boundary in its existing 64-byte
shared reader slot. A request-local queue keeps registration and removal O(1),
including across an unconditional retry. Registration ends after publication,
cached-response admission, or confirmation that the response cannot be cached.
Reload preserves old-worker registrations; only proven-dead processes are
reclaimed. The executor can query pre-operation writer registrations through
bounded IPC. A zero count alone does not permit FENCE removal: queued IPC,
unfinished durable transactions and physical processing still need closure.
This changes the internal shared-control ABI to 3; nginx-ext ABI remains 1.

Native file responses use `ADMIT_FILE` to verify the selected descriptor, bind
the captured key, and persist the observed URL association. Repeated observations
do not write the database. The executor checks persisted URL aliases against
FENCEs within count/byte budgets and returns a publication stamp and the highest
unsatisfied Invalidate sequence with the record.
The worker requires the same publication at final admission, closing the gap
between the database check and response reuse. No alias collection is sent to or
retained by the worker. Physical `INSPECT_FILE` remains a diagnostic operation.

The operation ledger also persists a monotonic write floor with each materialized
operation. PREPARE, uncommitted FINISH/recovery and new 304 validation writes
reject origins below that floor even after the rule leaves the resident table.
Committed files and completed validation receipts are not invalidated by this
check. A 304 request carries its own origin boundary separately from the stored
entity's watermark. The native physical-work coordinator does not yet invoke
materialization: these write guards do not establish complete FENCE retirement.

Indexed requests do not create memory-only entries: transport 502/504 and
intercepted errors are excluded, while ordinary disk-backed error responses keep
their configured cache policy. Native `error_page` conditions are not evaluated
again by the module. Enabling indexing requires every location with a content
handler and enabled upstream cache settings to inherit or configure
`cache_purge_key`; alternatively, disable its upstream cache. Parent contexts
also receive a runtime guard, so a dynamically selected cache path without an
index key cannot read or write an unindexed entry.

Changing between indexed and unindexed mode requires a full nginx restart to
replace legacy in-memory error state. Reload within indexed mode remains
supported, with the persistent index retained. These checks allow current URL,
key, prefix and host rules to use file admission instead of forcing every match
through BYPASS. Cached
`X-Accel-Redirect` responses wait for descriptor and TAG admission before selecting
their target. The module composes the existing per-request upstream parsing and
reinitialization callbacks, then uses native header-copy handlers and URI/named
location dispatch. Ignored redirects retain their ordinary cached-body behavior.
No new nginx core hook or private protocol configuration layout is used.

Read checks are bounded to 64 concurrent waits per worker, with a three-second
overall deadline. A PREPARED marker can wait for publication within that deadline.
No complete file index is loaded into worker memory. Successful upstream 304
responses capture TTL and TAG metadata before native cached-header parsing,
persist the validation transaction, and pass final FENCE admission before
reusing the entity. An absent direct upstream TAG header retains the old set;
a present empty header clears it. General expressions replace the set. A durable
304 preserves the full entity's original watermark and records a separate
conditional-validation boundary. That boundary satisfies Invalidate only; Delete
still requires a new entity. Repeated 304 transactions replace the conditional
proof without nesting it. Validation receipts carry the publication and any
remaining Invalidate requirement; stamp zero cannot authorize response reuse.
Under `NGX_RESTY_EXT`, a forced Invalidate sets the existing request `purged`
flag to prevent stale fallback and clears it after durable validation and
admission. Origin `must_revalidate` remains an independent boolean. `purged`
is request state only; invalidation recovery uses the persistent index and
operation ledger, with no purge bit stored in the native cache file header.
The shared node bit does not replace persistent operation ordering: an older
304 cannot satisfy a later Invalidate.
Without the macro, native configured stale fallback remains available.

File admission supports this behavior for current URL/key/prefix/host selectors,
TAGs and persisted URL aliases. Background Invalidate now writes a sparse
per-instance validation requirement to RocksDB, without rewriting cache files.
The existing worker executor resumes the bounded scan from durable checkpoints.
Admission checks that requirement even when no resident rule remains; successful
conditional validation satisfies it without replacing the entity. Requirements
are monotonic within one ledger and are collected with their retired file.
Index schema 6 upgrades schemas 1/2/3/4/5 from existing index records; upgrades
from 4/5 only change the schema marker and add no full scan. Older readers reject
it instead of ignoring deferred Delete or staged TAG changes. Pending NFV1 plans
cannot recover their removed old TAG membership and are conservatively discarded;
completed NFR1 receipts and unrelated cache files remain valid. NFV2 plans retain
the 16 KiB metadata limit; oversized proposed TAG/header combinations are rejected
before mutation, while retaining existing TAGs needs only a one-byte marker.

Persistent selector requirements also survive resident-rule retirement. The
operation ledger retains the latest Invalidate and Delete sequence independently
for each selector. Ordered predecessor seeks find matching bytewise prefixes
without scanning unrelated operation history. File admission uses these records
to protect URL aliases first observed after enumeration, across native zones.
Full entities fetched after the latest accepted operation skip this historical
lookup. Ledger schema 3 adds a sparse activation queue in the default CF; schema
2 upgrades by changing its marker, and schema 1 additionally replays selector
requirements through its existing bounded migration. Native cache files and
their index are not rebuilt.

Business `$host` is separate from `$cache_key_host`. The index records observed
host-to-key relationships, shared across every slice, Vary file and zone of the
key. File admission observes the current host and URL atomically before checking
their persisted relationships. A host or host+TAG operation can therefore affect
other business hosts sharing that key. Namespace+TAG remains explicitly scoped.
Host+TAG enumeration chooses between host-to-key traversal and TAG postings,
using dictionary reference counts as a bounded heuristic; bookmarks preserve the
chosen plan. Associations are reclaimed when the key loses its last file.

The schema upgrade resumes bounded batches from a durable cursor and does not
read or rewrite native cache files. Legacy records only recover the host they
actually stored; missing historical business hosts cannot be inferred from a
canonical cache host. Subsequent requests register their actual business hosts,
including checks against persistent requirements after resident-rule retirement.
Completed upgrades do not replay on restart. This internal storage schema change
does not change nginx-ext ABI, which remains 1.

Automatic Delete retirement and full writer/migration coverage remain unfinished.
Memory-only errors do not receive a separate TAG/alias index. Initial receipts
remain PREPARING; these partial consumers do not expose reliable public 202
acceptance or completed immediate purge semantics yet.

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
independently. Native file-response admission consumes this table. A separate
precontent test consumer still covers coordinator lifecycle; special response
paths and FENCE retirement remain unfinished.

The module-owned fill helper uses an initial header filter and the existing
upstream finalizer to defer publication
until asynchronous preparation has completed. It retains the request and cache
node through cancellation and reload, and works on unmodified nginx without a
custom store hook. FINISH verifies the actual final file because native update
does not return a rename result. `t/native_fill_runtime.py` covers the native
lifecycle separately. `t/capture_runtime.py` exercises real native fills through
the companion key module, worker IPC, durable preparation, rename and READY
publication. Selected-descriptor admission is connected for native file responses;
304 commits are connected; complete special-response coverage, eviction
coordination and background invalidation remain unfinished.

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
namespace directives or variables. Observed URL aliases are persisted during
file-response admission and survive restarts without index rebuilding.

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

Register the persistent purge addon after other header-filter addons, or load its
dynamic module last among them. Its admission filter must run before slice and
other filters modify response state. Startup and `nginx -t` reject an unsafe
filter order when `cache_purge_key` is configured.

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
