# ngx_http_cache_purge

This repository builds two HTTP modules in persistent mode:
`ngx_http_cache_rocksdb_module` provides the bounded RocksDB C API, and
`ngx_http_cache_purge_module` implements HTTP cache indexing and purge behavior.
Both modules remain in this repository. Production C++ is confined to the
RocksDB bridge in `rocksdb/`; the entire purge implementation uses C. Its
executor, shared controls and operation coordination share one source file.
The persistent index and filesystem publication logic each have their own C
implementation. Test-only C++ adapters and wire encoders live under `t/`.
Production code is organized into 11 source/header files across the two modules;
new behavior is added within these responsibility boundaries.

The executor attempts a bounded nonblocking send as soon as an IPC reply is
ready. Descriptor replies can send their one-byte rights envelope and ordinary
body in the same turn. Short writes and EAGAIN retain the remaining frame for
POLLOUT; ready replies do not wait behind the next background maintenance pass.

On Linux, the executor names its writer `cache_index` and its lookup thread
`cache_lookup`. After creating the lookup thread, the writer adds 10 to its own
nice value, capped at 19, as a best-effort scheduling hint. The HTTP worker,
lookup thread and nginx I/O pools retain their inherited priorities.

New cache-file records use atomic WAL batches with deferred synchronization.
The existing executor checks for pending WAL data every 100 ms; 64 batches or
one configured maximum batch of accumulated WAL bytes force an earlier sync.
A synchronous write and graceful close also flush preceding deferred records.
When no command is runnable but FINISH replies await that WAL group, the
executor synchronizes the group before sleeping. Available commands still join
the batch first; the periodic interval and existing size limits remain.
WAL synchronization uses the writer thread without a mutation queue. Ordinary
process restarts recover the WAL. Machine power loss may lose its unsynchronized
cache-record suffix;
files without a matching index identity refill instead of serving unverified
data. The interval is an executor scheduling target, not a hard deadline.
Purge acceptance, invalidation requirements, final task completion, retirement
and in-place 304 validation synchronize before acknowledgement. The executor's
intermediate checkpoints share the bounded WAL sync group: retirement flushes
its preceding instance reservation, and completion flushes all prior progress
before removing FENCE. Power loss can replay intermediate work, but cannot lose
an accepted task or turn incomplete work into a completed receipt. The explicit
checkpoint API remains synchronous, including replay of a deferred checkpoint.
After rename, the publication pool synchronizes the completed data, STAGED
identity, and target/source directory chains. FINISH carries a volatile flag
for that completed work; the executor still verifies the physical instance and
current rules before COMMIT_PENDING. Legacy calls and recovery perform the
same synchronization inside FINISH. Failure never sets the flag, and the flag
is not persisted in the file or index.
The executor then waits for that commit's existing WAL group before writing
READY and removing the publication intent. Each connection retains a 40-byte
owner-local wait record on 64-bit builds; the bounded recovery scan has one more.
Waiting does not repeat file syncs or hold a shared-control pin. FINISH does not
flush the WAL itself; the executor decides when to finish the shared group.
The original IPC deadline still applies. After the group syncs,
the publisher rechecks file identity and current rules before becoming readable.
Disconnects and timeouts leave the committed intent for bounded recovery. PREPARE
does not synchronize an unpublished file. A power loss that loses only READY
causes a MISS. In-place 304 validation and physical deletion retain their syncs.
STAGED keeps the 40-byte NCF1 marker. Ledger-backed READY files use NCF2 in the
same xattr: a 152-byte header plus 16 bytes per TAG, at most 1,176 bytes. It binds
the instance, physical stat, configured identity digest, entity/validation
watermarks and persistent Invalidate requirement. Native nginx cache bytes are
unchanged. Older module readers reject NCF2; a binary upgrade requires a full
restart, and downgrading is not a transparent reuse of these markers.

Background Invalidate persists the file requirement before advancing its task.
304 preparation and Delete durably revoke the old proof before header mutation
or unlink. Successful 304 validation publishes the new TAG set and watermarks
after its index transaction commits. A reader checks the exact descriptor and
both stat snapshots; legacy, revoked, replaced or unlinked instances do not
provide independent admission. Ordinary HTTP HITs use the selected file-proof
read pool and shared rules; `ADMIT_FILE` remains in 304 transactions. Larger xattrs
can allocate an extra filesystem block per object; their byte length alone is
not the disk cost.
The WAL sync group includes both single-file/native preparation and the batch
preparation API. Native representation bindings and their file intent remain in
the same atomic batch. Tests count actual WAL sync calls through these entry
points and verify process recovery and loss of an unsynchronized WAL suffix.
Executor retention of matched historical associations also joins this
group. A changing RETIRE or 304 validation intent synchronizes earlier retained
associations; deferred replacement and collection follow them in WAL order.
Explicit retention remains synchronous, including a replay of a deferred result.
Background collection of retired records and unreferenced key metadata also
joins this bounded sync group. After synchronous retirement, lost collection
progress restores retired records for another cleanup pass. If the lost suffix
also contains a cache replacement, the physical instance must still match its
index record; an older path mapping cannot authorize the replacement file.
Explicit collection APIs retain their synchronous contract.

Bounded dictionary digest probes can fill the existing shared RocksDB block
cache. Bulk prefix scans and forked cursors retain their no-fill policy, so
enumerating a purge does not populate the cache with the scanned range. Both
cursor APIs share the same snapshot deadlines, handle limits and memory budget.
Exact URL/key selectors already use direct lookups; this optimization targets
value interning and native representation or association queries.

HTTP cache invalidation for nginx proxy, FastCGI, SCGI, and uWSGI caches.

The module is undergoing a storage and lifecycle redesign. All builds use the
persistent index and connect RocksDB ownership, durable operations, and shared
control publication to
one executor in ordinary nginx worker 0. Real upstream fills now enter the
persistent index through standard module interfaces. File-response admission now
checks the selected descriptor and shared FENCE table. The development HTTP API
supports durable Invalidate/Delete submission, scoped multi-TAG operations and
status queries. SCM integration and multilevel propagation remain unfinished.

The repository now contains a bounded RocksDB storage bridge and a persistent
URL/key/file index. They support cross-zone lookup, scoped TAG postings, full
bytewise prefix scans, shared value storage, and batched fill/retirement records.
The component writer hosts that index on one thread and accepts bounded local
IPC independently of nginx's event loops. It supports cancellable owner
handoff, explicit shutdown, and paged queries without retaining server cursors
between requests. HTTP tests cover filling, invalidation, file cleanup and
restart recovery; full deployment and performance acceptance remain open.
Index state changes alone do not perform filesystem operations or provide
immediate invalidation.

The executor automatically revisits unfinished native file publications in
bounded background steps, including after owner restart or a lost reply. It
first acquires an independent OFD lease, so active writers remain protected.
Abandoned PREPARED records are retired because size and mtime cannot prove that
their data survived a power loss. COMMITTED intents have crossed the file and
directory barriers and can complete after identity and rule checks. Unrelated
files are preserved; inaccessible roots remain pending. Recovery and response
admission have independent responsibilities.

The executor can physically remove one indexed file under its exclusive path
lease. It durably retires the record before unlinking, checks the instance marker
and inode, and synchronizes parent directories. Retries distinguish removed,
missing and replaced files; a replacement is preserved. Per-file I/O failures
remain retryable without stopping the database owner. Retired records remain for
subsequent task and native-node cleanup. The executor now advances persisted
active Delete tasks automatically, one checkpoint or file attempt per event-loop
pass. It records the selected instance before retirement can remove it from
enumeration. Retirement makes that reservation durable in the same WAL barrier;
if an earlier reservation is lost, the instance remains enumerable. Busy leases
and per-file failures retain replayable work; other tasks and IPC continue. Full
entities fetched after the operation are preserved, including replacements.

For native nginx roots, background Delete now hands node accounting back to the
owning worker through one bounded notification slot. The executor retains the
path lease until the worker acknowledges the exact inode and physical key.
After a successful acknowledgement, the executor releases the task's record
reference and collects the retired metadata under the same lease. Other task
references defer collection. This avoids repeating file and node cleanup during
normal task completion; interrupted collection remains replayable on restart.
The event handler uses a try-lock and bounded tree lookup, without database or
filesystem I/O. It preserves replacements, retries nodes still in use or being
loaded/deleted, and subtracts the old node's block count once. Task progress is
not acknowledged before this step; a crash resumes the persisted file identity.
Task removal and retired-record collection take turns using that slot. A task
acknowledgement makes collection eligible immediately; a collection
acknowledgement gives task progress the next turn. Acknowledgement handling does
not immediately reuse the slot, so neither stream repeatedly postpones the other
under sustained cleanup.
Graceful executor shutdown releases the pending lease before DB ownership moves
to a new worker. No nginx request or cache-node pointers cross into the database
thread.

An unsent IPC call waits through a missing, refused, or full Unix socket during
executor handoff. Reconnect attempts use a 10 ms timer and keep the original call
deadline and queue budgets. Cancellation and worker shutdown release queued
descriptors and buffers. Calls that have sent any bytes are never automatically
replayed; a lost response remains ambiguous. Unexpected connection failures and
peer UID mismatches still fail explicitly.

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
The executor drains deferred Delete requirements and the retired-file queue
in bounded passes, preserving path writers and file IDs referenced by unfinished
tasks. Native cleanup requires worker acknowledgement before index collection.
Busy entries remain durable and do not block other entries; completed passes
back off for one second. A new full entity can satisfy Delete, while a 304 alone
cannot. Retired file IDs are diagnostic handles and may disappear after
collection; operation receipts remain durable.

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
policy. The HTTP handler waits for activation and READY before returning 202.
Receipt reads combine the runtime state and operation in one executor call.

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
Control publication must be ready. Validation checks the file's fixed URL,
host, namespace and actual TAG membership without an alias scan.

`INSPECT_FILE` checks the actual cache descriptor on the executor through bounded
IPC. It requires a READY marker, a committed current index record with no pending
intent, and matching root/zone/path and same-filesystem publication identity.
Executor inspection returns BUSY while a matching publication intent can still
complete. A PREPARED marker left by an aborted or superseded transaction is
rejected immediately instead of consuming the inspection timeout. READY files
under conditional validation still wait for that transaction to finish.
Unknown, changed, replaced and evicted instances are rejected without repairing
files or writing the database. The worker client duplicates and transfers
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
Conflicting late candidates are retired and cannot be admitted, including after
a marker write interrupted an earlier publication attempt. This local
watermark neither changes the cache key nor proves upstream CDN freshness.
Current selectors use the file's proof for Invalidate and Delete; already-covered
entities can HIT while their rules remain resident. An origin request started
before a locally accepted push may finish its response, but its old result must
not become a reusable local cache entry. Requests started after local acceptance
must avoid entries invalidated by that operation. This guarantee does not extend
to an in-flight fill on an upper tier that has not received the push.

Each worker announces its oldest active origin boundary in its existing 64-byte
shared reader slot. A request-local queue keeps registration and removal O(1),
including across an unconditional retry. Registration ends after publication,
cached-response admission, or confirmation that the response cannot be cached.
Reload preserves old-worker registrations; only proven-dead processes are
reclaimed. The executor can query pre-operation writer registrations through
bounded IPC. A zero count alone does not permit FENCE removal: queued IPC,
unfinished durable transactions and physical processing still need closure.
The internal shared-control ABI is 6; nginx-ext ABI remains 1.

Normal cached responses read the selected file's NCF2 proof on a separate nginx
thread pool, then match shared rules in the worker. They do not query RocksDB or
use executor IPC. The proof binds the configured namespace/URL/misc identity,
physical file, TAG digests, ledger and entity/validation requirements. The reader
also compares the selected native cache header inside the file-stat interval.
Shared-control publication changes force a reread before admission. Unknown,
revoked, PREPARED and STAGED proofs cause an unconditional refill. Ordinary
requests do not poll unfinished files or wait for their publication/WAL sync;
only the background publisher can make them READY. A refill may increase origin
traffic while publication is pending. No shared guard spans file I/O.
`ADMIT_FILE` remains in the background 304 transaction; `INSPECT_FILE` remains
diagnostic. Retained TAG lookup on MISS uses the separate read executor. Full
cache fills and 304 updates use bounded background jobs; foreground file reads
and end-to-end latency still require performance validation.

Configured workers reserve Linux file-descriptor table capacity during startup
for twice `worker_connections` plus 512 internal descriptors, capped by the
existing soft `RLIMIT_NOFILE`. The temporary descriptor is immediately closed;
the module neither raises that limit nor keeps dummy files open. This avoids
RCU waits when request-side descriptor duplication first expands a table shared
with background threads. The kernel rounds capacity upward; 1,024 entries need
about 8 KiB for file pointers plus bitmaps and allocator overhead per worker.
Other modules or workloads exceeding the reserved budget can still expand it.

The RocksDB bridge exposes owner-local batch durability tickets. Reading them
only inspects counters and never synchronizes the WAL. A later deferred batch
does not revoke an earlier synchronized ticket. Tickets reset on reopen and are
not persistent cache identities or worker-visible admission proofs. Opening the
store synchronizes its recovered WAL prefix before exposing the handle. File
publication uses these tickets and publishes NCF2 only after its index batch is
durable. Ordinary HITs use that file-side proof without reading the tickets.

The operation ledger also persists a monotonic write floor with each materialized
operation. PREPARE, uncommitted FINISH/recovery and new 304 validation writes
reject origins below that floor even after the rule leaves the resident table.
Committed files and completed validation receipts are not invalidated by this
check. A 304 request carries its own origin boundary separately from the stored
entity's watermark. The native coordinator materializes a drained operation only
after checking old origin registrations and the pending file-intent catalog in
the same executor step. It then republishes controls with FENCE removed while
preserving the independent propagation deadline.

Indexed requests do not create memory-only entries: transport 502/504 and
intercepted errors are excluded, while ordinary disk-backed error responses keep
their configured cache policy. Native `error_page` conditions are not evaluated
again by the module. `cache_purge_store` and `cache_purge_state_zone` configure
shared resources. Each protocol is activated separately with its inherited
`*_cache_purge on` switch; all four switches default to off. Disabled locations
keep native caching without evaluating purge identity or making capture IPC
requests. Native `*_cache off` remains a separate caching decision.

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
admission deadline. Pending publication is rejected after one proof read, without
a retry timer. Running file syscalls cannot be cancelled; their request pool and
descriptor remain alive until completion, including during disconnect/reload.
An expired decision cannot admit an entity. Queue overflow fails closed.
No complete file index is loaded into worker memory. Successful upstream 304
responses capture TTL and TAG metadata before native cached-header parsing,
verify the selected file proof, and match shared rules against both the old and
replacement TAG sets before reusing the entity. The response does not wait for
the writer: a separate bounded job persists the validation with its own file
descriptor, node reference and copied inputs. Capacity exhaustion or update
failure leaves the persistent cache unrenewed, so another access may need to
validate again. An absent direct upstream TAG header retains the old set;
a present empty header clears it. General expressions replace the set. A durable
304 preserves the full entity's original watermark and records a separate
conditional-validation boundary. That boundary satisfies Invalidate only; Delete
still requires a new entity. Repeated 304 transactions replace the conditional
proof without nesting it. The background transaction rechecks its revision,
native header and accepted requirements before publishing READY.
Under `NGX_RESTY_EXT`, a forced Invalidate sets the existing request `purged`
flag to prevent stale fallback and clears it after successful origin validation
and admission. Origin `must_revalidate` remains an independent boolean. `purged`
is request state only; invalidation recovery uses the persistent index and
operation ledger, with no purge bit stored in the native cache file header.
The shared node bit does not replace persistent operation ordering: an older
304 cannot satisfy a later Invalidate.
Without the macro, native configured stale fallback remains available.

File admission supports this behavior for current URL/key/prefix/host selectors,
TAGs and the fixed key identity. Background Invalidate writes a sparse
per-instance validation requirement to RocksDB and persists it in the file proof
before advancing its checkpoint. It does not rewrite the native cache header.
The existing worker executor resumes the bounded scan from durable checkpoints.
Admission checks that requirement even when no resident rule remains; successful
conditional validation satisfies it without replacing the entity. Requirements
are monotonic within one ledger and are collected with their retired file.
Index schema 14 stores one immutable URL/host/namespace/misc identity per key.
The URL secondary key contains only fixed URL, namespace and misc IDs; its value
is the internal key ID. One entry covers every slice, Vary file and cache zone.
Exact identity lookup uses a 32-byte binary digest, with no whole-key text
dictionary or duplicate text projection. The misc bytes have one shared
dictionary reference per identity and are returned independently. File proofs
bind these persisted values; empty and binary misc values remain distinct.
Prototype schemas 1-12 are rejected without modifying their data. Schema 13
upgrades its pending, retired, deferred-Delete, empty-key and native-retry
catalogs in resumable atomic batches before accepting new writer work. URL/TAG
identities and native files are preserved. The first upgrade can scan historical
RocksDB deletion markers; subsequent opens use the saved catalogs directly.
Downgrading an upgraded database is unsupported.

Each work catalog stores its head, tail and count plus links between active rows
in the existing default CF. Exact address mappings and links change in the same
batch. Snapshot traversal follows point lookups, so empty catalogs do not scan
historical deletion markers. No full catalog is loaded into memory and no new
CF, thread or core hook is needed. The batch entry limit is 2048 to accommodate
link updates; the existing byte and memory budgets still apply.
Reopening schema 14 reuses its WAL-backed index without scanning cache
directories. Native representation repair retains
its bounded durable cursor; unresolved identities cannot imply an empty TAG set.
Older module archives are rejected by a schema-specific configure link check.

Pending NFV1 plans cannot recover
their removed old TAG membership
and are conservatively discarded; completed NFR1 receipts and unrelated cache
files remain valid. NFV2 plans retain
the 16 KiB metadata limit; oversized proposed TAG/header combinations are rejected
before mutation, while retaining existing TAGs needs only a one-byte marker.

Persistent selector requirements also survive resident-rule retirement. The
operation ledger retains the latest Invalidate and Delete sequence independently
for each selector. Ordered predecessor seeks find matching bytewise prefixes
without scanning unrelated operation history. Publication and 304 validation use
these records to protect older entities after resident FENCE retirement, across
native zones. File admission reads the materialized per-file requirement.
Full entities fetched after the latest accepted operation skip this historical
lookup. Ledger schema 4 adds canonical TAG-set records on top of schema 3's
sparse activation queue. Schemas 2 and 3 upgrade by changing their marker;
schema 1 additionally replays selector requirements through its existing bounded
migration. Native cache files and their index are not rebuilt.

The storage layer can encode up to 64 sorted, unique TAG members as one operation
and persist their requirements in the same atomic write. Its namespace/host OR
queries traverse one member posting list at a time, with resumable work bounds
and no result-sized deduplication set. Parent and member queries share the
existing memory budget. The resident table stores one operation record and compact
TAG membership links, so overlapping members emit the operation once. Matching
does not allocate memory or access RocksDB. The public API accepts up to 64 TAGs,
sorts and deduplicates them, and uses one durable submission and effective time.

The host identity is parsed from `cache_purge_key_url`. It is fixed together with
URL, misc and namespace for every slice, Vary file and zone of that key. File
admission does not record aliases or infer a host from the native cache key.
Namespace+TAG remains explicitly scoped.
Host+TAG enumeration chooses between host-to-key traversal and TAG postings,
using dictionary reference counts as a bounded heuristic; bookmarks preserve the
chosen plan. Associations are reclaimed when the key loses its last file.

Schema 13 requires the compact identity described above; older prototype indexes
are rejected without changing their data. nginx-ext ABI remains 1.

Memory-only errors do not receive separate index records. Initial internal
receipts remain PREPARING until activation; the HTTP API waits for activation
and READY before acknowledging reliable acceptance.
While a new intent waits for durable acceptance, readers keep using the previous
effective control image. The admission gate closes only for the shared-image
switch, and the executor attempts to reopen it before dispatching file work.
An ambiguous write failure still closes admission and recovers from the ledger.
An unpublished intent cannot produce an effective HTTP receipt.

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
and completes WAL-indexed/READY-marker publication after a same-filesystem
rename. Retries recover interrupted publication without recreating an evicted
file. A busy FINISH keeps its lease and retries within the original publication
deadline. Cancellation and
deadline expiry release the retained resources for background recovery.
Raw index mutations cannot bypass these checks for physical records.
`ACQUIRE_PUBLICATION` also resolves the registered root and current file ID, so
the native fill consumer can replace an indexed instance safely. Cross-filesystem
copy verification remains unfinished. Added database, marker, and fsync work
runs on the executor; nginx's existing rename/copy path is unchanged.

A durable operation ledger now stores selectors, idempotency identities,
checkpoints, retry scheduling, and independent materialization/propagation states.
Acceptance remains `preparing` until native cache integration can attest effective
invalidation. Reliable submissions activate autonomously after publication;
physical cache processing continues in the background.

The shared control-table library uses bounded immutable images for exact,
prefix, host, and scoped TAG matching. Normal lookups do not acquire a mutex or
query RocksDB. FENCE and proxy-only PROPAGATE duties share records but expire
independently. Native file-response admission consumes this table. A separate
precontent test consumer also covers coordinator lifecycle.

The module-owned fill helper uses an initial header filter and the existing
upstream finalizer to transfer complete cache fills to bounded publication jobs.
Each job owns copied identity data, a temporary-file descriptor, the cache-node
reference and the origin registration. ACQUIRE, PREPARE and FINISH do not retain
the HTTP request or delay reuse of its keepalive connection. Capacity exhaustion
drops the cache fill; it does not wait for a publication slot. Jobs share the
existing 64-task and 2 MiB material limits per worker, have a three-second
deadline and keep draining workers alive until completion or cancellation.
Publication rename/stat and temporary-file cleanup use an explicit nginx thread
pool separate from HIT proof reads. Completion updates native cache-node
accounting on the worker event loop. It works on unmodified nginx without a
custom store hook. An in-flight filesystem operation cannot be cancelled: its
bounded task and lease remain owned until the thread finishes, even if the
three-second publication deadline expires. A timed-out job cannot start FINISH.
FINISH verifies the actual final instance after filesystem and cache-node
updates. `t/native_fill_runtime.py` covers the native
lifecycle separately. `t/capture_runtime.py` exercises real native fills through
the companion key module, worker IPC, WAL preparation, rename and READY
publication. It also covers selected-descriptor admission, 304 commits,
background invalidation, native-node cleanup and HTTP management requests.

Configure the store and state zone together at HTTP scope. Provide independent
identity expressions and enable the protocol at HTTP, server or location scope:

```nginx
cache_purge_key_url "$scheme://$host$uri";
cache_purge_key_misc $arg_variant;
cache_purge_key_namespace "";
proxy_cache_purge on;
```

`cache_purge_key_url` is required for an enabled location and must evaluate to
an absolute HTTP or HTTPS URL. The module preserves that value and derives its
host selector from the URL authority, rather than the incoming request Host.
`cache_purge_key_misc` and `cache_purge_key_namespace` default to empty strings.
Empty misc identifies one variant; empty namespace is a valid exact scope.
It never falls back to Host or matches every namespace.
Namespaces contain at most 255 printable ASCII bytes without spaces or commas.
The three expressions and `proxy_cache_purge`, `fastcgi_cache_purge`,
`scgi_cache_purge` and `uwsgi_cache_purge` support inheritance and expr `when`
blocks. Conditions follow expr's first-match order: place conditional entries
before an unconditional fallback in the same scope. Inactive expressions are
not evaluated. Invalid static values fail
configuration; missing or invalid dynamic identity prevents caching.

The module does not parse native cache-key bytes or depend on the key module's
variables, headers, APIs, format or load order. Operators may explicitly use
`$cache_key_url`, `$cache_key_misc` and `$cache_key_namespace` in these expressions.
Native `*_cache_key` expressions are unchanged, including custom formats.
Identity is captured once per cache operation. Physical main/Vary digests still
come from nginx; range is not an identity input and is not parsed from the key.
All physical slices and zones remain associated with their configured identity.

The index uses a bounded internal digest for the namespace/URL/misc identity.
Its URL secondary layout stores component IDs and the internal key ID, without
repeating URL or native key bytes. The digest is internal lookup material.
The protocol switches control index capture, invalidation checks and PURGE
handling. Key submissions use configured identity values, without an encoded
key input. Existing JSON submission/status endpoints remain available.
`cache_purge_key`, `cache_purge_url` and `cache_purge_tag_namespace` are obsolete
and rejected as unknown directives.

`cache_purge_tag` is an inherited complex value at HTTP, server or location scope.
With the expr module it also supports `when` blocks, including selection during
successful 304 updates. TAG values are comma separated, case sensitive, trimmed
and deduplicated; an empty value clears the new entity's TAG set. A normal HIT
retains the selected representation's recorded TAGs.

Each worker allows at most 64 reserved fills and detached publications combined,
within a 2 MiB logical material budget. Response headers reserve a slot and
bounded storage before nginx writes the cache file. One 128 KiB foreground query
frame remains available within that budget. The three-second publication IPC
deadline starts after the complete body is transferred to the background job.
Normal HITs are unaffected. Request inputs and each publication payload are
separately bounded to 128 KiB; these limits exclude allocator overhead and native
nginx buffers.
The existing publication pool also performs file and directory synchronization;
it remains separate from the proof-read pool. It uses owned paths and FDs,
without reading RocksDB or retaining an HTTP request. The fixed job context
keeps two root lengths and an instance ID; no new thread or index table is used.
Cache-file publication uses up to four reusable IPC clients per worker. It
selects the least busy client after the response body is complete, and keeps
that client for ACQUIRE/PREPARE/FINISH and retries. Selection rotates equal-load
choices, including before deferred finalizers submit their first RPC and while
other jobs wait for file I/O. The component
connection budget and worker count can reduce the pool size. Its approximately
512 KiB request-byte budget is divided across the pool, and the existing 64
publication slots bound aggregate pending work, including detached 304 jobs.
Background removal synchronizes the pinned parent directory after revoking the
file proof and unlinking the instance. If the parent is already missing, it
synchronizes the surviving ancestor chain instead. A failed directory barrier
leaves the retired instance available for retry.
Management calls and object reads retain their separate clients when a
publication connection share is available; otherwise publication reuses the
original control client. This lets other publications progress while one FINISH
waits for WAL synchronization without adding threads, polling
or a weaker READY condition. Idle publication connections close after one second.
The jobs do not retain the HTTP request, so keepalive reuse does not wait for
publication or 304 persistence. Adopted publication rename/stat and cleanup run
on the configured publication thread pool; native cleanup of rejected fills and
end-to-end performance remain separate validation work.
Within a worker, complete fills for the same pinned native file and configured
identity share an active publication when its purge watermark is at least as
new. Duplicate files use the existing bounded asynchronous cleanup without
another publication RPC. A fill started after a newer purge keeps its own
publication; detached 304 jobs are excluded. This does not coalesce work across
workers or increase publication limits.
If the publication queue or its memory budget is already full when response
headers arrive, the module skips caching before nginx creates the cache file.
The response is still served, and existing HITs are unaffected. Later fills
cannot consume a slow response's reserved slot or material storage. The request
releases unused reservations on cleanup; completed fills transfer ownership to
the existing asynchronous publisher. File-descriptor exhaustion, failed bodies
and native proxy buffering remain separate cleanup boundaries.
Generation namespaces, legacy zone/key PURGE syntax, REFRESH handlers, directory
walks and the old shared-memory task queue have been removed; obsolete directives
are rejected rather than silently ignored. New captures do not add arbitrary
request URL/Host aliases.
The backend rejects identity changes and no longer has alias observation,
pagination or revision APIs. This does not complete foreground database isolation.

## Current features

- Persistent URL, key, host, prefix and scoped TAG lookup across cache zones.
- Durable Invalidate/Delete acceptance and background file processing.
- PURGE on business or dedicated channels, using independently configured identity.
- Immediate request admission barriers and protection against old cache writers.
- Slice/Vary associations, conditional validation and restart recovery.
- Bounded database, IPC and shared-control memory budgets.

The default and only build uses RocksDB. Existing legacy configurations must
migrate to explicit index expressions and the current management interfaces. Propagation,
SCM integration and whole-system resource acceptance remain under development.

## Build

First build the storage, index, and component libraries on Linux
with RocksDB and OpenSSL development packages. RocksDB must provide its CMake
targets. The unified test runner requires Python 3.9+ and CMake 3.21+ for JUnit
reporting:

```sh
cmake -S . -B /tmp/cache-purge-store -DCACHE_PURGE_STORE_TESTS=ON
cmake --build /tmp/cache-purge-store
ctest --test-dir /tmp/cache-purge-store --output-on-failure
```

Its blocking APIs belong on the owning writer or attached read-only thread.
They must not run on an HTTP worker's event loop.

To build the runtime into nginx, supply the directory containing
the CMake archives when configuring the addon:

```sh
CACHE_PURGE_ROCKSDB_LIB_DIR=/tmp/cache-purge-store \
    ./configure --with-threads \
                --add-module=/path/to/ngx_http_cache_key_module \
                --add-module=/path/to/ngx_http_cache_purge
make
```

For dynamic modules use `--add-dynamic-module` and `make modules`. The build
from this repository produces two module files. For component-based identities,
compile with the key addon present and load that project's module before them:

```nginx
load_module modules/ngx_http_cache_key_module.so;
load_module modules/ngx_http_cache_rocksdb_module.so;
load_module modules/ngx_http_cache_purge_module.so;
```

The key addon is optional: native keys may use any format. Purge reads its
separate identity expressions and has no dependency on the key addon's C API
or relative load order.

Register the persistent purge addon after other header-filter addons, or load its
dynamic module last among them. Its admission filter must run before slice and
other filters modify response state. Startup and `nginx -t` reject an unsafe
filter order when the store/state pair is configured.

The module requires nginx 1.31.7 or newer for complex-value script status
propagation. Older nginx versions are rejected at configure time; loading a
module alone cannot prove that its cache-admission guards work. HTTP cache
support and `--with-threads` are required; configure rejects builds without them.
The persistent runtime also requires Linux and
`master_process on`. It uses standard worker initialization, timers, and exit
callbacks. It does
not require privileged-agent support or an additional nginx core patch. Only
worker 0 starts the component writer and one read-only executor; all workers use
bounded asynchronous IPC. Both share that worker's address space, failure domain
and the same RocksDB instance, CF handles, block cache and write-buffer manager.
No second DB is opened. Database handles and threads are never created before
fork.

On graceful reload, the old worker requests asynchronous executor shutdown so
long-lived HTTP responses do not keep the database owner indefinitely. Its
writer finishes the current operation, stops and joins its reader, then closes
the DB before the replacement opens it. The old worker can continue sending its
response and joins the stopped
executor when it exits. The component C ABI is 13 and the internal IPC version
is 2; rebuild its archives together with the addon. Upgrading from an earlier
component ABI or shared-control ABI 5
or earlier requires a full nginx restart; persisted cache files and indexes are
retained, but incompatible index schemas require a separate migration before
the new binary can open them. Old IPC frames are rejected before decoding.
nginx-ext ABI remains 1.
Override `CACHE_PURGE_ROCKSDB_LIBS` to use
a fixed RocksDB archive and its compression/runtime dependencies. The default
links the system shared RocksDB library. Static archives linked into a dynamic
nginx module must be built with PIC.

The implemented resource directives are:

```nginx
thread_pool cache_proof threads=1 max_queue=64;
thread_pool cache_publish threads=1 max_queue=64;

http {
    cache_purge_store /var/lib/nginx/cache-index cache=16m write_buffer=4m;
    cache_purge_state_zone purge_control 16m;
    cache_purge_read_pool cache_proof;
    cache_purge_publish_pool cache_publish;
    cache_purge_propagation_timeout 120s;
}
```

`cache_purge_read_pool` selects an nginx-managed pool for file proof reads;
`cache_purge_publish_pool` selects a separate pool for completed cache-file
publication. Both are required when a cached location enables capture. Selecting
the same pool is rejected. Keep both pools dedicated, with `max_queue=64` or
more; the example creates two threads per HTTP worker. Their memory and stacks
are additional to the database budget. Management-only configurations do not
need them. Neither directive selects a pool by default. If selecting nginx's
special `default` pool, configure its thread count explicitly.
Request-dependent directives support expr `when`; these HTTP-wide resource
selections are resolved at configuration time.

`cache` is the shared database cache budget, including the write-buffer charge;
it is not a process RSS limit. The database directory must be outside cache-file
directories and be a private directory owned by the configured worker UID.
New storage is created for that UID; existing agent-owned storage must be migrated
while stopped. Existing directories are not recursively chowned. Store and
state-zone identity changes require a full restart.
These directives initialize the component and its resources. Add an explicit
management location to expose the development HTTP API.

## PURGE requests

Enable any one of the four protocol switches in a location to handle `PURGE`
after nginx access controls. Normal requests still use their protocol's own
switch. PURGE is not forwarded to the upstream; disabled locations retain their
ordinary handler. Business and dedicated channels use the same durable
acceptance path and cover all matching zones. No cache zone or `proxy_pass` is
needed in a dedicated channel.

Control header names are independently configurable at http/server/location
scope and inherit. Defaults are:

| Directive | Default header | Value |
| --- | --- | --- |
| `cache_purge_type_header` | `X-Cache-Purge-Type` | `url` (default), `key`, `prefix`, `host`, `tag`, `host-tag` |
| `cache_purge_action_header` | `X-Cache-Purge-Action` | `delete` (default) or `invalidate` |
| `cache_purge_tags_header` | `X-Cache-Purge-Tags` | Comma-separated TAGs; any match, at most 64 |
| `cache_purge_operation_id_header` | `X-Cache-Purge-Operation-Id` | Optional stable operation ID |
| `cache_purge_issued_at_header` | `X-Cache-Purge-Issued-At` | Optional positive Unix timestamp in milliseconds |

Type and action values are case insensitive; TAGs are case sensitive. Duplicate
control headers, unsupported values and request bodies are rejected. The ID and
issued time must be supplied together. Omitting both generates a new ID and
uses the current time. For reliable retries after uncertain delivery, provide
both and keep them, the action and the target unchanged; conflicts return 409.
There is no module-side authentication.

Target values come only from configuration:

| Type | Target |
| --- | --- |
| `url` | Exact `cache_purge_key_url`, covering all indexed variants |
| `key` | Exact URL, misc and namespace tuple, covering all slices/Vary/zones |
| `prefix` | Literal byte prefix of configured URL; no glob or regex parsing |
| `host` | Host from the configured URL authority |
| `tag` | Configured namespace plus the TAG header |
| `host-tag` | Host from the configured URL plus the TAG header |

Only the required identity expressions are evaluated. Namespace TAG requests can
omit a URL input; their location still declares its URL expression. Empty misc
is one exact variant, and an empty namespace is one exact scope. The management
connection Host does not replace the configured target. No physical key or
filename is accepted or required.

A dedicated channel can select inputs entirely through configuration:

```nginx
location /purge/ {
    proxy_cache_purge on;
    cache_purge_key_url $http_x_cache_purge_url;
    cache_purge_key_misc $http_x_cache_purge_misc;
    cache_purge_key_namespace $http_x_cache_purge_namespace;
    allow 127.0.0.1;
    deny all;
}
```

The three identity header names above are configuration choices, not names
looked up by module code. A business channel can switch its misc expression to a
request header for PURGE using `map` or an expr `when` before its normal fallback.

```sh
curl -X PURGE http://127.0.0.1/purge/ \
    -H 'X-Cache-Purge-Url: https://example.test/video.mp4' \
    -H 'X-Cache-Purge-Type: key' \
    -H 'X-Cache-Purge-Misc: variant-a'
```

202 means the task is durable and its invalidation rules are active; it does not
wait for file processing. Closing the connection or restarting nginx does not
cancel accepted work. The JSON response contains the operation ID and progress.
A dedicated channel returns `status_url: null`; use a separately configured
`cache_purge_api` status endpoint with that ID. Internal key digests are not
exposed as user-entered keys. Normal managed HITs still use the database-backed
admission path while foreground isolation is being completed.

## Persistent management API

Propagation is one-way: L1 sends request headers to L2. L2 validates the
requirements, then serves a suitable cached response or applies Invalidate/Delete
and fetches upstream. Responses carry no propagation metadata or acknowledgement.
The configuration provides three independently inherited header-name directives:

```nginx
cache_purge_propagation_time_header X-Cache-Purge-Time;
cache_purge_propagation_mode_header X-Cache-Purge-Mode;
cache_purge_propagation_fence_header X-Cache-Purge-Fence;
```

`cache_purge_ignore_propagation on|off;` configures whether L2 should ignore
incoming propagation requirements; its default is `off`. With `NGX_EXPR`, it
uses the expression module's native flag slots and `when` blocks:

```nginx
expr ignore_l1_purge = $arg_ignore_l1 1;
when ignore_l1_purge {
    cache_purge_ignore_propagation on;
}
```

`when` is not a parameter of the directive. Expression candidates retain their
normal ordering and inheritance: the first matching or unconditional candidate
wins, and a child with only conditional candidates falls back to its parent.
Without the expression module, ordinary `on`/`off` remains available.

`cache_purge_propagation on|off;` independently selects outgoing propagation,
defaults to `off`, and uses the same native expression flag mechanism. Enable it
for a CDN parent route; leave it off for a final origin. For example, with a
deployment-provided `$upstream_type` variable:

```nginx
expr purge_parent str_eq $upstream_type parent;
when purge_parent {
    cache_purge_propagation on;
}
```

These two flags have independent inheritance and condition lists. An incoming
ignore decision does not disable this node's own outgoing requirements.
Header names must be distinct after inheritance, valid HTTP tokens, and
compatible with nginx's underscore handling; standard protocol/cache headers
cannot be reused as control headers. Capabilities exposes the effective names.

The control reader can collect bounded local propagation requirements and their
ledger identity from one immutable publication. If an object's TAGs are unknown,
it returns namespace/host-scoped TAG candidates for local index resolution.
Known empty TAGs remain distinct from missing metadata. The scope index reuses
operation records; it does not copy cache-file metadata into shared memory.
Output-budget exhaustion fails the lookup instead of silently truncating rules.
The sender now freezes a local requirement snapshot when the native proxy
`create_request` callback runs. Early rewrite/log reads do not create it. The
following variables read the same immutable snapshot:

```nginx
proxy_set_header X-Cache-Purge-Time  $cache_purge_propagation_time;
proxy_set_header X-Cache-Purge-Mode  $cache_purge_propagation_mode;
proxy_set_header X-Cache-Purge-Fence $cache_purge_propagation_fence;
proxy_set_header X-Cache-Purge-Namespace "";
```

Without the proxy filter framework, an enabled route must provide this complete
effective header group, using its configured names. Missing, duplicate, or
incorrect assignments fail before upstream I/O. Native `proxy_set_header` group
inheritance applies. With `NGX_RESTY_EXT && NGX_HTTP_PROXY_FILTER`, the module
updates the actual outgoing list automatically, removes duplicates, and checks
that another handler did not bypass or replace its fields. Both paths support
HTTP/1 and HTTP/2. FastCGI, SCGI, and uWSGI do not run the sender.

Native retries reuse their generated headers, including beyond the local
window. Later new requests omit expired active requirements. Published PREPARING
fences also propagate, closing the gap before first activation starts the clock.
Identical snapshots within a main request are shared across slice subrequests.
The sender coalesces matching operations into one maximum time per mode. Its
binary frame is always 20 bytes, or 28 base64 characters. Encoding uses stack
buffers; shared per-worker retained input/output data has a 2 MiB budget, plus
native request buffers and allocator overhead.

The NPF2 codec carries only the current object's Invalidate and Delete times.
TAGs, selectors, event IDs, ledger sequences and local windows are not sent.
Matching belongs to the sending node. Received times merge by mode and are not
interpreted as requests to enumerate other objects on the receiving node.
Indexed proxy requests now parse incoming requirements before cache use.
Duplicate, incomplete, or inconsistent fields return 400; the incoming ignore
flag discards them without affecting local rules. Admission compares the native
cache header's second-resolution `date` with the push time rounded down to a
second. Equal times are covered. No received requirement or extra timestamp is
written to the index.

Native `date` retains its response-processing semantics. An upper-tier request
started before an L1 push can finish afterward and store an older entity with a
later date if that tier has not received the push. The entity can then satisfy
L1's propagated requirement and refill L1. This cross-tier in-flight boundary is
accepted, along with the same-second boundary; `date` is not changed to request
start time. Local publication checks still reject the original pre-push writer
on a node that accepted the operation.

An older object is revalidated for Invalidate or fetched unconditionally for
Delete. An unmet incoming Delete suppresses conditional revalidation before an
expired object goes upstream. Accepted incoming requirements join local outgoing
requirements without a local expiry window.
Identical incoming envelopes are shared across slices of one main request.

Selected incoming Delete instances now enter the existing durable retirement
queue independently of the requesting client or a successful replacement.
Cleanup verifies the selected file and header, so delayed jobs cannot remove a
replacement or a successfully revalidated instance. Submission is bounded to
32 jobs and 256 KiB per worker, with a five-second retry deadline. Once retired,
normal persistent collection continues across executor restarts.

Shared rules resolve direct propagation requirements. Scoped TAG candidates
use the asynchronous request object query, with native representation identity
and current or retained membership.
The earlier file-only and history-only coordinator queries have been removed.
Their internal IPC opcode 31 is retired and rejected; opcode 32 remains the
request object query. Propagation results never grant cache admission.

Vary files retain the native main and variant hashes in their publication
metadata, including across conditional revalidation. Ordinary files keep the
original metadata size.

Proxy mutations preserve resolved object times, local deadlines and immutable
TAG memberships before replacement, TAG changes or collection. These records
keep the fixed key identity alive after the last file is collected. Incremental snapshot
queries match later local rules against retained memberships without loading all
metadata into memory. Native representation bindings also preserve access to
secondary Vary files after their main file is removed.

Legacy Vary records can recover missing hashes from a verified native cache
file, using the header layout supplied by the nginx build. Recovery streams the
key through a bounded buffer and verifies its digest, CRC and publication
identity; it never persists the raw key. Unresolved files remain explicit and
are retried in bounded batches, without rewriting unchanged rows or rescanning
the full index. A missing or conflicting file is not proof of an empty TAG set.

The index can enumerate distinct Vary definitions for a native main key, then
stream only the selected variant's current and retained TAG memberships from
the same snapshot. Definition scans seek past whole variant groups. Unfinished
migration and expired or corrupt snapshots return errors rather than a false
empty result; uncommitted file postings do not supply TAG memberships.

The coordinator matches the fixed request identity once and aggregates
selected memberships in bounded steps. It selects native Vary hashes from a transient snapshot of the
proxy request headers, preserving duplicate-header order and nginx's Accept-*
normalization. Headers are neither logged nor persisted. A control publication
change or expired snapshot discards the partial result and requires a retry.

Shared controls support both exact raw TAG input and fixed 16-byte SHA-256
prefixes. Each TAG membership has separate raw and digest lookup entries built
by the publisher; neither request lookup computes SHA-256. Both retain the same
namespace/host scope, group deduplication, output budgets and original operation
callbacks. Added entries and digest bytes count against the existing shared-bank
limit. Digests are internal matching material, not authentication or L2 headers.
NCF2 files carry these TAG digests. Ordinary HTTP HITs match them against shared
rules after the independent proof read; retained TAG lookup on MISS still uses
the database executor.
An empty namespace is a valid exact scope, distinct from every named namespace;
it never falls back to the request host or means all namespaces.

The per-key identity row contains five unsigned 64-bit values (file count,
URL, host, namespace and misc dictionary IDs) and the 32-byte internal digest.
Its 72-byte value needs no separate text dictionary record for that digest.
A fill cannot
change the identity of an existing key; a conflicting batch rolls back entirely.
One URL may still select multiple misc/namespace keys, with multiple zones,
slices and Vary files under each key. The URL posting is
`A | url_id | namespace_id | misc_id -> key_id`, with 25-byte keys and 8-byte
values; IDs are fixed-width big-endian integers. Misc bytes remain in the shared
dictionary, and enumeration order is by ID rather than by misc text.
Host-to-key postings remain;
the redundant reverse alias rows and alias revision counters have been removed.
Internal opcode 12 is retired and rejected without renumbering later operations.

Retention before a file mutation matches its fixed identity and TAGs in one
bounded executor call. Retained-history expiry reads the owning key's immutable
identity and checks later rules before collection. Neither path allocates alias
scan slots, stores pending alias results or extends a propagation deadline.
Dictionary references remain until both files and retained memberships release
the key; unowned-key collection removes its identity atomically.

Managed proxy requests resolve propagation associations only at the first-send
gate. A HIT does not query the representation index before reading its cache
file. When the serving control image has no outgoing requirements, the send gate
also skips the object lookup. Each reader retains only the image version and
latest propagation deadline. Active and PREPARING key, URL, prefix and host
rules are matched in shared memory before allocating an IPC payload. A complete
match with no relevant TAG candidate skips the executor, including when TAG
rules exist only in other namespaces or hosts. Matching is bounded to 64
results and 64 KiB; a relevant TAG candidate, limit or image change discards
partial results and uses the complete object query.
The gate rechecks the accepted sequence before sending request bytes and
preserves incoming requirements. This does not bypass local cache admission:
ordinary HITs use file proofs and shared rules through the selected read pool.
Retained TAG queries use the read-only executor. Fill transactions and durable
304 validation use the writer through independent background jobs. Complete
foreground performance acceptance still requires mixed-load measurements.

When a lookup is needed, one IPC request holds its bounded input until completion,
disconnect or the original deadline. The read endpoint admits at most four object
queries and advances eight steps per dispatch, allowing other peers to run.
Between batches each object retains one snapshot cursor and releases its
traversal cursors. Resumption seeks past the last returned row using the same
snapshot and deadline. Identity lookup and native-readiness checks use that same
snapshot, including while the writer replaces files. Each reader/writer handle
has up to eight cursors with the existing one-second lifetime; these are separate
budgets, at most sixteen handles with defaults. No full association index is
loaded into memory.
The read endpoint has at most eight connections, 128 KiB per request and 512 KiB
of connection buffers. Its single thread has a 256 KiB stack. Each of four
queries retains at most the existing 1 MiB index-query budget. These are bounded
allocations, not a statement of total RSS. Workers lazily create a separate read
IPC client with the existing 64-call/512 KiB client limit. `read.sock` accepts
only object propagation and diagnostic PING; mutation commands are rejected.
The read-only bridge cannot write, sync, inspect writer tickets or create other
readers. The writer cannot close while its reader exists. Disk read latency and
shared CPU/cache/I/O contention remain possible, but background file sync does
not hold the read dispatch queue.
Workers retry transient failures within the existing three-second deadline;
unresolved failures return 503 rather than treating missing results as empty.

Known current or retained edge TAG associations supply the outgoing object's
timestamps. Completely unknown objects proceed upstream without TAG-specific
retry or broader invalidation. Other slices and Vary variants do not contribute
their TAGs. Request headers are used only for local native Vary selection; TAGs
are never forwarded to L2. Real HTTP tests cover cross-zone selection, retention
through deletion and restart, 304 TAG replacement, and slice isolation.

Before the first upstream send, managed proxy requests check the accepted
operation sequence again. A newer operation causes a bounded asynchronous object
query through the existing upstream output filter. The module refreshes only its
control headers in the unsent HTTP/1 or HTTP/2 request and resumes native output;
this does not require a new nginx core hook. Unchanged sequences take the direct
path. Maintenance publication changes with the same ledger and operation sequence
preserve the completed result. Existing request bodies and native retry behavior
remain owned by nginx.

Send refresh shares the 64-waiter and 2 MiB propagation budgets, with a separate
three-second phase deadline. Timeout and cancellation release the waiting state;
unresolved requirements cannot be sent as empty headers. Origin admission records
the operation watermark immediately before the first send attempt, retaining the
older writer pin until completion. DNS-gated HTTP tests accept real TAG operations
after request creation and verify refreshed headers and new cache publication.
An origin response begun before local acceptance can finish, but its original
local writer cannot publish an old reusable entity.

A transient cache-entry publication conflict can skip the initial object query.
That BYPASS path resolves its requirements at the first-send gate and retries an
unavailable initial writer registration within the same three-second deadline.
It does not send an incomplete requirement or turn a transient missing snapshot
into an immediate request-creation error.

Proxy locations with `proxy_cache_purge on` also resolve propagation when
`proxy_cache` is off, a variable zone resolves to off, or the method is excluded
from caching. They use scratch key storage and the native key builder to
select current or retained slice/Vary associations, without opening cache files
or publishing index records. Independent identity expressions do not evaluate
unused key-module components. No key-module capture API is called. When using the key
module, use a version that confines automatic native capture to key creation
before upstream request buffers exist; a later scratch evaluation must not
retain a temporary cache context.

An uncached proxy that handles or forwards propagation requirements also needs
an effective native `proxy_cache_key` for representation selection, plus the
three independent identity expressions. These inputs can be inherited from HTTP
or server scope. Native key serialization is unrestricted.

A module-owned adapter at the existing post-create `proxy_bind` value boundary
installs the output gate; no extra bind directive or core patch is required.
It preserves the original binding expression and properties. Static binding
addresses pass through native parsing. Incoming requirements can cross an
uncached proxy hop; disabling propagation strips them on the origin leg.
An ignored input does not contribute a requirement, and malformed accepted input
is rejected before dispatch. No response acknowledgement is added.

Full propagation remains unfinished. File validation and retained-history expiry
use fixed identities; Vary and membership enumeration remains bounded.
Per-step matching budgets and shared cursor capacity still require loaded
performance and progress validation. Source tracing places the inspected
Edgeresty parent/origin selection before native proxy content. A precontent
probe and real three-tier HTTP/1 and HTTP/2 tests cover that timing, propagation,
304, failed revalidation, an uncached middle tier, restart, and the accepted
native-date in-flight boundary. They do not execute the complete production Lua
deployment. Broader body/retry/cancellation and concurrency combinations, full
deployment integration, and loaded memory/latency acceptance remain open.
Capabilities reports the sender transport as
`filter` or `variables`, while `proxy_propagation_ready` remains false.
The protocol has no module-side authentication, signatures, key management,
or response verification exchange.

```nginx
location /__scm/cache/v2/ {
    allow 127.0.0.1;
    allow ::1;
    deny all;

    cache_purge_api;
    cache_purge_tag_namespace_header X-Cache-Purge-Namespace;
}
```

The module does not authenticate management requests or check client addresses.
Restrict access using nginx listeners, `allow`/`deny`, or other configured access
controls. Event metadata is caller-supplied identity, and request digests support
idempotency; neither is an authentication proof. There is no management event
signature field. L1/L2 validation has a separate contract.

`cache_purge_api` requires a plain prefix location ending in `/`, an initialized
store and state zone, and no other content handler. It exposes:

- `GET`/`HEAD capabilities`: current readiness, limits, and control header names.
- `POST operations`: durably accept and activate an operation, then return 202.
- `GET`/`HEAD operations/{operation_id}`: read its receipt and cleanup progress.

Example request body:

```json
{
  "version": "ngx-http-cache-purge/v2",
  "operation_id": "push-1042",
  "mode": "invalidate",
  "selector": {"type": "url", "value": "https://example.test/video.mp4"},
  "event": {
    "issuer": "scm",
    "stream_id": "cache-purge",
    "sequence": "1042",
    "issued_at": "2026-10-27T10:00:00Z"
  }
}
```

JSON selectors include `url`, `host`, URL `prefix` and scoped `tag`. The optional
prefix `field` must be `url`. Whole-key and key-prefix submissions return 412;
use PURGE with configured URL/misc/namespace for exact key operations. Existing
accepted tasks retain their stored semantics and status access. Matching uses
the index across zones. Binary selector values
use `encoding: base64`. TAG accepts `values: ["tag-a", "tag-b"]` with
`match: any` and `scope: namespace|host`;
the scope value comes from the configured namespace header. Its default name is
`X-Cache-Purge-Namespace`, and the directive inherits at http/server/location
scope. Header renaming does not change the operation's semantic identity.

The API accepts at most 64 input TAG values, each at most 1024 bytes. After
sorting and deduplication, their internal encoding must fit 32768 bytes. A
duplicate singleton normalizes to the original single-TAG operation. Array order
and duplicate values do not change idempotency. With `encoding: base64`, each
array member is encoded separately. Only `match: any` and exact hosts are
supported; subdomain expansion remains pending.
Bodies are bounded at 256 KiB. Duplicate or unknown JSON fields, duplicate scope
headers, and malformed UTF-8/base64 are rejected. The module reserves an in-memory
body buffer and rejects file-only request buffering for API locations.

Every selector uses the same acceptance and background processing path,
including exact URL and key operations. 202 confirms durable acceptance and
active invalidation, without waiting for file processing. There is no separate
synchronous file-processing API for small operations. Disconnecting does not
undo an accepted operation. Reuse the same operation ID and semantic input
after an uncertain response; changed input returns 409. The acceptance deadline
is five seconds, and a timeout does not
prove that no submission occurred. Query the status endpoint or retry that ID.
Capabilities currently report proxy propagation as unavailable.

## Usage

The independent key module can be wired through configuration. Configure the
store and state zone as shown above, then reference its outputs explicitly.

```nginx
proxy_cache_path /var/cache/nginx keys_zone=cache_zone:4m;

server {
    listen 80;

    location / {
        proxy_cache cache_zone;
        proxy_cache_key $cache_key_full;
        cache_purge_key_url $cache_key_url;
        cache_purge_key_misc $cache_key_misc;
        cache_purge_key_namespace $cache_key_namespace;
        proxy_cache_purge on;
        proxy_pass http://origin;
    }

    location /__cache_purge/ {
        allow 127.0.0.1;
        deny all;
        cache_purge_api;
    }
}
```

Submit Invalidate/Delete operations to the management API. Authentication and
source restrictions are nginx configuration responsibilities.

## Tests and release artifacts

`make test` builds and runs the persistent backend suites. `make test-http`
also checks worker ownership and IPC, native fill ownership, and the complete
HTTP capture/purge suite. Run HTTP tests as an ordinary user:

```sh
make test-http NGINX_SOURCE=/path/to/nginx-git \
    CACHE_KEY_SOURCE=/path/to/ngx_http_cache_key_module \
    REPORT_DIR=/tmp/cache-http-results
make test-sanitize NGINX_SOURCE=/path/to/nginx-git \
    CACHE_KEY_SOURCE=/path/to/ngx_http_cache_key_module \
    REPORT_DIR=/tmp/cache-http-sanitized
```

Choose an empty report directory for each run. Failures stop the runner, retain
logs, and mark `run.json` as failed; an empty backend report is not success.
The nginx input is an unchanged Git source snapshot. The optional `--expr`
argument to `t/persistent_runtime.py` adds the expression module to HTTP tests.

The Dockerfile is only a disposable test environment, with no database service.
It uses a named `cache-key` build context so a local checkout can be tested
without publishing it. CI uses a pinned Git commit, which must exist remotely:

```sh
docker build -f t/Dockerfile --target integration \
    --build-context cache-key=/path/to/ngx_http_cache_key_module \
    -t cache-purge-test .
mkdir /tmp/cache-container-results
docker run --rm --network none --user "$(id -u):$(id -g)" \
    -e TEST_DYNAMIC=true \
    --mount type=bind,src=/tmp/cache-container-results,dst=/reports \
    cache-purge-test
```

Legacy TAP, generation, directory-walk and old queue fixtures are removed.
The manual benchmark workflow measures the current control table only; its
results do not represent HTTP or physical deletion throughput. PVS traces both
the C backend/C++ bridge and configured HTTP objects. Release builds package
both `.so` files, compile against the pinned key API, and exercise an actual
fill, HIT and durable Delete using those packaged files before comparing
repeat-build archive bytes. Set `CACHE_KEY_SOURCE` to a local key-module Git
checkout for local package verification; otherwise the builder fetches the
pinned revision. The key module remains a separate project's deliverable.
The target host needs the RocksDB shared library recorded
in `BUILD-INFO.txt`; it is not a standalone service.

Runtime regression tests and benchmark programs are under `t/`. Design, rollout,
and validation records are maintained outside this repository. The independent
`ngx_http_cache_key_module` supplies the new component-based cache identities.

## License

See [LICENSE](LICENSE). Original authorship notices remain in the source files.
