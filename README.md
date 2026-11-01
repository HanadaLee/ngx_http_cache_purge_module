# ngx_http_cache_purge_module

Delete or invalidate HTTP cache entries across nginx cache zones. A persistent RocksDB index locates matching files, while shared rules take effect before background file processing completes.

The repository includes `ngx_http_cache_purge_module`, `ngx_http_cache_rocksdb_module`, and `ngx_http_cache_key_module`. It supports proxy, FastCGI, uwsgi, and scgi caches. Proxy caches can also propagate purge requirements to a CDN parent. The key module builds cache keys from explicit components and exposes their values independently.

## Synopsis

```nginx
http {
    cache_purge_index_store /var/cache/nginx/index;
    cache_purge_state_zone purge 1m;

    proxy_cache_path /var/cache/nginx/content keys_zone=content:1m;

    server {
        listen 127.0.0.1:8080;
        server_name example.test;

        location / {
            cache_key_namespace "";
            cache_key_scheme $scheme;
            cache_key_host $host;
            cache_key_uri $request_uri;
            cache_key_misc "";
            cache_key_range "";

            proxy_cache content;
            proxy_cache_key $cache_key_full;
            proxy_cache_revalidate on;

            cache_purge_key_url $cache_key_url;
            cache_purge_key_misc $cache_key_misc;
            cache_purge_key_namespace $cache_key_namespace;
            cache_purge_tag $upstream_http_cache_tag;
            proxy_cache_purge on;

            proxy_pass http://127.0.0.1:8081;
        }

        location /__cache_purge/ {
            cache_purge_api;
        }
    }
}
```

This example explicitly defines all six key components using their default values. Adjust the `cache_key_*` directives when those inputs need to change.

Delete all indexed variants of `/asset`:

```sh
curl -i -X PURGE http://127.0.0.1:8080/asset -H 'Host: example.test'
```

A `202 Accepted` response confirms that the task is durable and its shared rule is published. File processing continues in the background. Use the returned `operation_id` with `/__cache_purge/operations/ID` to check completion.

## Installation

Requires Linux, nginx with HTTP cache support, CMake, a C++17 compiler, and the RocksDB development package. Build the backend libraries first:

```sh
module=/path/to/ngx_cache_purge
cmake -S "$module" -B "$module/build"
cmake --build "$module/build"
export CACHE_PURGE_ROCKSDB_LIB_DIR="$module/build"
```

Build nginx statically with `--add-module=/path/to/ngx_cache_purge`, or dynamically with `--add-dynamic-module=/path/to/ngx_cache_purge`. A dynamic build produces three modules:

```nginx
load_module modules/ngx_http_cache_key_module.so;
load_module modules/ngx_http_cache_rocksdb_module.so;
load_module modules/ngx_http_cache_purge_module.so;
```

Load RocksDB before purge. The examples use the bundled key module; it has no link dependency on either module and can be omitted when configuration does not reference it. Build the modules and backend libraries from the same source revision. The initial RocksDB bridge ABI and private storage formats are version 1. This implementation has not yet been released; development snapshots do not define an upgrade compatibility contract.

For the key module alone, use `--add-module=/path/to/ngx_cache_purge/key` or `--add-dynamic-module=/path/to/ngx_cache_purge/key`. This build needs neither RocksDB nor HTTP cache support. Do not add `key/` separately when adding the repository root.

The addon links with `-lrocksdb -lstdc++ -lm -pthread` by default. Set `CACHE_PURGE_ROCKSDB_LIBS` when a different link configuration is needed, such as static RocksDB with its compression dependencies.

Build `ngx_expr_module` statically before this addon in the nginx configure arguments to enable `when` blocks. Purge identities are configured through expressions, without linking to the key module or parsing its key format.

`NGX_RESTY_EXT` enables strict Invalidate handling. Automatic proxy request-header integration additionally requires `NGX_HTTP_PROXY_FILTER`. Ordinary nginx uses its native stale policy and the propagation variables described below.

## Conditional configuration

Location-level switches, identity expressions, TAG expressions, and control-header names support the expr module's `when` blocks in `http`, `server`, and `location`. `when` is a configuration block, not an extra directive parameter.

For example, select the misc value from a management header for PURGE requests:

```nginx
http {
    expr purge_request str_eq $request_method PURGE;

    server {
        location / {
            when purge_request {
                cache_key_misc $http_x_cache_purge_misc;
            }

            cache_key_misc $cookie_variant;
            proxy_cache_key $cache_key_full;
            cache_purge_key_url $cache_key_url;
            cache_purge_key_misc $cache_key_misc;
            cache_purge_key_namespace $cache_key_namespace;
        }
    }
}
```

The native cache key must represent the same variant as the configured purge identity. These directives describe index fields; they do not change `proxy_cache_key` or the corresponding key of another protocol.

Values are inherited unless overridden. A child `cache_purge_tag` replaces the inherited list; `when` selects a complete list using the expr module's candidate order. `cache_purge_index_store`, `cache_purge_state_zone`, `cache_purge_propagation_timeout`, and `cache_purge_api` do not support `when`. None of these directives supports nginx rewrite `if` blocks.

## Cache key components

The six directives accept one complex value in `http`, `server`, and `location`. With `ngx_expr_module`, all six also support `when` blocks. The first matching candidate wins; a child containing only conditional candidates falls back to its parent. Place conditional candidates before an unconditional value.

| Directive syntax | Default | Read-only output |
| :--- | :--- | :--- |
| `cache_key_namespace value;` | `""` | `$cache_key_namespace` |
| `cache_key_scheme value;` | `$scheme` | `$cache_key_scheme` |
| `cache_key_host value;` | `$host` | `$cache_key_host` |
| `cache_key_uri value;` | `$request_uri` | `$cache_key_uri` |
| `cache_key_misc value;` | `""` | `$cache_key_misc` |
| `cache_key_range value;` | `""` | `$cache_key_range` |

An empty namespace is valid and never falls back to the host. Nonempty namespaces accept up to 255 printable ASCII bytes, excluding spaces and commas. Other components preserve their input bytes. Argument filtering and URL normalization belong in the configured expressions; the module does not perform them automatically.

| Derived variable | Value |
| :--- | :--- |
| `$cache_key_url` | Scheme, `://`, host, and URI |
| `$cache_key_base` | Encoded namespace, scheme, host, URI, and misc |
| `$cache_key_full` | The base fields followed by the encoded range, including an empty range |

Base and full keys separate fields with the byte `0x1f`. A backslash inside a field becomes two backslashes; a field's own `0x1f` becomes the four printable bytes `\x1F`. These keys are binary. Use the individual component variables when constructing display strings or response headers.

```nginx
cache_key_namespace "";
cache_key_scheme $scheme;
cache_key_host $host;
cache_key_uri $request_uri;
cache_key_misc $cookie_variant;
cache_key_range $slice_range;

proxy_cache_key $cache_key_full;
cache_purge_key_url $cache_key_url;
cache_purge_key_misc $cache_key_misc;
cache_purge_key_namespace $cache_key_namespace;
```

Omit the range directive when the slice module is unavailable. The purge expressions explicitly associate the two modules; purge can use other business variables instead.

Outputs evaluate the current input values when nginx reads them. One assembled output evaluates each needed component once; values are not frozen across request phases. Use stable inputs for a cached representation, especially when the same values are read later for indexing or display. The module does not capture native cache state, install bypass predicates, or expose a snapshot C API.

Direct references from component expressions to this module's outputs are rejected during configuration. Indirect cycles and invalid dynamic namespaces log an error and fail variable evaluation. As with ordinary nginx variables, a caller may treat that failure as an empty value; it does not guarantee an HTTP error or prevent caching. Validate dynamic inputs in configuration rather than relying on this module as a request guard.

## Purge directives

### cache_purge_index_store

| Syntax: | **cache_purge_index_store** `path` [`cache=size`] [`buffer=size`] [`io_rate=size`]; |
| :--- | :--- |
| Default: | — |
| Context: | `http` |

Configures the persistent index. Use together with `cache_purge_state_zone`. Only one store is allowed, and its directory must be outside every native cache directory; neither path may contain the other.

| Parameter | Default | Meaning |
| :--- | :--- | :--- |
| `cache` | `16m` | RocksDB cache budget, including write-buffer accounting; at least `4m`. |
| `buffer` | `4m` | Write-buffer budget; at least `1m` and at most half of `cache`. |
| `io_rate` | `16m` | Combined RocksDB flush and compaction I/O limit in bytes per second. `0` disables the limit; other values must be at least `1k`. |

These budgets do not cap total process memory or the operating system's page cache. Ordinary index reads and WAL writes are not limited by `io_rate`. Changing the store path requires a full nginx restart.

### cache_purge_state_zone

| Syntax: | **cache_purge_state_zone** `name size`; |
| :--- | :--- |
| Default: | — |
| Context: | `http` |

Allocates shared memory for active rules, queued work, and completion state. The minimum size is `1m`. Configure one zone together with `cache_purge_index_store`.

At most 256 selector slots are available across tasks. Each TAG in a batch consumes one slot; other selectors consume one slot per task. The byte budget can limit admission before that count is reached. Changing the zone name or size requires a full restart.

### proxy_cache_purge

| Syntax: | **proxy_cache_purge** `on` \| `off`; |
| :--- | :--- |
| Default: | `proxy_cache_purge off;` |
| Context: | `http`, `server`, `location`, `when` |

Enables indexing and purge handling for proxy cache requests. Requires `cache_purge_index_store`, `cache_purge_state_zone`, and an explicit `cache_purge_key_url` expression.

Also accepts PURGE requests in the location. A dedicated purge location can enable this switch without `proxy_pass` or a selected cache zone; selection uses the shared index across zones.

### fastcgi_cache_purge

| Syntax: | **fastcgi_cache_purge** `on` \| `off`; |
| :--- | :--- |
| Default: | `fastcgi_cache_purge off;` |
| Context: | `http`, `server`, `location`, `when` |

Enables the same indexing and purge behavior for FastCGI caches, with the same configuration requirements as `proxy_cache_purge`. Parent propagation is not supported for FastCGI.

### uwsgi_cache_purge

| Syntax: | **uwsgi_cache_purge** `on` \| `off`; |
| :--- | :--- |
| Default: | `uwsgi_cache_purge off;` |
| Context: | `http`, `server`, `location`, `when` |

Enables the same indexing and purge behavior for uwsgi caches, with the same configuration requirements as `proxy_cache_purge`. Parent propagation is not supported for uwsgi.

### scgi_cache_purge

| Syntax: | **scgi_cache_purge** `on` \| `off`; |
| :--- | :--- |
| Default: | `scgi_cache_purge off;` |
| Context: | `http`, `server`, `location`, `when` |

Enables the same indexing and purge behavior for scgi caches, with the same configuration requirements as `proxy_cache_purge`. Parent propagation is not supported for scgi.

### cache_purge_key_url

| Syntax: | **cache_purge_key_url** `value`; |
| :--- | :--- |
| Default: | — |
| Context: | `http`, `server`, `location`, `when` |

Defines the URL identity used for indexing and PURGE selection. It must evaluate to an absolute HTTP or HTTPS URL, at most 32,768 bytes. An explicit expression is required wherever purge handling can be enabled; it does not inherit the native cache-key expression automatically.

Use consistent URL normalization for cache fills and purge requests. URL and prefix comparisons use the configured bytes; host selection uses the lowercase host extracted from the URL. The identity must exclude slice/range suffixes so that all slices share the same selection scope.

### cache_purge_key_misc

| Syntax: | **cache_purge_key_misc** `value`; |
| :--- | :--- |
| Default: | `cache_purge_key_misc "";` |
| Context: | `http`, `server`, `location`, `when` |

Defines the additional variant identity, at most 32,768 bytes. A `key` purge matches URL, misc, and namespace exactly, then covers their indexed slice and Vary files. The module does not accept a serialized native key or an MD5 cache filename.

### cache_purge_key_namespace

| Syntax: | **cache_purge_key_namespace** `value`; |
| :--- | :--- |
| Default: | `cache_purge_key_namespace "";` |
| Context: | `http`, `server`, `location`, `when` |

Defines the namespace stored with an indexed object and used by `key` and `tag` purges. Indexed namespace values may contain at most 32,768 bytes. An empty namespace is an exact scope; it does not fall back to the request host.

For a header-supplied namespace, configure an expression such as `$http_x_cache_purge_namespace`. There is no separate built-in namespace control header.

### cache_purge_tag

| Syntax: | **cache_purge_tag** `value ...`; |
| :--- | :--- |
| Default: | `cache_purge_tag "";` |
| Context: | `http`, `server`, `location`, `when` |

Defines TAGs associated with cached responses. Arguments can contain variables and are joined into one comma-separated list:

```nginx
cache_purge_tag $upstream_http_cache_tag extratag1 extratag2;
```

Each response may have up to 128 nonempty items, each at most 1,024 bytes. Empty items are ignored and surrounding spaces and tabs are trimmed. The count limit applies before deduplication. TAG names are compared exactly and must not contain commas, NUL, CR, or LF.

### cache_purge_type_header

| Syntax: | **cache_purge_type_header** `name`; |
| :--- | :--- |
| Default: | `cache_purge_type_header X-Cache-Purge-Type;` |
| Context: | `http`, `server`, `location`, `when` |

Names the PURGE selector header. Accepted values are `url`, `key`, `prefix`, `host`, `tag`, and `host-tag`; the default selector is `url`.

All header-name directives below accept a literal name of 1–255 ASCII letters, digits, hyphens, or underscores. Names cannot contain variables. Duplicate instances of a consumed control header are rejected.

### cache_purge_action_header

| Syntax: | **cache_purge_action_header** `name`; |
| :--- | :--- |
| Default: | `cache_purge_action_header X-Cache-Purge-Action;` |
| Context: | `http`, `server`, `location`, `when` |

Names the PURGE action header. Accepted values are `delete` and `invalidate`; the default action is `delete`.

### cache_purge_tags_header

| Syntax: | **cache_purge_tags_header** `name`; |
| :--- | :--- |
| Default: | `cache_purge_tags_header X-Cache-Purge-Tags;` |
| Context: | `http`, `server`, `location`, `when` |

Names the comma-separated TAG selector header for `tag` and `host-tag` purges. A request accepts 1–32 nonempty items, each at most 1,024 bytes. Trimming and empty-item handling follow `cache_purge_tag`; the 32-item limit applies before deduplication. Nginx's request-header buffer limits also apply.

TAGs are sorted and deduplicated before admission. Matching uses OR semantics, and overlapping TAGs affect each file once within the task.

### cache_purge_operation_id_header

| Syntax: | **cache_purge_operation_id_header** `name`; |
| :--- | :--- |
| Default: | `cache_purge_operation_id_header X-Cache-Purge-Operation-Id;` |
| Context: | `http`, `server`, `location`, `when` |

Names the operation identifier header for idempotent retries. Supply it together with the issued-at header, keeping both and the selector/action unchanged when retrying. IDs contain 1–128 ASCII letters, digits, hyphens, underscores, or periods. Reusing an ID for different work returns `409 Conflict`.

### cache_purge_issued_at_header

| Syntax: | **cache_purge_issued_at_header** `name`; |
| :--- | :--- |
| Default: | `cache_purge_issued_at_header X-Cache-Purge-Issued-At;` |
| Context: | `http`, `server`, `location`, `when` |

Names the purge timestamp header. The value is decimal Unix epoch milliseconds, at least `1000`, and is compared at native cache second precision. Omit both this header and the operation identifier to generate a new operation automatically. Supplying only one is rejected.

### proxy_cache_purge_propagation

| Syntax: | **proxy_cache_purge_propagation** `on` \| `off`; |
| :--- | :--- |
| Default: | `proxy_cache_purge_propagation off;` |
| Context: | `http`, `server`, `location`, `when` |

Enables outgoing purge headers for managed proxy requests. Enable it when the target is a CDN parent and leave it off when the target is the origin. Use `when` to select this behavior if routing depends on a condition. It does not control whether incoming propagation headers are honored.

### cache_purge_ignore_propagation

| Syntax: | **cache_purge_ignore_propagation** `on` \| `off`; |
| :--- | :--- |
| Default: | `cache_purge_ignore_propagation off;` |
| Context: | `http`, `server`, `location`, `when` |

When enabled, ignores incoming purge propagation headers. This can be selected conditionally with `when`. It does not disable locally submitted purge tasks or their rules.

### cache_purge_propagation_timeout

| Syntax: | **cache_purge_propagation_timeout** `time`; |
| :--- | :--- |
| Default: | `cache_purge_propagation_timeout 120s;` |
| Context: | `http` |

Sets how long a locally accepted purge contributes outgoing parent headers. The window is intended to allow invalidation to reach other cache tiers; it is not a deadline for background file processing. The parent compares cache timestamps without applying another propagation-age window.

### cache_purge_propagation_time_header

| Syntax: | **cache_purge_propagation_time_header** `name`; |
| :--- | :--- |
| Default: | `cache_purge_propagation_time_header X-Cache-Purge-Time;` |
| Context: | `http`, `server`, `location`, `when` |

Names the incoming and outgoing parent purge timestamp header, in Unix epoch milliseconds. It is separate from the issued-at header used to submit a management operation.

### cache_purge_propagation_action_header

| Syntax: | **cache_purge_propagation_action_header** `name`; |
| :--- | :--- |
| Default: | `cache_purge_propagation_action_header X-Cache-Purge-Action;` |
| Context: | `http`, `server`, `location`, `when` |

Names the incoming and outgoing parent purge action header: `delete` or `invalidate`. This is configured independently of `cache_purge_action_header`, although both default to the same name.

### cache_purge_api

| Syntax: | **cache_purge_api**; |
| :--- | :--- |
| Default: | — |
| Context: | `location` |

Installs a management handler in a prefix location. It accepts GET and HEAD requests for paths relative to that location:

| Path | Response |
| :--- | :--- |
| `capabilities` | Readiness, propagation timeout, and index-cleanup counters. |
| `operations/ID` | Admission and background-processing status for an operation. |

Task responses include `operation_id`, `effective`, `durable`, `effective_at`, `propagation_until`, and a `cleanup` object with `state`, `scan_done`, `visited`, `affected`, and `error`. Processing is complete when `cleanup.state` is `done`. Unknown IDs return `404`; unavailable admission or status processing returns `503`.

This handler does not accept JSON POST submissions. Use PURGE in an enabled location to submit work. The module performs no authentication; configure access control with nginx.

## Purge requests

PURGE requests have no body and can be sent to the original business location or a dedicated purge location. All selectors search the configured index across cache zones.

| Type | Matching scope |
| :--- | :--- |
| `url` | Exact configured URL, across all misc and namespace variants. |
| `key` | Exact configured URL, misc, and namespace, covering all indexed slice/Vary files. |
| `prefix` | Literal prefix of the configured URL, across all misc and namespace variants. |
| `host` | Host extracted from the configured URL, across paths, misc values, and namespaces. |
| `tag` | Any listed TAG within the configured namespace. |
| `host-tag` | Any listed TAG within the host extracted from the configured URL. |

For prefix purges, supply the prefix itself without a trailing wildcard. Regex selectors and physical cache-filename selectors are not supported.

The `delete` action removes matching files. The `invalidate` action expires them for upstream validation. With `NGX_RESTY_EXT`, only successful validation permits reuse: a 304 reuses the entity, a 200 replaces it, and an upstream failure cannot serve the invalidated entity. Enable the protocol's native cache-revalidation directive to allow conditional revalidation. Without the extension, ordinary nginx stale policy applies.

A dedicated endpoint can obtain identity fields from configured headers:

```nginx
location = /purge {
    proxy_cache_purge on;
    cache_purge_key_url $http_x_cache_purge_url;
    cache_purge_key_misc $http_x_cache_purge_misc;
    cache_purge_key_namespace $http_x_cache_purge_namespace;
}
```

Use this location in a server with the store and shared zone configured. These URL, misc, and namespace header names are chosen by the expressions; they are not built-in control headers.

```sh
curl -i -X PURGE http://127.0.0.1:8080/purge \
    -H 'X-Cache-Purge-Type: key' \
    -H 'X-Cache-Purge-Action: invalidate' \
    -H 'X-Cache-Purge-Url: http://example.test/asset' \
    -H 'X-Cache-Purge-Misc: language=en' \
    -H 'X-Cache-Purge-Namespace: assets'
```

Acceptance waits for task persistence and shared-rule publication, not file enumeration. Disconnecting or timing out does not cancel an accepted task. Use the operation ID and issued-at headers to retry the same operation reliably.

## Proxy propagation

Propagation is one-way: L1 sends a timestamp and action to L2 on an upstream request. L2 compares them with its native cache date and either serves the cache or applies Delete/Invalidate before obtaining a response. Responses carry no receipt or other propagation metadata. This protocol applies only to proxy caches.

With both `NGX_RESTY_EXT` and `NGX_HTTP_PROXY_FILTER`, the module sets outgoing headers through the proxy request-header framework. Otherwise, configure `proxy_set_header` in the managed parent-facing location:

```nginx
proxy_cache_purge_propagation on;
proxy_set_header X-Cache-Purge-Time $cache_purge_propagation_time;
proxy_set_header X-Cache-Purge-Action $cache_purge_propagation_action;
```

Use the configured propagation header names on both tiers and in these `proxy_set_header` directives. Incoming time and action headers must be present together. `cache_purge_ignore_propagation on` bypasses their interpretation.

TAG names are not sent upstream. Known TAG associations remain available for the propagation window after a local file is removed. A proxy MISS may query these retained associations in the background before sending upstream bytes; a cache HIT does not query RocksDB. A URL with no known TAG association has no TAG propagation requirement.

### Variables

| Variable | Value |
| :--- | :--- |
| `$cache_purge_propagation_time` | Applicable purge timestamp in Unix epoch milliseconds. |
| `$cache_purge_propagation_action` | Applicable action: `delete` or `invalidate`. |

Both variables are empty when propagation is disabled or no applicable requirement exists. Internal `$cache_purge_*_guard` variables are installed by the module and do not need manual configuration.

## Implementation

The native nginx cache manager owns the RocksDB index and processes tasks in bounded batches. HTTP workers use shared memory and an internal queue; they do not open RocksDB or load the entire index into memory. Ordinary cache HITs do not perform index I/O or wait for background file maintenance.

The index stores URL and TAG relationships with shared values and compact file references. Invalidate updates the native expiry field and the existing in-memory `purged` flag; no module-specific metadata is added to cache files, extended attributes, sidecars, or response headers. Only the RocksDB bridge uses C++; the purge implementation is C.

Normal restarts reuse the persistent index and pending tasks. Missing-file records are collected in the background, retaining known TAG relationships for propagation. The management API exposes cumulative `index_cleanup.checked`, `index_cleanup.retained`, and `index_cleanup.errors` counters; these counters survive reloads and reset on a full restart.

### Limits

- Indexing starts with managed cache fills. Existing unindexed files require an explicit migration or a fresh cache-key namespace when enabling the module.
- A full restart loses the in-memory `purged` flag. Expired files then follow ordinary nginx stale policy.
- Cache dates have second precision. An entry dated in the same second as a purge is treated as refreshed.
- A parent request started before a purge may finish afterward and receive a newer native cache date. This in-flight boundary is accepted.
- A first MISS can be stored after a purge has already completed without finding an indexed file. Preventing that response from becoming a later HIT is not implemented.
- Unknown TAG associations have no first-MISS propagation compensation. Large active rule sets and sustained high-TAG fills can affect throughput and tail latency.

## Tests

Build and run the standalone storage, catalog, and task tests:

```sh
cmake -S . -B build -DCACHE_PURGE_STORE_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

The HTTP runner builds an isolated nginx snapshot from a Git checkout and exercises PURGE admission, task status, cross-zone selection, native file operations, reloads, and cache-manager recovery:

```sh
python3 t/native_cache_runtime.py \
    --nginx /path/to/nginx-git-checkout \
    --extension 0 --backend /path/to/ngx_cache_purge/build \
    --public-module --no-thread-pool \
    --output /tmp/native-cache.json
```

Use `--extension 1` with nginx-ext to test strict Invalidate handling. `--dynamic`, `--slice`, and `--protocols` add separate-module loading, sliced caches, and FastCGI/uwsgi/scgi coverage. `--expr` and `--proxy-filter` take paths to optional modules. The public suite uses the bundled key module for variant and slice identities. Sanitizer runs require `--sanitize` and backend libraries built with `CACHE_PURGE_STORE_SANITIZE=ON`.

`t/persistent_runtime.py` also runs the key variable suite against the combined build. To verify a standalone key build, run `python3 t/key_runtime.py --nginx /path/to/nginx --output /tmp/key.json`; add `--without-cache`, `--dynamic`, or `--expr-source /path/to/expr` as needed.

`t/native_propagation_runtime.py` checks two cache tiers, propagation headers, retained TAG relationships, and the outgoing timeout using an already built nginx binary. `t/native_cache_benchmark.py` compares foreground latency, throughput, and process memory with and without indexing and background purges. Run either with `--help` for its options; benchmark results depend on workload and resource limits.

## Author and license

Maintained by Hanada. Based on ngx_cache_purge by FRiCKLE, Piotr Sikora, and Denys Denysov.

Licensed under the [BSD 2-Clause License](LICENSE).
