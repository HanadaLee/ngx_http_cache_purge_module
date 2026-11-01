# ngx_http_cache_purge_module

Delete or invalidate HTTP cache entries across nginx cache zones. A persistent RocksDB index locates matching files, while shared rules take effect before background file processing completes.

The repository includes `ngx_http_cache_purge_module`, `ngx_http_cache_rocksdb_module`, and `ngx_http_cache_key_module`. It supports proxy, FastCGI, uwsgi, and scgi caches. Proxy caches can also propagate purge requirements to a CDN parent. The key module builds cache keys from explicit components and exposes their values independently.

## Synopsis

```nginx
http {
    cache_purge_index_store /var/cache/nginx/index;
    cache_purge_state_zone zone=purge:1m;

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

            cache_purge_tags $upstream_http_cache_tag;
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

Releases are source tags. Build the modules against the nginx or OpenResty source and dependencies used by your server. GitHub Actions checks branch updates, pull requests, and `5.*` tags; it does not publish prebuilt modules.

## Upgrading to 5.1.0

Version 5.1.0 uses `cache_key_*` for both indexing and purge selection. All selectors are scoped to the configured namespace, including URL, prefix, host, and host-TAG. An empty namespace selects only the empty namespace.

- Remove `cache_purge_key_*` directives and configure the corresponding `cache_key_*` components. Use `$cache_key_base` for zone selection and `$cache_key_full` as the native cache key.
- Rename `cache_purge_tag` to `cache_purge_tags`. Select response or PURGE inputs with locations or `when` blocks.
- Replace management header-name directives with value expressions. Configure operation inputs with `cache_purge_operation_headers` and incoming/outgoing propagation header names with `cache_purge_propagation_headers`.
- Rebuild all three modules together. Stop nginx, remove old cache contents and index/state files, then start with empty cache, index, and state paths. A reload is insufficient for this upgrade.

Releases before version 5 also use a different purge interface. Enable each protocol with `*_cache_purge on;` and submit PURGE requests for Delete or Invalidate. Restrict management locations through nginx access controls.

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

Load the key and RocksDB modules before purge, in the order shown above. All three modules build through the repository's root `config`; there is no separate key-module build. Build the modules and backend libraries from the same source revision.

The addon links with `-lrocksdb -lstdc++ -lm -pthread` by default. Set `CACHE_PURGE_ROCKSDB_LIBS` when a different link configuration is needed, such as static RocksDB with its compression dependencies.

Build `ngx_expr_module` statically before this addon in the nginx configure arguments to enable `when` blocks. Use `cache_key_*` to select identity components from variables.

`NGX_RESTY_EXT` enables strict Invalidate handling. Automatic proxy request-header integration additionally requires `NGX_HTTP_PROXY_FILTER`. Ordinary nginx uses its native stale policy and the propagation variables described below.

## Conditional configuration

Location-level switches, key components, input expressions, and propagation header names support the expr module's `when` blocks in `http`, `server`, and `location`. `when` is a configuration block, not an extra directive parameter.

The first matching candidate wins. A child containing only conditional candidates falls back to its parent. Place conditional candidates before an unconditional value.

For example, select the misc and TAG values from management headers for PURGE requests:

```nginx
http {
    expr purge_request str_eq $request_method PURGE;

    server {
        location / {
            when purge_request {
                cache_key_misc $http_x_cache_purge_misc;
                cache_purge_tags $http_x_cache_purge_tags;
            }

            cache_key_misc $cookie_variant;
            cache_purge_tags $upstream_http_cache_tag;
            proxy_cache_key $cache_key_full;
        }
    }
}
```

Purge reads the same key components as cache lookup. Configure `proxy_cache_key $cache_key_full;` or the corresponding key directive for another protocol.

Values are inherited unless overridden. A child `cache_purge_tags` replaces the inherited list; `when` selects a complete list. The two parameters of `cache_purge_operation_headers` and `cache_purge_propagation_headers` are also inherited and selected as complete pairs. `cache_purge_index_store`, `cache_purge_state_zone`, `cache_purge_propagation_timeout`, and `cache_purge_api` do not support `when`. None of these directives supports nginx rewrite `if` blocks.

## Cache key components

The six directives each accept one complex value.

### cache_key_namespace

| Syntax: | **cache_key_namespace** `value`; |
| :--- | :--- |
| Default: | `cache_key_namespace "";` |
| Context: | `http`, `server`, `location`, `when` |

Sets the namespace used to isolate cache keys, purge rules, TAGs, and operation identifiers. An empty namespace is valid and stays empty. Nonempty values accept up to 255 printable ASCII bytes, excluding spaces and commas.

The selected value is available through the read-only variable `$cache_key_namespace`.

### cache_key_scheme

| Syntax: | **cache_key_scheme** `value`; |
| :--- | :--- |
| Default: | `cache_key_scheme $scheme;` |
| Context: | `http`, `server`, `location`, `when` |

Sets the scheme component of the cache key and URL.

The selected value is available through the read-only variable `$cache_key_scheme`.

### cache_key_host

| Syntax: | **cache_key_host** `value`; |
| :--- | :--- |
| Default: | `cache_key_host $host;` |
| Context: | `http`, `server`, `location`, `when` |

Sets the host component of the cache key and URL.

The selected value is available through the read-only variable `$cache_key_host`.

### cache_key_uri

| Syntax: | **cache_key_uri** `value`; |
| :--- | :--- |
| Default: | `cache_key_uri $request_uri;` |
| Context: | `http`, `server`, `location`, `when` |

Sets the URI component of the cache key and URL. Configure argument filtering and URL normalization in this expression when needed.

The selected value is available through the read-only variable `$cache_key_uri`.

### cache_key_misc

| Syntax: | **cache_key_misc** `value`; |
| :--- | :--- |
| Default: | `cache_key_misc "";` |
| Context: | `http`, `server`, `location`, `when` |

Sets additional values that distinguish cached variants of the same URL, such as a language or content-selection variable.

The selected value is available through the read-only variable `$cache_key_misc`.

### cache_key_range

| Syntax: | **cache_key_range** `value`; |
| :--- | :--- |
| Default: | `cache_key_range "";` |
| Context: | `http`, `server`, `location`, `when` |

Sets the range component of the full cache key. Use `$slice_range` with sliced caching so that each slice has a distinct full key while sharing the same base key.

The selected value is available through the read-only variable `$cache_key_range`.

### Variables

All eight variables are read-only. The six component variables expose the evaluated values of their corresponding directives, before key encoding.

| Variable | Value |
| :--- | :--- |
| `$cache_key_namespace` | Namespace selected by `cache_key_namespace` |
| `$cache_key_scheme` | Scheme selected by `cache_key_scheme` |
| `$cache_key_host` | Host selected by `cache_key_host` |
| `$cache_key_uri` | URI selected by `cache_key_uri` |
| `$cache_key_misc` | Variant value selected by `cache_key_misc` |
| `$cache_key_range` | Range selected by `cache_key_range` |
| `$cache_key_base` | Encoded namespace, scheme, host, URI, and misc |
| `$cache_key_full` | The base fields followed by the encoded range, including an empty range |

Base and full keys use an encoding that keeps their components distinct. Use the individual component variables when constructing readable strings or response headers.

```nginx
cache_key_namespace "";
cache_key_scheme $scheme;
cache_key_host $host;
cache_key_uri $request_uri;
cache_key_misc $cookie_variant;
cache_key_range $slice_range;

proxy_cache_key $cache_key_full;
```

Omit the range directive when the slice module is unavailable. Purge derives its URL identity from scheme, `://`, host, and URI; range does not narrow purge selection.

Use stable component values throughout each request so that cache lookup, indexing, and displayed values identify the same response.

Avoid circular variable references and validate dynamic namespace values. Invalid values are logged and can produce an empty cache-key value; they do not automatically reject the request.

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

These budgets do not cap total process memory or the operating system's page cache. Ordinary index reads and WAL writes are not limited by `io_rate`. Changing the store path requires stopping nginx and starting it again.

### cache_purge_state_zone

| Syntax: | **cache_purge_state_zone** `zone=name:size` [`state=file`]; |
| :--- | :--- |
| Default: | — |
| Context: | `http` |

Allocates shared memory for active rules, queued work, and completion state. The minimum size is `1m`. Configure one zone together with `cache_purge_index_store`.

At most 256 selector slots are available across tasks. Each TAG in a batch consumes one slot; other selectors consume one slot per task. The byte budget can limit admission before that count is reached. Changing the zone name or size requires stopping nginx and starting it again.

The optional `state` parameter saves the zone on an orderly nginx shutdown and restores it at startup. It requires nginx-ext with shared-memory persistence enabled; unsupported builds reject this parameter. Relative file paths use the nginx prefix. Use a writable path outside native cache directories, and retain it across container recreation. After successful startup, the restored file is removed; the next orderly shutdown writes a fresh snapshot.

```nginx
cache_purge_state_zone zone=purge:1m state=/var/lib/nginx/purge.zone;
```

Restored rules retain their original propagation deadlines; restarting does not extend the propagation window. Accepted tasks resume from the persistent index, and cleanup counters are retained. State files must belong to the same index and configuration and must not be shared between independent nginx instances.

This saves purge rules. To retain the strict Invalidate behavior of existing cache entries, also enable nginx-ext persistence for each native cache zone:

```nginx
proxy_cache_path /var/cache/nginx/content
                 keys_zone=content:10m
                 keys_state=/var/lib/nginx/content.zone;
```

With the same compatible build, zone sizes, paths, and preserved files, this retains cache state across an orderly restart. A master process killed with `SIGKILL` cannot save a new snapshot. Incompatible snapshots can be discarded, and size or address conflicts can prevent startup; nginx-ext reports these conditions in the error log.

### proxy_cache_purge

| Syntax: | **proxy_cache_purge** `on` \| `off`; |
| :--- | :--- |
| Default: | `proxy_cache_purge off;` |
| Context: | `http`, `server`, `location`, `when` |

Enables indexing and purge handling for proxy cache requests. Requires `cache_purge_index_store` and `cache_purge_state_zone`. Cache identity comes from the effective `cache_key_*` values.

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

### cache_purge_tags

| Syntax: | **cache_purge_tags** `value ...`; |
| :--- | :--- |
| Default: | `cache_purge_tags "";` |
| Context: | `http`, `server`, `location`, `when` |

Defines TAGs for cached responses or a TAG purge. Each argument accepts variables and comma-separated values:

```nginx
cache_purge_tags $upstream_http_cache_tag extratag1 extratag2;
```

A PURGE location can use `cache_purge_tags $http_x_cache_purge_tags;`.

Cached responses retain the first 128 nonempty items; TAG purges retain the first 32. Additional items are ignored with a notice-level log, without interrupting the response or rejecting the purge. Limits apply before deduplication, in argument and comma order. Spaces and tabs around items are trimmed; empty items are ignored. Retained items may contain at most 1,024 bytes and cannot contain NUL, CR, or LF. Comparison is case-sensitive.

A TAG purge matches any retained item. Repeated or overlapping TAGs affect each file once within the task.

### cache_purge_type

| Syntax: | **cache_purge_type** `value`; |
| :--- | :--- |
| Default: | `cache_purge_type $http_x_cache_purge_type;` |
| Context: | `http`, `server`, `location`, `when` |

Sets the PURGE selector from a variable or literal: `url`, `key`, `prefix`, `host`, `tag`, or `host-tag`. An empty value selects `url`.

### cache_purge_action

| Syntax: | **cache_purge_action** `value`; |
| :--- | :--- |
| Default: | `cache_purge_action $http_x_cache_purge_action;` |
| Context: | `http`, `server`, `location`, `when` |

Sets the PURGE action from a variable or literal: `delete` or `invalidate`. An empty value defaults to `delete`.

### cache_purge_operation_headers

| Syntax: | **cache_purge_operation_headers** `id=value issued_at=value`; |
| :--- | :--- |
| Default: | See below. |
| Context: | `http`, `server`, `location`, `when` |

```nginx
cache_purge_operation_headers
    id=$http_x_cache_purge_operation_id
    issued_at=$http_x_cache_purge_issued_at;
```

Both named parameters are required, in either order, and accept variables.

When both values are empty, the module generates an operation ID and timestamp. Supplying only one value is rejected. IDs contain 1–128 ASCII letters, digits, hyphens, underscores, or periods. The timestamp is decimal Unix epoch milliseconds, at least `1000`, compared at native cache second precision.

Keep both values and the selector/action unchanged when retrying. Reusing an ID for different work in the same namespace returns `409 Conflict`; different namespaces may use the same ID independently.

### proxy_cache_purge_propagation

| Syntax: | **proxy_cache_purge_propagation** `on` \| `off`; |
| :--- | :--- |
| Default: | `proxy_cache_purge_propagation off;` |
| Context: | `http`, `server`, `location`, `when` |

Enables outgoing purge headers for managed proxy requests. Enable it when the target is a CDN parent and leave it off when the target is the origin.

### cache_purge_ignore_propagation

| Syntax: | **cache_purge_ignore_propagation** `on` \| `off`; |
| :--- | :--- |
| Default: | `cache_purge_ignore_propagation off;` |
| Context: | `http`, `server`, `location`, `when` |

Ignores incoming purge propagation headers when enabled.

### cache_purge_propagation_timeout

| Syntax: | **cache_purge_propagation_timeout** `time`; |
| :--- | :--- |
| Default: | `cache_purge_propagation_timeout 120s;` |
| Context: | `http` |

Sets how long a locally accepted purge contributes outgoing parent headers. The window is intended to allow invalidation to reach other cache tiers; it is not a deadline for background file processing. The parent compares cache timestamps without applying another propagation-age window.

### cache_purge_propagation_headers

| Syntax: | **cache_purge_propagation_headers** `time=name action=name`; |
| :--- | :--- |
| Default: | See below. |
| Context: | `http`, `server`, `location`, `when` |

```nginx
cache_purge_propagation_headers
    time=X-Cache-Purge-Time
    action=X-Cache-Purge-Action;
```

Sets the two proxy propagation header names for both incoming and outgoing requests. Both named parameters are required, in either order. Names must differ, case-insensitively, and contain 1–255 ASCII letters, digits, hyphens, or underscores. Names are literal and cannot contain variables.

The time header carries Unix epoch milliseconds; the action header carries `delete` or `invalidate`. An absent or empty time header means no incoming requirement. A nonempty time requires one valid action header. Header names are matched case-insensitively.

On ordinary nginx, use these names in both `proxy_set_header` directives shown below.

### cache_purge_api

| Syntax: | **cache_purge_api**; |
| :--- | :--- |
| Default: | — |
| Context: | `location` |

Provides status endpoints in a prefix location. It accepts GET and HEAD requests for paths relative to that location:

| Path | Response |
| :--- | :--- |
| `capabilities` | Readiness, propagation timeout, and index-cleanup counters. |
| `operations/ID` | Admission and background-processing status for an operation. |

Task responses include `operation_id`, `effective`, `durable`, `effective_at`, `propagation_until`, and a `cleanup` object with `state`, `scan_done`, `visited`, `affected`, and `error`. Processing is complete when `cleanup.state` is `done`. Lookup uses the location's `cache_key_namespace`. Unknown IDs and IDs belonging only to another namespace return `404`; unavailable admission or status processing returns `503`.

This handler does not accept JSON POST submissions. Use PURGE in an enabled location to submit work. The module performs no authentication; configure access control with nginx.

## Purge requests

PURGE requests have no body and can be sent to the original business location or a dedicated purge location. All selectors search across cache zones within `cache_key_namespace`. An empty namespace matches only the empty namespace. URL identity is formed as scheme, `://`, host, and URI from `cache_key_*`. Host selection reads the host component directly. No URL parsing or protocol restriction is applied.

| Type | Matching scope |
| :--- | :--- |
| `url` | Exact URL, across all misc, slice, and Vary variants in the namespace. |
| `key` | Exact configured URL, misc, and namespace, covering all indexed slice/Vary files. |
| `prefix` | Literal URL prefix, across all misc variants in the namespace. |
| `host` | Exact `cache_key_host`, across paths and misc variants in the namespace. |
| `tag` | Any listed TAG within the configured namespace. |
| `host-tag` | Any listed TAG within the namespace and exact `cache_key_host`. |

For prefix purges, supply the prefix itself without a trailing wildcard. Regex selectors and physical cache-filename selectors are not supported.

The `delete` action removes matching files. The `invalidate` action expires them for upstream validation. With `NGX_RESTY_EXT`, only successful validation permits reuse: a 304 reuses the entity, a 200 replaces it, and an upstream failure cannot serve the invalidated entity. Enable the protocol's native cache-revalidation directive to allow conditional revalidation. Without the extension, ordinary nginx stale policy applies.

A dedicated endpoint can obtain identity fields from configured headers:

```nginx
location = /purge {
    proxy_cache_purge on;
    cache_key_scheme $http_x_cache_purge_scheme;
    cache_key_host $http_x_cache_purge_host;
    cache_key_uri $http_x_cache_purge_uri;
    cache_key_misc $http_x_cache_purge_misc;
    cache_key_namespace $http_x_cache_purge_namespace;
    cache_purge_tags $http_x_cache_purge_tags;
}
```

Use this location in a server with the store and shared zone configured. These component header names are chosen by the expressions. Configure the status location with the same namespace source.

```sh
curl -i -X PURGE http://127.0.0.1:8080/purge \
    -H 'X-Cache-Purge-Type: key' \
    -H 'X-Cache-Purge-Action: invalidate' \
    -H 'X-Cache-Purge-Scheme: http' \
    -H 'X-Cache-Purge-Host: example.test' \
    -H 'X-Cache-Purge-Uri: /asset' \
    -H 'X-Cache-Purge-Misc: language=en' \
    -H 'X-Cache-Purge-Namespace: assets'
```

Acceptance waits for task persistence and shared-rule publication, not file enumeration. Disconnecting or timing out does not cancel an accepted task. Use the operation ID and issued-at headers to retry the same operation reliably.

## Proxy propagation

Propagation is one-way: L1 sends a timestamp and action to L2 on an upstream request. L2 compares them with its native cache date and either serves the cache or applies Delete/Invalidate before obtaining a response. Responses carry no receipt or other propagation metadata. This protocol applies only to proxy caches.

Builds with both `NGX_RESTY_EXT` and `NGX_HTTP_PROXY_FILTER` set outgoing headers automatically. Other builds require `proxy_set_header` in the managed parent-facing location:

```nginx
proxy_cache_purge_propagation on;
proxy_set_header X-Cache-Purge-Time $cache_purge_propagation_time;
proxy_set_header X-Cache-Purge-Action $cache_purge_propagation_action;
```

Use the configured propagation header names on both tiers and in these `proxy_set_header` directives. A nonempty incoming time requires a valid action. Without an incoming time, no propagated requirement is applied. `cache_purge_ignore_propagation on` bypasses their interpretation.

TAG propagation uses the edge node's known URL-to-TAG associations, which remain available for the propagation window after a local file is removed. The parent receives the applicable timestamp and action. URLs with no known TAG association follow normal upstream handling.

### Variables

| Variable | Value |
| :--- | :--- |
| `$cache_purge_propagation_time` | Applicable purge timestamp in Unix epoch milliseconds. |
| `$cache_purge_propagation_action` | Applicable action: `delete` or `invalidate`. |

Both variables are empty when propagation is disabled or no applicable requirement exists.

## Runtime behavior

Purge tasks are processed in the background. Ordinary cache HITs do not wait for background file maintenance.

A configuration reload (`nginx -s reload`) keeps the master process running and preserves state in unchanged cache and purge zones. A restart means stopping all nginx processes and then starting nginx again. This includes a graceful shutdown with `nginx -s quit`; forced termination, process crashes, and power loss are abnormal shutdowns.

Reloads and restarts reuse the persistent index and resume recorded pending tasks. Without native cache-zone persistence, completed Invalidate operations no longer enforce their stricter stale-response restriction after a restart; affected entries follow ordinary nginx stale policy. This applies even after a graceful shutdown. Use native cache-zone persistence together with `cache_purge_state_zone state=...` when both cache state and purge rules must survive an orderly restart.

Missing-file records are collected in the background, retaining known TAG relationships for propagation. The management API exposes cumulative `index_cleanup.checked`, `index_cleanup.retained`, and `index_cleanup.errors` counters; these counters survive reloads with the same purge zone. Across a stop and start, they are retained when a saved purge zone is restored and otherwise reset.

### Limits

- Indexing starts with managed cache fills. Start with empty cache directories when enabling the module.
- Cache dates have second precision. An entry dated in the same second as a purge is treated as refreshed.
- A parent request started before a purge may finish afterward with a cache timestamp newer than the purge. Subsequent timestamp checks can treat that response as refreshed.
- A first cache fill still in progress when a purge completes can become a later HIT if the purge found no indexed entry for it.
- TAG propagation covers known associations. URLs first encountered after a TAG purge follow normal upstream handling. Large active rule sets and responses with many TAGs can increase resource use and latency.

## Tests

GitHub Actions tests both nginx and nginx-ext with static, dynamic, and sanitizer builds. nginx-ext runs also enable expr, proxy-header integration, and shared-zone persistence. Release tags run the extended Clang and Valgrind checks.

Run either container profile locally:

```sh
mkdir -p t/test-results
NGINX_VARIANT=nginx docker compose -f t/docker-compose.test.yml run --build --rm nginx-test
NGINX_VARIANT=nginx-ext docker compose -f t/docker-compose.test.yml run --build --rm nginx-test
```

Use a fresh report directory for each run; the runner rejects existing results.

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

With a persistence-capable nginx-ext build, `--state` also verifies saved native cache zones, the first request after restart, and retained propagation rules. The combined runner `t/persistent_runtime.py` accepts the same option with `--extension 1`.

`t/persistent_runtime.py` also runs the key variable suite against the combined build. To run it against an existing combined binary, use `python3 t/key_runtime.py --binary /path/to/nginx --output /tmp/key.json`; add `--modules /path/to/modules` for a dynamic build and `--expr` when enabled.

`t/native_propagation_runtime.py` checks two cache tiers, propagation headers, retained TAG relationships, and the outgoing timeout using an already built nginx binary. `t/native_cache_benchmark.py` compares foreground latency, throughput, and process memory with and without indexing and background purges. Run either with `--help` for its options; benchmark results depend on workload and resource limits.

## Author and license

Maintained by Hanada. Based on ngx_cache_purge by FRiCKLE, Piotr Sikora, and Denys Denysov.

Licensed under the [BSD 2-Clause License](LICENSE).
