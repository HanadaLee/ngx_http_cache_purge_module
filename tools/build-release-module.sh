#!/usr/bin/env bash
set -euo pipefail

: "${NGINX_VERSION:?Set the exact NGINX version}"
: "${MODULE_VERSION:?Set the module release tag}"
[[ "$NGINX_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
if [[ ! "$MODULE_VERSION" =~ ^4\.[0-9]+\.[0-9]+(-[0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
    echo "Invalid MODULE_VERSION: expected 4.x.y[-prerelease] without a v prefix" >&2
    exit 1
fi
module_dir=$(pwd)
export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct)}
[[ "$SOURCE_DATE_EPOCH" =~ ^[0-9]+$ ]]
export TZ=UTC LC_ALL=C
umask 022
output_dir=${RELEASE_OUTPUT_DIR:-$module_dir/dist}
mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
arch=$(uname -m)
case "$arch" in x86_64|aarch64) ;; *) echo "Unsupported architecture: $arch" >&2; exit 1;; esac
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
name="ngx_http_cache_purge-${MODULE_VERSION}-nginx-${NGINX_VERSION}-linux-${arch}"
stage="$work/$name"
mkdir -p "$stage"
sh t/download.sh "https://nginx.org/download/nginx-${NGINX_VERSION}.tar.gz" "$work/nginx.tar.gz"
tar --no-same-owner -xzf "$work/nginx.tar.gz" -C "$work"
cmake -S "$module_dir" -B "$work/backend" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    "-DCMAKE_C_FLAGS=-ffile-prefix-map=$work=/usr/src/build -ffile-prefix-map=$module_dir=/usr/src/ngx_http_cache_purge" \
    "-DCMAKE_CXX_FLAGS=-ffile-prefix-map=$work=/usr/src/build -ffile-prefix-map=$module_dir=/usr/src/ngx_http_cache_purge"
cmake --build "$work/backend" --parallel "${BUILD_JOBS:-2}"
export CACHE_PURGE_ROCKSDB_LIB_DIR="$work/backend"
cd "$work/nginx-$NGINX_VERSION"
./configure --with-compat --with-http_ssl_module \
    --add-dynamic-module="$module_dir" \
    --with-cc-opt="-ffile-prefix-map=$work=/usr/src/build -ffile-prefix-map=$module_dir=/usr/src/ngx_http_cache_purge"
make -j"${BUILD_JOBS:-2}"
cp objs/ngx_http_cache_key_module.so objs/ngx_http_cache_rocksdb_module.so objs/ngx_http_cache_purge_module.so "$stage/"
# Exercise the delivered .so, rather than a statically linked module.
mkdir -p "$work/runtime/logs" "$work/runtime/cache"
user_directive=
if [[ $(id -u) == 0 ]]; then user_directive="user $(id -un) $(id -gn);"; fi
cat > "$work/runtime/nginx.conf" <<EOF
$user_directive
load_module $stage/ngx_http_cache_key_module.so;
load_module $stage/ngx_http_cache_rocksdb_module.so;
load_module $stage/ngx_http_cache_purge_module.so;
pid logs/nginx.pid;
error_log stderr;
events {}
http {
    access_log off;
    cache_purge_index_store $work/runtime/index cache=16m buffer=4m;
    cache_purge_state_zone release_control 4m;
    proxy_cache_path $work/runtime/cache keys_zone=release:1m;
    server {
        listen 127.0.0.1:18080;
        location /__cache_purge/ { cache_purge_api; allow 127.0.0.1; deny all; }
    }
}
EOF
objs/nginx -p "$work/runtime/" -c nginx.conf -t
python3 "$module_dir/t/release_runtime.py" --nginx "$PWD/objs/nginx" \
    --modules "$stage" \
    --root "$work/package-test" --output "$work/package-test.json"
cp "$module_dir/LICENSE" "$stage/"
cp "$module_dir/key/LICENSE" "$stage/LICENSE-key"
cp "$module_dir/README.md" "$stage/README.md"
{
    printf 'Module: %s\nCommit: %s\nNGINX: %s\nArchitecture: %s\n' \
        "$MODULE_VERSION" "$(git -C "$module_dir" rev-parse HEAD)" "$NGINX_VERSION" "$arch"
    printf 'SOURCE_DATE_EPOCH: %s\n' "$SOURCE_DATE_EPOCH"
    printf 'Tracked source changes: %s\n' "$(git -C "$module_dir" status --porcelain --untracked-files=no | wc -l)"
    cat "$work/package-test.json"
    cat /etc/os-release
    getconf GNU_LIBC_VERSION
    cc --version
    ld --version
    python3 --version
    tar --version
    gzip --version
    dpkg-query -W -f='${Package}=${Version}\n' | sort
    objs/nginx -V 2>&1
    sha256sum "$work/nginx.tar.gz"
    pkg-config --modversion rocksdb
    for shared in "$stage"/*.so; do
        readelf -d "$shared"
        readelf --version-info "$shared"
    done
} > "$stage/BUILD-INFO.txt"
# Record stable source paths, not ephemeral runner directories.
python3 - "$stage/BUILD-INFO.txt" "$work" "$module_dir" <<'PYTHON'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
text = path.read_text().replace(sys.argv[2], '/usr/src/build')
path.write_text(text.replace(sys.argv[3], '/usr/src/ngx_http_cache_purge'))
PYTHON
(cd "$stage" && sha256sum ngx_http_cache_key_module.so ngx_http_cache_rocksdb_module.so ngx_http_cache_purge_module.so > SHA256SUMS)
chmod 644 "$stage"/*
tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 \
    --numeric-owner --format=gnu -cf - -C "$work" "$name" \
    | gzip -n > "$output_dir/$name.tar.gz"
python3 - "$stage" "$output_dir/$name.zip" "$SOURCE_DATE_EPOCH" <<'PYTHON'
import pathlib, sys, time, zipfile
stage = pathlib.Path(sys.argv[1])
# ZIP timestamps have a 1980 lower bound and two-second precision.
stamp = time.gmtime(max(315532800, int(sys.argv[3])))[:6]
with zipfile.ZipFile(sys.argv[2], 'w', zipfile.ZIP_DEFLATED) as archive:
    for source in sorted(stage.iterdir()):
        info = zipfile.ZipInfo(f'{stage.name}/{source.name}', stamp)
        info.create_system = 3
        info.external_attr = 0o100644 << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(info, source.read_bytes())
PYTHON
