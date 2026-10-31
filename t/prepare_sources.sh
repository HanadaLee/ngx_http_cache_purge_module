#!/usr/bin/env bash
# Download immutable inputs into a new test-source directory.
set -euo pipefail
: "${NGINX_VERSION:?Set an exact nginx version}"
: "${TEST_SOURCE_DIR:?Set a new directory for test inputs}"
[[ "$NGINX_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
key_revision=${CACHE_KEY_REVISION:-ae3584f589f1d048996c50a78b3015f1fcd36eed}
[[ "$key_revision" =~ ^[0-9a-f]{40}$ ]]
key_repository=${CACHE_KEY_REPOSITORY:-https://git.hanada.info/hanada/ngx_http_cache_key_module.git}
repo=$(cd "$(dirname "$0")/.." && pwd)
if [[ -e "$TEST_SOURCE_DIR" ]]; then
    echo 'TEST_SOURCE_DIR must not already exist' >&2
    exit 1
fi
mkdir -p "$TEST_SOURCE_DIR/nginx" "$TEST_SOURCE_DIR/key"
sh "$repo/t/download.sh" "https://nginx.org/download/nginx-$NGINX_VERSION.tar.gz" "$TEST_SOURCE_DIR/nginx.tar.gz"
tar --no-same-owner -xzf "$TEST_SOURCE_DIR/nginx.tar.gz" --strip-components=1 -C "$TEST_SOURCE_DIR/nginx"
git -C "$TEST_SOURCE_DIR/nginx" init -q
git -C "$TEST_SOURCE_DIR/nginx" add .
git -C "$TEST_SOURCE_DIR/nginx" -c user.name='Test source snapshot' -c user.email='test@localhost' \
    commit -qm "nginx $NGINX_VERSION source archive"
sha256sum "$TEST_SOURCE_DIR/nginx.tar.gz"
if [[ ${SKIP_KEY:-0} == 1 ]]; then exit 0; fi
git -C "$TEST_SOURCE_DIR/key" init -q
git -C "$TEST_SOURCE_DIR/key" remote add origin "$key_repository"
git -C "$TEST_SOURCE_DIR/key" fetch --depth=1 origin "$key_revision"
git -C "$TEST_SOURCE_DIR/key" checkout --detach -q FETCH_HEAD
test "$(git -C "$TEST_SOURCE_DIR/key" rev-parse HEAD)" = "$key_revision"
