#!/usr/bin/env bash
# Download immutable inputs into a new test-source directory.
set -euo pipefail
: "${NGINX_VERSION:?Set an exact nginx version}"
: "${TEST_SOURCE_DIR:?Set a new directory for test inputs}"
[[ "$NGINX_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
repo=$(cd "$(dirname "$0")/.." && pwd)
if [[ -e "$TEST_SOURCE_DIR" ]]; then
    echo 'TEST_SOURCE_DIR must not already exist' >&2
    exit 1
fi
mkdir -p "$TEST_SOURCE_DIR/nginx"
sh "$repo/t/download.sh" "https://nginx.org/download/nginx-$NGINX_VERSION.tar.gz" "$TEST_SOURCE_DIR/nginx.tar.gz"
tar --no-same-owner -xzf "$TEST_SOURCE_DIR/nginx.tar.gz" --strip-components=1 -C "$TEST_SOURCE_DIR/nginx"
git -C "$TEST_SOURCE_DIR/nginx" init -q
git -C "$TEST_SOURCE_DIR/nginx" add .
git -C "$TEST_SOURCE_DIR/nginx" -c user.name='Test source snapshot' -c user.email='test@localhost' \
    commit -qm "nginx $NGINX_VERSION source archive"
sha256sum "$TEST_SOURCE_DIR/nginx.tar.gz"
