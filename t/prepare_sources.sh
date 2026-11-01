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
case "${NGINX_VARIANT:-nginx}" in
    nginx)
        source="https://nginx.org/download/nginx-$NGINX_VERSION.tar.gz"
        ;;
    nginx-ext)
        [[ "$NGINX_VERSION" == 1.31.8 ]]
        revision=a20d23e3ea8ef3f21b510f62e0b38799399b3eff
        source="https://git.hanada.info/hanada/nginx-ext/-/archive/$revision/nginx-ext-$revision.tar.gz"
        for dependency in \
            ngx_expr_module:02bfc60f238874bbc42fc769dab6e5e0d75164cf \
            ngx_http_proxy_filter_module:694245db7bfbf227b82dba2930797aac26929081
        do
            name="${dependency%%:*}"
            commit="${dependency#*:}"
            mkdir "$TEST_SOURCE_DIR/$name"
            sh "$repo/t/download.sh" \
                "https://git.hanada.info/hanada/$name/-/archive/$commit/$name-$commit.tar.gz" \
                "$TEST_SOURCE_DIR/$name.tar.gz"
            tar --no-same-owner -xzf "$TEST_SOURCE_DIR/$name.tar.gz" \
                --strip-components=1 -C "$TEST_SOURCE_DIR/$name"
        done
        ;;
    *)
        echo 'NGINX_VARIANT must be nginx or nginx-ext' >&2
        exit 1
        ;;
esac
sh "$repo/t/download.sh" "$source" "$TEST_SOURCE_DIR/nginx.tar.gz"
tar --no-same-owner -xzf "$TEST_SOURCE_DIR/nginx.tar.gz" --strip-components=1 -C "$TEST_SOURCE_DIR/nginx"
git -C "$TEST_SOURCE_DIR/nginx" init -q
git -C "$TEST_SOURCE_DIR/nginx" add .
git -C "$TEST_SOURCE_DIR/nginx" -c user.name='Test source snapshot' -c user.email='test@localhost' \
    commit -qm "${NGINX_VARIANT:-nginx} $NGINX_VERSION source archive: $source"
sha256sum "$TEST_SOURCE_DIR"/*.tar.gz
