# Test-only images. The production runtime uses the native nginx cache manager.
ARG UBUNTU_VERSION=24.04
FROM ubuntu:${UBUNTU_VERSION} AS tools
ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=secret,id=apt_mirror \
    --mount=type=secret,id=apt_ca,mode=0444 \
    if [ -s /run/secrets/apt_mirror ]; then \
        mirror="$(cat /run/secrets/apt_mirror)"; \
        if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then \
            sources=/etc/apt/sources.list.d/ubuntu.sources; \
        else sources=/etc/apt/sources.list; fi; \
        sed -i "s|http://archive.ubuntu.com/ubuntu/|$mirror|g" "$sources"; \
    fi && \
    ca_option="" && \
    if [ -s /run/secrets/apt_ca ]; then \
        ca_option="-o Acquire::https::CaInfo=/run/secrets/apt_ca"; \
    fi && \
    apt-get $ca_option update && apt-get $ca_option install -y --no-install-recommends \
    ca-certificates build-essential cmake pkg-config clang valgrind libc6-dbg \
    cppcheck libpcre2-dev libssl-dev zlib1g-dev librocksdb-dev libsnappy-dev \
    libbz2-dev liblz4-dev libzstd-dev curl git perl python3 gnupg strace \
    && rm -rf /var/lib/apt/lists/*

FROM tools AS core-source
WORKDIR /src
COPY t/download.sh t/prepare_sources.sh /src/t/
ARG NGINX_VERSION=1.31.7
RUN NGINX_VERSION="$NGINX_VERSION" \
    TEST_SOURCE_DIR=/opt/cache-test-sources bash t/prepare_sources.sh
ENV NGINX_SOURCE=/opt/cache-test-sources/nginx \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

FROM core-source AS integration
COPY . /src/
RUN mkdir /reports && chown 1000:1000 /reports
USER 1000:1000
CMD ["python3", "t/persistent_runtime.py", "--reports", "/reports"]

FROM integration AS verify

FROM core-source AS pvs-studio
RUN --mount=type=secret,id=apt_mirror \
    --mount=type=secret,id=apt_ca,mode=0444 \
    if [ -s /run/secrets/apt_mirror ]; then \
        mirror="$(cat /run/secrets/apt_mirror)"; \
        if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then \
            sources=/etc/apt/sources.list.d/ubuntu.sources; \
        else sources=/etc/apt/sources.list; fi; \
        sed -i "s|http://archive.ubuntu.com/ubuntu/|$mirror|g" "$sources"; \
    fi && \
    ca_option="" && \
    if [ -s /run/secrets/apt_ca ]; then \
        ca_option="-o Acquire::https::CaInfo=/run/secrets/apt_ca"; \
        export CURL_CA_BUNDLE=/run/secrets/apt_ca; \
    fi && \
    apt-get $ca_option update && apt-get $ca_option install -y --no-install-recommends \
        build-essential ca-certificates curl gnupg perl strace zlib1g-dev && \
    curl --fail --location --retry 2 --connect-timeout 20 --max-time 180 \
        https://files.pvs-studio.com/etc/pubkey.txt -o /tmp/pvs-studio.asc && \
    gpg --batch --yes --dearmor --output /usr/share/keyrings/pvs-studio.gpg /tmp/pvs-studio.asc && \
    printf '%s\n' \
        'deb [arch=amd64 signed-by=/usr/share/keyrings/pvs-studio.gpg] https://files.pvs-studio.com/deb viva64-release pvs-studio' \
        > /etc/apt/sources.list.d/pvs-studio.list && \
    apt-get $ca_option update && apt-get $ca_option install -y --no-install-recommends pvs-studio && \
    rm -f /tmp/pvs-studio.asc && rm -rf /var/lib/apt/lists/*
ENV PVS_REPORT_DIR=/reports/pvs-studio
CMD ["bash", "t/pvs_studio.sh"]
