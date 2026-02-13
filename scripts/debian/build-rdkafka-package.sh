#! /usr/bin/env bash

: "${RDKAFKA_VERSION=2.13.0}"
: "${RDKAFKA_SHA256=3bd351601d8ebcbc99b9a1316cae1b83b00edbcf9411c34287edf1791c507600}"

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  g++-14 \
  gcc-14 \
  ca-certificates \
  ccache \
  checkinstall \
  cmake \
  curl \
  dh-make \
  make \
  ninja-build \
  pkg-config \
  libssl-dev \
  libsasl2-dev \
  libcurl4-openssl-dev \
  liblz4-dev \
  libzstd-dev \
  zlib1g-dev
apt-get install -y --reinstall \
  lsb-base \
  lsb-release

mkdir -p source
pushd source
curl -L "https://github.com/confluentinc/librdkafka/archive/refs/tags/v${RDKAFKA_VERSION}.tar.gz" -o rdkafka.tar.gz
echo "${RDKAFKA_SHA256}  rdkafka.tar.gz" | sha256sum -c -
tar -xzf rdkafka.tar.gz --strip-components=1
rm rdkafka.tar.gz

cmake -B build -G Ninja \
  -DCMAKE_INSTALL_PREFIX=/usr/local \
  -DCMAKE_BUILD_TYPE=Release \
  -DRDKAFKA_BUILD_STATIC=OFF \
  -DRDKAFKA_BUILD_EXAMPLES=OFF \
  -DRDKAFKA_BUILD_TESTS=OFF \
  -DWITH_SSL=ON \
  -DWITH_SASL=ON \
  -DWITH_CURL=ON \
  -DWITH_ZLIB=ON \
  -DWITH_ZSTD=ON \
  -DWITH_LZ4_EXT=ON

cmake --build build --parallel

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/confluentinc/librdkafka" \
  --pkglicense="BSD-2-Clause" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${RDKAFKA_VERSION}" \
  --pkgrelease="TENZIR" \
  --pkgname=rdkafka \
  --requires="libc6,libgcc1,libstdc++6,libssl3,libsasl2-2,libcurl4,liblz4-1,libzstd1,zlib1g" \
  cmake --install build
