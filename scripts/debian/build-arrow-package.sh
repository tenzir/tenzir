#! /usr/bin/env bash

set -euo pipefail

: "${ARROW_TAG=apache-arrow-21.0.0}"
: "${ARROW_VERSION=$(printf '%s' "$ARROW_TAG" | sed 's@[^0-9]*\(.*\)@\1@')}"

CMAKE_INSTALL_PREFIX=/usr/local
export CMAKE_INSTALL_PREFIX

apt-get -qq update
apt-get install --no-install-recommends -y \
  bison \
  build-essential \
  ca-certificates \
  checkinstall \
  ccache \
  cmake \
  curl \
  dh-make \
  flex \
  libboost-dev \
  libboost-filesystem-dev \
  libboost-system-dev \
  libcurl4-openssl-dev \
  libminizip-dev \
  libre2-dev \
  libsasl2-dev \
  libssl-dev \
  libabsl-dev \
  libyaml-dev \
  libxml2-dev \
  lsb-base \
  lsb-release \
  pkg-config \
  unzip \
  wget \
  zlib1g-dev

SOURCE_TREE="/tmp/src/arrow"
mkdir -p "${SOURCE_TREE}"
pushd "${SOURCE_TREE}"
curl -L "https://github.com/apache/arrow/archive/refs/heads/main.tar.gz" | tar -xz --strip-components=1
cd cpp
cmake -B build -S . \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} \
  -DAWSSDK_SOURCE=SYSTEM \
  -DBOOST_SOURCE=SYSTEM \
  -DGOOGLE_CLOUD_CPP_SOURCE=SYSTEM \
  -DARROW_FILESYSTEM=ON \
  -DARROW_AZURE=ON \
  -DARROW_GCS=ON \
  -DARROW_COMPUTE=ON \
  -DARROW_S3=ON \
  -DARROW_PARQUET=ON \
  -DARROW_WITH_BROTLI=ON \
  -DARROW_WITH_BZ2=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_WITH_ZSTD=ON \
  "${EXTRA_CMAKE_ARGS[@]}"

cmake --build build --parallel "$(nproc --all)"

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/apache/arrow/" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${ARROW_VERSION}" \
  --pkgrelease="TENZIR" \
  --pkgname=arrow \
  --requires="libc6,libboost-filesystem-dev,libboost-system-dev,libcurl4,libgcc1,libre2-11,libssl3,libstdc++6,libxml2,zlib1g" \
  cmake --install build --strip
