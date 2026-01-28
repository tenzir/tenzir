#! /usr/bin/env bash

: "${FLUENT_BIT_TAG=v4.2.2}"

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  bison \
  ca-certificates \
  ccache \
  cmake \
  curl \
  dh-make \
  flex \
  libminizip-dev \
  libsasl2-dev \
  libssl-dev \
  libsystemd-dev \
  libyaml-dev \
  make \
  pkg-config \
  unzip \
  wget \
  zlib1g-dev
apt-get install -y --reinstall \
  lsb-base \
  lsb-release

mkdir -p source
pushd source
curl -L "https://github.com/fluent/fluent-bit/archive/refs/tags/${FLUENT_BIT_TAG}.tar.gz" | tar -xz --strip-components=1
cmake -B build \
  -DFLB_RELEASE=ON \
  "${EXTRA_CMAKE_ARGS[@]}"

cmake --build build --parallel "$(nproc --all)"

cd build
cpack -G DEB -D CPACK_STRIP_FILES=ON
mv fluent-bit_*.deb /tmp
