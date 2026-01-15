#! /usr/bin/env bash

: "${FLUENT_BIT_TAG=v3.2.9}"

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  bison \
  g++-14 \
  gcc-14 \
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

# Use gcc-14 for Fluent Bit - it doesn't compile with gcc-15 due to
# stricter type checking (onigmo incompatible-pointer-types errors and
# processor_sql parser yylex declaration conflicts).
cmake -B build \
  -DCMAKE_C_COMPILER=gcc-14 \
  -DCMAKE_CXX_COMPILER=g++-14 \
  -DFLB_RELEASE=ON \
  "${EXTRA_CMAKE_ARGS[@]}"

cmake --build build --parallel "$(nproc --all)"

cd build
cpack -G DEB -D CPACK_STRIP_FILES=ON
mv fluent-bit_*.deb /tmp
