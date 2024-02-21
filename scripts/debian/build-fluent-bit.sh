#! /usr/bin/env bash

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  bison \
  build-essential \
  ca-certificates \
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
curl -L 'https://github.com/fluent/fluent-bit/archive/refs/tags/v2.2.2.tar.gz' | tar -xz --strip-components=1
cmake -B build -S . -DFLB_RELEASE=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc
cmake --build build --parallel "$(nproc --all)"
cd build
cpack -G DEB -D CPACK_STRIP_FILES=ON
mv fluent-bit_*.deb /root
popd
rm -rf source
