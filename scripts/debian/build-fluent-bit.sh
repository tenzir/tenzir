#! /usr/bin/env bash

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  curl ca-certificates build-essential \
  cmake make wget unzip dh-make libminizip-dev \
  libsystemd-dev zlib1g-dev flex bison \
  libssl-dev libsasl2-dev libyaml-dev pkg-config && \
  apt-get install -y --reinstall lsb-base lsb-release

mkdir -p /tmp/fluent-bit
cd /tmp/fluent-bit
curl -L 'https://github.com/fluent/fluent-bit/archive/refs/tags/v2.2.2.tar.gz' | tar -xz --strip-components=1
cmake -B build -S . -DFLB_RELEASE=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc
cmake --build build
cmake --install build
cd build
cpack -G DEB
