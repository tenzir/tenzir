#! /usr/bin/env bash

set -euo pipefail

: "${ARROW_ADBC_VERSION=21}"

CMAKE_INSTALL_PREFIX=/usr/local
export CMAKE_INSTALL_PREFIX

apt-get update
apt-get -y --no-install-recommends install \
  build-essential \
  ca-certificates \
  checkinstall \
  ccache \
  cmake \
  curl \
  g++-14 \
  gcc-14 \
  gnupg2 gnupg-agent lsb-release \
  golang-go

curl -L "https://github.com/apache/arrow-adbc/archive/refs/tags/apache-arrow-adbc-${ARROW_ADBC_VERSION}.tar.gz" | tar -xz --strip-components=1
cmake -B build c/ \
  -DCMAKE_BUILD_TYPE=Release \
  -DADBC_DRIVER_SNOWFLAKE=ON \
  -DADBC_DRIVER_MANAGER=ON
cmake --build build

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --include=build/install_manifest.txt \
  --pkgsource="https://github.com/apache/arrow-adbc/" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${ARROW_ADBC_VERSION}.0.0" \
  --pkgrelease="TENZIR" \
  --pkgname=arrow-adbc \
  cmake --install build --strip
