#! /usr/bin/env bash

: "${JEMALLOC_TAG=5.3.0}"
: "${JEMALLOC_COMMIT=54eaed1d8b56b1aa528be3bdd1877e59c56fa90c}"

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  g++-14 \
  gcc-14 \
  ca-certificates \
  checkinstall \
  curl \
  dh-make \
  make \
  pkg-config
apt-get install -y --reinstall \
  lsb-base \
  lsb-release

mkdir -p source
pushd source
curl -L "https://github.com/facebook/jemalloc/archive/refs/tags/${JEMALLOC_TAG}.tar.gz" | tar -xz --strip-components=1

JEMALLOC_INSTALL_PREFIX=/usr/local
export CMAKE_INSTALL_PREFIX

./autogen.sh \
  --prefix="${JEMALLOC_INSTALL_PREFIX}" \
  --with-version="${JEMALLOC_TAG}-0-g${JEMALLOC_COMMIT}" \
  --with-jemalloc-prefix=je_tenzir_ \
  --with-private-namespace=je_tenzir_private_ \
  --without-export \
  --disable-cxx \
  --disable-libdl

make -j "$(nproc || true)"

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/facebook/jemalloc/" \
  --pkglicense="BSD-2-Clause" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${JEMALLOC_TAG}" \
  --pkgrelease="TENZIR" \
  --pkgname=jemalloc \
  --requires="libc6,libgcc1" \
  make install
