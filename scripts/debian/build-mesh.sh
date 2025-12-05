#! /usr/bin/env bash

set -euo pipefail

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
  git \
  gnupg2 \
  gnupg-agent \
  lsb-release

git clone https://github.com/plasma-umass/Mesh mesh --depth 1
cd mesh
cmake -B build -S . \
  -DCMAKE_BUILD_TYPE=Release \
  -DINSTALL_MESH=ON \
  -DSYS_WIDE_INSTALL=ON
cmake --build build

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/plasma-umass/Mesh" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="0.0.1" \
  --pkgrelease="TENZIR" \
  --pkgname=mesh \
  cmake --build build --target install_mesh
