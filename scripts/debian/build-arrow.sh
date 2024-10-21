#! /usr/bin/env bash
# This script creates an arrow package that is tailored to Tenzir's needs.
# This is required because as of Arrow 17 the official debian packages lack
# support for the Azure filesystem integration.

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  bison \
  build-essential \
  ca-certificates \
  checkinstall \
  cmake \
  libcurl4-openssl-dev \
  dh-make \
  flex \
  libminizip-dev \
  libsasl2-dev \
  libssl-dev \
  libabsl-dev \
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
curl -L 'https://github.com/apache/arrow/archive/refs/tags/apache-arrow-17.0.0.tar.gz' | tar -xz --strip-components=1
cd cpp
cmake -B build -S . \
  -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr \
  -DCMAKE_INSTALL_SYSCONFDIR=/etc \
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
  -DARROW_WITH_ZSTD=ON
cmake --build build --parallel "$(nproc --all)"
cmake --install build
cd build
checkinstall \
  --pakdir / \
  --pkgsource="https://github.com/apache/arrow" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="17.0.0" \
  --pkgrelease="TENZIR" \
  --pkgname=arrow \
  --requires="libc6,libcurl4,libgcc1,libssl3,libstdc++6,libxml2,zlib1g" \
  make install
popd
rm -rf source
ls -l /
