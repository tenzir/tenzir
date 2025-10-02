#! /usr/bin/env bash

set -euo pipefail

: "${GOOGLE_CLOUD_CPP_TAG=v2.36.0}"
: "${GOOGLE_CLOUD_CPP_VERSION=$(printf '%s' "$GOOGLE_CLOUD_CPP_TAG" | sed 's@[^0-9]*\(.*\)@\1@')}"

CMAKE_INSTALL_PREFIX=/usr/local
export CMAKE_INSTALL_PREFIX

apt-get -qq update
apt-get install --no-install-recommends -y \
  build-essential \
  ca-certificates \
  checkinstall \
  ccache \
  cmake \
  curl \
  git \
  g++-14 \
  gcc-14 \
  libc-ares-dev \
  libcurl4-openssl-dev \
  libgrpc++-dev \
  libgtest-dev \
  libprotobuf-dev \
  nlohmann-json3-dev \
  pkg-config \
  protobuf-compiler \
  protobuf-compiler-grpc

SOURCE_BASE="/tmp/src"
mkdir -p "${SOURCE_BASE}"
pushd "${SOURCE_BASE}"

### crc32c

git clone --depth 1 --shallow-submodules --recurse-submodules https://github.com/google/crc32c
pushd crc32c
cmake -B build -S . \
  -DBUILD_SHARED_LIBS=ON \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} \
  -DCRC32C_BUILD_TESTS=OFF \
  -DCRC32C_BUILD_BENCHMARKS=OFF \
  -DCRC32C_BUILD_BENCHMARKS=OFF \
  -DCMAKE_CXX_STANDARD=20

cmake --build build --parallel "$(nproc --all)"

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/google/crc32c/" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="1.1.0" \
  --pkgrelease="TENZIR" \
  --pkgname=crc32c \
  --requires="libc6" \
  cmake --install build --strip

popd

SOURCE_TREE="${SOURCE_BASE}/google-cloud-cpp"
mkdir -p "${SOURCE_TREE}"
pushd "${SOURCE_TREE}"

### Google Cloud CPP

curl -L "https://github.com/googleapis/google-cloud-cpp/archive/refs/tags/${GOOGLE_CLOUD_CPP_TAG}.tar.gz" | tar -xz --strip-components=1
cmake -B build -S . \
  -DBUILD_SHARED_LIBS=ON \
  -DBUILD_TESTING=OFF \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} \
  -DGOOGLE_CLOUD_CPP_ENABLE="storage;pubsub;logging" \
  -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF \
  -DGOOGLE_CLOUD_CPP_WITH_MOCKS=OFF \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_CXX_STANDARD=20

cmake --build build --parallel "$(nproc --all)"

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/googleapis/google-cloud-cpp/" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${GOOGLE_CLOUD_CPP_VERSION}" \
  --pkgrelease="TENZIR" \
  --pkgname=google-cloud-cpp \
  --requires="libc6,libcurl4-openssl-dev,libssl3,libstdc++6,nlohmann-json3-dev,zlib1g" \
  cmake --install build --strip
