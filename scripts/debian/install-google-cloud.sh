#! /usr/bin/env bash

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  git \
  libgrpc++-dev \
  libgtest-dev \
  nlohmann-json3-dev

INSTALL_PREFIX="/opt/google-cloud-cpp"

mkdir -p source
pushd source

git clone --depth 1 --shallow-submodules --recurse-submodules  https://github.com/google/crc32c
pushd crc32c
cmake -B build -S . \
-G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
  -DCRC32C_BUILD_TESTS=0 \
  -DCRC32C_BUILD_BENCHMARKS=0 \
  -DCRC32C_BUILD_BENCHMARKS=0 \
  -DCMAKE_CXX_STANDARD=20
cmake --build build --parallel "$(nproc --all)"
cmake --install build --strip
popd

mkdir google-cloud-cpp
pushd google-cloud-cpp
curl -L 'https://github.com/googleapis/google-cloud-cpp/archive/refs/tags/v2.30.0.tar.gz' | tar -xz --strip-components=1
cmake -B build -S . \
  -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}" \
  -DCRC32C_DIR="${INSTALL_PREFIX}" \
  -DBUILD_TESTING=OFF \
  -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF \
  -DGOOGLE_CLOUD_CPP_WITH_MOCKS=OFF \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_CXX_STANDARD=20
cmake --build build --parallel "$(nproc --all)"
cmake --install build --strip
popd

popd
rm -rf source
ls -l /

echo "/opt//opt/google-cloud-cpp/lib" > /etc/ld.so.conf.d/google-cloud-cpp.conf
ldconfig
