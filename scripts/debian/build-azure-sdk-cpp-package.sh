#! /usr/bin/env bash

: "${AZURE_SDK_CPP_TAG=azure-identity_1.9.0}"
: "${AZURE_SDK_CPP_VERSION=1.9.0}"

CMAKE_INSTALL_PREFIX=/usr/local
export CMAKE_INSTALL_PREFIX

set -euo pipefail

apt-get update
apt-get -y --no-install-recommends install \
  build-essential \
  ca-certificates \
  checkinstall \
  ccache \
  cmake \
  curl \
  libcurl4-openssl-dev \
  libssl-dev \
  libxml2-dev \
  ninja-build \
  pkg-config \
  uuid-dev

SOURCE_TREE="/tmp/src/azure-sdk-cpp"
mkdir -p "${SOURCE_TREE}"
cd "${SOURCE_TREE}"
curl -L "https://github.com/Azure/azure-sdk-for-cpp/archive/refs/tags/${AZURE_SDK_CPP_TAG}.tar.gz" | tar -xz --strip-components=1

export AZURE_SDK_DISABLE_AUTO_VCPKG=TRUE

# ICU 75.1+ requires C++17 but Azure SDK uses C++14, disable ICU C++ API
export CXXFLAGS="${CXXFLAGS:-} -DU_SHOW_CPLUSPLUS_API=0"

cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} \
  -DBUILD_TESTING=OFF \
  -DBUILD_SAMPLES=OFF \
  -DBUILD_PERFORMANCE_TESTS=OFF \
  -DBUILD_WINDOWS_UWP=ON \
  -DCMAKE_UNITY_BUILD=OFF \
  -DDISABLE_AZURE_CORE_OPENTELEMETRY=ON \
  -DWARNINGS_AS_ERRORS=OFF \
  "${EXTRA_CMAKE_ARGS[@]}"

cmake --build build --parallel "$(nproc --all)"

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/Azure/azure-sdk-for-cpp/" \
  --pkglicense="MIT" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${AZURE_SDK_CPP_VERSION}" \
  --pkgrelease="TENZIR" \
  --pkgname=azure-sdk-cpp \
  --requires="libc6,libcurl4,libgcc1,libssl3,libstdc++6,libxml2" \
  cmake --install build --strip
