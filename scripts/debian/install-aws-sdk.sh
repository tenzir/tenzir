#! /usr/bin/env bash

# The AWS SDK Git version tag.
TAG=1.11.449

# Where to install the SDK.
INSTALL_PREFIX="/usr"

set -euo pipefail

apt-get update
apt-get -y --no-install-recommends install \
    build-essential \
    ca-certificates \
    checkinstall \
    cmake \
    git \
    libcurl4-openssl-dev \
    libpulse-dev \
    libssl-dev \
    uuid-dev \
    wget \
    xz-utils \
    zlib1g-dev

SOURCE_TREE="/tmp/aws-sdk-cpp"
git clone --depth 1 --shallow-submodules --recurse-submodules --branch ${TAG} \
    https://github.com/aws/aws-sdk-cpp.git "${SOURCE_TREE}"
cd "${SOURCE_TREE}"
cmake -B build \
    -DBUILD_ONLY="sqs;cognito-identity;config;identity-management;s3;sts;transfer" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCPP_STANDARD=17 \
    -DENABLE_TESTING=OFF \
    -DCUSTOM_MEMORY_MANAGEMENT=OFF \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}"
cmake --build build --parallel
cd build
checkinstall \
  --fstrans=no \
  --pakdir / \
  --pkgsource="https://github.com/aws/aws-sdk-cpp/" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${TAG}" \
  --pkgrelease="TENZIR" \
  --pkgname=aws-sdk-cpp \
  --requires="libc6,libcurl4,libgcc1,libssl3,libstdc++6,zlib1g" \
  make install
cd /
rm -rf "${SOURCE_TREE}"
ls -l /
