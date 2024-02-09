#! /usr/bin/env bash

# The AWS SDK Git version tag.
TAG=1.11.261

# Where to install the SDK.
INSTALL_PREFIX="/opt/aws-sdk-cpp"

set -euo pipefail

apt-get update
apt-get -y --no-install-recommends install \
    build-essential \
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
    -DBUILD_ONLY="sqs" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCPP_STANDARD=17 \
    -DENABLE_TESTING=OFF \
    -DCUSTOM_MEMORY_MANAGEMENT=OFF \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}"
cmake --build build --parallel
cmake --install build --strip
rm -rf "${SOURCE_TREE}"
