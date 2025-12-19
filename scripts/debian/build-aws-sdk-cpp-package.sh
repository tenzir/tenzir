#! /usr/bin/env bash

: "${AWS_SDK_CPP_TAG=1.11.449}"
: "${AWS_SDK_CPP_VERSION=$(printf '%s' "$AWS_SDK_CPP_TAG" | sed 's@[^0-9]*\(.*\)@\1@')}"

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
  git \
  libcurl4-openssl-dev \
  libpulse-dev \
  libssl-dev \
  uuid-dev \
  wget \
  xz-utils \
  zlib1g-dev

SOURCE_TREE="/tmp/src/aws-sdk-cpp"
git clone --depth 1 --shallow-submodules --recurse-submodules --branch "${AWS_SDK_CPP_TAG}" \
  https://github.com/aws/aws-sdk-cpp.git "${SOURCE_TREE}"
cd "${SOURCE_TREE}"
cmake -B build \
  -DBUILD_ONLY="sqs;cognito-identity;config;identity-management;logs;s3;sts;transfer" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} \
  -DCPP_STANDARD=17 \
  -DENABLE_TESTING=OFF \
  -DCUSTOM_MEMORY_MANAGEMENT=OFF \
  "${EXTRA_CMAKE_ARGS[@]}"

cmake --build build --parallel "$(nproc --all)"

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/aws/aws-sdk-cpp/" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${AWS_SDK_CPP_VERSION}" \
  --pkgrelease="TENZIR" \
  --pkgname=aws-sdk-cpp \
  --requires="libc6,libcurl4,libgcc1,libssl3,libstdc++6,zlib1g" \
  cmake --install build --strip
