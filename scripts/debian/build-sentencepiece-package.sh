#! /usr/bin/env bash

set -euo pipefail

# Builds SentencePiece as a static library against the system protobuf,
# regenerating its protobuf files with the system protoc. The Debian package
# ships generated code from protobuf 3.14, which aborts at runtime inside
# processes that load the system protobuf (pulled in by Arrow and gRPC), and
# bundling sentencepiece's internal protobuf instead deadlocks through symbol
# interposition with the loaded libprotobuf. One consistent protobuf per
# process is the only combination that works.

: "${SENTENCEPIECE_VERSION=0.2.1}"
SENTENCEPIECE_SHA256="c1a59e9259c9653ad0ade653dadff074cd31f0a6ff2a11316f67bee4189a8f1b"

apt-get -qq update
apt-get install --no-install-recommends -y \
  ca-certificates \
  ccache \
  checkinstall \
  cmake \
  curl \
  libprotobuf-dev \
  protobuf-compiler

curl -fsSL -o /tmp/sentencepiece.tar.gz \
  "https://github.com/google/sentencepiece/archive/refs/tags/v${SENTENCEPIECE_VERSION}.tar.gz"
echo "${SENTENCEPIECE_SHA256}  /tmp/sentencepiece.tar.gz" | sha256sum -c -
tar -xzf /tmp/sentencepiece.tar.gz -C /tmp
cd "/tmp/sentencepiece-${SENTENCEPIECE_VERSION}"

cmake -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr/local \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DSPM_ENABLE_SHARED=OFF \
  -DSPM_PROTOBUF_PROVIDER=package

cmake --build build --parallel "$(nproc --all)"

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/google/sentencepiece/" \
  --pkglicense="ASL2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${SENTENCEPIECE_VERSION}" \
  --pkgrelease="TENZIR" \
  --pkgname=sentencepiece \
  --requires="libc6,libstdc++6" \
  cmake --install build --strip
