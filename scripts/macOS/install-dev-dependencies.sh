#! /usr/bin/env bash

set -euo pipefail

brew --version
brew install --overwrite \
    apache-arrow \
    bash \
    boost \
    ccache \
    coreutils \
    cppzmq \
    flatbuffers \
    fluent-bit \
    fmt \
    gnu-sed \
    grpc \
    http-parser \
    libpcap \
    librdkafka \
    libunwind-headers \
    llvm \
    ninja \
    openssl \
    pandoc \
    pkg-config \
    poetry \
    protobuf \
    rsync \
    simdjson \
    socat \
    spdlog \
    tcpdump \
    xxhash \
    yaml-cpp \
    yara \
    yarn
