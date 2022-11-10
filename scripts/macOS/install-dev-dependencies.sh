#! /usr/bin/env bash

set -euo pipefail

brew --version
brew install \
    apache-arrow \
    asio \
    ccache \
    flatbuffers \
    fmt \
    gnu-sed \
    http-parser \
    libpcap \
    libunwind-headers \
    llvm \
    ninja \
    openssl \
    pandoc \
    pkg-config \
    pnpm \
    rsync \
    simdjson \
    spdlog \
    tcpdump \
    xxhash \
    yaml-cpp
