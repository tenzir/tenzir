#!/bin/sh
set -e
brew --version
brew install \
    asio \
    apache-arrow \
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
    rsync \
    simdjson \
    spdlog \
    tcpdump \
    yaml-cpp \
    xxhash
