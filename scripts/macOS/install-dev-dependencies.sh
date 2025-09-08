#! /usr/bin/env bash

set -euo pipefail

brew --version
brew install --overwrite \
    apache-arrow \
    aws-sdk-cpp \
    bash \
    bats \
    boost \
    c-ares \
    ccache \
    coreutils \
    cppzmq \
    flatbuffers \
    fluent-bit \
    fmt \
    gnu-sed \
    grpc \
    libmaxminddb \
    libpcap \
    librdkafka \
    libunwind-headers \
    llvm@20 \
    ninja \
    nmap \
    pandoc \
    parallel \
    poetry \
    protobuf \
    rabbitmq-c \
    reproc \
    rsync \
    simdjson \
    socat \
    spdlog \
    tcpdump \
    uv \
    xxhash \
    yaml-cpp \
    yara \
    yarn
