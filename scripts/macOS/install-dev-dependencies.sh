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
    libmaxminddb \
    libpcap \
    librdkafka \
    libunwind-headers \
    llvm \
    ninja \
    nmap \
    openssl \
    pandoc \
    parallel \
    pipx \
    pkg-config \
    poetry \
    protobuf \
    rabbitmq-c \
    rsync \
    simdjson \
    socat \
    spdlog \
    tcpdump \
    xxhash \
    yaml-cpp \
    yara \
    yarn
