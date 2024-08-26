#! /usr/bin/env bash

set -euo pipefail

brew --version
brew install --overwrite \
    apache-arrow \
    aws-sdk-cpp \
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
    llvm@17 \
    ninja \
    nmap \
    pandoc \
    parallel \
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
