#! /usr/bin/env bash

set -euo pipefail

brew --version
brew install --overwrite \
  apache-arrow \
  aws-sdk-cpp \
  bash \
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
  mimalloc \
  ninja \
  nmap \
  pandoc \
  parallel \
  poetry \
  protobuf \
  rabbitmq-c \
  reproc \
  rsync \
  socat \
  spdlog \
  tcpdump \
  uv \
  xxhash \
  yaml-cpp \
  yara \
  yarn
