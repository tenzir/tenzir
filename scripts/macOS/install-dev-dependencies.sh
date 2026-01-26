#! /usr/bin/env bash

set -euo pipefail

brew --version
brew install --overwrite \
  apache-arrow \
  aws-sdk-cpp \
  azure-storage-blobs-cpp \
  bash \
  blake3 \
  boost \
  c-ares \
  ccache \
  coreutils \
  cppzmq \
  double-conversion \
  fast_float \
  flatbuffers \
  fluent-bit \
  fmt \
  glog \
  gnu-sed \
  grpc \
  libevent \
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
