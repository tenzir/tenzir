#! /usr/bin/env bash

set -euo pipefail

brew --version
dependencies=(
  apache-arrow
  aws-sdk-cpp
  azure-storage-blobs-cpp
  bash
  blake3
  boost
  c-ares
  ccache
  cnats
  coreutils
  cppzmq
  double-conversion
  fast_float
  flatbuffers
  fluent-bit
  fmt
  glog
  gnu-sed
  grpc
  icu4c
  libevent
  libmaxminddb
  libpcap
  librdkafka
  libunwind-headers
  llvm@20
  mimalloc
  ninja
  nmap
  pandoc
  parallel
  poetry
  protobuf
  rabbitmq-c
  reproc
  rsync
  socat
  spdlog
  tcpdump
  uv
  xxhash
  yaml-cpp
  yara-x
  yarn
)

brew_rc=0
HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK=1 \
  brew install --overwrite --skip-post-install "${dependencies[@]}" || brew_rc="$?"
if ((brew_rc != 0)); then
  missing=()
  outdated=()
  for dependency in "${dependencies[@]}"; do
    if ! brew list --versions "${dependency}" &>/dev/null; then
      missing+=("${dependency}")
    elif outdated_formula="$(brew outdated --quiet "${dependency}")"; then
      [[ -n "${outdated_formula}" ]] && outdated+=("${dependency}")
    else
      outdated+=("${dependency}")
    fi
  done
  if ((${#missing[@]} || ${#outdated[@]})); then
    echo "Homebrew left dependencies missing or outdated: ${missing[*]} ${outdated[*]}" >&2
    exit "${brew_rc}"
  fi
  echo "::warning::Homebrew exited ${brew_rc} after installing all dependencies"
fi
