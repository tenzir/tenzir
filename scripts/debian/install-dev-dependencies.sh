#! /usr/bin/env bash

set -euo pipefail

apt-get update
apt-get -y --no-install-recommends install \
  build-essential \
  ca-certificates \
  ccache \
  cmake-data \
  cmake \
  cppzmq-dev \
  curl \
  flatbuffers-compiler-dev \
  git-core \
  gnupg2 gnupg-agent jq \
  libboost-context-dev \
  libboost-dev \
  libboost-filesystem-dev \
  libboost-program-options-dev \
  libboost-regex-dev \
  libboost-stacktrace-dev \
  libboost-thread-dev \
  libboost-url-dev \
  libc-ares-dev \
  libdouble-conversion-dev \
  libevent-dev \
  libfast-float-dev \
  libflatbuffers-dev \
  libfmt-dev \
  libgflags-dev \
  libgoogle-glog-dev \
  libgrpc-dev \
  libgrpc++-dev \
  libmaxminddb-dev \
  libmimalloc-dev \
  libpcap-dev tcpdump \
  libprotobuf-dev \
  librabbitmq-dev \
  librdkafka-dev \
  libre2-dev \
  libreproc-dev \
  libspdlog-dev \
  libssl-dev \
  libsqlite3-dev \
  libunwind-dev \
  libxxhash-dev \
  libyaml-cpp-dev \
  libyara-dev \
  liblz4-dev \
  libzstd-dev \
  lsb-release \
  lsof \
  ncat \
  nmap \
  ninja-build \
  pandoc \
  parallel \
  pkg-config \
  protobuf-compiler \
  protobuf-compiler-grpc \
  python3-dev \
  python3-openssl \
  python3-pip \
  python3-venv \
  robin-map-dev \
  socat \
  wget \
  yara

# yarn (via npm, since the yarn apt repo has an expired GPG key)
mkdir -p /etc/apt/keyrings
curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
apt-get update
apt-get -y --no-install-recommends install nodejs
npm install -g yarn

# uv
curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh

# Poetry
export POETRY_HOME=/opt/poetry
curl -sSL https://install.python-poetry.org | python3 - --version 1.8.2
ln -nsf /opt/poetry/bin/poetry /usr/local/bin/poetry
