#! /usr/bin/env bash

set -euo pipefail

apt-get update
# Install gcc-15 and g++-15 from unstable
apt-get -y --no-install-recommends install -t unstable gcc-15 g++-15
# Install libjansson from unstable first to get consistent versions
# (trixie has version skew between libjansson4 and libjansson-dev)
apt-get -y --no-install-recommends install -t unstable libjansson4 libjansson-dev
# Install remaining packages from trixie
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
  libflatbuffers-dev \
  libfmt-dev \
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

# yarn
mkdir -p /etc/apt/keyrings
wget -O /etc/apt/keyrings/nodesource.asc https://dl.yarnpkg.com/debian/pubkey.gpg
echo "deb [signed-by=/etc/apt/keyrings/nodesource.asc] https://dl.yarnpkg.com/debian/ stable main nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
apt-get update
apt-get -y install yarn

# uv
curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh

# Poetry
export POETRY_HOME=/opt/poetry
curl -sSL https://install.python-poetry.org | python3 - --version 1.8.2
ln -nsf /opt/poetry/bin/poetry /usr/local/bin/poetry
