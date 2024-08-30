#! /usr/bin/env bash

set -euo pipefail

apt-get update
apt-get -y --no-install-recommends install \
    bats \
    bats-assert \
    bats-support \
    build-essential \
    ca-certificates \
    ccache \
    cmake-data \
    cmake \
    cppzmq-dev \
    curl \
    flatbuffers-compiler-dev \
    g++-12 \
    gcc-12 \
    git-core \
    gnupg2 gnupg-agent\
    jq \
    libboost1.81-dev \
    libboost-filesystem1.81-dev \
    libboost-url1.81-dev \
    libflatbuffers-dev \
    libfmt-dev \
    libgrpc-dev \
    libgrpc++-dev \
    libhttp-parser-dev \
    libmaxminddb-dev \
    libpcap-dev tcpdump \
    libprotobuf-dev \
    librabbitmq-dev \
    librdkafka-dev \
    libre2-dev \
    libspdlog-dev \
    libssl-dev \
    libsqlite3-dev \
    libunwind-dev \
    libxxhash-dev \
    libyaml-cpp-dev \
    libyara-dev \
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
    software-properties-common \
    wget \
    yara

codename="$(lsb_release --codename --short)"

# Apache Arrow
wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-${codename}.deb"
apt-get -y --no-install-recommends install ./"apache-arrow-apt-source-latest-${codename}.deb"
apt-get update
# The apt download sometimes fails with a 403. We employ a similar workaround as
# arrow itself: https://github.com/apache/arrow/pull/36836.
# See also: https://github.com/apache/arrow/issues/35292.
apt-get -y --no-install-recommends install libarrow-dev=15.0.2-1 libprotobuf-dev libparquet-dev=15.0.2-1 || \
  apt-get -y --no-install-recommends install libarrow-dev=15.0.2-1 libprotobuf-dev libparquet-dev=15.0.2-1 || \
  apt-get -y --no-install-recommends install libarrow-dev=15.0.2-1 libprotobuf-dev libparquet-dev=15.0.2-1
rm ./"apache-arrow-apt-source-latest-${codename}.deb"

# Node 18.x and Yarn
NODE_MAJOR=18
mkdir -p /etc/apt/keyrings
wget -O /etc/apt/keyrings/nodesource.asc https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key
echo "deb [signed-by=/etc/apt/keyrings/nodesource.asc] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
apt-get update
apt-get -y install yarn

# Poetry
export POETRY_HOME=/opt/poetry
curl -sSL https://install.python-poetry.org | python3 - --version 1.8.2
ln -nsf /opt/poetry/bin/poetry /usr/local/bin/poetry
