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
    libhttp-parser-dev \
    libpcap-dev tcpdump \
    libprotobuf-dev \
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
    ninja-build \
    pandoc \
    pkg-config \
    protobuf-compiler \
    protobuf-compiler-grpc \
    python3-dev \
    python3-openssl \
    python3-pip \
    python3-venv \
    robin-map-dev \
    software-properties-common \
    wget \
    yara

codename="$(lsb_release --codename --short)"

# Apache Arrow
wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-${codename}.deb"
apt-get -y --no-install-recommends install ./"apache-arrow-apt-source-latest-${codename}.deb"
apt-get update
apt-get -y --no-install-recommends install libarrow-dev=13.0.0-1 libprotobuf-dev libparquet-dev=13.0.0-1
rm ./"apache-arrow-apt-source-latest-${codename}.deb"

# Fluent-bit
# TODO: The official package contains a conflicting static build of jemalloc.
# https://github.com/fluent/fluent-bit/pull/8005 attempts to clean that up.
#wget -O - 'https://packages.fluentbit.io/fluentbit.key' | tee /usr/share/keyrings/fluentbit.asc >/dev/null
#echo "deb [signed-by=/usr/share/keyrings/fluentbit.asc] https://packages.fluentbit.io/debian/${codename} ${codename} main" | tee /etc/apt/sources.list.d/fluentbit.list
#apt-get update
#apt-get -y install fluent-bit
# A custom package generated from git tag with
# ```
# cd packaging
# FLB_JEMALLOC=OFF ./build.sh -d debian/bookworm
# ```
wget https://storage.googleapis.com/tenzir-public-data/fluent-bit-packages/debian/bookworm/fluent-bit_2.1.10_amd64.deb
apt-get -y --no-install-recommends install ./fluent-bit_2.1.10_amd64.deb
rm ./fluent-bit_2.1.10_amd64.deb

# Node 18.x and Yarn
NODE_MAJOR=18
mkdir -p /etc/apt/keyrings
wget -O /etc/apt/keyrings/nodesource.asc https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key
echo "deb [signed-by=/etc/apt/keyrings/nodesource.asc] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list
apt-get update
apt-get -y install yarn

# Poetry
export POETRY_HOME=/opt/poetry
curl -sSL https://install.python-poetry.org | python3 - --version 1.4.0
ln -nsf /opt/poetry/bin/poetry /usr/local/bin/poetry
