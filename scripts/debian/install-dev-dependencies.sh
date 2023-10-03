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
    libflatbuffers-dev \
    libfmt-dev \
    libhttp-parser-dev \
    libpcap-dev tcpdump \
    librabbitmq-dev \
    librdkafka-dev \
    libre2-dev \
    libspdlog-dev \
    libssl-dev \
    libsqlite3-dev \
    libunwind-dev \
    libxxhash-dev \
    libyaml-cpp-dev \
    lsb-release \
    ninja-build \
    pandoc \
    pkg-config \
    python3-dev \
    python3-openssl \
    python3-pip \
    python3-venv \
    robin-map-dev \
    software-properties-common \
    wget

# Apache Arrow
wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb"
apt-get -y --no-install-recommends install ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
apt-get update
apt-get -y --no-install-recommends install libarrow-dev=13.0.0-1 libprotobuf-dev libparquet-dev=13.0.0-1
rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

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
