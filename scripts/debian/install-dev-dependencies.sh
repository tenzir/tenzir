#! /usr/bin/env bash

set -euo pipefail

echo 'deb http://deb.debian.org/debian bullseye-backports main' > /etc/apt/sources.list.d/backports.list 
apt-get update 
apt-get -y --no-install-recommends install \
    build-essential \
    ca-certificates \
    ccache \
    cmake-data/bullseye-backports \
    cmake/bullseye-backports \
    flatbuffers-compiler-dev \
    g++-10 \
    gcc-10 \
    git-core \
    gnupg2 gnupg-agent\
    jq \
    libasio-dev \
    libflatbuffers-dev \
    libfmt-dev \
    libhttp-parser-dev \
    libpcap-dev tcpdump \
    libre2-dev \
    libspdlog-dev \
    libssl-dev \
    libunwind-dev \
    libxxhash-dev \
    libyaml-cpp-dev \
    lsb-release \
    ninja-build \
    pandoc \
    pkg-config \
    python3-dev \
    python3-pip \
    python3-venv \
    robin-map-dev \
    software-properties-common \
    wget

# Apache Arrow
wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb"
apt-get -y --no-install-recommends install ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
apt-get update
apt-get -y --no-install-recommends install libarrow-dev=11.0.0-1 libprotobuf-dev libparquet-dev=11.0.0-1
rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

# Node 18.x and Yarn
wget -O - 'https://deb.nodesource.com/setup_18.x' | bash -
wget -O - 'https://dl.yarnpkg.com/debian/pubkey.gpg' | gpg --dearmor | tee /usr/share/keyrings/yarnkey.gpg >/dev/null
echo "deb [signed-by=/usr/share/keyrings/yarnkey.gpg] https://dl.yarnpkg.com/debian stable main" | tee /etc/apt/sources.list.d/yarn.list
apt-get update
apt-get -y install yarn

# Poetry
python3 -m pip install poetry
