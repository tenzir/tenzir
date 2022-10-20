#!/bin/sh
set -e
echo 'deb http://deb.debian.org/debian bullseye-backports main' > /etc/apt/sources.list.d/backports.list 
apt-get update 
apt-get -y --no-install-recommends install \
    build-essential \
    ca-certificates \
    ccache \
    cmake/bullseye-backports \
    cmake-data/bullseye-backports \
    flatbuffers-compiler-dev \
    g++-10 \
    gcc-10 \
    git-core \
    gnupg2 gnupg-agent\
    jq \
    libasio-dev \
    libcaf-dev \
    libbroker-dev \
    libflatbuffers-dev \
    libfmt-dev \
    libpcap-dev tcpdump \
    libhttp-parser-dev \
    libsimdjson-dev \
    libspdlog-dev \
    libssl-dev \
    libunwind-dev \
    libyaml-cpp-dev \
    libxxhash-dev \
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
wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb"
apt-get -y --no-install-recommends install ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
apt-get update
apt-get -y --no-install-recommends install libarrow-dev=9.0.0-1 libprotobuf-dev libparquet-dev=9.0.0-1
rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
