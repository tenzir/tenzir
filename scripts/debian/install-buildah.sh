#! /usr/bin/env bash

set -euo pipefail

sudo apt-get -y -qq update
sudo apt-get -y --no-install-recommends install \
  bats \
  btrfs-progs \
  git \
  go-md2man \
  libapparmor-dev \
  libglib2.0-dev \
  libgpgme11-dev \
  libseccomp-dev \
  libselinux1-dev \
  make \
  skopeo \
  protobuf-compiler

# Build buildah
git clone https://github.com/containers/buildah buildah --depth 1 --branch v1.42.0 --single-branch
make -C buildah buildah

## Build runtime dependencies for buildah

# netavark -- container network stack
git clone https://github.com/containers/netavark --depth 1 --branch v1.16.1 --single-branch
make -C netavark

# crun -- OCI runtime and C lib for containers
curl -L "https://github.com/containers/crun/releases/download/1.24/crun-1.24-linux-$(dpkg --print-architecture)" --output crun
chmod +x crun

# passt -- translation layer between a Layer-2 network interface and native Layer-4 sockets
git clone https://passt.top/passt --depth 1 --branch 2025_09_19.623dbf6 --single-branch
make -C passt

sudo mkdir -p /usr/local/{bin,libexec/podman}

sudo mv crun /usr/local/bin/
sudo mv buildah/bin/* /usr/local/bin/
sudo mv netavark/bin/* /usr/local/libexec/podman/
sudo make -C passt install

rm -rf buildah netavark passt
