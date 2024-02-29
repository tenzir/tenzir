#! /usr/bin/env bash

set -euo pipefail

codename="$(lsb_release --codename --short)"

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
wget https://storage.googleapis.com/tenzir-public-data/fluent-bit-packages/debian/${codename}/fluent-bit_2.2.2_amd64.deb
apt-get -y --no-install-recommends install ./fluent-bit_2.2.2_amd64.deb
rm ./fluent-bit_2.2.2_amd64.deb
