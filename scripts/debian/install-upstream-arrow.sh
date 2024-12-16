#! /usr/bin/env bash

set -euo pipefail

apt-get update

codename="$(lsb_release --codename --short)"

wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-${codename}.deb"
apt-get -y --no-install-recommends install ./"apache-arrow-apt-source-latest-${codename}.deb"
apt-get update
# The apt download sometimes fails with a 403. We employ a similar workaround as
# arrow itself: https://github.com/apache/arrow/pull/36836.
# See also: https://github.com/apache/arrow/issues/35292.
apt-get -y --no-install-recommends install libarrow-dev=18.0.0-1 libprotobuf-dev libparquet-dev=18.0.0-1 || \
  apt-get -y --no-install-recommends install libarrow-dev=18.0.0-1 libprotobuf-dev libparquet-dev=18.0.0-1 || \
  apt-get -y --no-install-recommends install libarrow-dev=18.0.0-1 libprotobuf-dev libparquet-dev=18.0.0-1
rm ./"apache-arrow-apt-source-latest-${codename}.deb"

