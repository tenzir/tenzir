#! /usr/bin/env bash

: "${OPENTELEMETRY_CPP_VERSION=1.27.0}"
: "${OPENTELEMETRY_CPP_SHA256=d09c2e8dd95bbc1d6ee493a89f32a4736879948d0eb59ad58c855022d1f55cc1}"
# The OTLP exporter needs the opentelemetry-proto definitions, which are not
# bundled in the release tarball, so we fetch them separately.
: "${OPENTELEMETRY_PROTO_VERSION=1.10.0}"
: "${OPENTELEMETRY_PROTO_SHA256=52c85df79badc45da7e6a8735e8090b05a961b0208756187e1492a40db2d1f5f}"

set -euo pipefail

apt-get -qq update
apt-get install --no-install-recommends -y \
  g++ \
  gcc \
  ca-certificates \
  ccache \
  checkinstall \
  cmake \
  curl \
  dh-make \
  make \
  ninja-build \
  pkg-config \
  libcurl4-openssl-dev \
  libprotobuf-dev \
  nlohmann-json3-dev \
  protobuf-compiler
apt-get install -y --reinstall \
  lsb-base \
  lsb-release

mkdir -p source
pushd source
curl -L "https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v${OPENTELEMETRY_CPP_VERSION}.tar.gz" -o opentelemetry-cpp.tar.gz
echo "${OPENTELEMETRY_CPP_SHA256}  opentelemetry-cpp.tar.gz" | sha256sum -c -
tar -xzf opentelemetry-cpp.tar.gz --strip-components=1
rm opentelemetry-cpp.tar.gz

curl -L "https://github.com/open-telemetry/opentelemetry-proto/archive/refs/tags/v${OPENTELEMETRY_PROTO_VERSION}.tar.gz" -o opentelemetry-proto.tar.gz
echo "${OPENTELEMETRY_PROTO_SHA256}  opentelemetry-proto.tar.gz" | sha256sum -c -
mkdir -p opentelemetry-proto
tar -xzf opentelemetry-proto.tar.gz --strip-components=1 -C opentelemetry-proto
rm opentelemetry-proto.tar.gz

# We only need the OTLP/HTTP exporter; everything else (gRPC, Prometheus,
# Elasticsearch, tests, examples) is disabled to keep the build lean.
cmake -B build -G Ninja \
  -DCMAKE_INSTALL_PREFIX=/usr/local \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=ON \
  -DOPENTELEMETRY_INSTALL=ON \
  -DOTELCPP_PROTO_PATH="$(pwd)/opentelemetry-proto" \
  -DWITH_OTLP_HTTP=ON \
  -DWITH_OTLP_GRPC=OFF \
  -DWITH_PROMETHEUS=OFF \
  -DWITH_ELASTICSEARCH=OFF \
  -DWITH_BENCHMARK=OFF \
  -DWITH_EXAMPLES=OFF \
  -DWITH_FUNC_TESTS=OFF \
  -DBUILD_TESTING=OFF

cmake --build build --parallel

checkinstall \
  --fstrans=no \
  --pakdir /tmp \
  --pkgsource="https://github.com/open-telemetry/opentelemetry-cpp" \
  --pkglicense="Apache-2.0" \
  --deldesc=no \
  --nodoc \
  --pkgversion="${OPENTELEMETRY_CPP_VERSION}" \
  --pkgrelease="TENZIR" \
  --pkgname=opentelemetry-cpp \
  --requires="libc6,libstdc++6,libcurl4,libprotobuf32" \
  cmake --install build
