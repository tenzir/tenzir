FROM public.ecr.aws/docker/library/debian:trixie-slim AS runtime-base

FROM gcc:15-trixie AS build-base

ENV CC="gcc" \
    CXX="g++" \
    CMAKE_C_COMPILER_LAUNCHER=ccache \
    CCACHE_DIR=/ccache \
    CMAKE_CXX_COMPILER_LAUNCHER=ccache \
    CMAKE_INSTALL_PREFIX=/usr/local

RUN rm -f /etc/apt/apt.conf.d/docker-clean

# -- aws-sdk-cpp-package -------------------------------------------------------

FROM build-base AS aws-sdk-cpp-package

COPY scripts/debian/build-aws-sdk-cpp-package.sh .
RUN ./build-aws-sdk-cpp-package.sh

# -- google-cloud-cpp-package --------------------------------------------------

FROM build-base AS google-cloud-cpp-package

COPY scripts/debian/build-google-cloud-cpp-package.sh .
RUN ./build-google-cloud-cpp-package.sh

# -- azure-sdk-cpp-package -----------------------------------------------------

FROM build-base AS azure-sdk-cpp-package

COPY scripts/debian/build-azure-sdk-cpp-package.sh .
RUN ./build-azure-sdk-cpp-package.sh

# -- arrow-package -------------------------------------------------------------

FROM build-base AS arrow-package

COPY --from=aws-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=google-cloud-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=azure-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY scripts/debian/build-arrow-package.sh .
COPY nix/overrides/arrow-cpp-eager-struct-fields.patch /patches/
RUN apt-get update && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    ./build-arrow-package.sh

# -- jemalloc-package ----------------------------------------------------------

FROM build-base AS jemalloc-package

COPY scripts/debian/build-jemalloc-package.sh .
RUN ./build-jemalloc-package.sh

# -- rdkafka-package -----------------------------------------------------------

FROM build-base AS rdkafka-package

COPY scripts/debian/build-rdkafka-package.sh .
RUN ./build-rdkafka-package.sh

# -- fluent-bit-package --------------------------------------------------------

FROM build-base AS fluent-bit-package

COPY --from=rdkafka-package /tmp/*.deb /tmp/custom-packages/
RUN apt-get update && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb
COPY scripts/debian/build-fluent-bit-package.sh .
RUN ./build-fluent-bit-package.sh

# -- arrow-adbc-package --------------------------------------------------------

FROM build-base AS arrow-adbc-package
COPY scripts/debian/build-arrow-adbc-package.sh .
COPY --from=arrow-package /tmp/*.deb /tmp/custom-packages/
RUN apt-get update && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    ./build-arrow-adbc-package.sh
RUN dpkg -c /tmp/arrow-adbc*.deb

# -- yara-x-package ------------------------------------------------------------

FROM build-base AS yara-x-package

ENV RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    PATH=/opt/cargo/bin:$PATH
COPY scripts/build-yara-x-capi.sh scripts/
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates curl openssl pkg-config && \
    curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
      https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain 1.91.1 && \
    scripts/build-yara-x-capi.sh /usr/local && \
    rm -rf /var/lib/apt/lists/*

# -- dependencies --------------------------------------------------------------

FROM build-base AS dependencies
LABEL maintainer="engineering@tenzir.com"

WORKDIR /tmp/tenzir

COPY --from=arrow-package /tmp/*.deb /tmp/custom-packages/
COPY --from=aws-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=azure-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=fluent-bit-package /tmp/*.deb /tmp/custom-packages/
COPY --from=jemalloc-package /tmp/*.deb /tmp/custom-packages/
COPY --from=google-cloud-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=arrow-adbc-package /tmp/*.deb /tmp/custom-packages/
COPY --from=rdkafka-package /tmp/*.deb /tmp/custom-packages/
COPY --from=yara-x-package /usr/local/include/yara_x.h /usr/local/include/
COPY --from=yara-x-package /usr/local/lib/libyara_x_capi.* /usr/local/lib/
COPY --from=yara-x-package /usr/local/lib/pkgconfig/yara_x_capi.pc /usr/local/lib/pkgconfig/

COPY ./scripts/build-yara-x-capi.sh ./scripts/
COPY ./scripts/debian/install-dev-dependencies.sh ./scripts/debian/
RUN ./scripts/debian/install-dev-dependencies.sh && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    rm -rf /tmp/custom-packages && \
    rm -rf /var/lib/apt/lists/*

# -- development ---------------------------------------------------------------

FROM dependencies AS development

ENV PREFIX="/opt/tenzir" \
    PATH="/opt/tenzir/bin:/opt/tenzir/libexec:${PATH}" \
    CC="gcc" \
    CXX="g++"

# When changing these, make sure to also update the corresponding entries in the
# flake.nix file.
ENV TENZIR_CACHE_DIRECTORY="/var/cache/tenzir" \
    TENZIR_STATE_DIRECTORY="/var/lib/tenzir" \
    TENZIR_LOG_FILE="/var/log/tenzir/server.log" \
    TENZIR_ENDPOINT="0.0.0.0"

# Tenzir sources. Copied in the development stage (not dependencies) so the
# published dependency image carries only third-party build inputs — never
# Tenzir or proprietary-plugin source.
COPY changelog ./changelog
COPY cmake ./cmake
COPY libtenzir ./libtenzir
COPY libtenzir_test ./libtenzir_test
COPY python ./python
COPY schema ./schema
COPY scripts ./scripts
COPY tenzir ./tenzir
COPY plugins ./plugins
COPY CMakeLists.txt LICENSE README.md VERSIONING.md \
     tenzir.yaml.example version.json ./

# Additional arguments to be passed to CMake.
ARG TENZIR_BUILD_OPTIONS

ENV LDFLAGS="-Wl,--copy-dt-needed-entries"
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -B build -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" \
      -D CMAKE_BUILD_TYPE:STRING="Release" \
      -D TENZIR_ENABLE_AVX_INSTRUCTIONS:BOOL="OFF" \
      -D TENZIR_ENABLE_AVX2_INSTRUCTIONS:BOOL="OFF" \
      -D TENZIR_ENABLE_UNIT_TESTS:BOOL="ON" \
      -D TENZIR_ENABLE_DEVELOPER_MODE:BOOL="OFF" \
      -D TENZIR_ENABLE_BUNDLED_CAF:BOOL="ON" \
      -D TENZIR_ENABLE_BUNDLED_FACEBOOK_LIBS:BOOL="ON" \
      -D TENZIR_ENABLE_BUNDLED_SIMDJSON:BOOL="ON" \
      -D TENZIR_ENABLE_MANPAGES:BOOL="OFF" \
      -D TENZIR_ENABLE_PYTHON_BINDINGS_DEPENDENCIES:BOOL="ON" \
    ${TENZIR_BUILD_OPTIONS} && \
  cmake --build build --parallel && \
    cmake --build build --target unit-tests && \
    cmake --install build --component Runtime --prefix /opt/tenzir-runtime && \
    cmake --install build && \
    rm -rf build

RUN mkdir -p \
      $PREFIX/etc/tenzir \
      /var/cache/tenzir \
      /var/lib/tenzir \
      /var/log/tenzir

WORKDIR /var/lib/tenzir
VOLUME ["/var/lib/tenzir"]

ENTRYPOINT ["tenzir"]
CMD ["--help"]

# -- tenzir-untested -----------------------------------------------------------

FROM runtime-base AS tenzir-untested

# When changing these, make sure to also update the entries in the flake.nix
# file.
ENV PREFIX="/opt/tenzir" \
    PATH="/opt/tenzir/bin:/opt/tenzir/libexec:${PATH}" \
    TENZIR_CACHE_DIRECTORY="/var/cache/tenzir" \
    TENZIR_STATE_DIRECTORY="/var/lib/tenzir" \
    TENZIR_LOG_FILE="/var/log/tenzir/server.log" \
    TENZIR_ENDPOINT="0.0.0.0"

RUN useradd --system --user-group tenzir
COPY --from=development --chown=tenzir:tenzir /opt/tenzir-runtime/ /opt/tenzir/
COPY --from=development --chown=tenzir:tenzir /var/cache/tenzir/ /var/cache/tenzir/
COPY --from=development --chown=tenzir:tenzir /var/lib/tenzir/ /var/lib/tenzir/
COPY --from=development --chown=tenzir:tenzir /var/log/tenzir/ /var/log/tenzir/

# The build toolchain's libstdc++ is newer than the distro's; ship its
# runtime so that objects referencing newer GLIBCXX symbols load.
COPY --from=build-base /usr/local/lib64/libstdc++.so.6* /usr/local/lib64/

COPY --from=arrow-package /tmp/*.deb /tmp/custom-packages/
COPY --from=aws-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=azure-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=fluent-bit-package /tmp/*.deb /tmp/custom-packages/
COPY --from=jemalloc-package /tmp/*.deb /tmp/custom-packages/
COPY --from=google-cloud-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=arrow-adbc-package /tmp/*.deb /tmp/custom-packages/
COPY --from=rdkafka-package /tmp/*.deb /tmp/custom-packages/
COPY --from=yara-x-package /usr/local/lib/libyara_x_capi.so* /usr/local/lib/

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates \
      gnupg2 \
      libasan8 \
      libboost-context1.83.0 \
      libboost-date-time1.83.0 \
      libboost-filesystem1.83.0 \
      libboost-iostreams1.83.0 \
      libboost-program-options1.83.0 \
      libboost-regex1.83.0 \
      libboost-stacktrace1.83.0 \
      libboost-thread1.83.0 \
      libboost-url1.83.0 \
      libc++1 \
      libc++abi1 \
      libcap2-bin \
      libdouble-conversion3 \
      libevent-2.1-7 \
      libevent-pthreads-2.1-7 \
      libflatbuffers23.5.26 \
      libfmt10 \
      libgoogle-glog0v6 \
      libgrpc++1.51 \
      libhttp-parser2.9 \
      libicu76 \
      libmaxminddb0 \
      libmimalloc3 \
      libnats3.10 \
      libpcap0.8 \
      libprotobuf32 \
      librabbitmq4 \
      libre2-11 \
      libreproc++14 \
      libspdlog1.15 \
      libunwind8 \
      libxxhash-dev \
      libyaml-cpp0.8 \
      libzmq5 \
      liblz4-1 \
      libzstd1 \
      lsb-release \
      openssl \
      python3 \
      python3-venv \
      robin-map-dev && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    apt-get -y --no-install-recommends --allow-remove-essential purge \
      libpam0g \
      libpam-modules-bin \
      login \
      util-linux && \
    rm -rf /tmp/custom-packages && \
    rm -rf /var/lib/apt/lists/* && \
    echo /usr/local/lib64 > /etc/ld.so.conf.d/000-gcc-toolchain.conf && \
    ldconfig

USER tenzir:tenzir

WORKDIR /var/lib/tenzir
VOLUME ["/var/cache/tenzir", "/var/lib/tenzir"]

# Verify that Tenzir starts up correctly.
RUN tenzir 'version'

ENTRYPOINT ["tenzir"]
CMD ["--help"]

# -- tenzir-integration --------------------------------------------------------

FROM tenzir-untested AS tenzir-integration

COPY --chown=tenzir:tenzir libtenzir/aux/opentelemetry-proto/ \
    ./libtenzir/aux/opentelemetry-proto/
COPY --chown=tenzir:tenzir test/ ./test
RUN XDG_CACHE_HOME=/tmp XDG_DATA_HOME=/tmp \
    TENZIR_BINARY="$PREFIX/bin/tenzir" \
    TENZIR_NODE_BINARY="$PREFIX/bin/tenzir-node" \
    "${PREFIX}/libexec/uv" tool run \
    --python ">=3.12" \
    --with grpcio \
    --with grpcio-tools \
    --with googleapis-common-protos \
    --with protobuf \
    --with trustme \
    tenzir-test \
    --root test \
    -j $(nproc) && \
    echo "success" > /tmp/tenzir-integration-result

# -- tenzir-tested -------------------------------------------------------------

# Gate the published images on the integration tests by depending on their
# result artifact.
FROM tenzir-untested AS tenzir-tested
COPY --from=tenzir-integration /tmp/tenzir-integration-result /tmp/tenzir-integration-result

# -- tenzir-node ---------------------------------------------------------------

FROM tenzir-tested AS tenzir-node

ENTRYPOINT ["tenzir-node"]

# -- tenzir-demo ---------------------------------------------------------------

FROM tenzir-node AS tenzir-demo

COPY /scripts/install-demo-node-package.tql /tmp/install-demo-node-package.tql
ENV TENZIR_DEMAND__MAX_BATCHES=3 \
    TENZIR_START__COMMANDS="exec --file /tmp/install-demo-node-package.tql"

# -- tenzir --------------------------------------------------------------------

# The default target when none is specified.
FROM tenzir-tested AS tenzir
