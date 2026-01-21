FROM public.ecr.aws/docker/library/debian:trixie-slim AS runtime-base

FROM runtime-base AS build-base

ENV CC="gcc-14" \
    CXX="g++-14" \
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
COPY nix/overrides/arrow-cpp-fields-race.patch /patches/
RUN apt-get update && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    ./build-arrow-package.sh

# -- jemalloc-package ----------------------------------------------------------

FROM build-base AS jemalloc-package

COPY scripts/debian/build-jemalloc-package.sh .
RUN ./build-jemalloc-package.sh

# -- fluent-bit-package --------------------------------------------------------

FROM build-base AS fluent-bit-package

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

COPY ./scripts/debian/install-dev-dependencies.sh ./scripts/debian/
RUN ./scripts/debian/install-dev-dependencies.sh && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    rm -rf /tmp/custom-packages && \
    rm -rf /var/lib/apt/lists/*

# Tenzir
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

# -- development ---------------------------------------------------------------

FROM dependencies AS development

ENV PREFIX="/opt/tenzir" \
    PATH="/opt/tenzir/bin:/opt/tenzir/libexec:${PATH}" \
    CC="gcc-14" \
    CXX="g++-14"

# When changing these, make sure to also update the corresponding entries in the
# flake.nix file.
ENV TENZIR_CACHE_DIRECTORY="/var/cache/tenzir" \
    TENZIR_STATE_DIRECTORY="/var/lib/tenzir" \
    TENZIR_LOG_FILE="/var/log/tenzir/server.log" \
    TENZIR_ENDPOINT="0.0.0.0"

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

# -- plugins -------------------------------------------------------------------

FROM development AS plugins-source

WORKDIR /tmp/tenzir

# -- tenzir-de -----------------------------------------------------------------

FROM runtime-base AS tenzir-de

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

COPY --from=arrow-package /tmp/*.deb /tmp/custom-packages/
COPY --from=aws-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=azure-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=fluent-bit-package /tmp/*.deb /tmp/custom-packages/
COPY --from=jemalloc-package /tmp/*.deb /tmp/custom-packages/
COPY --from=google-cloud-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=arrow-adbc-package /tmp/*.deb /tmp/custom-packages/

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates \
      gnupg2 \
      libasan8 \
      libboost-filesystem1.83.0 \
      libboost-url1.83.0 \
      libboost-stacktrace1.83.0 \
      libc++1 \
      libc++abi1 \
      libcap2-bin \
      libflatbuffers23.5.26 \
      libfmt10 \
      libgrpc++1.51 \
      libhttp-parser2.9 \
      libmaxminddb0 \
      libmimalloc3 \
      libpcap0.8 \
      libprotobuf32 \
      librabbitmq4 \
      librdkafka++1 \
      libre2-11 \
      libreproc++14 \
      libspdlog1.15 \
      libunwind8 \
      libxxhash-dev \
      libyaml-cpp0.8 \
      libyara10 \
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
    rm -rf /var/lib/apt/lists/*

USER tenzir:tenzir

WORKDIR /var/lib/tenzir
VOLUME ["/var/cache/tenzir", "/var/lib/tenzir"]

# Verify that Tenzir starts up correctly.
RUN tenzir 'version'

ENTRYPOINT ["tenzir"]
CMD ["--help"]

# -- tenzir-node-de ------------------------------------------------------------

FROM tenzir-de AS tenzir-node-de

ENTRYPOINT ["tenzir-node"]

# -- third-party-plugins -------------------------------------------------------------------

FROM plugins-source AS compaction-plugin

COPY contrib/tenzir-plugins/compaction ./contrib/tenzir-plugins/compaction
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/compaction -B build-compaction -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-compaction --parallel 1 && \
    DESTDIR=/plugin/compaction cmake --install build-compaction --component Runtime && \
    rm -rf build-compaction

FROM plugins-source AS context-plugin

COPY contrib/tenzir-plugins/context ./contrib/tenzir-plugins/context
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/context -B build-context -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-context --parallel && \
    DESTDIR=/plugin/context cmake --install build-context --component Runtime && \
    rm -rf build-context

FROM plugins-source AS pipeline-manager-plugin

COPY contrib/tenzir-plugins/pipeline-manager ./contrib/tenzir-plugins/pipeline-manager
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/pipeline-manager -B build-pipeline-manager -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-pipeline-manager --parallel && \
    DESTDIR=/plugin/pipeline-manager cmake --install build-pipeline-manager --component Runtime && \
    rm -rf build-pipeline-manager

FROM plugins-source AS packages-plugin

# TODO: We can't run the packages integration tests here at the moment, since
# they require the context and pipeline-manager plugins to be available.
COPY contrib/tenzir-plugins/packages ./contrib/tenzir-plugins/packages
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/packages -B build-packages -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-packages --parallel && \
    DESTDIR=/plugin/packages cmake --install build-packages --component Runtime && \
    rm -rf build-packages

FROM plugins-source AS platform-plugin

COPY contrib/tenzir-plugins/platform ./contrib/tenzir-plugins/platform
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/platform -B build-platform -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-platform --parallel && \
    DESTDIR=/plugin/platform cmake --install build-platform --component Runtime && \
    rm -rf build-platform

FROM plugins-source AS snowflake-plugin

COPY contrib/tenzir-plugins/snowflake ./contrib/tenzir-plugins/snowflake
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/snowflake -B build-snowflake -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-snowflake --parallel && \
    DESTDIR=/plugin/snowflake cmake --install build-snowflake --component Runtime && \
    rm -rf build-snowflake

FROM plugins-source AS to_amazon_security_lake-plugin

COPY contrib/tenzir-plugins/to_amazon_security_lake ./contrib/tenzir-plugins/to_amazon_security_lake
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/to_amazon_security_lake -B build-to_amazon_security_lake -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-to_amazon_security_lake --parallel && \
    DESTDIR=/plugin/to_amazon_security_lake cmake --install build-to_amazon_security_lake --component Runtime && \
    rm -rf build-to_amazon_security_lake

FROM plugins-source AS to_azure_log_analytics-plugin

COPY contrib/tenzir-plugins/to_azure_log_analytics ./contrib/tenzir-plugins/to_azure_log_analytics
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/to_azure_log_analytics -B build-to_azure_log_analytics -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-to_azure_log_analytics --parallel && \
    DESTDIR=/plugin/to_azure_log_analytics cmake --install build-to_azure_log_analytics --component Runtime && \
    rm -rf build-to_azure_log_analytics

FROM plugins-source AS to_splunk-plugin

COPY contrib/tenzir-plugins/to_splunk ./contrib/tenzir-plugins/to_splunk
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/to_splunk -B build-to_splunk -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-to_splunk --parallel && \
    DESTDIR=/plugin/to_splunk cmake --install build-to_splunk --component Runtime && \
    rm -rf build-to_splunk

FROM plugins-source AS to_google_secops-plugin

COPY contrib/tenzir-plugins/to_google_secops ./contrib/tenzir-plugins/to_google_secops
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/to_google_secops -B build-to_google_secops -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-to_google_secops --parallel && \
    DESTDIR=/plugin/to_google_secops cmake --install build-to_google_secops --component Runtime && \
    rm -rf build-to_google_secops

FROM plugins-source AS to_google_cloud_logging-plugin

COPY contrib/tenzir-plugins/to_google_cloud_logging ./contrib/tenzir-plugins/to_google_cloud_logging
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/to_google_cloud_logging -B build-to_google_cloud_logging -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" \
      -D CMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH};/opt/google-cloud-cpp" && \
    cmake --build build-to_google_cloud_logging --parallel && \
    DESTDIR=/plugin/to_google_cloud_logging cmake --install build-to_google_cloud_logging --component Runtime && \
    rm -rf build-to_google_cloud_logging

FROM plugins-source AS to_sentinelone_data_lake-plugin

COPY contrib/tenzir-plugins/to_sentinelone_data_lake ./contrib/tenzir-plugins/to_sentinelone_data_lake
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/to_sentinelone_data_lake -B build-to_sentinelone_data_lake -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-to_sentinelone_data_lake --parallel && \
    DESTDIR=/plugin/to_sentinelone_data_lake cmake --install build-to_sentinelone_data_lake --component Runtime && \
    rm -rf build-to_sentinelone_data_lake

FROM plugins-source AS vast-plugin

COPY contrib/tenzir-plugins/vast ./contrib/tenzir-plugins/vast
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S contrib/tenzir-plugins/vast -B build-vast -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-vast --parallel && \
    DESTDIR=/plugin/vast cmake --install build-vast --component Runtime && \
    rm -rf build-vast

FROM plugins-source AS cloudwatch-plugin

RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/cloudwatch -B build-cloudwatch -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-cloudwatch --parallel && \
    DESTDIR=/plugin/cloudwatch cmake --install build-cloudwatch --component Runtime && \
    rm -rf build-cloudwatch

# -- tenzir-ce-untested --------------------------------------------------------

FROM tenzir-de AS tenzir-ce-untested

COPY --from=compaction-plugin --chown=tenzir:tenzir /plugin/compaction /
COPY --from=context-plugin --chown=tenzir:tenzir /plugin/context /
COPY --from=pipeline-manager-plugin --chown=tenzir:tenzir /plugin/pipeline-manager /
COPY --from=packages-plugin --chown=tenzir:tenzir /plugin/packages /
COPY --from=platform-plugin --chown=tenzir:tenzir /plugin/platform /
COPY --from=snowflake-plugin --chown=tenzir:tenzir /plugin/snowflake /
COPY --from=to_amazon_security_lake-plugin --chown=tenzir:tenzir /plugin/to_amazon_security_lake /
COPY --from=to_azure_log_analytics-plugin --chown=tenzir:tenzir /plugin/to_azure_log_analytics /
COPY --from=to_splunk-plugin --chown=tenzir:tenzir /plugin/to_splunk /
COPY --from=to_google_secops-plugin --chown=tenzir:tenzir /plugin/to_google_secops /
COPY --from=to_google_cloud_logging-plugin --chown=tenzir:tenzir /plugin/to_google_cloud_logging /
COPY --from=to_sentinelone_data_lake-plugin --chown=tenzir:tenzir /plugin/to_sentinelone_data_lake /
COPY --from=vast-plugin --chown=tenzir:tenzir /plugin/vast /
COPY --from=cloudwatch-plugin --chown=tenzir:tenzir /plugin/cloudwatch /

USER tenzir:tenzir

# -- tenzir-ce-integration -----------------------------------------------------

FROM tenzir-ce-untested AS tenzir-ce-integration

COPY test/ ./test
RUN XDG_CACHE_HOME=/tmp XDG_DATA_HOME=/tmp "${PREFIX}/libexec/uv" tool run \
    --python ">=3.12" \
    tenzir-test \
    --tenzir-binary "$PREFIX/bin/tenzir" \
    --tenzir-node-binary "$PREFIX/bin/tenzir-node" \
    --root test \
    -j $(nproc) && \
    echo "success" > /tmp/tenzir-integration-result

# -- tenzir-ce -----------------------------------------------------------------

FROM tenzir-ce-untested AS tenzir-ce
COPY --from=tenzir-ce-integration /tmp/tenzir-integration-result /tmp/tenzir-integration-result

# -- tenzir-node-ce ------------------------------------------------------------

FROM tenzir-ce AS tenzir-node-ce

ENTRYPOINT ["tenzir-node"]

# -- tenzir-demo --------------------------------------------------------------

FROM tenzir-node-ce AS tenzir-demo

COPY /scripts/install-demo-node-package.tql /tmp/install-demo-node-package.tql
ENV TENZIR_DEMAND__MAX_BATCHES=3 \
    TENZIR_START__COMMANDS="exec --file /tmp/install-demo-node-package.tql"

# -- tenzir-node -----------------------------------------------------------------

FROM tenzir-node-ce AS tenzir-node

# -- tenzir ----------------------------------------------------------------------

# As a last stage we re-introduce the community edition as tenzir so that it's
# the default when not specifying a build target.
FROM tenzir-ce AS tenzir
