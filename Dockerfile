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

# -- arrow-package -------------------------------------------------------------

FROM build-base AS arrow-package

COPY --from=aws-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=google-cloud-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY scripts/debian/build-arrow-package.sh .
RUN apt-get update && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    ./build-arrow-package.sh

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
COPY --from=fluent-bit-package /tmp/*.deb /tmp/custom-packages/
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

# -- bundled-plugins -------------------------------------------------------------------

FROM plugins-source AS amqp-plugin

COPY plugins/amqp ./plugins/amqp
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/amqp -B build-amqp -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-amqp --parallel && \
    DESTDIR=/plugin/amqp cmake --install build-amqp --component Runtime && \
    rm -rf build-amqp

FROM plugins-source AS azure-blob-storage-plugin

COPY plugins/azure-blob-storage ./plugins/azure-blob-storage
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/azure-blob-storage -B build-azure-blob-storage -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-azure-blob-storage --parallel && \
    DESTDIR=/plugin/azure-blob-storage cmake --install build-azure-blob-storage --component Runtime && \
    rm -rf build-azure-blob-storage

FROM plugins-source AS clickhouse-plugin

COPY plugins/clickhouse ./plugins/clickhouse
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/clickhouse -B build-clickhouse -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-clickhouse --parallel && \
    DESTDIR=/plugin/clickhouse cmake --install build-clickhouse --component Runtime && \
    rm -rf build-clickhouse

FROM plugins-source AS fluent-bit-plugin

COPY plugins/fluent-bit ./plugins/fluent-bit
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/fluent-bit -B build-fluent-bit -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-fluent-bit --parallel && \
    DESTDIR=/plugin/fluent-bit cmake --install build-fluent-bit --component Runtime && \
    rm -rf build-fluent-bit

FROM plugins-source AS gcs-plugin

COPY plugins/gcs ./plugins/gcs
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/gcs -B build-gcs -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-gcs --parallel && \
    DESTDIR=/plugin/gcs cmake --install build-gcs --component Runtime && \
    rm -rf build-gcs

FROM plugins-source AS google-cloud-pubsub-plugin

COPY plugins/google-cloud-pubsub ./plugins/google-cloud-pubsub
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/google-cloud-pubsub -B build-google-cloud-pubsub -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" \
      -D CMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH};/opt/google-cloud-cpp" && \
    cmake --build build-google-cloud-pubsub --parallel && \
    DESTDIR=/plugin/google-cloud-pubsub cmake --install build-google-cloud-pubsub --component Runtime && \
    rm -rf build-google-cloud-pubsub

FROM plugins-source AS kafka-plugin

COPY plugins/kafka ./plugins/kafka
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/kafka -B build-kafka -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-kafka --parallel && \
    DESTDIR=/plugin/kafka cmake --install build-kafka --component Runtime && \
    rm -rf build-kafka

FROM plugins-source AS nic-plugin

COPY plugins/nic ./plugins/nic
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/nic -B build-nic -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-nic --parallel && \
    DESTDIR=/plugin/nic cmake --install build-nic --component Runtime && \
    rm -rf build-nic

FROM plugins-source AS parquet-plugin

COPY plugins/parquet ./plugins/parquet
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/parquet -B build-parquet -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-parquet --parallel && \
    DESTDIR=/plugin/parquet cmake --install build-parquet --component Runtime && \
    rm -rf build-parquet

FROM plugins-source AS s3-plugin

COPY plugins/s3 ./plugins/s3
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/s3 -B build-s3 -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-s3 --parallel && \
    DESTDIR=/plugin/s3 cmake --install build-s3 --component Runtime && \
    rm -rf build-s3

FROM plugins-source AS sigma-plugin

COPY plugins/sigma ./plugins/sigma
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/sigma -B build-sigma -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-sigma --parallel && \
    DESTDIR=/plugin/sigma cmake --install build-sigma --component Runtime && \
    rm -rf build-sigma

FROM plugins-source AS sqs-plugin

COPY plugins/sqs ./plugins/sqs
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/sqs -B build-sqs -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-sqs --parallel && \
    DESTDIR=/plugin/sqs cmake --install build-sqs --component Runtime && \
    rm -rf build-sqs

FROM plugins-source AS from_velociraptor-plugin

COPY plugins/from_velociraptor ./plugins/from_velociraptor
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/from_velociraptor -B build-from_velociraptor -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-from_velociraptor --parallel && \
    DESTDIR=/plugin/from_velociraptor cmake --install build-from_velociraptor --component Runtime && \
    rm -rf build-from_velociraptor

FROM plugins-source AS web-plugin

COPY plugins/web ./plugins/web
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/web -B build-web -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-web --parallel && \
    DESTDIR=/plugin/web cmake --install build-web --component Runtime && \
    rm -rf build-web

FROM plugins-source AS yara-plugin

COPY plugins/yara ./plugins/yara
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/yara -B build-yara -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-yara --parallel && \
    DESTDIR=/plugin/yara cmake --install build-yara --component Runtime && \
    rm -rf build-yara

FROM plugins-source AS zmq-plugin

COPY plugins/zmq ./plugins/zmq
RUN --mount=target=/ccache,type=cache,from=cache-context \
    cmake -S plugins/zmq -B build-zmq -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-zmq --parallel && \
    DESTDIR=/plugin/zmq cmake --install build-zmq --component Runtime && \
    rm -rf build-zmq

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
COPY --from=fluent-bit-package /tmp/*.deb /tmp/custom-packages/
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
      robin-map-dev \
      wget && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    rm -rf /tmp/custom-packages && \
    rm -rf /var/lib/apt/lists/*

USER tenzir:tenzir

WORKDIR /var/lib/tenzir
VOLUME ["/var/cache/tenzir", "/var/lib/tenzir"]

# Verify that Tenzir starts up correctly.
RUN tenzir 'version'

ENTRYPOINT ["tenzir"]
CMD ["--help"]

COPY --from=amqp-plugin --chown=tenzir:tenzir /plugin/amqp /
COPY --from=azure-blob-storage-plugin --chown=tenzir:tenzir /plugin/azure-blob-storage /
COPY --from=clickhouse-plugin --chown=tenzir:tenzir /plugin/clickhouse /
COPY --from=fluent-bit-plugin --chown=tenzir:tenzir /plugin/fluent-bit /
COPY --from=gcs-plugin --chown=tenzir:tenzir /plugin/gcs /
COPY --from=google-cloud-pubsub-plugin --chown=tenzir:tenzir /plugin/google-cloud-pubsub /
COPY --from=kafka-plugin --chown=tenzir:tenzir /plugin/kafka /
COPY --from=nic-plugin --chown=tenzir:tenzir /plugin/nic /
COPY --from=parquet-plugin --chown=tenzir:tenzir /plugin/parquet /
COPY --from=s3-plugin --chown=tenzir:tenzir /plugin/s3 /
COPY --from=sigma-plugin --chown=tenzir:tenzir /plugin/sigma /
COPY --from=sqs-plugin --chown=tenzir:tenzir /plugin/sqs /
COPY --from=from_velociraptor-plugin --chown=tenzir:tenzir /plugin/from_velociraptor /
COPY --from=web-plugin --chown=tenzir:tenzir /plugin/web /
COPY --from=yara-plugin --chown=tenzir:tenzir /plugin/yara /
COPY --from=zmq-plugin --chown=tenzir:tenzir /plugin/zmq /

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
