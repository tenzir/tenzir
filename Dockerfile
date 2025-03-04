FROM debian:bookworm-slim AS runtime-base

FROM runtime-base AS build-base

ENV CC="gcc-12" \
    CXX="g++-12" \
    CMAKE_INSTALL_PREFIX=/usr/local

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

# -- dependencies --------------------------------------------------------------

FROM build-base AS dependencies
LABEL maintainer="engineering@tenzir.com"

WORKDIR /tmp/tenzir

COPY --from=arrow-package /tmp/*.deb /tmp/custom-packages/
COPY --from=aws-sdk-cpp-package /tmp/*.deb /tmp/custom-packages/
COPY --from=fluent-bit-package /tmp/*.deb /tmp/custom-packages/
COPY --from=google-cloud-cpp-package /tmp/*.deb /tmp/custom-packages/

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
COPY CMakeLists.txt LICENSE README.md tenzir.spdx.json VERSIONING.md \
     tenzir.yaml.example version.json ./

# -- development ---------------------------------------------------------------

FROM dependencies AS development

ENV PREFIX="/opt/tenzir" \
    PATH="/opt/tenzir/bin:${PATH}" \
    CC="gcc-12" \
    CXX="g++-12"

# When changing these, make sure to also update the corresponding entries in the
# flake.nix file.
ENV TENZIR_CACHE_DIRECTORY="/var/cache/tenzir" \
    TENZIR_STATE_DIRECTORY="/var/lib/tenzir" \
    TENZIR_LOG_FILE="/var/log/tenzir/server.log" \
    TENZIR_ENDPOINT="0.0.0.0"

# Additional arguments to be passed to CMake.
ARG TENZIR_BUILD_OPTIONS

RUN cmake -B build -G Ninja \
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
    CTEST_OUTPUT_ON_FAILURE=1 cmake --build build --target test && \
    cmake --build build --target bats && \
    cmake --install build --strip --component Runtime --prefix /opt/tenzir-runtime && \
    cmake --install build --strip && \
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

RUN apt-get -y --no-install-recommends install \
      bats \
      bats-assert \
      bats-support

# -- bundled-plugins -------------------------------------------------------------------

FROM plugins-source AS amqp-plugin

COPY plugins/amqp ./plugins/amqp
RUN cmake -S plugins/amqp -B build-amqp -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-amqp --parallel && \
      cmake --build build-amqp --target bats && \
      DESTDIR=/plugin/amqp cmake --install build-amqp --strip --component Runtime && \
      rm -rf build-build-amqp

FROM plugins-source AS azure-blob-storage-plugin

COPY plugins/azure-blob-storage ./plugins/azure-blob-storage
RUN cmake -S plugins/azure-blob-storage -B build-azure-blob-storage -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-azure-blob-storage --parallel && \
      cmake --build build-azure-blob-storage --target bats && \
      DESTDIR=/plugin/azure-blob-storage cmake --install build-azure-blob-storage --strip --component Runtime && \
      rm -rf build-build-azure-blob-storage

FROM plugins-source AS fluent-bit-plugin

COPY plugins/fluent-bit ./plugins/fluent-bit
RUN cmake -S plugins/fluent-bit -B build-fluent-bit -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-fluent-bit --parallel && \
      cmake --build build-fluent-bit --target bats && \
      DESTDIR=/plugin/fluent-bit cmake --install build-fluent-bit --strip --component Runtime && \
      rm -rf build-build-fluent-bit

FROM plugins-source AS gcs-plugin

COPY plugins/gcs ./plugins/gcs
RUN cmake -S plugins/gcs -B build-gcs -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-gcs --parallel && \
      cmake --build build-gcs --target bats && \
      DESTDIR=/plugin/gcs cmake --install build-gcs --strip --component Runtime && \
      rm -rf build-build-gcs

FROM plugins-source AS google-cloud-pubsub-plugin

COPY plugins/google-cloud-pubsub ./plugins/google-cloud-pubsub
RUN cmake -S plugins/google-cloud-pubsub -B build-google-cloud-pubsub -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" \
        -D CMAKE_PREFIX_PATH="${CMAKE_PREFIX_PATH};/opt/google-cloud-cpp" && \
      cmake --build build-google-cloud-pubsub --parallel && \
      cmake --build build-google-cloud-pubsub --target bats && \
      DESTDIR=/plugin/google-cloud-pubsub cmake --install build-google-cloud-pubsub --strip --component Runtime && \
      rm -rf build-build-google-cloud-pubsub

FROM plugins-source AS kafka-plugin

COPY plugins/kafka ./plugins/kafka
RUN cmake -S plugins/kafka -B build-kafka -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-kafka --parallel && \
      cmake --build build-kafka --target bats && \
      DESTDIR=/plugin/kafka cmake --install build-kafka --strip --component Runtime && \
      rm -rf build-build-kafka

FROM plugins-source AS nic-plugin

COPY plugins/nic ./plugins/nic
RUN cmake -S plugins/nic -B build-nic -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-nic --parallel && \
      cmake --build build-nic --target bats && \
      DESTDIR=/plugin/nic cmake --install build-nic --strip --component Runtime && \
      rm -rf build-build-nic

FROM plugins-source AS parquet-plugin

COPY plugins/parquet ./plugins/parquet
RUN cmake -S plugins/parquet -B build-parquet -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-parquet --parallel && \
    cmake --build build-parquet --target bats && \
    DESTDIR=/plugin/parquet cmake --install build-parquet --strip --component Runtime && \
    rm -rf build-build-parquet

FROM plugins-source AS s3-plugin

COPY plugins/s3 ./plugins/s3
RUN cmake -S plugins/s3 -B build-s3 -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-s3 --parallel && \
      cmake --build build-s3 --target bats && \
      DESTDIR=/plugin/s3 cmake --install build-s3 --strip --component Runtime && \
      rm -rf build-build-s3

FROM plugins-source AS sigma-plugin

COPY plugins/sigma ./plugins/sigma
RUN cmake -S plugins/sigma -B build-sigma -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-sigma --parallel && \
      cmake --build build-sigma --target bats && \
      DESTDIR=/plugin/sigma cmake --install build-sigma --strip --component Runtime && \
      rm -rf build-build-sigma

FROM plugins-source AS sqs-plugin

COPY plugins/sqs ./plugins/sqs
RUN cmake -S plugins/sqs -B build-sqs -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-sqs --parallel && \
      cmake --build build-sqs --target bats && \
      DESTDIR=/plugin/sqs cmake --install build-sqs --strip --component Runtime && \
      rm -rf build-build-sqs

FROM plugins-source AS from_velociraptor-plugin

COPY plugins/from_velociraptor ./plugins/from_velociraptor
RUN cmake -S plugins/from_velociraptor -B build-from_velociraptor -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-from_velociraptor --parallel && \
      cmake --build build-from_velociraptor --target bats && \
      DESTDIR=/plugin/from_velociraptor cmake --install build-from_velociraptor --strip --component Runtime && \
      rm -rf build-build-from_velociraptor

FROM plugins-source AS web-plugin

COPY plugins/web ./plugins/web
RUN cmake -S plugins/web -B build-web -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-web --parallel && \
      cmake --build build-web --target bats && \
      DESTDIR=/plugin/web cmake --install build-web --strip --component Runtime && \
      rm -rf build-build-web

FROM plugins-source AS yara-plugin

COPY plugins/yara ./plugins/yara
RUN cmake -S plugins/yara -B build-yara -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-yara --parallel && \
      cmake --build build-yara --target bats && \
      DESTDIR=/plugin/yara cmake --install build-yara --strip --component Runtime && \
      rm -rf build-build-yara

FROM plugins-source AS zmq-plugin

COPY plugins/zmq ./plugins/zmq
RUN cmake -S plugins/zmq -B build-zmq -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-zmq --parallel && \
      cmake --build build-zmq --target bats && \
      DESTDIR=/plugin/zmq cmake --install build-zmq --strip --component Runtime && \
      rm -rf build-build-zmq

# -- tenzir-de -----------------------------------------------------------------

FROM runtime-base AS tenzir-de

# When changing these, make sure to also update the entries in the flake.nix
# file.
ENV PREFIX="/opt/tenzir" \
    PATH="/opt/tenzir/bin:${PATH}" \
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

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates \
      gnupg2 \
      libasan6 \
      libboost-filesystem++1.81 \
      libboost-url1.81 \
      libboost-stacktrace1.81 \
      libc++1 \
      libc++abi1 \
      libflatbuffers2 \
      libfmt9 \
      libgrpc++1.51 \
      libhttp-parser2.9 \
      libmaxminddb0 \
      libpcap0.8 \
      libprotobuf32 \
      librabbitmq4 \
      librdkafka++1 \
      libre2-9 \
      libspdlog1.10 \
      libunwind8 \
      libxxhash-dev \
      libyaml-cpp0.7 \
      libyara9 \
      libzmq5 \
      lsb-release \
      openssl \
      python3 \
      python3-venv \
      robin-map-dev \
      wget && \
    apt-get -y --no-install-recommends install /tmp/custom-packages/*.deb && \
    rm -rf /tmp/custom-packages && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /var/lib/tenzir
VOLUME ["/var/cache/tenzir", "/var/lib/tenzir"]

# Verify that Tenzir starts up correctly.
RUN tenzir 'version'

ENTRYPOINT ["tenzir"]
CMD ["--help"]

COPY --from=amqp-plugin --chown=tenzir:tenzir /plugin/amqp /
COPY --from=azure-blob-storage-plugin --chown=tenzir:tenzir /plugin/azure-blob-storage /
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

FROM plugins-source AS azure-log-analytics-plugin

COPY contrib/tenzir-plugins/azure-log-analytics ./contrib/tenzir-plugins/azure-log-analytics
RUN cmake -S contrib/tenzir-plugins/azure-log-analytics -B build-azure-log-analytics -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-azure-log-analytics --parallel && \
      cmake --build build-azure-log-analytics --target bats && \
      DESTDIR=/plugin/azure-log-analytics cmake --install build-azure-log-analytics --strip --component Runtime && \
      rm -rf build-build-azure-log-analytics

FROM plugins-source AS compaction-plugin

COPY contrib/tenzir-plugins/compaction ./contrib/tenzir-plugins/compaction
RUN cmake -S contrib/tenzir-plugins/compaction -B build-compaction -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-compaction --parallel 1 && \
      cmake --build build-compaction --target bats && \
      DESTDIR=/plugin/compaction cmake --install build-compaction --strip --component Runtime && \
      rm -rf build-compaction

FROM plugins-source AS context-plugin

COPY contrib/tenzir-plugins/context ./contrib/tenzir-plugins/context
RUN cmake -S contrib/tenzir-plugins/context -B build-context -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-context --parallel && \
      cmake --build build-context --target bats && \
      DESTDIR=/plugin/context cmake --install build-context --strip --component Runtime && \
      rm -rf build-context

FROM plugins-source AS pipeline-manager-plugin

COPY contrib/tenzir-plugins/pipeline-manager ./contrib/tenzir-plugins/pipeline-manager
RUN cmake -S contrib/tenzir-plugins/pipeline-manager -B build-pipeline-manager -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-pipeline-manager --parallel && \
      cmake --build build-pipeline-manager --target bats && \
      DESTDIR=/plugin/pipeline-manager cmake --install build-pipeline-manager --strip --component Runtime && \
      rm -rf build-pipeline-manager

FROM plugins-source AS packages-plugin

# TODO: We can't run the packages integration tests here at the moment, since
# they require the context and pipeline-manager plugins to be available.
COPY contrib/tenzir-plugins/packages ./contrib/tenzir-plugins/packages
RUN cmake -S contrib/tenzir-plugins/packages -B build-packages -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-packages --parallel && \
      DESTDIR=/plugin/packages cmake --install build-packages --strip --component Runtime && \
      rm -rf build-packages

FROM plugins-source AS platform-plugin

COPY contrib/tenzir-plugins/platform ./contrib/tenzir-plugins/platform
RUN cmake -S contrib/tenzir-plugins/platform -B build-platform -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-platform --parallel && \
      cmake --build build-platform --target bats && \
      DESTDIR=/plugin/platform cmake --install build-platform --strip --component Runtime && \
      rm -rf build-platform

FROM plugins-source AS snowflake-plugin

COPY contrib/tenzir-plugins/snowflake ./contrib/tenzir-plugins/snowflake
RUN cmake -S contrib/tenzir-plugins/snowflake -B build-snowflake -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-snowflake --parallel && \
      cmake --build build-snowflake --target bats && \
      DESTDIR=/plugin/snowflake cmake --install build-snowflake --strip --component Runtime && \
      rm -rf build-snowflake

FROM plugins-source AS to_asl-plugin

COPY contrib/tenzir-plugins/to_asl ./contrib/tenzir-plugins/to_asl
RUN cmake -S contrib/tenzir-plugins/to_asl -B build-to_asl -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-to_asl --parallel && \
      cmake --build build-to_asl --target bats && \
      DESTDIR=/plugin/to_asl cmake --install build-to_asl --strip --component Runtime && \
      rm -rf build-to_asl

FROM plugins-source AS to_splunk-plugin

COPY contrib/tenzir-plugins/to_splunk ./contrib/tenzir-plugins/to_splunk
RUN cmake -S contrib/tenzir-plugins/to_splunk -B build-to_splunk -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-to_splunk --parallel && \
      cmake --build build-to_splunk --target bats && \
      DESTDIR=/plugin/to_splunk cmake --install build-to_splunk --strip --component Runtime && \
      rm -rf build-to_splunk

FROM plugins-source AS vast-plugin

COPY contrib/tenzir-plugins/vast ./contrib/tenzir-plugins/vast
RUN cmake -S contrib/tenzir-plugins/vast -B build-vast -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-vast --parallel && \
      cmake --build build-vast --target bats && \
      DESTDIR=/plugin/vast cmake --install build-vast --strip --component Runtime && \
      rm -rf build-vast

# -- tenzir-ce -------------------------------------------------------------------

FROM tenzir-de AS tenzir-ce-arm64

COPY --from=azure-log-analytics-plugin --chown=tenzir:tenzir /plugin/azure-log-analytics /
COPY --from=compaction-plugin --chown=tenzir:tenzir /plugin/compaction /
COPY --from=context-plugin --chown=tenzir:tenzir /plugin/context /
COPY --from=pipeline-manager-plugin --chown=tenzir:tenzir /plugin/pipeline-manager /
COPY --from=packages-plugin --chown=tenzir:tenzir /plugin/packages /
COPY --from=platform-plugin --chown=tenzir:tenzir /plugin/platform /
COPY --from=to_asl-plugin --chown=tenzir:tenzir /plugin/to_asl /
COPY --from=to_splunk-plugin --chown=tenzir:tenzir /plugin/to_splunk /
COPY --from=vast-plugin --chown=tenzir:tenzir /plugin/vast /

FROM tenzir-ce-arm64 AS tenzir-ce-amd64

USER root:root

RUN if [ "$(uname -m)" = "x86_64" ]; then \
      wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb" && \
      apt-get -y --no-install-recommends install \
        ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
      apt-get update && \
      apt-get -y --no-install-recommends install libadbc-driver-manager104 libadbc-driver-snowflake104 && \
      snowflake_sopath="$(ldconfig -p | grep snowflake | grep -o '[^ ]*$')"; echo ${snowflake_sopath}; ln -s ${snowflake_sopath} ${snowflake_sopath%.*} && \
      rm -rf /var/lib/apt/lists/*; \
    fi

COPY --from=snowflake-plugin --chown=tenzir:tenzir /plugin/snowflake /

FROM tenzir-ce-${TARGETARCH} AS tenzir-ce

# -- tenzir-node-ce ------------------------------------------------------------

FROM tenzir-ce AS tenzir-node-ce

ENTRYPOINT ["tenzir-node"]

# -- tenzir-demo --------------------------------------------------------------

FROM tenzir-node-ce AS tenzir-demo

COPY /scripts/install-demo-node-package.tql /tmp/install-demo-node-package.tql
ENV TENZIR_START__COMMANDS="exec --file /tmp/install-demo-node-package.tql" \
    TENZIR_TQL2="true"

# -- tenzir-node -----------------------------------------------------------------

FROM tenzir-node-ce AS tenzir-node

# -- tenzir ----------------------------------------------------------------------

# As a last stage we re-introduce the community edition as tenzir so that it's
# the default when not specifying a build target.
FROM tenzir-ce AS tenzir
