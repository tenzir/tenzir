# -- fluent-bit-package -----------------------------------------------------------

FROM debian:bookworm-slim AS fluent-bit-package

ENV CC="gcc-12" \
    CXX="g++-12"

WORKDIR /tmp/fluent-bit
COPY scripts/debian/build-fluent-bit.sh ./scripts/debian/
RUN ./scripts/debian/build-fluent-bit.sh && \
    rm -rf /var/lib/apt/lists/*

# -- dependencies --------------------------------------------------------------

FROM debian:bookworm-slim AS dependencies
LABEL maintainer="engineering@tenzir.com"

ENV CC="gcc-12" \
    CXX="g++-12"

WORKDIR /tmp/tenzir

COPY --from=fluent-bit-package /root/fluent-bit_*.deb /root/
COPY scripts/debian/install-dev-dependencies.sh ./scripts/debian/
COPY scripts/debian/build-arrow.sh ./scripts/debian/
RUN ./scripts/debian/install-dev-dependencies.sh && \
    apt-get -y --no-install-recommends install /root/fluent-bit_*.deb && \
    rm /root/fluent-bit_*.deb && \
    rm -rf /var/lib/apt/lists/*
COPY scripts/debian/install-aws-sdk.sh ./scripts/debian/
RUN ./scripts/debian/install-aws-sdk.sh

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
      -D CMAKE_PREFIX_PATH="/opt/aws-sdk-cpp" \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" \
      -D CMAKE_BUILD_TYPE:STRING="Release" \
      -D TENZIR_ENABLE_AVX_INSTRUCTIONS:BOOL="OFF" \
      -D TENZIR_ENABLE_AVX2_INSTRUCTIONS:BOOL="OFF" \
      -D TENZIR_ENABLE_UNIT_TESTS:BOOL="OFF" \
      -D TENZIR_ENABLE_DEVELOPER_MODE:BOOL="OFF" \
      -D TENZIR_ENABLE_BUNDLED_CAF:BOOL="ON" \
      -D TENZIR_ENABLE_BUNDLED_SIMDJSON:BOOL="ON" \
      -D TENZIR_ENABLE_MANPAGES:BOOL="OFF" \
      -D TENZIR_ENABLE_PYTHON_BINDINGS_DEPENDENCIES:BOOL="ON" \
      ${TENZIR_BUILD_OPTIONS} && \
    cmake --build build --parallel && \
    cmake --build build --target integration && \
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

# -- tenzir-de -----------------------------------------------------------------

FROM debian:bookworm-slim AS tenzir-de

# When changing these, make sure to also update the entries in the flake.nix
# file.
ENV PREFIX="/opt/tenzir" \
    PATH="/opt/tenzir/bin:${PATH}" \
    TENZIR_CACHE_DIRECTORY="/var/cache/tenzir" \
    TENZIR_STATE_DIRECTORY="/var/lib/tenzir" \
    TENZIR_LOG_FILE="/var/log/tenzir/server.log" \
    TENZIR_ENDPOINT="0.0.0.0"

RUN useradd --system --user-group tenzir
COPY --from=development --chown=tenzir:tenzir $PREFIX/ $PREFIX/
COPY --from=development --chown=tenzir:tenzir /var/cache/tenzir/ /var/cache/tenzir/
COPY --from=development --chown=tenzir:tenzir /var/lib/tenzir/ /var/lib/tenzir/
COPY --from=development --chown=tenzir:tenzir /var/log/tenzir/ /var/log/tenzir/
COPY --from=development /opt/aws-sdk-cpp/lib/ /opt/aws-sdk-cpp/lib/
COPY --from=dependencies /arrow_*.deb /root/
COPY --from=fluent-bit-package /root/fluent-bit_*.deb /root/

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates \
      gnupg2 \
      libasan6 \
      libboost-filesystem++1.81 \
      libboost-url1.81 \
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
    apt-get -y --no-install-recommends install /root/arrow_*.deb && \
    apt-get -y --no-install-recommends install /root/fluent-bit_*.deb && \
    rm /root/arrow_*.deb /root/fluent-bit_*.deb && \
    rm -rf /var/lib/apt/lists/* && \
    echo "/opt/aws-sdk-cpp/lib" > /etc/ld.so.conf.d/aws-cpp-sdk.conf && \
    ldconfig

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

# -- plugins -------------------------------------------------------------------

FROM development AS plugins-source

WORKDIR /tmp/tenzir
COPY plugins ./plugins
COPY contrib/tenzir-plugins ./contrib/tenzir-plugins

RUN apt-get -y --no-install-recommends install \
      bats \
      bats-assert \
      bats-support

# -- bundled-plugins -------------------------------------------------------------------

FROM plugins-source AS google-cloud-pubsub-plugin

COPY scripts/debian/install-google-cloud.sh ./scripts/debian/
RUN ./scripts/debian/install-google-cloud.sh
RUN cmake -S plugins/google-cloud-pubsub -B build-google-cloud-pubsub -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" \
        -D CMAKE_PREFIX_PATH="/opt/google-cloud-cpp;" && \
      cmake --build build-google-cloud-pubsub --parallel && \
      cmake --build build-google-cloud-pubsub --target integration && \
      DESTDIR=/plugin/google-cloud-pubsub cmake --install build-google-cloud-pubsub --strip --component Runtime && \
      rm -rf build-build-google-cloud-pubsub

FROM plugins-source AS amqp-plugin

RUN cmake -S plugins/amqp -B build-amqp -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-amqp --parallel && \
      cmake --build build-amqp --target integration && \
      DESTDIR=/plugin/amqp cmake --install build-amqp --strip --component Runtime && \
      rm -rf build-build-amqp

FROM plugins-source AS azure-blob-storage-plugin

RUN cmake -S plugins/azure-blob-storage -B build-azure-blob-storage -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-azure-blob-storage --parallel && \
      cmake --build build-azure-blob-storage --target integration && \
      DESTDIR=/plugin/azure-blob-storage cmake --install build-azure-blob-storage --strip --component Runtime && \
      rm -rf build-build-azure-blob-storage

FROM plugins-source AS fluent-bit-plugin

RUN cmake -S plugins/fluent-bit -B build-fluent-bit -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-fluent-bit --parallel && \
      cmake --build build-fluent-bit --target integration && \
      DESTDIR=/plugin/fluent-bit cmake --install build-fluent-bit --strip --component Runtime && \
      rm -rf build-build-fluent-bit

FROM plugins-source AS gcs-plugin

RUN cmake -S plugins/gcs -B build-gcs -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-gcs --parallel && \
      cmake --build build-gcs --target integration && \
      DESTDIR=/plugin/gcs cmake --install build-gcs --strip --component Runtime && \
      rm -rf build-build-gcs

FROM plugins-source AS kafka-plugin

RUN cmake -S plugins/kafka -B build-kafka -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-kafka --parallel && \
      cmake --build build-kafka --target integration && \
      DESTDIR=/plugin/kafka cmake --install build-kafka --strip --component Runtime && \
      rm -rf build-build-kafka

FROM plugins-source AS nic-plugin

RUN cmake -S plugins/nic -B build-nic -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-nic --parallel && \
      cmake --build build-nic --target integration && \
      DESTDIR=/plugin/nic cmake --install build-nic --strip --component Runtime && \
      rm -rf build-build-nic

FROM plugins-source AS s3-plugin

RUN cmake -S plugins/s3 -B build-s3 -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-s3 --parallel && \
      cmake --build build-s3 --target integration && \
      DESTDIR=/plugin/s3 cmake --install build-s3 --strip --component Runtime && \
      rm -rf build-build-s3

FROM plugins-source AS sigma-plugin

RUN cmake -S plugins/sigma -B build-sigma -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-sigma --parallel && \
      cmake --build build-sigma --target integration && \
      DESTDIR=/plugin/sigma cmake --install build-sigma --strip --component Runtime && \
      rm -rf build-build-sigma

FROM plugins-source AS sqs-plugin

RUN cmake -S plugins/sqs -B build-sqs -G Ninja \
        -D CMAKE_PREFIX_PATH="/opt/aws-sdk-cpp" \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-sqs --parallel && \
      cmake --build build-sqs --target integration && \
      DESTDIR=/plugin/sqs cmake --install build-sqs --strip --component Runtime && \
      rm -rf build-build-sqs

FROM plugins-source AS velociraptor-plugin

RUN cmake -S plugins/velociraptor -B build-velociraptor -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-velociraptor --parallel && \
      cmake --build build-velociraptor --target integration && \
      DESTDIR=/plugin/velociraptor cmake --install build-velociraptor --strip --component Runtime && \
      rm -rf build-build-velociraptor

FROM plugins-source AS web-plugin

RUN cmake -S plugins/web -B build-web -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-web --parallel && \
      cmake --build build-web --target integration && \
      DESTDIR=/plugin/web cmake --install build-web --strip --component Runtime && \
      rm -rf build-build-web

FROM plugins-source AS yara-plugin

RUN cmake -S plugins/yara -B build-yara -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-yara --parallel && \
      cmake --build build-yara --target integration && \
      DESTDIR=/plugin/yara cmake --install build-yara --strip --component Runtime && \
      rm -rf build-build-yara

FROM plugins-source AS zmq-plugin

RUN cmake -S plugins/zmq -B build-zmq -G Ninja \
        -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-zmq --parallel && \
      cmake --build build-zmq --target integration && \
      DESTDIR=/plugin/zmq cmake --install build-zmq --strip --component Runtime && \
      rm -rf build-build-zmq

# -- third-party-plugins -------------------------------------------------------------------

FROM plugins-source AS azure-log-analytics-plugin

RUN cmake -S contrib/tenzir-plugins/azure-log-analytics -B build-azure-log-analytics -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-azure-log-analytics --parallel && \
      cmake --build build-azure-log-analytics --target integration && \
      DESTDIR=/plugin/azure-log-analytics cmake --install build-azure-log-analytics --strip --component Runtime && \
      rm -rf build-build-azure-log-analytics

FROM plugins-source AS compaction-plugin

RUN cmake -S contrib/tenzir-plugins/compaction -B build-compaction -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-compaction --parallel && \
      cmake --build build-compaction --target integration && \
      DESTDIR=/plugin/compaction cmake --install build-compaction --strip --component Runtime && \
      rm -rf build-compaction

FROM plugins-source AS context-plugin

RUN cmake -S contrib/tenzir-plugins/context -B build-context -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-context --parallel && \
      cmake --build build-context --target integration && \
      DESTDIR=/plugin/context cmake --install build-context --strip --component Runtime && \
      rm -rf build-context

FROM plugins-source AS pipeline-manager-plugin

RUN cmake -S contrib/tenzir-plugins/pipeline-manager -B build-pipeline-manager -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-pipeline-manager --parallel && \
      cmake --build build-pipeline-manager --target integration && \
      DESTDIR=/plugin/pipeline-manager cmake --install build-pipeline-manager --strip --component Runtime && \
      rm -rf build-pipeline-manager

FROM plugins-source AS packages-plugin

# TODO: We can't run the packages integration tests here at the moment, since
# they require the context and pipeline-manager plugins to be available.
RUN cmake -S contrib/tenzir-plugins/packages -B build-packages -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-packages --parallel && \
      DESTDIR=/plugin/packages cmake --install build-packages --strip --component Runtime && \
      rm -rf build-packages

FROM plugins-source AS platform-plugin

RUN cmake -S contrib/tenzir-plugins/platform -B build-platform -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-platform --parallel && \
      cmake --build build-platform --target integration && \
      DESTDIR=/plugin/platform cmake --install build-platform --strip --component Runtime && \
      rm -rf build-platform

FROM plugins-source AS vast-plugin

RUN cmake -S contrib/tenzir-plugins/vast -B build-vast -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-vast --parallel && \
      cmake --build build-vast --target integration && \
      DESTDIR=/plugin/vast cmake --install build-vast --strip --component Runtime && \
      rm -rf build-vast

# -- tenzir-ce -------------------------------------------------------------------

FROM tenzir-de AS tenzir-ce

COPY --from=google-cloud-pubsub-plugin --chown=tenzir:tenzir /plugin/google-cloud-pubsub /
COPY --from=amqp-plugin --chown=tenzir:tenzir /plugin/amqp /
COPY --from=azure-blob-storage-plugin --chown=tenzir:tenzir /plugin/azure-blob-storage /
COPY --from=fluent-bit-plugin --chown=tenzir:tenzir /plugin/fluent-bit /
COPY --from=gcs-plugin --chown=tenzir:tenzir /plugin/gcs /
COPY --from=kafka-plugin --chown=tenzir:tenzir /plugin/kafka /
COPY --from=nic-plugin --chown=tenzir:tenzir /plugin/nic /
COPY --from=s3-plugin --chown=tenzir:tenzir /plugin/s3 /
COPY --from=sigma-plugin --chown=tenzir:tenzir /plugin/sigma /
COPY --from=sqs-plugin --chown=tenzir:tenzir /plugin/sqs /
COPY --from=velociraptor-plugin --chown=tenzir:tenzir /plugin/velociraptor /
COPY --from=web-plugin --chown=tenzir:tenzir /plugin/web /
COPY --from=yara-plugin --chown=tenzir:tenzir /plugin/yara /
COPY --from=zmq-plugin --chown=tenzir:tenzir /plugin/zmq /

COPY --from=azure-log-analytics-plugin --chown=tenzir:tenzir /plugin/azure-log-analytics /
COPY --from=compaction-plugin --chown=tenzir:tenzir /plugin/compaction /
COPY --from=context-plugin --chown=tenzir:tenzir /plugin/context /
COPY --from=pipeline-manager-plugin --chown=tenzir:tenzir /plugin/pipeline-manager /
COPY --from=packages-plugin --chown=tenzir:tenzir /plugin/packages /
COPY --from=platform-plugin --chown=tenzir:tenzir /plugin/platform /
COPY --from=vast-plugin --chown=tenzir:tenzir /plugin/vast /

# -- tenzir-node-ce ------------------------------------------------------------

FROM tenzir-ce AS tenzir-node-ce

ENTRYPOINT ["tenzir-node"]

# -- tenzir-demo --------------------------------------------------------------

FROM tenzir-node-ce AS tenzir-demo

ENV TENZIR_START__COMMANDS="exec \"from https://raw.githubusercontent.com/tenzir/library/main/demo-node/package.yaml | package add\""

# -- tenzir-node -----------------------------------------------------------------

FROM tenzir-node-ce AS tenzir-node

# -- tenzir ----------------------------------------------------------------------

# As a last stage we re-introduce the community edition as tenzir so that it's
# the default when not specifying a build target.
FROM tenzir-ce AS tenzir
