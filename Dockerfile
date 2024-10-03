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
COPY plugins ./plugins
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
    cmake --build build --parallel
RUN cmake --build build --target integration
RUN cmake --install build --strip

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
    rm /root/fluent-bit_*.deb /root/arrow_*.deb && \
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
COPY contrib/tenzir-plugins ./contrib/tenzir-plugins

FROM plugins-source AS azure-log-analytics-plugin

RUN cmake -S contrib/tenzir-plugins/azure-log-analytics \
      -B build-azure-log-analytics -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-azure-log-analytics --parallel
RUN cmake --build build-azure-log-analytics --target integration
RUN DESTDIR=/plugin/azure-log-analytics cmake --install build-azure-log-analytics --strip --component Runtime

FROM plugins-source AS compaction-plugin

RUN cmake -S contrib/tenzir-plugins/compaction -B build-compaction -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-compaction --parallel
RUN cmake --build build-compaction --target integration
RUN DESTDIR=/plugin/compaction cmake --install build-compaction --strip --component Runtime

FROM plugins-source AS context-plugin

RUN cmake -S contrib/tenzir-plugins/context -B build-context -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-context --parallel
RUN cmake --build build-context --target integration
RUN DESTDIR=/plugin/context cmake --install build-context --strip --component Runtime

FROM plugins-source AS pipeline-manager-plugin

RUN cmake -S contrib/tenzir-plugins/pipeline-manager -B build-pipeline-manager -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-pipeline-manager --parallel
RUN cmake --build build-pipeline-manager --target integration
RUN DESTDIR=/plugin/pipeline-manager cmake --install build-pipeline-manager --strip --component Runtime

FROM plugins-source AS packages-plugin

# TODO: We can't run the packages integration tests here at the moment, since
# they require the context and pipeline-manager plugins to be available.
RUN cmake -S contrib/tenzir-plugins/packages -B build-packages -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-packages --parallel
RUN DESTDIR=/plugin/packages cmake --install build-packages --strip --component Runtime

FROM plugins-source AS platform-plugin

RUN cmake -S contrib/tenzir-plugins/platform -B build-platform -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-platform --parallel
RUN cmake --build build-platform --target integration
RUN DESTDIR=/plugin/platform cmake --install build-platform --strip --component Runtime

FROM plugins-source AS vast-plugin

RUN cmake -S contrib/tenzir-plugins/vast -B build-vast -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
    cmake --build build-vast --parallel
RUN cmake --build build-vast --target integration
RUN DESTDIR=/plugin/vast cmake --install build-vast --strip --component Runtime

# -- tenzir-ce -------------------------------------------------------------------

FROM tenzir-de AS tenzir-ce

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
