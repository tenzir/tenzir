# -- dependencies --------------------------------------------------------------

FROM debian:bookworm-slim AS dependencies
LABEL maintainer="engineering@tenzir.com"

ENV CC="gcc-12" \
    CXX="g++-12"

WORKDIR /tmp/tenzir

COPY scripts ./scripts

RUN ./scripts/debian/install-dev-dependencies.sh && rm -rf /var/lib/apt/lists/*

# Tenzir
COPY changelog ./changelog
COPY cmake ./cmake
COPY examples ./examples
COPY libvast ./libvast
COPY libvast_test ./libvast_test
COPY plugins ./plugins
COPY python ./python
COPY schema ./schema
COPY vast ./vast
COPY CMakeLists.txt LICENSE README.md Tenzir.spdx VERSIONING.md \
     tenzir.yaml.example version.json ./

# -- development ---------------------------------------------------------------

FROM dependencies AS development

ENV PREFIX="/opt/tenzir" \
    PATH="/opt/tenzir/bin:${PATH}" \
    CC="gcc-12" \
    CXX="g++-12" \
    TENZIR_DB_DIRECTORY="/var/lib/tenzir" \
    TENZIR_LOG_FILE="/var/log/tenzir/server.log" \
    TENZIR_ENDPOINT="0.0.0.0"

# Additional arguments to be passed to CMake.
ARG VAST_BUILD_OPTIONS

RUN cmake -B build -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" \
      -D CMAKE_BUILD_TYPE:STRING="Release" \
      -D VAST_ENABLE_AVX_INSTRUCTIONS:BOOL="OFF" \
      -D VAST_ENABLE_AVX2_INSTRUCTIONS:BOOL="OFF" \
      -D VAST_ENABLE_UNIT_TESTS:BOOL="OFF" \
      -D VAST_ENABLE_DEVELOPER_MODE:BOOL="OFF" \
      -D VAST_ENABLE_BUNDLED_CAF:BOOL="ON" \
      -D VAST_ENABLE_BUNDLED_SIMDJSON:BOOL="ON" \
      -D VAST_ENABLE_MANPAGES:BOOL="OFF" \
      -D VAST_ENABLE_PYTHON_BINDINGS_DEPENDENCIES:BOOL="ON" \
      ${VAST_BUILD_OPTIONS} && \
    cmake --build build --parallel && \
    cmake --install build --strip && \
    rm -rf build

RUN mkdir -p $PREFIX/etc/tenzir /var/log/tenzir /var/lib/tenzir

EXPOSE 5158/tcp

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
    TENZIR_DB_DIRECTORY="/var/lib/tenzir" \
    TENZIR_LOG_FILE="/var/log/tenzir/server.log" \
    TENZIR_ENDPOINT="0.0.0.0"

RUN useradd --system --user-group tenzir
COPY --from=development --chown=tenzir:tenzir $PREFIX/ $PREFIX/
COPY --from=development --chown=tenzir:tenzir /var/lib/tenzir/ /var/lib/tenzir
COPY --from=development --chown=tenzir:tenzir /var/log/tenzir/ /var/log/tenzir

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates \
      gnupg2 \
      libasan6 \
      libboost-filesystem++1.81 \
      libc++1 \
      libc++abi1 \
      libflatbuffers2 \
      libfmt9 \
      libhttp-parser2.9 \
      libpcap0.8 \
      libre2-9 \
      libspdlog1.10 \
      libunwind8 \
      libxxhash-dev \
      libyaml-cpp0.7 \
      lsb-release \
      openssl \
      robin-map-dev \
      wget && \
    wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb" && \
    apt-get -y --no-install-recommends install \
      ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get -y --no-install-recommends install libarrow1100 libparquet1100 && \
    rm -rf /var/lib/apt/lists/*

USER tenzir:tenzir

EXPOSE 5158/tcp
WORKDIR /var/lib/tenzir
VOLUME ["/var/lib/tenzir"]

# Verify that Tenzir starts up correctly.
RUN tenzir version

ENTRYPOINT ["tenzir"]
CMD ["--help"]

# -- tenzir-node-de ------------------------------------------------------------

FROM tenzir-de AS tenzir-node-de

ENTRYPOINT ["tenzir-node"]

# -- plugins -------------------------------------------------------------------

FROM development AS plugins-source

WORKDIR /tmp/tenzir
COPY contrib/tenzir-plugins ./contrib/tenzir-plugins

FROM plugins-source AS compaction-plugin

RUN cmake -S contrib/tenzir-plugins/compaction -B build-compaction -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-compaction --parallel && \
      DESTDIR=/plugin/compaction cmake --install build-compaction --strip --component Runtime && \
      rm -rf build-compaction

FROM plugins-source AS inventory-plugin

RUN cmake -S contrib/tenzir-plugins/inventory -B build-inventory -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-inventory --parallel && \
      DESTDIR=/plugin/inventory cmake --install build-inventory --strip --component Runtime && \
      rm -rf build-inventory

FROM plugins-source AS matcher-plugin

RUN cmake -S contrib/tenzir-plugins/matcher -B build-matcher -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-matcher --parallel && \
      DESTDIR=/plugin/matcher cmake --install build-matcher --strip --component Runtime && \
      rm -rf build-matcher

FROM plugins-source AS netflow-plugin

RUN cmake -S contrib/tenzir-plugins/netflow -B build-netflow -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-netflow --parallel && \
      DESTDIR=/plugin/netflow cmake --install build-netflow --strip --component Runtime && \
      rm -rf build-netflow

FROM plugins-source AS pipeline-manager-plugin

RUN cmake -S contrib/tenzir-plugins/pipeline_manager -B build-pipeline_manager -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-pipeline_manager --parallel && \
      DESTDIR=/plugin/pipeline_manager cmake --install build-pipeline_manager --strip --component Runtime && \
      rm -rf build-pipeline_manager

FROM plugins-source AS platform-plugin

RUN cmake -S contrib/tenzir-plugins/platform -B build-platform -G Ninja \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" && \
      cmake --build build-platform --parallel && \
      DESTDIR=/plugin/platform cmake --install build-platform --strip --component Runtime && \
      rm -rf build-platform

# -- tenzir-ce -------------------------------------------------------------------

FROM tenzir-de AS tenzir-ce

COPY --from=matcher-plugin --chown=tenzir:tenzir /plugin/matcher /
COPY --from=netflow-plugin --chown=tenzir:tenzir /plugin/netflow /
COPY --from=pipeline-manager-plugin --chown=tenzir:tenzir /plugin/pipeline_manager /
COPY --from=platform-plugin --chown=tenzir:tenzir /plugin/platform /

# -- tenzir-node-ce ------------------------------------------------------------

FROM tenzir-ce AS tenzir-node-ce

ENTRYPOINT ["tenzir-node"]

# -- tenzir-demo --------------------------------------------------------------

FROM tenzir-ce AS tenzir-demo

USER root:root
COPY demo-node /demo-node
RUN apt-get update && \
    apt install -y \
        curl \
        bc \
        zstd && \
        rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/demo-node/entrypoint.bash"]

# -- tenzir-ee -------------------------------------------------------------------

FROM tenzir-ce AS tenzir-ee

COPY --from=compaction-plugin --chown=tenzir:tenzir /plugin/compaction /

# -- tenzir-node-ee ------------------------------------------------------------

FROM tenzir-ee AS tenzir-node-ee

ENTRYPOINT ["tenzir-node"]

# -- tenzir-node -----------------------------------------------------------------

FROM tenzir-node-ce AS tenzir-node

# -- tenzir ----------------------------------------------------------------------

# As a last stage we re-introduce the community edition as tenzir so that it's
# the default when not specifying a build target.
FROM tenzir-ce AS tenzir
