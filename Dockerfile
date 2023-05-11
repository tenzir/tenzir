# -- dependencies --------------------------------------------------------------

FROM debian:bookworm-slim AS dependencies
LABEL maintainer="engineering@tenzir.com"

ENV CC="gcc-12" \
    CXX="g++-12"

WORKDIR /tmp/vast

COPY scripts ./scripts

RUN ./scripts/debian/install-dev-dependencies.sh && rm -rf /var/lib/apt/lists/*

# VAST
COPY changelog ./changelog
COPY cmake ./cmake
COPY contrib/tools ./contrib/tools
COPY examples ./examples
COPY libvast ./libvast
COPY libvast_test ./libvast_test
COPY plugins ./plugins
COPY python ./python
COPY schema ./schema
COPY vast ./vast
COPY CMakeLists.txt LICENSE VAST.spdx README.md VERSIONING.md \
     vast.yaml.example version.json ./

# -- development ---------------------------------------------------------------

FROM dependencies AS development

ENV PREFIX="/opt/tenzir/vast" \
    PATH="/opt/tenzir/vast/bin:${PATH}" \
    CC="gcc-12" \
    CXX="g++-12" \
    VAST_DB_DIRECTORY="/var/lib/vast" \
    VAST_LOG_FILE="/var/log/vast/server.log" \
    VAST_ENDPOINT="0.0.0.0"

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

RUN mkdir -p $PREFIX/etc/vast /var/log/vast /var/lib/vast

EXPOSE 5158/tcp

WORKDIR /var/lib/vast
VOLUME ["/var/lib/vast"]

ENTRYPOINT ["vast"]
CMD ["--help"]

# -- production ----------------------------------------------------------------

FROM debian:bookworm-slim AS production

# When changing these, make sure to also update the entries in the flake.nix
# file.
ENV PREFIX="/opt/tenzir/vast" \
    PATH="/opt/tenzir/vast/bin:${PATH}" \
    VAST_DB_DIRECTORY="/var/lib/vast" \
    VAST_LOG_FILE="/var/log/vast/server.log" \
    VAST_ENDPOINT="0.0.0.0"

RUN useradd --system --user-group vast
COPY --from=development --chown=vast:vast $PREFIX/ $PREFIX/
COPY --from=development --chown=vast:vast /var/lib/vast/ /var/lib/vast
COPY --from=development --chown=vast:vast /var/log/vast/ /var/log/vast

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates \
      gnupg2 \
      libasan6 \
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

USER vast:vast

EXPOSE 5158/tcp
WORKDIR /var/lib/vast
VOLUME ["/var/lib/vast"]

# Verify that VAST starts up correctly.
RUN vast version

ENTRYPOINT ["vast"]
CMD ["--help"]
