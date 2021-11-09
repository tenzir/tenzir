# -- dependencies --------------------------------------------------------------

FROM debian:bullseye-slim AS dependencies
LABEL maintainer="engineering@tenzir.com"

ENV CC="gcc-10" \
    CXX="g++-10"

WORKDIR /tmp/vast

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      build-essential \
      ca-certificates \
      cmake \
      flatbuffers-compiler-dev \
      g++-10 \
      gcc-10 \
      git-core \
      gnupg2 \
      jq \
      libcaf-dev \
      libbroker-dev \
      libflatbuffers-dev \
      libfmt-dev \
      libpcap-dev tcpdump \
      libsimdjson-dev \
      libspdlog-dev \
      libssl-dev \
      libunwind-dev \
      libyaml-cpp-dev \
      libxxhash-dev \
      lsb-release \
      ninja-build \
      pkg-config \
      python3-dev \
      python3-pip \
      python3-venv \
      robin-map-dev \
      wget && \
    wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb" && \
    apt-get -y --no-install-recommends install \
      ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get -y --no-install-recommends install libarrow-dev libprotobuf-dev && \
    rm -rf /var/lib/apt/lists/*

# VAST
COPY changelog ./changelog
COPY cmake ./cmake
COPY doc ./doc
COPY examples ./examples
COPY libvast ./libvast
COPY libvast_test ./libvast_test
COPY plugins ./plugins
COPY schema ./schema
COPY scripts ./scripts
COPY tools ./tools
COPY vast ./vast
COPY LICENSE LICENSE.3rdparty README.md BANNER CMakeLists.txt vast.yaml.example ./

# Resolve repository-internal symlinks.
# TODO: We should try to get rid of these long-term, as Docker does not work
# well with repository-internal symlinks. The pyvast symlink is unnecessary, and
# the integration test symlinks we can get rid of by copying the integration
# test directory to the build directory when building VAST.
RUN ln -sf ../../pyvast/pyvast examples/jupyter/pyvast && \
    ln -sf ../../vast.yaml.example vast/integration/vast.yaml.example && \
    ln -sf ../../vast/integration/data/ plugins/pcap/data/ && \
    ln -sf ../vast/integration/misc/scripts/print-arrow.py scripts/print-arrow.py

# -- development ---------------------------------------------------------------

FROM dependencies AS development

ENV PREFIX="/opt/tenzir/vast" \
    PATH="/opt/tenzir/vast/bin:${PATH}" \
    CC="gcc-10" \
    CXX="g++-10"

# Additional arguments to be passed to CMake.
ARG VAST_BUILD_OPTIONS

RUN cmake -B build -G Ninja \
      ${VAST_BUILD_OPTIONS} \
      -D CMAKE_INSTALL_PREFIX:STRING="$PREFIX" \
      -D CMAKE_BUILD_TYPE:STRING="Release" \
      -D VAST_ENABLE_UNIT_TESTS:BOOL="OFF" \
      -D VAST_ENABLE_DEVELOPER_MODE:BOOL="OFF" \
      -D VAST_PLUGINS:STRING="plugins/pcap;plugins/broker" && \
    cmake --build build --parallel && \
    cmake --install build --strip && \
    rm -rf build

RUN mkdir -p $PREFIX/etc/vast /var/log/vast /var/lib/vast
COPY systemd/vast.yaml $PREFIX/etc/vast/vast.yaml

EXPOSE 42000/tcp

WORKDIR /var/lib/vast
VOLUME ["/var/lib/vast"]

ENTRYPOINT ["vast"]
CMD ["--help"]

# -- production ----------------------------------------------------------------

FROM debian:bullseye-slim AS production

ENV PREFIX="/opt/tenzir/vast" \
    PATH="/opt/tenzir/vast/bin:${PATH}"

RUN useradd --system --user-group vast
COPY --from=development --chown=vast:vast $PREFIX/ $PREFIX/
COPY --from=development --chown=vast:vast /var/lib/vast/ /var/lib/vast
COPY --from=development --chown=vast:vast /var/log/vast/ /var/log/vast

RUN apt-get update && \
    apt-get -y --no-install-recommends install \
      ca-certificates \
      gnupg2 \
      libasan5 \
      libcaf-core0.17 \
      libcaf-io0.17 \
      libcaf-openssl0.17 \
      libbroker2 \
      libc++1 \
      libc++abi1 \
      libflatbuffers1 \
      libfmt7 \
      libpcap0.8 \
      libsimdjson5 \
      libspdlog1 \
      libunwind8 \
      libyaml-cpp0.6 \
      libxxhash-dev \
      lsb-release \
      openssl \
      robin-map-dev \
      wget && \
    wget "https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb" && \
    apt-get -y --no-install-recommends install \
      ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get -y --no-install-recommends install libarrow600 && \
    rm -rf /var/lib/apt/lists/*

USER vast:vast

EXPOSE 42000/tcp
WORKDIR /var/lib/vast
VOLUME ["/var/lib/vast"]

ENTRYPOINT ["vast"]
CMD ["--help"]
