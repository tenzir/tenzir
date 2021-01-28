ARG DOCKER_BUILD=build

# build image, install build-dependencies and build vast
FROM debian:buster-backports as build
LABEL builder=true
LABEL maintainer="engineering@tenzir.com"

ENV PREFIX /usr/local
ENV CC gcc-8
ENV CXX g++-8
ENV BUILD_TYPE Release
ENV BUILD_DIR /tmp/src

# Compiler and dependency setup
RUN apt-get -qq update && apt-get -qqy install \
  build-essential gcc-8 g++-8 ninja-build libbenchmark-dev libpcap-dev tcpdump \
  libssl-dev python3-dev python3-pip python3-venv git-core jq gnupg2 \
  libyaml-cpp-dev libsimdjson-dev wget
RUN pip3 install --upgrade pip && pip install --upgrade cmake && \
  cmake --version

# flatbuffers
RUN echo "deb http://www.deb-multimedia.org buster main" | tee -a /etc/apt/sources.list && \
  apt-key adv --keyserver hkp://pool.sks-keyservers.net:80 --recv-keys 5C808C2B65558117 && \
  apt-get -qq update
RUN apt-get -qqy install libflatbuffers-dev flatbuffers-compiler-dev

# spdlog
RUN echo "deb http://deb.debian.org/debian buster-backports main" | tee -a /etc/apt/sources.list && apt-get -qq update
RUN apt-get -qqy -t buster-backports install libfmt-dev libspdlog-dev
# Apache Arrow (c.f. https://arrow.apache.org/install/)
# TODO: Arrow CMake is broken for 2.0 on Debian/Ubuntu. Switch to 3.0 once available
RUN wget https://apache.bintray.com/arrow/debian/pool/buster/main/a/apache-arrow-archive-keyring/apache-arrow-archive-keyring_1.0.1-1_all.deb && \
  apt-get -qqy install ./apache-arrow-archive-keyring_1.0.1-1_all.deb && \
  apt-get -qq update && \
  apt-get -qqy install libarrow-dev=1.0.1-1

# VAST
WORKDIR $BUILD_DIR/vast
COPY cmake ./cmake
COPY doc ./doc
COPY libvast ./libvast
COPY libvast_test ./libvast_test
COPY examples ./examples
COPY schema ./schema
COPY scripts ./scripts
COPY tools ./tools
COPY vast ./vast
COPY .clang-format .cmake-format LICENSE LICENSE.3rdparty README.md BANNER CMakeLists.txt configure vast.yaml.example ./
RUN ./configure \
    --prefix=$PREFIX \
    --build-type=$BUILD_TYPE \
    --log-level=INFO \
    --generator=Ninja
RUN cmake --build build --parallel
RUN CTEST_OUTPUT_ON_FAILURE=1 cmake --build build --target test
RUN cmake --build build --target integration
RUN cmake --build build --target install

# copy prebuilt image
FROM debian:buster-backports as prebuilt
ENV PREFIX /usr/local

RUN apt-get -qq update && apt-get -qqy install rsync

COPY opt/vast /opt/vast
RUN rsync -avh /opt/vast/* $PREFIX

# "Hack": Docker does not allow to use COPY --from=${BUILD_ARG}, but we can name
# a build stage after the contents of a build-arg and then use the stage as
# argument in COPY --from="stage"
FROM $DOCKER_BUILD as build_type

# Production image: copy application
FROM debian:buster-backports
ENV PREFIX /usr/local

COPY --from=build_type $PREFIX/ $PREFIX/
RUN apt-get -qq update && apt-get -qq install -y libc++1 libc++abi1 libpcap0.8 \
  openssl libsimdjson4 libyaml-cpp0.6 libasan5 libflatbuffers1 wget gnupg2

# Apache Arrow (c.f. https://arrow.apache.org/install/)
# TODO: Arrow CMake is broken for 2.0 on Debian/Ubuntu. Switch to 3.0 once available
RUN wget https://apache.bintray.com/arrow/debian/pool/buster/main/a/apache-arrow-archive-keyring/apache-arrow-archive-keyring_1.0.1-1_all.deb && \
  apt-get -qqy install ./apache-arrow-archive-keyring_1.0.1-1_all.deb && \
  apt-get -qq update && \
  apt-get -qqy install libarrow-dev=1.0.1-1


EXPOSE 42000/tcp

RUN echo "Adding vast user" && useradd --system --user-group vast

RUN mkdir -p /etc/vast /var/log/vast /var/lib/vast
COPY systemd/vast.yaml /etc/vast/vast.yaml
RUN chown -R vast:vast /var/log/vast /var/lib/vast

WORKDIR /var/lib/vast
VOLUME ["/var/lib/vast"]

USER vast:vast
ENTRYPOINT ["vast"]
CMD ["--help"]
