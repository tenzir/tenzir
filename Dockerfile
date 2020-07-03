FROM debian:buster-slim as builder
LABEL maintainer="engineering@tenzir.com"
LABEL builder=true

ENV PREFIX /usr/local
ENV CC gcc-8
ENV CXX g++-8
ENV BUILD_TYPE Release
ENV BUILD_DIR /tmp/src
ENV DEBIAN_FRONTEND noninteractive

# Compiler and dependency setup
RUN apt-get -qq update && apt-get -qqy install \
  build-essential gcc-8 g++-8 ninja-build libbenchmark-dev libpcap-dev \
  libssl-dev python3-dev python3-pip python3-venv git-core jq tcpdump
RUN pip3 install --upgrade pip && pip install --upgrade cmake && \
  cmake --version

# VAST
WORKDIR $BUILD_DIR/vast
COPY aux ./aux
COPY cmake ./cmake
COPY doc ./doc
COPY integration ./integration
COPY libvast ./libvast
COPY libvast_test ./libvast_test
COPY schema ./schema
COPY scripts ./scripts
COPY tools ./tools
COPY vast ./vast
COPY .clang-format .cmake-format BANNER CMakeLists.txt configure vast.conf ./
RUN ./configure \
    --prefix=$PREFIX \
    --build-type=$BUILD_TYPE \
    --log-level=INFO \
    --generator=Ninja \
    --without-arrow
RUN cmake --build build && \
  cmake --build build --target test && \
  cmake --build build --target integration && \
  cmake --build build --target install


# Production image: copy application
FROM debian:buster-slim
LABEL maintainer="engineering@tenzir.com"

ENV PREFIX /usr/local

COPY --from=builder $PREFIX/ $PREFIX/
RUN apt-get -qq update && apt-get -qq install -y libc++1 libc++abi1 libpcap0.8 \
  openssl
EXPOSE 42000/tcp

RUN echo "Adding vast user" && useradd --system --user-group vast

RUN mkdir -p /etc/vast /var/log/vast /var/db/vast
COPY systemd/vast.conf /etc/vast/vast.conf
RUN chown -R vast:vast /var/log/vast /var/db/vast

WORKDIR /var/db/vast
VOLUME ["/var/db/vast"]

USER vast:vast
ENTRYPOINT ["vast"]
CMD ["--help"]
