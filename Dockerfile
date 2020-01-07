FROM debian:buster-slim as builder
LABEL maintainer="engineering@tenzir.com"
LABEL builder=true

ENV PREFIX /usr/local
ENV CC gcc-8
ENV CXX g++-8
ENV BUILD_TYPE Release
ENV BUILD_DIR /tmp/src
ENV DEBIAN_FRONTEND noninteractive
ARG BRANCH
ENV BRANCH master

# Compiler and dependency setup
RUN apt-get -qq update && apt-get -qqy install \
  build-essential gcc-8 g++-8 ninja-build libbenchmark-dev libpcap-dev \
  libssl-dev libatomic1 python3-dev python3-pip python3-venv git-core jq tcpdump
RUN pip3 install --upgrade pip && pip install --upgrade cmake && \
  cmake --version

# VAST
RUN git clone --recursive https://github.com/tenzir/vast $BUILD_DIR/vast
WORKDIR $BUILD_DIR/vast
RUN git checkout ${BRANCH}
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
COPY --from=builder /usr/lib/x86_64-linux-gnu/libatomic.so.1 /usr/lib/x86_64-linux-gnu/libatomic.so.1
RUN apt-get -qq update && apt-get -qq install -y libc++1 libc++abi1 libpcap0.8 \
  openssl
RUN echo "Adding tenzir user" && \
  groupadd --gid 20097 tenzir && useradd --system --uid 20097 --gid tenzir tenzir

EXPOSE 42000/tcp
WORKDIR /data
RUN chown -R tenzir:tenzir /data
VOLUME ["/data"]

USER tenzir:tenzir
ENTRYPOINT ["/usr/local/bin/vast"]
CMD ["--help"]
