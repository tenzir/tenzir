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
  libssl-dev python3-dev python3-pip python3-venv git-core jq tcpdump
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
RUN apt-get -qq update && apt-get -qq install -y libc++1 libc++abi1 libpcap0.8 \
  openssl
RUN echo "Adding vast user" && useradd --system --user-group vast

EXPOSE 42000/tcp
WORKDIR /var/db/vast
RUN chown -R vast:vast /var/db/vast
VOLUME ["/var/db/vast"]

USER vast:vast
ENTRYPOINT ["vast"]
CMD ["--help"]
