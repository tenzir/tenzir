FROM debian:buster-slim
LABEL maintainer="tobias.mayer@tenzir.com"

ENV CC gcc-8
ENV CXX g++-8

# Compiler and dependency setup
RUN apt-get -qq update && apt-get -qqy install \
    gcc-8 g++-8 cmake git-core ninja-build \
    libpcap-dev libssl-dev libatomic1
