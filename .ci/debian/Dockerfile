FROM debian:buster-slim
LABEL maintainer="tobias.mayer@tenzir.com"

ENV CC gcc-8
ENV CXX g++-8

# Compiler and dependency setup
RUN apt-get -qq update && apt-get -qqy install \
    gcc-8 g++-8 cmake git-core ninja-build clang-format-7 libbenchmark-dev \
    python3 python3-venv libpcap-dev libssl-dev libatomic1 jq tcpdump rsync hub \
    pandoc
RUN update-alternatives --install /usr/bin/python python /usr/bin/python2.7 1 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.7 2 && \
    update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-7 100
