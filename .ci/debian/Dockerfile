FROM debian:buster-slim
LABEL maintainer="tobias.mayer@tenzir.com"

ENV CC gcc-8
ENV CXX g++-8

# Compiler and dependency setup
RUN apt-get -qq update && apt-get -qqy install \
    gcc-8 g++-8 cmake git-core ninja-build libbenchmark-dev \
    python3 python3-pip python3-venv libpcap-dev libssl-dev libatomic1 jq \
    tcpdump rsync hub pandoc
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install clang-format
