# VAST
#
# VERSION               0.1

FROM        ubuntu:14.04
MAINTAINER  Matthias Vallentin <matthias@bro.org>

ENV         PREFIX /usr/local
ENV         PARALLELISM 4

# Compiler and dependcy setup
RUN apt-get update && apt-get -y install cmake git build-essential tmux wget
RUN apt-get update && apt-get -y install clang-3.4 libc++-dev libboost-dev g++- gcc-
RUN apt-get update && apt-get -y install libedit-dev libpcap-dev

RUN mkdir -p $PREFIX/src

# CAF
RUN cd $PREFIX/src/ && \
    git clone https://github.com/actor-framework/actor-framework.git
RUN cd $PREFIX/src/actor-framework && \
    git pull && \
    git checkout 0.11.0 && \
    ./configure --prefix=$PREFIX --no-examples && \
    make -j $PARALLELISM && \
    make test && \
    make install

# VAST
ADD . $PREFIX/src/vast
RUN cd $PREFIX/src/vast && \
    ./configure --prefix=$PREFIX --with-boost=/usr && \
    make && \
    make install

RUN ldconfig
CMD ["/bin/bash"]
