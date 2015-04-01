# VAST
#
# VERSION               0.1

FROM        ubuntu:14.04.1
MAINTAINER  Matthias Vallentin <matthias@bro.org>

ENV         PREFIX /usr/local
ENV         PARALLELISM 4
ENV         CC clang-3.5
ENV         CXX clang++-3.5

# Compiler and dependcy setup
RUN apt-get update && apt-get -y install cmake git build-essential tmux wget
RUN apt-get update && apt-get -y install clang-3.5 libc++-dev libc++abi-dev \
      libboost-dev libpcap-dev libedit-dev libgoogle-perftools-dev gcc-

RUN mkdir -p $PREFIX/src

# CAF
RUN cd $PREFIX/src/ && \
    git clone https://github.com/actor-framework/actor-framework.git
RUN cd $PREFIX/src/actor-framework && \
    git checkout develop && \
    ./configure --prefix=$PREFIX --no-examples && \
    make -j $PARALLELISM && \
    make test && \
    make install

# VAST
# (No parallel build because it consumes too much memory.)
ADD . $PREFIX/src/vast
RUN cd $PREFIX/src/vast && \
    ./configure --prefix=$PREFIX && \
    make && \
    make install

RUN ldconfig
CMD ["/bin/bash"]
