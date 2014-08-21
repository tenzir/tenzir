# vast
#
# VERSION               0.1

FROM      ubuntu:14.04

ENV PREFIX /usr/local
ENV PARALLELISM 4

# Compiler setup
RUN apt-get update && apt-get -y install cmake git build-essential
RUN apt-get update && \
    apt-get -y install clang-3.4 libc++-dev libboost-dev g++- gcc-

RUN mkdir -p $PREFIX/src

# CAF
RUN cd $PREFIX/src/ && \
    git clone https://github.com/actor-framework/actor-framework.git
RUN cd $PREFIX/src/actor-framework && \
    git pull && \
    git checkout 8dcc3e72 && \
    ./configure --prefix=$PREFIX --no-examples && \
    make -j $PARALLELISM && \
    make test && \
    make install

# VAST
ADD . $PREFIX/src/vast
RUN cd $PREFIX/src/vast && \
    ./configure --prefix=$PREFIX --with-boost=/usr && \
    make -j $PARALLELISM && \
    make install

RUN ldconfig
CMD ["/bin/bash"]

