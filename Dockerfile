# VAST
#
# VERSION               0.1

FROM        ubuntu:15.04
MAINTAINER  Matthias Vallentin <matthias@bro.org>

ENV PREFIX /usr/local
ENV CC clang
ENV CXX clang++

# Compiler and dependcy setup
RUN apt-get -qq update && apt-get -qqy install clang libc++-dev cmake git-core
RUN apt-get -qq update && apt-get -qqy install \
    libboost-dev libpcap-dev libedit-dev libgoogle-perftools-dev
RUN apt-get -qq update && apt-get -qqy install vim-tiny tmux wget

# By placing the ADD directive at this point, we build both CAF and VAST
# every time. This ensures that the CI integration will always fetch a fresh
# CAF tree, regardless of the Docker cache. The correct way to handle this
# would be to provide a CAF docker image and use it in the FROM directive.
ADD . $PREFIX/src/vast

# CAF
WORKDIR $PREFIX/src
RUN git clone https://github.com/actor-framework/actor-framework.git caf
WORKDIR caf
RUN git checkout develop
RUN ./configure --prefix=$PREFIX --build-type=Release --no-examples --no-opencl
RUN make
RUN make test
RUN make install
RUN ldconfig

# VAST
WORKDIR $PREFIX/src/vast
RUN ./configure --prefix=$PREFIX
RUN make
RUN make test
RUN make install

RUN ldconfig
CMD ["/bin/bash"]
