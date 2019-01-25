# VAST
#
# VERSION               0.1

FROM        ubuntu:18.04
MAINTAINER  Matthias Vallentin <matthias@bro.org>

ENV PREFIX /usr/local
ENV CC clang
ENV CXX clang++
ENV BUILD_TYPE Release

# Compiler and dependency setup
RUN apt-get -qq update && apt-get -qqy install \
    clang libc++-dev libc++abi-dev cmake git-core
RUN apt-get -qq update && apt-get -qqy install \
    libpcap-dev libedit-dev libgoogle-perftools-dev openssl libssl1.0-dev

# By placing the ADD directive at this point, we build both CAF and VAST
# every time. This ensures that the CI integration will always fetch a fresh
# CAF tree, regardless of the Docker cache. The correct way to handle this
# would be to provide a CAF docker image and use it in the FROM directive.
ADD . $PREFIX/src/vast

# CAF
WORKDIR $PREFIX/src
RUN git clone https://github.com/actor-framework/actor-framework.git caf
WORKDIR caf
RUN ./configure --prefix=$PREFIX --build-type=$BUILD_TYPE \
    --no-examples --no-opencl --no-unit-tests --no-python
RUN make -C build
RUN make -C build install

# Broker
#WORKDIR $PREFIX/src
#RUN git clone --recurse-submodules https://github.com/zeek/broker.git
#WORKDIR broker
#RUN ./configure --prefix=$PREFIX --with-caf=$PREFIX --build-type=$BUILD_TYPE \
#    --disable-python --disable-docs --disable-tests
#RUN make -C build
#RUN make -C build install

# VAST
WORKDIR $PREFIX/src/vast
RUN ./configure --prefix=$PREFIX --build-type=$BUILD_TYPE --log-level=DEBUG
RUN make -C build
RUN make -C build test
RUN make -C build install

# Remove build tools
RUN apt-get purge clang libc++-dev libc++abi-dev cmake git-core
RUN apt-get autoremove

CMD ["/bin/bash"]
