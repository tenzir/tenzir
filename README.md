# VAST

[![Build Status][jenkins-badge]][jenkins-url]
[![Docker Container][docker-badge]][docker-url]
[![Gitter][gitter-badge]](https://gitter.im/mavam/vast)

**Visibility Across Space and Time (VAST)** is a unified platform for network
forensics and incident response.

## Synopsis

Start a VAST node with debug log verbosity in the foreground and spawn all core
actors:

    vastd -l 5 -f -c

Import [Bro](http://www.bro.org) logs or a PCAP trace in one shot:

    zcat *.log.gz | vast import bro
    vast import pcap < trace.pcap

Query VAST and get the result back as PCAP trace:

    vast export pcap -h "sport > 60000/tcp && src !in 10.0.0.0/8"

## Resources

- [Documentation](https://github.com/mavam/vast/wiki)
- [Issue board](https://waffle.io/mavam/vast)
- [Chat](https://gitter.im/mavam/vast)
- [Contribution guidelines](CONTRIBUTING.md)
- [Project page](http://www.icir.org/vast)
- Mailing lists:
    - [vast@icir.org][mailing-list]: general help and discussion
    - [vast-commits@icir.org][mailing-list-commits]: full diffs of git commits

## Installation

### Docker

The [VAST docker container](https://registry.hub.docker.com/u/mavam/vast/)
provides a quick way to get up and running:

    docker pull mavam/vast
    docker run --rm -ti mavam/vast
    > vast -h

### Source Build

Building VAST involves the following steps:

    ./configure
    make
    make test
    make install

Required dependencies:

- A C++14 compiler:
  - Clang >= 3.4
  - GCC >= 4.9
- [CMake](http://www.cmake.org)
- [CAF](https://github.com/actor-framework/actor-framework)
- [Boost](http://www.boost.org) (headers only)

Optional:

- [libpcap](http://www.tcpdump.org)
- [gperftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)
- [md2man](https://github.com/sunaku/md2man)

#### FreeBSD

VAST development primarily takes place on FreeBSD because it ships with a C++14
compiler and provides all dependencies natively, which one can install as
follows:

    pkg install cmake boost-libs caf google-perftools

#### Linux

To the best of our knowledge, no distribution currently comes with an apt
compiler out of the box. On recent Debian-based distributions (e.g., Ubuntu
14.04.1), getting a working toolchain requires installing the following
packages:

    apt-get install cmake clang-3.5 libc++-dev libc++abi-dev \
      libboost-dev libpcap-dev libgoogle-perftools-dev

CAF still needs manual installation.

#### Mac OS

Mac OS Yosemite also ships with a working C++14 compiler.
[Homebrew](http://brew.sh) makes it easy to install the dependencies:

    brew install cmake boost google-perftools
    brew install caf --HEAD

## License

VAST comes with a [3-clause BSD
licence](https://raw.github.com/mavam/vast/master/COPYING).

[mailing-list]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast
[mailing-list-commits]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast-commits
[jenkins-url]: http://mobi39.cpt.haw-hamburg.de/view/VAST%20Build%20Status/
[jenkins-badge]: http://mobi39.cpt.haw-hamburg.de/buildStatus/icon?job=VAST/master%20branch
[gitter-badge]: https://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-green.svg
[docker-url]: https://quay.io/repository/mavam/vast
[docker-badge]: https://quay.io/repository/mavam/vast/status 
