# VAST

[![Build Status][jenkins-badge]][jenkins-url]
[![Docker Container][docker-badge]][docker-url]
[![Gitter][gitter-badge]](https://gitter.im/mavam/vast)

**Visibility Across Space and Time (VAST)** is a unified platform for network
forensics and incident response.

## Synopsis

Import a PCAP trace in one shot:

    zcat *.log.gz | vast -I bro
    vast -C -I pcap < trace.pcap

Query VAST and get the result back as PCAP trace:

    vast -E bro -q -e 'sport > 60000/tcp && src !in 10.0.0.0/8'

In a distributed setup start a VAST *core*:

    vast -C

Import [Bro](http://www.bro.org) logs and send them to the core:

    zcat *.log.gz | vast -I bro

Run a historical query, asking for activity in the past, at most 100 results,
printed as Bro conn.log:

    vast -E bro -l 100 -q -e '&type == "conn" && :addr in 192.168.0.0/24'

Run a continuous query, subscribing to all future matches:

    vast -E bro -c -e 'conn.id.orig_h == 6.6.6.6'

Start the interactive console and submit a query:

    vast -Q
    > ask
    ? &time > now - 2d && :string == "http"

## Resources

- [Project page](http://www.icir.org/vast)
- [Documentation](https://github.com/mavam/vast/wiki)
- [Issue board](https://waffle.io/mavam/vast)
- [Chat](https://gitter.im/mavam/vast)
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

#### FreeBSD

VAST development primarily takes place on FreeBSD because it ships with a C++14
compiler and provides all dependencies natively, which one can install as
follows:

    pkg install cmake boost-libs caf libedit google-perftools

#### Linux

To the best of our knowledge, no distribution currently comes with an apt
compiler out of the box. On recent Debian-based distributions (e.g., Ubuntu
14.04.1), getting a working toolchain requires installing the following
packages:

    apt-get install cmake clang-3.5 libc++-dev libc++abi-dev \
      libboost-dev libpcap-dev libedit-dev libgoogle-perftools-dev

CAF still needs manual installation.

#### Mac OS

Mac OS Yosemite also ships with a working C++14 compiler.
[Homebrew](http://brew.sh) makes it easy to install the dependencies:

    brew install cmake boost caf google-perftools

## License

VAST comes with a [3-clause BSD
licence](https://raw.github.com/mavam/vast/master/COPYING).

[mailing-list]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast
[mailing-list-commits]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast-commits
[jenkins-url]: http://mobi39.cpt.haw-hamburg.de/view/VAST%20Build%20Status/
[jenkins-badge]: http://mobi39.cpt.haw-hamburg.de/buildStatus/icon?job=VAST/master%20branch
[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[docker-url]: https://quay.io/repository/mavam/vast
[docker-badge]: https://quay.io/repository/mavam/vast/status 
