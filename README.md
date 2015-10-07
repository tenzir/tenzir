# VAST

[![Build Status][jenkins-badge]][jenkins-url]
[![Docker Container][docker-badge]][docker-url]
[![Gitter][gitter-badge]](https://gitter.im/mavam/vast)

**Visibility Across Space and Time (VAST)** is a unified platform for network
forensics and incident response.

## Synopsis

Import a PCAP trace into a local VAST node in one shot:

    vast -n import pcap < trace.pcap

Query a local node and get the result back as PCAP trace:

    vast -n export pcap -h "sport > 60000/tcp && src !in 10.0.0.0/8" \
      | ipsumdump --collate -w - \
      | tcpdump -r - -nl

Start a node with debug log verbosity in the foreground:

    vast -e 10.0.0.1 -l 5 start -f

Send [Bro](http://www.bro.org) logs to the remote node:

    zcat *.log.gz | vast -e 10.0.0.1 import bro

## Resources

- [Documentation](https://github.com/mavam/vast/wiki)
- [Issue board](https://waffle.io/mavam/vast)
- [Chat](https://gitter.im/mavam/vast)
- [Contribution guidelines](CONTRIBUTING.md)
- [Project page](http://vast.tools)
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
  - GCC >= 5
- [CMake](http://www.cmake.org)
- [CAF](https://github.com/actor-framework/actor-framework) (develop branch)

Optional:

- [libpcap](http://www.tcpdump.org)
- [gperftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)
- [md2man](https://github.com/sunaku/md2man)

#### FreeBSD

FreeBSD ships with a C++14 compiler. One can install as the dependencies as
follows:

    pkg install cmake google-perftools

Even though FreeBSD provides a CAF port, VAST depends the develop branch and
therefore requires a manual CAF installation.

#### Linux

On recent Debian-based distributions (e.g., Ubuntu 15.04), getting a working
toolchain involves installing the following packages:

    apt-get install clang libc++-dev cmake libpcap-dev libgoogle-perftools-dev

CAF offers binary packages via [openSUSE's Build Service][caf-obs].

#### Mac OS

Mac OS Yosemite also ships with a working C++14 compiler.
[Homebrew](http://brew.sh) makes it easy to install the dependencies:

    brew install cmake google-perftools
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
[caf-obs]: https://build.opensuse.org/package/show/devel:libraries:caf/caf
