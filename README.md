# VAST

[![Build Status][jenkins-badge]][jenkins-url]
[![Chat][chat-badge]][chat-url]
[![License][license-badge]][license-url]

**Visibility Across Space and Time (VAST)** is a unified platform for network
forensics and incident response.

## Synopsis

Import a PCAP trace into a local VAST node in one shot:

    vast -l import pcap < trace.pcap

Query a local node and get the result back as PCAP trace:

    vast -l export pcap -h "sport > 60000/tcp && src !in 10.0.0.0/8" \
      | ipsumdump --collate -w - \
      | tcpdump -r - -nl

Start a node listening at 10.0.0.1 in the foreground:

    vast -e 10.0.0.1 start -f

Send [Bro](http://www.bro.org) logs to the remote node:

    zcat *.log.gz | vast -e 10.0.0.1 import bro

## Resources

- [Project page](http://vast.io)
- [Documentation](http://docs.vast.io)
- [Issue board](http://vast.fail)
- [Contribution guidelines](CONTRIBUTING.md)

### Contact

- Chat: [Gitter][chat-url]
- Twitter: [@vast_io](https://twitter.com/vast_io)
- Mailing lists:
    - [vast@icir.org][mailing-list]: general help and discussion
    - [vast-commits@icir.org][mailing-list-commits]: full diffs of git commits

## Installation

### Source Build

Building VAST involves the following steps:

    ./configure
    make
    make test
    make install

Required dependencies:

- A C++14 compiler:
  - Clang 3.5
  - GCC 6
- [CMake](http://www.cmake.org)
- [CAF](https://github.com/actor-framework/actor-framework) (develop branch)

Optional:

- [libpcap](http://www.tcpdump.org)
- [gperftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)
- [md2man](https://github.com/sunaku/md2man)

#### FreeBSD

FreeBSD ships with a C++14 compiler.
One can install as the dependencies as follows:

    pkg install cmake google-perftools

Even though FreeBSD provides a CAF port, VAST depends the develop branch and
therefore requires a manual CAF installation.

#### Linux

On recent Debian-based distributions (e.g., Ubuntu 15.04), getting a working
toolchain involves installing the following packages:

    apt-get install clang libc++-dev cmake libpcap-dev libgoogle-perftools-dev

CAF offers binary packages via [openSUSE's Build Service][caf-obs].

#### Mac OS

Mac OS also ships with a C++14 compiler out of the box.
[Homebrew](http://brew.sh) makes it easy to install the dependencies:

    brew install cmake google-perftools
    brew install caf --HEAD

## Scientific Use

When referring to VAST in a scientific context, please use the following
citation:

    @InProceedings{nsdi16:vast,
      author    = {Matthias Vallentin and Vern Paxson and Robin Sommer},
      title     = {{VAST: A Unified Platform for Interactive Network Forensics}},
      booktitle = {Proceedings of the USENIX Symposium on Networked Systems
                   Design and Implementation (NSDI)},
      month     = {March},
      year      = {2016}
    }

You can [download the paper][vast-paper] from the [NSDI '16
proceedings][nsdi-proceedings].

## License

VAST comes with a [3-clause BSD licence][license-url].

[mailing-list]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast
[mailing-list-commits]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast-commits
[chat-badge]: https://img.shields.io/badge/gitter-chat-brightgreen.svg
[chat-url]: https://gitter.im/vast-io/vast
[jenkins-url]: https://jenkins.inet.haw-hamburg.de/view/VAST%20Build%20Status/
[jenkins-badge]: https://jenkins.inet.haw-hamburg.de/buildStatus/icon?job=VAST/master%20branch
[license-badge]: https://img.shields.io/badge/License-BSD-blue.svg
[license-url]: https://raw.github.com/vast-io/vast/master/COPYING

[caf-obs]: https://build.opensuse.org/package/show/devel:libraries:caf/caf
[vast-paper]: https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-vallentin.pdf
[nsdi-proceedings]: https://www.usenix.org/conference/nsdi16/technical-sessions
