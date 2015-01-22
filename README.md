# VAST

**Visibility Across Space and Time (VAST)** is a unified platform for network
forensics and incident response.

## Synopsis

Start a core:

    vast -C

Import data:

    zcat *.log.gz | vast -I bro
    vast -I pcap < trace.pcap

Run a query of at most 100 results, printed as [Bro](http://www.bro.org)
conn.log:

    vast -E bro -l 100 -q '&type == "conn" && :addr in 192.168.0.0/24'

Start the interactive console and submit a query:

    vast -Q
    > ask
    ? &time > now - 2d && :string == "http"

## Resources

- [Project page](http://www.icir.org/vast)
- [Documentation](https://github.com/mavam/vast/wiki)
- [Issue board](https://waffle.io/mavam/vast)
- IRC: [#vast](http://webchat.freenode.net/?channels=vast) @
  [Freenode](https://freenode.net)
- Mailing lists:
    - [vast@icir.org][vast]: general help and discussion
    - [vast-commits@icir.org][vast-commits]: full diffs of git commits

[vast]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast
[vast-commits]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast-commits

## Installation

### Docker

To get up and running quickly, use the [Docker
container](https://registry.hub.docker.com/u/mavam/vast/):

    docker pull mavam/vast
    docker run --rm -ti mavam/vast
    > vast -h

### Source Build

Building VAST takes the following steps:

    ./configure
    make
    make test
    make install

Required dependencies:

- [Clang >= 3.4](http://clang.llvm.org/)
- [CMake](http://www.cmake.org)
- [CAF](https://github.com/actor-framework/actor-framework)
- [Boost](http://www.boost.org) (headers only)

Optional:

- [libpcap](http://www.tcpdump.org)
- [libedit](http://thrysoee.dk/editline)
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
14.04.1), getting a working toolchain involves installing the following
packages:

    apt-get install cmake clang-3.5 libc++-dev libc++abi-dev \
      libboost-dev libpcap-dev libedit-dev libgoogle-perftools-dev

CAF still requires manual installation, which requires the same steps as
outlined above for VAST.

#### Mac OS

Mac OS Yosemite also ships with a working C++14 compiler. We recommend
[Homebrew](http://brew.sh) for installing dependencies:

    brew install cmake boost actor-framework google-perftools

## License

VAST comes with a [3-clause BSD
licence](https://raw.github.com/mavam/vast/master/COPYING).
