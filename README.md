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

## Documentation

Please see the [wiki](https://github.com/mavam/vast/wiki) for documentation.

## Installation

VAST exists as Docker container:

    docker pull mavam/vast
    docker run --rm -t -i vast /bin/bash
    > vast -h

For instructions on how to build VAST manually, read on.

### Dependencies

Required:

- [Clang >= 3.4](http://clang.llvm.org/) or [GCC >= 4.9](http://gcc.gnu.org)
- [CMake](http://www.cmake.org)
- [CAF](https://github.com/actor-framework/actor-framework)
- [Boost](http://www.boost.org) (headers only)

Optional:

- [libpcap](http://www.tcpdump.org)
- [libedit](http://thrysoee.dk/editline)
- [gperftools](http://code.google.com/p/google-perftools)
- [Broccoli](http://www.bro-ids.org)
- [Doxygen](http://www.doxygen.org)

### Building

Although modern C++ compiler offer a feature-complete implementation of the
C++14 standard, installing a working build toolchain turns out to be
non-trivial on some platforms. Feedback about what works and what doesn't
is very much appreciated.

First define a few environment variables:

    export PREFIX=/opt/vast
    export CC=/path/to/cc
    export CXX=/path/to/c++
    export LD_LIBRARY_PATH=$PREFIX/lib

Then setup CAF as follows:

    git checkout 8dcc3e72
    ./configure --prefix=$PREFIX --no-examples
    make
    make test
    make install

Finally build, test, and install VAST:

    ./configure --prefix=$PREFIX
    make
    make test
    make install

## License

VAST comes with a 3-clause BSD licence, see
[COPYING](https://raw.github.com/mavam/vast/master/COPYING) for details.
