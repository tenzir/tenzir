VAST
====

<!--
[![Build Status](https://secure.travis-ci.org/mavam/vast.png)](http://travis-ci.org/mavam/vast)
-->

**Visibility Across Space and Time (VAST)** is a unified platform for network
forensics and incident response.


Synopsis
--------

Start a core:

    vast -C

Import data:

    zcat *.log.gz | vast -I

Run a query of at most 100 results, printed as a [Bro](http://www.bro.org) log:

    vast -E -o bro -l 100 -q ':addr in 192.168.0.0/24'

Start the interactive console and submit a query:

    vast -Q
    > ask
    ? &time > now - 2d && :string == "http"


Documentation
-------------

- [Wiki](https://github.com/mavam/vast/wiki)


Installation
------------

### Dependencies

Required:

- [Clang >= 3.4](http://clang.llvm.org/) or [GCC >= 4.9](http://gcc.gnu.org)
- [CMake](http://www.cmake.org)
- [CAF](https://github.com/actor-framework/actor-framework)
- [Boost](http://www.boost.org) (headers only)

Optional:

- [Broccoli](http://www.bro-ids.org)
- [Editline](http://thrysoee.dk/editline/)
- [gperftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)

### Building

Although modern C++ compiler offer a feature-complete implementation of the
C++14 standard, installing a working build toolchain turns out to be
non-trivial on some platforms. This document bundles some tips that help to
create a working build environment. Feedback about what works and what doesn't
is very much appreciated.

Due to ABI incompatibilities of GCC's libstdc++ and Clang's libc++, it is
impossible to mix and match the two in one application. As a result, one needs
to build all with either Clang or GCC. The following steps show either path.

If you are using a 64-bit version of Linux, make sure to use a recent version
of [libunwind](http://www.nongnu.org/libunwind/index.html) when enabling
[gperftools](http://code.google.com/p/gperftools/), because there exist
[known](http://code.google.com/p/gperftools/issues/detail?id=66)
[bugs](https://code.google.com/p/gperftools/source/browse/README) that
cause segmentation faults when linking against the system-provided version. On
Mac OS, a [similar issue](https://code.google.com/p/gperftools/issues/detail?id=413) exists.
When compiling gperftools with a libunwind from a custom prefix, then you need
to specificy a few environment variables to make autoconf happy:

    CFLAGS="-I$PREFIX/include" \
    CXXFLAGS="-I$PREFIX/include" \
    LDFLAGS="-L$PREFIX/lib" \
    ./configure --prefix=$PREFIX


#### Compiler Bootstrapping

- **Clang**

  To bootstrap a Clang toolchain we recommend Robin Sommer's
  [install-clang](https://github.com/rsmmr/install-clang) configured with
  libcxxabi:

      export PREFIX=/opt/vast
      ./install-clang -a libcxxabi -j 16 $PREFIX

- **GCC**

  To bootstrap GCC, please first consult the [official installation
  guide](http://gcc.gnu.org/wiki/InstallingGCC). The following steps worked for
  me.

  After unpacking the source:

      ./contrib/download_prerequisites
      mkdir build
      cd build

      export PREFIX=/opt/vast
      export BUILD=x86_64-redhat-linux
      ../configure --prefix=$PREFIX --enable-languages=c++ \
          --enable-shared=libstdc++ --disable-multilib \
          --build=$BUILD --enable-threads=posix --enable-tls

      make
      make install

  To make sure that the freshly built libstdc++ will be used, you may have to
  adapt the `(DY)LD_LIBRARY_PATH` to include `$PREFIX/lib` (plus
  `$PREFIX/lib64` for some architectures). Also make sure that the new GCC is
  in your `$PATH`, ready to be picked up by subsequent dependency
  configurations.


#### Building VAST

First define a few environment variables used by the build harnesses:

    export PREFIX=/opt/vast
    export CC=/path/to/cc
    export CXX=/path/to/c++
    export LD_LIBRARY_PATH=$PREFIX/lib

Thereafter build and install VAST:

    ./configure --prefix=$PREFIX
    make
    make test
    make install


License
-------

VAST comes with a 3-clause BSD licence, see
[COPYING](https://raw.github.com/mavam/vast/master/COPYING) for details.
