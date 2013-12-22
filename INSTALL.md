Although modern compiler have feature-complete implementation of the C++11
standard, installing a working build toolchain turns out to be non-trivial on
some platforms. This document bundles some tips that help to create a working
build environment. Feedback about what works and what doesn't is very much
appreciated.

If you are using a 64-bit version of Linux, make sure to use a recent version
of [libunwind](http://www.nongnu.org/libunwind/index.html) when enabling
[gperftools](http://code.google.com/p/gperftools/), because there exist
[known](http://code.google.com/p/gperftools/issues/detail?id=66)
[bugs](https://code.google.com/p/gperftools/source/browse/README) that
cause segmentation faults when linking against the system-provided version. On
Mac OS, a [similar issue](https://code.google.com/p/gperftools/issues/detail?id=413) exists.

The C++ STL layer is different from the ABI layer. While the former is provided
by Clang's libc++, the latter needs to be installed separately, e.g., via
pathscale's [libcxxrt](https://github.com/pathscale/libcxxrt) or the LLVM
equivalent [libcxxabi](http://libcxxabi.llvm.org). 

Due to ABI incompatibilities of GCC's libstdc++ and Clang's libc++, it is
impossible to mix and match the two in one application. As a result, one needs
to build all with either Clang or GCC. The following steps show either path.

Clang
-----

Since installing Clang turns out to be non-trivial, this tutorial assumes a
clang compiler installed via Robin Sommer's
[install-clang](https://github.com/rsmmr/install-clang) script.

First, let's make sure that Clang is in the `$PATH` and export the following
environment variables:

    export CC=/path/to/clang
    export CXX=/path/to/clang++
    export LD_LIBRARY_PATH=/opt/llvm/lib

In this tutorial, we install all Clang-related dependencies in the same prefix:

    export PREFIX=/opt/prefix/clang

### [Boost](http://www.boost.org)

    ./bootstrap.sh --with-toolset=clang --prefix=$PREFIX \
        --with-libraries=system,test
    
    ./b2 --layout=tagged variant=debug,release threading=multi \
        cxxflags="-std=c++11 -stdlib=libc++" linkflags="-stdlib=libc++" \
        install

### [Libcppa](https://github.com/Neverlord/libcppa)

    ./configure --prefix=$PREFIX
    make
    make test
    make install

### VAST

    ./configure --prefix=$PREFIX
    make
    make test
    make install

GCC
---

The C++11 features VAST uses require GCC version 4.8 or greater. If your
distribution does not provide one out of the box, you may have to build GCC in
your home directory. Here are some brief hints how to do so, the [official
installation guide](http://gcc.gnu.org/wiki/InstallingGCC) also contains useful
instructions.

First, the installation sequence becomes easier when working with the
installation prefix as an environment variable:

    export PREFIX=/path/to/installation-prefix

Then,

    ./contrib/download_prerequisites
    mkdir build
    cd build

    export BUILD=x86_64-redhat-linux
    ../configure --prefix=$PREFIX --enable-languages=c++ \
        --enable-shared=libstdc++ --disable-multilib \
        --build=$BUILD --enable-threads=posix --enable-tls 

    make
    make install

To make sure that the freshly built libstdc++ will be used, you may have to
adapt the `(DY)LD_LIBRARY_PATH` to include `$PREFIX/lib` (plus `$PREFIX/lib64`
for some architectures). Also make sure that the new GCC is in your `$PATH`,
ready to be picked up by subsequent dependency configurations.

### [Boost](http://www.boost.org)

    ./bootstrap.sh --with-toolset=gcc --prefix=$PREFIX \
        --with-libraries=system,test
    
    ./b2 --layout=tagged variant=debug,release threading=multi \
        cxxflags="-std=c++11"
    
    ./b2 install

### [Libcppa](https://github.com/Neverlord/libcppa)

    ./configure --enable-debug --prefix=$PREFIX
    make
    make test
    make install

### VAST

    ./configure --enable-debug --prefix=$PREFIX
    make
    make test
    make install
