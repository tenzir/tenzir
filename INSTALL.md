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


## Compiler Bootstrapping

### Clang

To bootstrap a Clang toolchain we recommend Robin Sommer's
[install-clang](https://github.com/rsmmr/install-clang) configured with
libcxxabi:

    export PREFIX=/opt/vast
    ./install-clang -a libcxxabi -j 16 $PREFIX

### GCC

To bootstrap GCC, please first consult the [official installation
guide](http://gcc.gnu.org/wiki/InstallingGCC). The following steps worked
appear to work.

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
adapt the `(DY)LD_LIBRARY_PATH` to include `$PREFIX/lib` (plus `$PREFIX/lib64`
for some architectures). Also make sure that the new GCC is in your `$PATH`,
ready to be picked up by subsequent dependency configurations.


## Building VAST

Let us begin with defining a few environment variables used throughout the
build process:

    export PREFIX=/opt/vast
    export CC=/path/to/cc
    export CXX=/path/to/c++
    export LD_LIBRARY_PATH=$PREFIX/lib

### [Boost](http://www.boost.org)

Boost Jam does not honor the `CC` and `CXX` environment variables, so make sure
that the correct compiler executable is in your `PATH`.

#### Clang

    ./bootstrap.sh --with-toolset=clang --prefix=$PREFIX \
        --with-libraries=system,test
    
    ./b2 --layout=tagged variant=debug,release threading=multi \
        cxxflags="-std=c++11 -stdlib=libc++" linkflags="-stdlib=libc++" \
        install

#### GCC

    ./bootstrap.sh --with-toolset=gcc --prefix=$PREFIX \
        --with-libraries=system,test
    
    ./b2 --layout=tagged variant=debug,release threading=multi \
        cxxflags="-std=c++11" \
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
