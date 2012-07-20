Installation notes
==================

Due to ABI incompatibilities of GCC's libstdc++ and Clang's libc++, it is
impossible to mix and match the two in one application. As a result, one needs
to build all with either Clang or GCC.

C++11 is still in an early adoption phase, rendering the installation procedure
a tricky task. This document bundles some tips that help to create a working
build environment. Feedback about what works for you and what doesn't is very
much appreciated.

The two currently supported platforms are **Mac OSX** (Darwin) and **Linux**.
It is currently impossible to support Windows platforms, as Visual Studio lacks
important C++11 features.

In C++, the STL layer is different from the ABI layer. While the former is
provided by Clang's libc++, the latter needs to be installed separately,
e.g., via [libcxxrt][libcxxrt] or the LLVM
equivalent [libcxxabi](http://libcxxabi.llvm.org). 

Clang
-----

There is generally good Clang support on Mac OS (Darwin), as it is the favored
platform of the Clang developers. Still, the Clang version shipping with
Macports uses libstdc++ as opposed to Clang's libc++. On Linux,
[libcxxrt][libcxxrt] has proven to work well in combination with
[libunwind](http://www.nongnu.org/libunwind/index.html).

Make sure that Clang is in your `$PATH` and export the following
environment variables:

    export LLVM=/opt/llvm       # Contains LLVM/Clang binaries.
    export CC=clang
    export CXX=clang++
    export CXXFLAGS="-std=c++11 -stdlib=libc++"
    export LDFLAGS="-L$LLVM/lib -stdlib=libc++"
    export LD_LIBRARY_PATH=$LLVM/lib

On Linux, it may be necessary to extend the linker flags when using libcxxrt:

    export LDFLAGS="$LDFLAGS -lpthread -lcxxrt"

We recommend to install all Clang-related dependencies in the same prefix:

    export PREFIX=/path/to/installation-prefix

### [Boost](http://www.boost.org)

    ./bootstrap.sh --with-toolset=clang --prefix=$PREFIX \
        --with-libraries=date_time,iostreams,system,thread,test,filesystem,program_options
    
    ./b2 --layout=tagged variant=debug,release threading=multi \
        cxxflags="-std=c++11 -stdlib=libc++" linkflags="-L$LLVM/lib -stdlib=libc++"
    
    ./b2 install

### [Libcppa](https://github.com/Neverlord/libcppa)

    ./configure --enable-debug --disable-context-switching --prefix=$PREFIX \
        --with-clang=$LLVM/bin/clang++ --with-boost=$PREFIX
    make
    make test
    make install

### Øevent

    ./configure --prefix=$PREFIX --with-boost=$PREFIX
    make
    make test
    make install

### VAST

    ./configure --enable-debug --prefix=$PREFIX \
        --with-boost=$PREFIX --with-libcppa=$PREFIX --with-0event=$PREFIX
    make
    make test
    make install

GCC
---

The C++11 requirement dictates GCC version 4.7 or greater. If your distribution
does not provide one out of the box, you may have to build GCC in your home
directory. Here are some brief hints how to do so.

Then,

    mkdir build
    cd build

    export BUILD=x86_64-redhat-linux
    ../configure --prefix=$PREFIX --enable-languages=c,c++ \
        --enable-shared=libstdc++ --disable-multilib \
        --build=$BUILD --enable-threads=posix --enable-tls 

    make
    make install

To make sure that the freshly built libstdc++ will be used, you may have to
adapt the `(DY)LD_LIBRARY_PATH` to include `$PREFIX/lib` (plus `$PREFIX/lib64
for some architectures.) Also make sure that the new GCC is in your `$PATH`,
ready to picked up by subsequent dependency configurations.

Before proceeding, you may find it useful to set the following environment
variables:

    export PREFIX=/path/to/installation-prefix

### [Boost](http://www.boost.org)

    ./bootstrap.sh --with-toolset=gcc --prefix=$PREFIX \
        --with-libraries=date_time,iostreams,system,thread,test,filesystem,program_options
    
    ./b2 --layout=tagged variant=debug,release threading=multi \
        cxxflags="-std=c++11"
    
    ./b2 install

### [Libcppa](https://github.com/Neverlord/libcppa)

    ./configure --enable-debug --prefix=$PREFIX --with-boost=$PREFIX
    make
    make test
    make install

### Øevent

    ./configure --prefix=$PREFIX --with-boost=$PREFIX
    make
    make test
    make install

### VAST

    ./configure --enable-debug --prefix=$PREFIX \
        --with-boost=$PREFIX --with-libcppa=$PREFIX --with-0event=$PREFIX
    make
    make test
    make install



[libcxxrt]: https://github.com/pathscale/libcxxrt
