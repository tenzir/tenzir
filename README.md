VAST
====

[![Build Status](https://secure.travis-ci.org/mavam/vast.png)](http://travis-ci.org/mavam/vast)

**Visibility Across Space and Time (VAST)** is a real-time platform for network
forensics and incident response.

Dependencies
------------

Required:

- [CMake](http://www.cmake.org)
- [Clang >= 3.2](http://clang.llvm.org/) or [g++ >= 4.7](http://gcc.gnu.org)
- [C++ Boost Libraries](http://www.boost.org)
- [libcppa](https://github.com/Neverlord/libcppa)
- [0event](https://github.com/mavam/ze)

Optional:

- [Broccoli](http://www.bro-ids.org)
- [Google perftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)

Please consult
[INSTALL.md](https://github.com/mavam/vast/blob/master/INSTALL.md) for guidance
on installing the dependencies.

Installation
------------

VAST uses CMake for the build process but ships with autotools-like wrapper
scripts. Please see

    ./configure --help

for available configuration options. By default, the build takes place in the
sub-directory `build`. After configuring you can kick off the compilation with

    make

and may optionally install VAST into your prefix with

    make install

Documentation
-------------

- [Wiki](https://github.com/mavam/vast/wiki)

License
-------

VAST comes with a BSD-style licence, see
[COPYING](https://raw.github.com/mavam/vast/master/COPYING) for details.
