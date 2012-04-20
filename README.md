**Visibility Across Space and Time (VAST)** is a network forensic platoform for
real-time incident response. 

Synopsis
========

Display available options:

    vast -h
    vast -z     # detailed options

Start VAST with the archive and ingestion component:

    vast -AI

Installation
============

VAST requires the following packages:

* A C++11 compiler, e.g., [g++ 4.7](http://gcc.gnu.org) or
  [Clang 3.1](http://clang.llvm.org/)
* [CMake](http://www.cmake.org)
* [C++ Boost Libraries](http://www.boost.org)
* [0event](https://github.com/mavam/ze)
* [Broccoli](http://www.bro-ids.org)

Optional:

* [Google perftools](http://code.google.com/p/google-perftools)
* [Doxygen](http://www.doxygen.org)

The build process uses CMake with with autotools-like wrapper scripts. Please
see

    ./configure --help

for available configuration options. By default, the build process creates a
directory `build` in which the compilation takes place. After configuring, you
kick off the compilation with

    make

and may install VAST with

    make install

Usage
=====

TODO

License
=======

VAST comes with a BSD-style licence, see COPYING for details.
