VAST
====

<!--
[![Build Status](https://secure.travis-ci.org/mavam/vast.png)](http://travis-ci.org/mavam/vast)
-->

**Visibility Across Space and Time (VAST)** is a real-time platform for network
forensics and incident response.


Synopsis
--------

Start a server:

    vast -a

Ingest data from standard input:

    zcat *.log.gz | vast -I -r -

Start the client and submit a query:

    vast -C
    > ask
    ? &time > now - 2d && :string == "http"


Documentation
-------------

- [Wiki](https://github.com/mavam/vast/wiki)


Installation
------------

### Dependencies

Required:

- [CMake](http://www.cmake.org)
- [Clang >= 3.4](http://clang.llvm.org/) or [g++ >= 4.8](http://gcc.gnu.org)
- [libcppa](https://github.com/Neverlord/libcppa)
- [Boost](http://www.boost.org)

Optional:

- [Broccoli](http://www.bro-ids.org)
- [Editline](http://thrysoee.dk/editline/)
- [Gperftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)

### Building

Please consult the [installation instructions](INSTALL.md) for guidance on how
to get VAST up and running.


License
-------

VAST comes with a BSD-style licence, see
[COPYING](https://raw.github.com/mavam/vast/master/COPYING) for details.
