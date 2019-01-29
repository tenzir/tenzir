# VAST

[![Build Status][jenkins-badge]][jenkins-url]
[![Coverage][coverage-badge]][coverage-url]
[![Chat][chat-badge]][chat-url]
[![License][license-badge]][license-url]

**Visibility Across Space and Time (VAST)** is a platform for network forensics
at scale.

## Synopsis

Start a VAST node:

    vast start

Ingest a bunch of [Zeek](http://www.zeek.org) logs:

    zcat *.log.gz | vast import zeek

Run a query over the last hour, rendered as JSON:

    vast export json '&time > now - 1 hour && :addr == 6.6.6.6'

Ingest a [PCAP](https://en.wikipedia.org/wiki/Pcap) trace with a 1024-byte flow
cut-off:

    vast import pcap -c 1024 < trace.pcap

Run a query over PCAP data, sort the packets, and feed them into `tcpdump`:

    vast export pcap "sport > 60000/tcp && src !in 10.0.0.0/8" \
      | ipsumdump --collate -w - \
      | tcpdump -r - -nl

## Resources

- [Project page](http://vast.io)
- [Documentation](http://docs.vast.io)
- [Contribution guidelines](CONTRIBUTING.md)

### Contact

- Chat: [Gitter][chat-url]
- Twitter: [@vast_io](https://twitter.com/vast_io)
- Mailing lists:
    - [vast@icir.org][mailing-list]: general help and discussion
    - [vast-commits@icir.org][mailing-list-commits]: full diffs of git commits

## Installation

Required dependencies:

- A C++17 compiler:
  - GCC >= 8
  - Clang >= 5
  - Apple Clang >= 9.1
- [CMake](http://www.cmake.org)
- [CAF](https://github.com/actor-framework/actor-framework) (master branch)

Optional dependencies:

- [Broker](https://github.com/zeek/broker)
- [libpcap](http://www.tcpdump.org)
- [gperftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)
- [md2man](https://github.com/sunaku/md2man)

### Source Build

Building VAST involves the following steps:

    ./configure
    make
    make test
    make install

The `configure` script is a small wrapper that passes build-related variables
to CMake. For example, to use [ninja](https://ninja-build.org) as build
generator, add `--generator=Ninja` to the command line. Passing `--help` shows
all available options.

The `doc` target builds the API documentation locally:

    make doc

## Docker

A convenience `docker_build.sh` script is provided with the source
distribution. Simply invoke `docker_build.sh` without arguments,
it will create the images and save them as `tar.gz` archives.

To run the container, you need to provide a volume to the mountpoint `/data`:

```
# The default command will print the help message
$ docker run -v /tmp/vast:/data vast-io/vast
# Use detach and publish the default port to start a VAST node
$ docker run -d -p 42000:42000 -v /tmp/vast:/data vast-io/vast start
# Import a zeek conn log to the detached server instance
$ cat bro_conn.log | docker run -i -v /tmp/vast:/data vast-io/vast -e '172.17.0.2' import bro
```

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
[jenkins-url]: https://jenkins.inet.haw-hamburg.de/blue/organizations/jenkins/VAST%2Fvast
[jenkins-badge]: https://jenkins.inet.haw-hamburg.de/buildStatus/icon?job=VAST/vast/master
[coverage-url]: https://jenkins.inet.haw-hamburg.de/job/VAST/job/vast/job/master/cobertura/
[coverage-badge]: https://img.shields.io/jenkins/c/https/jenkins.inet.haw-hamburg.de/job/VAST/job/vast/job/master.svg?style=flat
[license-badge]: https://img.shields.io/badge/license-BSD-blue.svg
[license-url]: https://raw.github.com/vast-io/vast/master/COPYING

[caf-obs]: https://build.opensuse.org/package/show/devel:libraries:caf/caf
[vast-paper]: https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-vallentin.pdf
[nsdi-proceedings]: https://www.usenix.org/conference/nsdi16/technical-sessions
