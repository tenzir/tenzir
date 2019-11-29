<p align="center">
  <img src="./doc/assets/vast.svg" width="75%" alt="VAST">
</p>

**Visibility Across Space and Time (VAST)** is a scalable foundation for
a security operations center (SOC): a rich data model for security data,
high-throughput ingestion of telemetry, low-latency search, and flexible export
in various formats.

[![Build Status][ci-badge]][ci-url]
[![Chat][chat-badge]][chat-url]
[![License][license-badge]][license-url]
[![Changelog][changelog-badge]][changelog-url]
[![Latest Release][latest-release-badge]][latest-release-url]
[![LGTM Score C++][lgtm-badge]][lgtm-url]

## Synopsis

Start a VAST node:

```sh
vast start
```

Ingest a bunch of [Zeek](http://www.zeek.org) logs:

```sh
zcat *.log.gz | vast import zeek
```

Run a query over the last hour, rendered as JSON:

```sh
vast export json '#timestamp > 1 hour ago && (6.6.6.6 || 5353/udp)'
```

Ingest a [PCAP](https://en.wikipedia.org/wiki/Pcap) trace with a 1024-byte
flow cut-off:

```sh
vast import pcap -c 1024 < trace.pcap
```

Run a query over PCAP data, sort the packets, and feed them into `tcpdump`:

```sh
vast export pcap "sport > 60000/tcp && src !in 10.0.0.0/8" \
  | ipsumdump --collate -w - \
  | tcpdump -r - -nl
```

## Resources

- [Chat][chat-url]
- [Documentation][docs]
- [Contribution guidelines][contributing-url]
- [Changelog][changelog-url]

## Installation

Required dependencies:

- A C++17 compiler:
  - GCC >= 8
  - Clang >= 6
  - Apple Clang >= 9.1
- [CMake](http://www.cmake.org) >= 3.11

Optional dependencies:

- [libpcap](http://www.tcpdump.org)
- [gperftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)
- [Pandoc](https://github.com/jgm/pandoc)

### Source Build

Building VAST involves the following steps:

```sh
git submodule update --recursive --init
./configure
cmake --build build
cmake --build build --target test
cmake --build build --target install
```

The `configure` script is a small wrapper that passes build-related variables
to CMake. For example, to use [Ninja](https://ninja-build.org) as build
generator, add `--generator=Ninja` to the command line. Passing `--help` shows
all available options.

The `doc` target builds the API documentation locally:

```sh
cmake --build build --target doc
```

## Docker

The source ships with the convenience script `scripts/docker-build`, which will
create the Docker images and save them as `tar.gz` archives (when invoked
without arguments).

To run the container, you need to provide a volume to the mountpoint `/data`.
The default command will print the help message:

```sh
docker run -v /tmp/vast:/data vast-io/vast
```

Create a Docker network since we'll be running multiple containers which
connect to each other:

```sh
docker network create -d bridge --subnet 172.42.0.0/16 vast_nw
```

Use detach and publish the default port to start a VAST node:

```sh
docker run --network=vast_nw --name=vast_node --ip="172.42.0.2" -d -v \
  /tmp/vast:/data vast-io/vast start
```

Import a Zeek conn log to the detached server instance:

```sh
docker run --network=vast_nw -i -v /tmp/vast:/data vast-io/vast -e '172.42.0.2' \
  import zeek < zeek_conn.log
```

Other subcommands, like `export` and `status`, can be used just like the
`import` command shown above.

## Scientific Use

When referring to VAST in a scientific context, please use the following
citation:

```bibtex
@InProceedings{nsdi16:vast,
  author    = {Matthias Vallentin and Vern Paxson and Robin Sommer},
  title     = {{VAST: A Unified Platform for Interactive Network Forensics}},
  booktitle = {Proceedings of the USENIX Symposium on Networked Systems
               Design and Implementation (NSDI)},
  month     = {March},
  year      = {2016}
}
```

You can [download the paper][vast-paper] from the [NSDI '16
proceedings][nsdi-proceedings].

## License

VAST comes with a [3-clause BSD licence][license-url].

<p align="center">
  Developed with ❤️ by <strong><a href="https://tenzir.com">Tenzir</a></strong>
</p>

[docs]: https://docs.tenzir.com
[mailing-list]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast
[mailing-list-commits]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast-commits
[chat-badge]: https://img.shields.io/badge/gitter-chat-brightgreen.svg
[chat-url]: https://gitter.im/tenzir/chat
[ci-url]: https://cirrus-ci.com/github/tenzir/vast
[ci-badge]: https://img.shields.io/cirrus/github/tenzir/vast
[license-badge]: https://img.shields.io/badge/license-BSD-blue.svg
[license-url]: https://raw.github.com/vast-io/vast/master/COPYING
[changelog-badge]: https://img.shields.io/badge/view-changelog-green.svg
[changelog-url]: CHANGELOG.md
[contributing-url]: https://github.com/tenzir/.github/blob/master/contributing.md
[latest-release-badge]: https://img.shields.io/github/commits-since/tenzir/vast/latest.svg?color=green
[latest-release-url]: https://github.com/tenzir/vast/releases
[lgtm-badge]: https://img.shields.io/lgtm/grade/cpp/github/tenzir/vast.svg?logo=lgtm
[lgtm-url]: https://lgtm.com/projects/g/tenzir/vast/context:cpp

[vast-paper]: https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-vallentin.pdf
[nsdi-proceedings]: https://www.usenix.org/conference/nsdi16/technical-sessions
