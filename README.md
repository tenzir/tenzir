<p align="center">
  <img src="./doc/assets/vast.svg" width="75%" alt="VAST">
</p>

<h1 align="center">
  VAST &mdash; Visibility Across Space and Time
</h1>
<h4 align="center">

The highly scalable foundation for a security operations center (SOC).

[![Build Status][ci-badge]][ci-url]
[![LGTM Score C++][lgtm-badge]][lgtm-url]
[![Changelog][changelog-badge]][changelog-url]
[![Latest Release][latest-release-badge]][latest-release-url]
[![Chat][chat-badge]][chat-url]
[![License][license-badge]][license-url]

[_Getting Started_](#getting-started) &mdash;
[_Documentation_][docs] &mdash;
[_Development_][contributing-url] &mdash;
[_Changelog_][changelog-url] &mdash;
[_License and Scientific Use_](#license-and-scientific-use)

Chat with us on [Gitter][chat-url].
</h4>

## Key Features

VAST is a network telemetry engine that greatly speeds up the workflow to
investigate and react on complex cyber attacks.

- __High-throughput ingestion__ VAST can import various log formats from [Zeek](https://www.zeek.org/)
  or [Suricata](https://suricata-ids.org/), and supports high-throughput
  ingestion of up to 100k events per second.
- __Interactive queries__ VAST's multi-level indexing approach effortlessly
  handles terrabytes of data and delivers sub-second response times.
- __Built for network forensics__ VAST's data store is purpose-built to support
  common queries in the domain, such as checking indicators over the entire time
  spectrum, without restrictions.
- __Pivot between PCAP and logs__ VAST provides the missing link to pivot from IDS
  logs to the corresponding PCAP data and vice versa.
- __Flexible export__ VAST supports export to common text formats, like JSON and
  CSV, export of PCAP files, and [Apache Arrow](https://arrow.apache.org/).

## Getting Started

The `master` branch always reflects to most recent code of VAST.

```sh
git clone --recursive https://github.com/tenzir/vast
```

Once you have all dependencies in place, simply build VAST with the following
commands:

```sh
./configure
cmake --build build
cmake --build build --target test
cmake --build build --target install
```

Make sure to check out the [installation guide][installation-url] for more
detailed instructions on how to setup VAST.

**Start a VAST node**:

```sh
vast start
```

**Ingest [Zeek](http://www.zeek.org) logs of various kinds**:

```sh
zcat *.log.gz | vast import zeek
```

**Run a query over the last hour, rendered as JSON**:

```sh
vast export json '#timestamp > 1 hour ago && (6.6.6.6 || 5353/udp)'
```

**Ingest a [PCAP](https://en.wikipedia.org/wiki/Pcap) trace with a 1024-byte
flow cut-off**:

```sh
vast import pcap -c 1024 < trace.pcap
```

**Run a query over PCAP data, sort the packets, and feed them into** `tcpdump`:

```sh
vast export pcap "sport > 60000/tcp && src !in 10.0.0.0/8" \
  | ipsumdump --collate -w - \
  | tcpdump -r - -nl
```

## License and Scientific Use

VAST comes with a [3-clause BSD license][license-url].

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

<p align="center">
  Developed with ❤️ by <strong><a href="https://tenzir.com">Tenzir</a></strong>
</p>

[docs]: https://docs.tenzir.com
[mailing-list]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast
[mailing-list-commits]: http://mailman.icsi.berkeley.edu/mailman/listinfo/vast-commits
[chat-badge]: https://img.shields.io/badge/gitter-chat-brightgreen.svg
[chat-url]: https://gitter.im/tenzir/chat
[ci-url]: https://github.com/tenzir/vast/actions?query=branch%3Amaster
[ci-badge]: https://github.com/tenzir/vast/workflows/VAST/badge.svg?branch=master
[license-badge]: https://img.shields.io/badge/license-BSD-blue.svg
[license-url]: https://raw.github.com/vast-io/vast/master/COPYING
[changelog-badge]: https://img.shields.io/badge/view-changelog-green.svg
[changelog-url]: CHANGELOG.md
[contributing-url]: https://github.com/tenzir/.github/blob/master/contributing.md
[latest-release-badge]: https://img.shields.io/github/commits-since/tenzir/vast/latest.svg?color=green
[latest-release-url]: https://github.com/tenzir/vast/releases
[installation-url]: INSTALLATION.md
[lgtm-badge]: https://img.shields.io/lgtm/grade/cpp/github/tenzir/vast.svg?logo=lgtm
[lgtm-url]: https://lgtm.com/projects/g/tenzir/vast/context:cpp

[vast-paper]: https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-vallentin.pdf
[nsdi-proceedings]: https://www.usenix.org/conference/nsdi16/technical-sessions
