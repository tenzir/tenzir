<p align="center">
  <img src="./doc/assets/vast.svg" width="75%" alt="VAST">
</p>

<h1 align="center">
  VAST &mdash; Visibility Across Space and Time
</h1>
<h4 align="center">

The network telemetry engine for data-driven security investigations.

[![Build Status][ci-badge]][ci-url]
[![Changelog][changelog-badge]][changelog-url]
[![Latest Release][latest-release-badge]][latest-release-url]
[![Chat][chat-badge]][chat-url]
[![License][license-badge]][license-url]

[_Getting Started_](#getting-started) &mdash;
[_Installation_][installation-url] &mdash;
[_Documentation_][docs] &mdash;
[_Development_][contributing-url] &mdash;
[_Changelog_][changelog-url] &mdash;
[_License and Scientific Use_](#license-and-scientific-use)

Chat with us on [Gitter][chat-url].
</h4>

## Key Features

- **High-Throughput Ingestion**: import numerous log formats over 100k
  events/second, including [Zeek](https://www.zeek.org/),
  [Suricata](https://suricata-ids.org/), JSON, and CSV.

- **Low-Latency Queries**: sub-second response times over the
  entire data lake, thanks to multi-level bitmap indexing and actor model
  concurrency. Particularly helpful for instant indicator checking over the
  entire dataset.

- **Flexible Export**: access data in common text formats (ASCII, JSON, CSV),
  in binary form (MRT, PCAP), or via zero-copy relay through
  [Apache Arrow](https://arrow.apache.org/) for arbitrary downstream analysis.

- **Powerful Data Model and Query Language**: the generic semi-structured data
  model allows for expressing complex data in a typed fashion. An intuitive
  query language that feels like grep and awk at scale enables powerful
  subsetting of data with domain-specific operations, such as top-*k* prefix
  search for IP addresses and subset relationships.

- **Schema Pivoting**: the missing link to navigate between related events,
  e.g., extracting a PCAP for a given IDS alert, or locating all related logs
  for a given query.

## Getting Started

Clone the `master` branch to get the most recent version of VAST.

```sh
git clone --recursive https://github.com/tenzir/vast
```

Once you have all dependencies in place, build VAST with the following
commands:

```sh
./configure
cmake --build build
cmake --build build --target test
cmake --build build --target integration
cmake --build build --target install
```

The [installation guide][installation-url] contains more detailed and
platform-specific instructions on how to build and install VAST.

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
flow cutoff**:

```sh
vast import pcap -c 1024 < trace.pcap
```

**Run a query over PCAP data, sort the packets by time, and feed them into**
`tcpdump`:

```sh
vast export pcap "sport > 60000/tcp && src !in 10.0.0.0/8" \
  | ipsumdump --collate -w - \
  | tcpdump -r - -nl
```

## License and Scientific Use

VAST comes with a [3-clause BSD license][license-url]. When referring to VAST
in a scientific context, please use the following citation:

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

[vast-paper]: https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-vallentin.pdf
[nsdi-proceedings]: https://www.usenix.org/conference/nsdi16/technical-sessions
