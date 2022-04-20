<p align="center">

<img src="./docs/static/img/vast-white.svg#gh-dark-mode-only" width="75%" alt="VAST">
<img src="./docs/static/img/vast-black.svg#gh-light-mode-only" width="75%" alt="VAST">

</p>

<h1 align="center">
  Visibility Across Space and Time
</h1>
<h4 align="center">

The network telemetry engine for data-driven security investigations.

[![Build Status][ci-badge]][ci-url]
[![Static Build Status][ci-static-badge]][ci-static-url]
[![Examples Status][ci-examples-badge]][ci-examples-url]
[![Changelog][changelog-badge]][changelog-url]
[![Since Release][since-release-badge]][since-release-url]
[![License][license-badge]][license-url]

[_Getting Started_](#getting-started) &mdash;
[_Installation_][installation-url] &mdash;
[_Documentation_][docs] &mdash;
[_Development_][contributing-url] &mdash;
[_Changelog_][changelog-url] &mdash;
[_License and Scientific Use_](#license-and-scientific-use)
</h4>
<div align="center">

[![Chat][chat-badge]][chat-url]
</div>

## Key Features

- **High-Throughput Ingestion**: import numerous log formats over 100k
  events/second, including [Zeek](https://www.zeek.org/),
  [Suricata](https://suricata-ids.org/), JSON, and CSV.

- **Low-Latency Queries**: sub-second response times over the entire data lake,
  thanks to multi-level bitmap indexing and actor model concurrency.
  Particularly helpful for instant indicator checking over the entire dataset.

- **Flexible Export**: access data in common text formats (ASCII, JSON, CSV), in
  binary form (MRT, PCAP), or via zero-copy relay through [Apache
  Arrow](https://arrow.apache.org/) for arbitrary downstream analysis.

- **Powerful Data Model and Query Language**: the generic semi-structured data
  model allows for expressing complex data in a typed fashion. An intuitive
  query language that feels like grep and awk at scale enables powerful
  subsetting of data with domain-specific operations, such as top-*k* prefix
  search for IP addresses and subset relationships.

- **Schema Pivoting**: the missing link to navigate between related events,
  e.g., extracting a PCAP for a given IDS alert, or locating all related logs
  for a given query.

## Get VAST

We offer pre-packaged versions of VAST for download:
- **stable**: see the artifacts of the latest official [VAST release][latest-release]
- **development**: we offer a [static
build](https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-static-latest.tar.gz) of the master branch for Linux

```sh
curl -L -O https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-static-latest.tar.gz
```

Unpack the archive. It contains three folders `bin`, `etc`, and `share`. To get
started invoke the binary in the `bin` directory directly.

```sh
tar xfz vast-static-latest.tar.gz
bin/vast --help
```

To install VAST locally, simply place the unpacked directories in your install
prefix, e.g., `/usr/local`.

The [installation guide][installation-url] contains more detailed and
platform-specific instructions on how to build and install VAST for all
supported platforms.

## Getting Started

Here are some commands to get a first glimpse of what VAST can do for you.

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
vast export json ':timestamp > 1 hour ago && (6.6.6.6 || 5353/udp)'
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

VAST comes with a [3-clause BSD license][license-url]. When referring to VAST in
a scientific context, please use the following citation:

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

[docs]: https://docs.tenzir.com/vast
[chat-badge]: https://img.shields.io/badge/Slack-Tenzir%20Community%20Chat-brightgreen?logo=slack&color=purple&style=flat
[chat-url]: http://slack.tenzir.com
[ci-url]: https://github.com/tenzir/vast/actions?query=branch%3Amaster+workflow%3AVAST
[ci-badge]: https://github.com/tenzir/vast/workflows/VAST/badge.svg?branch=master
[ci-examples-url]: https://github.com/tenzir/vast/actions?query=branch%3Amaster+workflow%3A%22Jupyter+Notebook%22
[ci-examples-badge]: https://github.com/tenzir/vast/workflows/Jupyter%20Notebook/badge.svg?branch=master
[ci-static-url]: https://github.com/tenzir/vast/actions?query=branch%3Amaster+workflow%3A%22VAST+Static%22
[ci-static-badge]: https://github.com/tenzir/vast/workflows/VAST%20Static/badge.svg?branch=master&event=push
[license-badge]: https://img.shields.io/badge/license-BSD-blue.svg
[license-url]: https://raw.github.com/vast-io/vast/master/COPYING
[changelog-badge]: https://img.shields.io/badge/view-changelog-green.svg
[changelog-url]: CHANGELOG.md
[contributing-url]: https://github.com/tenzir/.github/blob/master/contributing.md
[since-release-badge]: https://img.shields.io/github/commits-since/tenzir/vast/latest.svg?color=green
[since-release-url]: https://github.com/tenzir/vast/compare/2021.06.24...master
[latest-release]: https://github.com/tenzir/vast/releases/latest
[installation-url]: https://docs.tenzir.com/vast/installation/overview

[vast-paper]: https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-vallentin.pdf
[nsdi-proceedings]: https://www.usenix.org/conference/nsdi16/technical-sessions
