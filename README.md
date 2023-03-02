<p align="center">

<img src="./web/static/img/vast-white.svg#gh-dark-mode-only" width="75%" alt="VAST">
<img src="./web/static/img/vast-black.svg#gh-light-mode-only" width="75%" alt="VAST">

</p>

<h1 align="center">
  Visibility Across Space and Time
</h1>
<h4 align="center">

VAST is an open-source pipeline and storage engine for security event data.

[![Build Status][ci-badge]][ci-url]
[![Static Build Status][ci-static-badge]][ci-static-url]
[![CII Best Practices][cii-best-practices-badge]][cii-best-practices-url]

[ci-badge]: https://github.com/tenzir/vast/workflows/VAST/badge.svg?branch=master
[ci-url]: https://github.com/tenzir/vast/actions?query=branch%3Amaster+workflow%3AVAST
[ci-static-badge]: https://github.com/tenzir/vast/workflows/VAST%20Static/badge.svg?branch=master&event=push
[ci-static-url]: https://github.com/tenzir/vast/actions?query=branch%3Amaster+workflow%3A%22VAST+Static%22
[cii-best-practices-badge]: https://bestpractices.coreinfrastructure.org/projects/6366/badge
[cii-best-practices-url]: https://bestpractices.coreinfrastructure.org/projects/6366

[About](https://vast.io/docs/about) &mdash;
[Try](https://vast.io/docs/try) &mdash;
[Use](https://vast.io/docs/use) &mdash;
[Understand](https://vast.io/docs/understand) &mdash;
[Contribute](https://vast.io/docs/contribute) &mdash;
[Develop](https://vast.io/docs/develop)
</h4>
<div align="center">

[![Chat][chat-badge]](https://vast.io/discord)
</div>

[chat-badge]: https://img.shields.io/badge/Discord-Community%20Chat-brightgreen?logo=discord&color=purple&style=social

<!-- Keep in sync with https://vast.io/about -->

VAST is the open-source processing and storage engine for security event data.

<img src="./web/static/img/building-blocks.excalidraw.svg#gh-dark-mode-only" width="75%" alt="VAST">
<img src="./web/static/img/building-blocks.excalidraw.svg#gh-light-mode-only" width="75%" alt="VAST">

VAST uses [dataflow pipelines](/docs/understand/language/pipelines) as unified
abstraction for data acquisition, reshaping, routing, integration with
third-party tools, and both live and historical query execution powered by a
builtin indexed storage engine optimized for detection and response workloads.

Consider VAST if you want to:

- Filter, shape, aggregate, and enrich security events before they hit your SIEM
  or data lake
- Normalize, enrich, and deduplicate events prior to passing them downstream
- Store, compact, and search event data in an open storage format
  ([Apache Parquet](https://parquet.apache.org/) &
  [Feather](https://arrow.apache.org/docs/python/feather.html))
- Perform high-bandwidth analytics with any data tool powered by
  [Apache Arrow](https://arrow.apache.org)
- Operationalize threat intelligence for live and retrospective detection
- Build your own security data lake or federated XDR architecture

## Get VAST

Use our packages or Docker to get up and running. For Debian, use:

```bash
curl -L -O https://vast.io/download/vast-linux-static.deb
dpkg -i vast-linux-static.deb
```

For non-Debian Linux, use our static binary:

```bash
mkdir -p /opt/vast/ && cd /opt/vast
curl -L -O https://vast.io/download/vast-linux-static.tar.gz
tar xzf -C /opt/vast vast-linux-static.tar.gz
export "PATH:/opt/vast/bin:$PATH"
bin/vast --help
```

Or pull our official Docker image and [take it from
there](https://vast.io/docs/setup/deploy/docker):

```bash
docker pull tenzir/vast
```

Once you have local VAST installation, follow the [quick start
guide](https://vast.io/docs/try) or read a TL;DR below on how to get started.

## Try VAST

VAST consists of two pieces: pipelines and storage.

Once you have a VAST executable, you would typically start a VAST node and
interact with it like a service. , which is
basically a container for storage and piplines.

**Start a VAST node**:

```bash
vast start
```

**Ingest [Zeek](http://www.zeek.org) logs of various kinds**:

```bash
zcat *.log.gz | vast import zeek
```

**Run a query over the last hour, rendered as JSON**:

```bash
vast export json ':timestamp > 1 hour ago && (6.6.6.6 || 5353/udp)'
```

**Ingest a [PCAP](https://en.wikipedia.org/wiki/Pcap) trace with a 1024-byte
flow cutoff**:

```bash
vast import pcap -c 1024 < trace.pcap
```

**Run a query over PCAP data, sort the packets by time, and feed them into**
`tcpdump`:

```bash
vast export pcap "sport > 60000/tcp && src !in 10.0.0.0/8" \
  | ipsumdump --collate -w - \
  | tcpdump -r - -nl
```

## License

VAST comes with a [3-clause BSD license][license-url].

[license-url]: https://raw.github.com/vast-io/vast/master/LICENSE

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

You can [download the paper][vast-paper] from the [NSDI'16
proceedings][nsdi-proceedings].

[vast-paper]: https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-vallentin.pdf
[nsdi-proceedings]: https://www.usenix.org/conference/nsdi16/technical-sessions

<p align="center">
  Developed with ❤️ by <strong><a href="https://tenzir.com">Tenzir</a></strong>
</p>
