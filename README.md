<p align="center">

<img src="./web/static/img/vast-white.svg#gh-dark-mode-only" width="75%" alt="VAST">
<img src="./web/static/img/vast-black.svg#gh-light-mode-only" width="75%" alt="VAST">

</p>

<h1 align="center">
  Visibility Across Space and Time
</h1>
<h4 align="center">

[About](https://vast.io/docs/about) |
[Try](https://vast.io/docs/try) |
[Use](https://vast.io/docs/use) |
[Understand](https://vast.io/docs/understand) |
[Contribute](https://vast.io/docs/contribute) |
[Develop](https://vast.io/docs/develop)
</h4>
<div align="center">

[![Chat][chat-badge]](https://vast.io/discord)
</div>

[chat-badge]: https://img.shields.io/badge/Discord-Community%20Chat-brightgreen?logo=discord&color=purple&style=social

<!-- Keep in sync with https://vast.io/about -->

VAST is the open-source pipeline and storage engine for security.

![Building Blocks](./web/static/img/readme/building-blocks.excalidraw.light.png#gh-light-mode-only)
![Building Blocks](./web/static/img/readme/building-blocks.excalidraw.dark.png#gh-dark-mode-only)

VAST offers dataflow **pipelines** for data acquisition, reshaping, routing, and
integration of security tools. Pipelines transport richly typed data frames to
enable efficient analytical high-bandwidth streaming workloads. VAST's open
**storage engine** uses the same dataflow language to deliver a unified
abstraction for batch and stream processing to drive a wide variety of security
use cases.

A **VAST node** provides managed pipelines and storage as a continuously running
service. You can run pipelines across multiple nodes to create a distributed
security data architecture.

![Building Blocks](./web/static/img/readme/architecture-nodes.excalidraw.light.png#gh-light-mode-only)
![Building Blocks](./web/static/img/readme/architecture-nodes.excalidraw.dark.png#gh-dark-mode-only)

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

## Get Started

Our [quickstart guide](https://vast.io/docs/try/quickstart) showcases how you
can start exploring Zeek and Suricata data with VAST. Start here to get a first
impression of VAST.

To get hands-on with VAST, follow these steps:

1. [Download](https://vast.io/docs/setup/download) VAST
2. [Start](https://vast.io/docs/setup/deploy) a VAST node
3. [Run](https://vast.io/docs/setup/use) pipelines to import/export data

If you have any questions when reading our [docs](https://vast.io/docs), feel
free to start a [GitHub discussion](https://github.com/tenzir/vast/discussions)
or swing by our [Discord chat](https://vast.io/discord)—we're here to help!

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
proceedings website][nsdi-proceedings].

[vast-paper]: https://www.usenix.org/system/files/conference/nsdi16/nsdi16-paper-vallentin.pdf
[nsdi-proceedings]: https://www.usenix.org/conference/nsdi16/technical-sessions

<p align="center">
  Developed with ❤️ by <strong><a href="https://tenzir.com">Tenzir</a></strong>
</p>
