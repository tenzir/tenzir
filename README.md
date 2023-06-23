<p align="center">
<img src="./web/static/img/tenzir-white.svg#gh-dark-mode-only" width="60%" alt="Tenzir">
<img src="./web/static/img/tenzir-black.svg#gh-light-mode-only" width="60%" alt="Tenzir">
</p>
<h3 align="center">
Easy Data Pipelines for Security Teams
</h3>
</p>

<h4 align="center">

[Get Started](https://docs.tenzir.com/next/get-started) |
[User Guides](https://docs.tenzir.com/next/user-guides)

</h4>
<div align="center">

[![Chat][chat-badge]](https://discord.tenzir.com)

</div>

[chat-badge]: https://img.shields.io/badge/Discord-Community%20Chat-brightgreen?logo=discord&color=purple&style=social

## Start Here

Dive right in and install Tenzir:

```bash
curl -L get.tenzir.app | sh
```

Check out our [documentation](https://docs.tenzir.com) for detailed setup
instruction, user guides, and reference material.

## What is Tenzir?

Tenzir is a distributed platform for processing and storing security event data
in a pipeline dataflow model, providing the following abstractions:

- Tenzir's **pipelines** consist of powerful operators that perform computations
  over [Arrow](https://arrow.apache.org) data frames. The [Tenzir Query Language
  (TQL)](https://docs.tenzir.com/next/language) makes it easy to express
  pipelines—akin to Splunk and Kusto.
- Tenzir's indexed **storage engine** persists dataflows in an open format
  ([Parquet](https://parquet.apache.org/) &
  [Feather](https://arrow.apache.org/docs/python/feather.html)) so that you can
  access them with any query engine, or run pipelines over selective historical
  workloads.
- Tenzir **nodes** offer a managed runtime for pipelines and storage.
- Interconnected nodes form a **data fabric** and pipelines can span across them
  to implement sophisticated security architectures.

## What can I do with Tenzir?

Use Tenzir if you want to:

- Filter, shape, and enrich events before they hit your SIEM or data lake
- Normalize, enrich, aggregate, and deduplicate structured event data
- Store, compact, and search event data in an open storage format
- Operationalize threat intelligence for live and retrospective detection
- Build your own security data lake
- Create a federated detection and response architectures

![Building Blocks](./web/static/img/readme/architecture-nodes.excalidraw.light.png#gh-light-mode-only)
![Building Blocks](./web/static/img/readme/architecture-nodes.excalidraw.dark.png#gh-dark-mode-only)

## License

The open-source editions of Tenzir comes with a **3-clause BSD license**.

Please see <https://tenzir.com/pricing> for commercial editions.

<p align="center">
  Developed with ❤️ by <strong><a href="https://tenzir.com">Tenzir</a></strong>
</p>
