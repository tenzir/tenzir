<p align="center">
<img src="./web/static/img/tenzir-white.svg#gh-dark-mode-only" width="60%" alt="Tenzir">
<img src="./web/static/img/tenzir-black.svg#gh-light-mode-only" width="60%" alt="Tenzir">
</p>
<h3 align="center">
Open Source Data Pipelines for Security Teams
</h3>
</p>

<h4 align="center">

[Documentation](https://docs.tenzir.com)

</h4>
<div align="center">

[![Chat][chat-badge]](https://discord.tenzir.com)

</div>

[chat-badge]: https://img.shields.io/badge/Discord-Community%20Chat-brightgreen?logo=discord&color=purple&style=social

## Start Here

Dive right in for free at [app.tenzir.com][app] and explore the cloud-based
Tenzir demo. When you're ready for deploying your own node, run our installer
that guides your through setup process:

```bash
curl https://get.tenzir.app | sh
```

## What is Tenzir?

Tenzir processes and stores data security event data using *pipelines*, *nodes*,
and the *platform*:

1. **Pipeline**: A dataflow of operators for producing, transforming, and
   consuming data. The `tenzir` binary runs a pipeline.
2. **Node**: Hosts concurrently running pipelines. A node also has a
   storage engine with a thin layer of indexing on top of raw Parquet/Feather
   partitions. The `tenzir-node` binary spawns a node.
3. **Platform**: Manages nodes and account user data. Nodes connect to the
   platform and you can manage them through [app.tenzir.com][app].

![Tenzir Moving Parts](./web/static/img/readme/moving-parts.light.png#gh-light-mode-only)
![Tenzir Moving Parts](./web/static/img/readme/moving-parts.dark.png#gh-dark-mode-only)

Check out our [documentation](https://docs.tenzir.com) for detailed setup
instructions, user guides, and reference material.

## What can I do with Tenzir?

Use Tenzir if you want to:

- Filter, shape, and enrich events before they hit your SIEM or data lake
- Normalize, enrich, aggregate, and deduplicate structured event data
- Store, compact, and search event data in an open storage format
- Operationalize threat intelligence for live and retrospective detection
- Build your own security data lake and need an ETL layer
- Create a federated detection and response architectures

## License

The majority of our code is open source and comes with a **BSD 3-clause
license**. Visit <https://tenzir.com/pricing> for commercial editions and [read
the FAQs](https://docs.tenzir.com/faqs) for further details.

[app]: https://app.tenzir.com
