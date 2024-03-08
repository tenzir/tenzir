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

Dive right in at [app.tenzir.com][app] and play with a cloud-based Tenzir demo.
Once you're ready for deploying your own node, run our installer that guides
your through setup process:

```bash
curl https://get.tenzir.app | sh
```

## What is Tenzir?

Tenzir is a data pipeline solution for optimizing cloud and data costs, running
detections and analytics, building a centralized security data lake, or creating
a decentralized security data fabric.

![Tenzir Moving Parts](./web/static/img/readme/platform-and-nodes.light.png#gh-light-mode-only)
![Tenzir Moving Parts](./web/static/img/readme/platform-and-nodes.dark.png#gh-dark-mode-only)

The key abstractions in Tenzir are:

1. **Pipeline**: A dataflow of operators for producing, transforming, and
   consuming data. The `tenzir` binary runs a pipeline stand-alone..
2. **Node**: Manages pipelines. A node also has a custom storage engine built on
   top of Parquet/Feather partitions. The `tenzir-node` binary spawns a node.
3. **Platform**: Offers a management layer for nodes. Nodes connect to the
   platform and you can manage them at [app.tenzir.com][app].

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

The pipeline executor and majority of the node code is open source and comes
with a **BSD 3-clause license**. Visit <https://tenzir.com/pricing> for
commercial editions and [read the FAQs](https://docs.tenzir.com/faqs) for
further details.

[app]: https://app.tenzir.com
