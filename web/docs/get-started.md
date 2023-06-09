# Get Started

Welcome to Tenzir! If you have any questions, do not hesitate to join our
[community chat](/discord) or open a [GitHub
discussion](https://github.com/tenzir/tenzir/discussions).

## What is Tenzir?

<!-- Keep in sync with project README at https://github.com/tenzir/tenzir -->

Tenzir is the open-source pipeline and storage engine for security event data.

![Tenzir Building Blocks](/img/building-blocks.excalidraw.svg)

Tenzir offers dataflow **pipelines** for data acquisition, reshaping, routing,
and integration of security tools. Pipelines transport richly typed data frames
to enable efficient analytical high-bandwidth streaming workloads. Tenzir's open
**storage engine** uses the same dataflow language to deliver a unified
abstraction for batch and stream processing to drive a wide variety of security
use cases.

A **Tenzir node** provides managed pipelines and storage as a continuously
running service. You can run pipelines across multiple nodes to create a
distributed security data architecture.

![Tenzir Building Blocks](/img/architecture-nodes.excalidraw.svg)

Consider Tenzir if you want to:

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
