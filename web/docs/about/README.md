# About

Welcome to the VAST! If you have any questions, do not hesitate to join our
[community chat](/discord) or open a [GitHub
discussion](https://github.com/tenzir/vast/discussions).

## What is VAST?

<!-- Keep in sync with project README at https://github.com/tenzir/vast -->

VAST is the open-source pipeline and storage engine for security event data.

![VAST Building Blocks](/img/building-blocks.excalidraw.svg)

VAST offers dataflow **pipelines** for data acquisition, reshaping, routing, and
integration of security tools. Pipelines transport richly typed data frames to
enable efficient analytical high-bandwidth streaming workloads. VAST's open
**storage engine** uses the same dataflow language to deliver a unified
abstraction for batch and stream processing to drive a wide variety of security
use cases.

A **VAST node** provides managed pipelines and storage as a continuously running
service. You can run pipelines across multiple nodes to create a distributed
security data architecture.

![VAST Building Blocks](/img/architecture-nodes.excalidraw.svg)

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

If you're unsure whether VAST is the right tool for your use case, keep reading.

## What's Next?

The structure of the documentation follows the user journey:

1. [Try](../try/README.md): tells you how to quickly get your hands on VAST to
   give it a shot.
   👉 *Begin here if you want to test-drive VAST.*
2. [Setup](../setup/README.md): describes how you can download, install, and
   configure VAST in a variety of environments.
   👉 *Start here if you want to deploy VAST.*
3. [Use](../use/README.md): explains how to work with VAST, e.g., ingesting
   data, running queries, matching threat intelligence, or integrating it with
   other security tools.
   👉 *Go here if you have a running VAST, and want to explore what you can do
   with it.*
4. [Understand](../understand/README.md): describes the system design and
   architecture, e.g., the pipeline language, the data model, and the
   implementation in terms actor model for concurrency and distribution.
   👉 *Read here if you want to know why VAST is built the way it is.*
5. [Contribute](../contribute/README.md): lays out our contribution guidelines
   and explains how to contribute to the code base, either in terms of
   documentation or source code.
   👉 *Read this if you want to become a contributor.*
6. [Develop](../develop/README.md): provides developer-oriented resources to
   work on VAST, e.g., write own plugins or enhance the source code.
   👉 *Look here if you are ready to get your hands dirty and write code.*
