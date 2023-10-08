# Glossary

<!--
This glossary adheres to the following template for defining terms:

    ## TERM

    Brief definition without using TERM.

    One additional paragraphs that provide additional information to
    understand TERM to its full extent. High-level only to understand the
    concept, without going into details, which should be links below this
    paragraph.

    - Link to relevant material
    - Other link to more information
    - ...

This convention is not enforced technically.
-->

This page defines central terms in the Tenzir ecosystem.

:::note missing term?
If you are missing a term, please open a [GitHub Discussion][new-discussion] or
ping us in our [Discord chat](/discord).
:::

[new-discussion]: https://github.com/orgs/tenzir/discussions/new?category=questions-answers

## App

Web user interface to access [platform](#platform) at
[app.tenzir.com](https://app.tenzir.com).

The app is a web application that partially runs in the user's browser. It is
written in [Svelte](https://svelte.dev/).

- [Use the app](./setup-guides/use-the-app/README.md)

## Catalog

Maintains [partition](#partition) ownership and metadata.

The catalog is a component in the [node](#node) that owns the
[partitions](#partition), keeps metadata about them, and maintains a set of
sparse secondary indexes to identify relevant partitions for a given query. It
offers a transactional interface for adding and removing partitions.

- [Tune catalog
  fragmentation](./setup-guides/tune-performance/README.md#tune-catalog-fragmentation)
- [Configure catalog and partition indexes](./setup-guides/tune-performance/README.md#configure-catalog-and-partition-indexes)

## Connector

Manages chunks of raw bytes by interacting with a resource.

A connector is either a *loader* that acquires bytes from a resource, or a
*saver* that sends bytes to a resource. Example connectors are
[`file`](./connectors/file.md), [`kafka`](./connectors/kafka.md), and
[`nic`](./connectors/nic.md).

- See all available [connectors](./connectors.md)

## Format

Translates between bytes and events.

A format is either a *parser* that converts bytes to events, or a *printer*
that converts events to bytes. Example formats are [`json`](./formats/json.md),
[`cef`](./formats/cef), and [`pcap`](./formats/pcap.md).

- See all available [formats](./formats.md)

## Index

Optional data structures for accelerating historical queries.

Tenzir has *sparse* and *dense* indexes. Sparse indexes live in memory and point
to [partitions](#partition), whereas dense indexes live within a partition and
point to individual rows within the partition.

- [Configure catalog and partition indexes](./setup-guides/tune-performance/README.md#configure-catalog-and-partition-indexes)

## Node

A host for [pipelines](#pipeline) and storage reachable over the network.

The `tenzir-node` binary starts a node in a dedicated server process that
listens on TCP port 5158.

- [Deploy a node](./setup-guides/deploy-a-node/README.md)
- [Use the app](./setup-guides/use-the-app/README.md) to manage a node
- Use the [REST API](./rest-api.md) to manage a node
- [Import into a node](./user-guides/import-into-a-node.md)
- [Export from a node](./user-guides/export-from-a-node.md)

## Metrics

Runtime statistics about pipeline execution.

- [Collect metrics](./setup-guides/collect-metrics.md)

## Operator

The building block of a [pipeline](#pipeline).

An operator is a [source](#source), [transformation](#transformation), or
[sink](#sink).

- See all available [operators](./operators.md)

## Partition

The horizontal scaling unit of the storage attached to a [node](#node).

A partition contains the raw data and optionally a set of indexes. Supported
formats are [Parquet](https://parquet.apache.org) or
[Feather](https://arrow.apache.org/docs/python/feather.html).

- [Control the partition size](./setup-guides/tune-performance/README.md#control-the-partition-size)
- [Configure catalog and partition indexes](./setup-guides/tune-performance/README.md#configure-catalog-and-partition-indexes)
- [Select the store format](./setup-guides/tune-performance/README.md#select-the-store-format)
- [Adjust the store
  compression](./setup-guides/tune-performance/README.md#adjust-the-store-compression)
- [Rebuild partitions](./setup-guides/tune-performance/README.md#rebuild-partitions)

## Pipeline

Combines a set of [operators](#operator) into a dataflow graph.

- Understand [how pipelines work](./pipelines.md)
- Understand the [pipeline language](./language.md)
- [Run a pipeline](./user-guides/run-a-pipeline/README.md)

## Platform

Control plane for nodes and pipelines, accessible through [app](#app) at
[app.tenzir.com](https://app.tenzir.com).

- [Use the app](./setup-guides/use-the-app/README.md)

## Schema

A named record type describing the top-level structure of a data frame.

[Schemas](./data-model/schemas.md)

- [Show available schemas](./user-guides/show-available-schemas.md)

## Sink

An [operator](#operator) consuming input, without producing any output.

- See all available [sinks](./operators/sinks/README.md)

## Source

An [operator](#operator) producing output, without consuming any input.

- See all available [sources](./operators/sources/README.md)

## TQL

An acronym for *Tenzir Query Language*.

TQL is the language in which users write [pipelines](#pipeline).

- Learn more about the [language](./language.md)

## Transformation

An [operator](#operator) consuming both input and producing output.

- See all [transformations](./operators/transformations/README.md)
