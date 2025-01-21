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

## Catalog

Maintains [partition](#partition) ownership and metadata.

The catalog is a component in the [node](#node) that owns the
[partitions](#partition), keeps metadata about them, and maintains a set of
sparse secondary indexes to identify relevant partitions for a given query. It
offers a transactional interface for adding and removing partitions.

- [Tune catalog
  fragmentation](./installation/tune-performance/README.md#tune-catalog-fragmentation)
- [Configure the catalog](./installation/tune-performance/README.md#configure-the-catalog)

## Connector

Manages chunks of raw bytes by interacting with a resource.

A connector is either a *loader* that acquires bytes from a resource, or a
*saver* that sends bytes to a resource. Loaders are implemented as ordinary
[operators](tql2/operators.md) prefixed with `load_*` while savers are prefixed with
`save_*`.

## Context

A stateful object used for in-band enrichment.

Contexts come in various types, such as a lookup table, Bloom filter, and GeoIP
database. They live inside a node and you can enrich with them in other
pipelines.

- Read more about [enrichment](./enrichment/README.md)

## Destination

An pipeline ending with an [output](#output) operator preceded by a
[`subscribe`](tql2/operators/subscribe.md) input operator.

- Learn more about [pipelines](pipelines/README.md)

## Format

Translates between bytes and events.

A format is either a *parser* that converts bytes to events, or a *printer*
that converts events to bytes. Example formats are [`json`](./formats/json.md),
[`cef`](./formats/cef), and [`pcap`](./formats/pcap.md).

- See all available [formats](./formats.md)

## Index

Optional data structures for accelerating historical queries.

Tenzir has *sparse* indexes. Sparse indexes live in memory and point to
[partitions](#partition).

- [Configure the catalog](./installation/tune-performance/README.md#configure-the-catalog)

## Input

An [operator](#operator) that only producing data, without consuming anything.

- Learn more about [pipelines](pipelines/README.md)

## Integration

A set of pipelines to integrate with a third-party product.

An integration describes use cases in combination with a specific product or
tool. Based on the depth of the configuration, this may require configuration on
either end.

- [List of all integrations](integrations.md)

## Library

A collection of [packages](#package).

Our community library is [freely available at
GitHub](https://github.com/tenzir/library).

## Loader

A connector that acquires bytes.

A loader is the dual to a [saver](#saver). It has a no input and only performs a
side effect that acquires bytes. Use a loader implicitly with the
[`from`](tql2/operators/from.md) operator or explicitly with the `load_*`
operators.

- Learn more about [pipelines](pipelines/README.md)

## Node

A host for [pipelines](#pipeline) and storage reachable over the network.

The `tenzir-node` binary starts a node in a dedicated server process that
listens on TCP port 5158.

- [Deploy a node](./installation/deploy-a-node/README.md)
- Use the [REST API](./rest-api.md) to manage a node
- [Import into a node](./usage/import-into-a-node/README.md)
- [Export from a node](./usage/export-from-a-node/README.md)

## Metrics

Runtime statistics about pipeline execution.

- [Collect metrics](./usage/collect-metrics.md)

## OCSF

The [Open Cybersecurity Schema Framework (OCSF)](https://schema.ocsf.io) is a
cross-vendor schema for security event data. Our [community library](#library)
contains packages that map data sources to OCSF.

- [Map data to COSF](tutorials/map-data-to-ocsf/README.md)

## Operator

The building block of a [pipeline](#pipeline).

An operator is a [input](#input), [transformation](#transformation), or
[output](#output).

- See all available [operators](./tql2/operators.md)

## Output

An [operator](#operator) consuming data, without producing anything.

- Learn more about [pipelines](pipelines/README.md)

## PaC

The acronym PaC stands for *Pipelines as Code*. It is meant as an adaptation of
[Infrastructure as Code
(IaC)](https://en.wikipedia.org/wiki/Infrastructure_as_code) with pipelines
represent the (data) infrastructure that is provisioning as code.

- Learn how to provision [piplines as
  code](./usage/run-pipelines/README.md#as-code).

## Package

A collection of [pipelines](#pipeline) and [contexts](#context).

- Read more about [packages](packages.md)
- [Write a package](tutorials/write-a-package.md)

## Parser

A bytes-to-events operator.

A parser is the dual to a [printer](#printer). Use a parser implicitly in the
[`from`](./tql2/operators/from.md) operator.

- Learn more about [pipelines](pipelines/README.md)
- See [all formats](./formats.md)

## Partition

The horizontal scaling unit of the storage attached to a [node](#node).

A partition contains the raw data and optionally a set of indexes. Supported
formats are [Parquet](https://parquet.apache.org) or
[Feather](https://arrow.apache.org/docs/python/feather.html).

- [Control the partition size](./installation/tune-performance/README.md#control-the-partition-size)
- [Configure catalog and partition indexes](./installation/tune-performance/README.md#configure-catalog-and-partition-indexes)
- [Select the store format](./installation/tune-performance/README.md#select-the-store-format)
- [Adjust the store
  compression](./installation/tune-performance/README.md#adjust-the-store-compression)
- [Rebuild partitions](./installation/tune-performance/README.md#rebuild-partitions)

## Pipeline

Combines a set of [operators](#operator) into a dataflow graph.

- Understand [how pipelines work](pipelines/README.md)
- [Run a pipeline](./usage/run-pipelines/README.md)

## Platform

Control plane for nodes and pipelines, accessible through [app](#app) at
[app.tenzir.com](https://app.tenzir.com).

## Printer

An events-to-bytes operator.

A [format](#format) that translates events into bytes.

A printer is the dual to a [parser](#parser). Use a parser implicitly in the
[`to`](./tql2/operators/to.md) operator.

- Learn more about [pipelines](pipelines/README.md)
- See [all formats](./formats.md)

## Saver

A [connector](#connector) that emits bytes.

A saver is the dual to a [loader](#loader). It has a no output and only performs
a side effect that emits bytes. Use a saver implicitly with the
[`to`](tql2/operators/to.md) operator or explicitly with the `save_*`
operators.

- Learn more about [pipelines](pipelines/README.md)

## Schema

A top-level record type of an event.

- [Show available schemas](./usage/show-available-schemas.md)

## Source

An pipeline starting with an [input](#input) operator followed by a
[`publish`](tql2/operators/publish.md) output operator.

- Learn more about [pipelines](pipelines/README.md)

## TQL

An acronym for *Tenzir Query Language*.

TQL is the language in which users write [pipelines](#pipeline).

- Learn more about the [language](./tql2/language/statements.md)

## Transformation

An [operator](#operator) consuming both input and producing output.

- Learn more about [pipelines](pipelines/README.md)
