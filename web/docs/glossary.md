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
*saver* that sends bytes to a resource. Example connectors are
[`file`](./connectors/file.md), [`kafka`](./connectors/kafka.md), and
[`nic`](./connectors/nic.md).

- See all available [connectors](./connectors.md)

## Context

A stateful object used for in-band enrichment.

Contexts live inside a node and you can manage them with the
[`context`](./operators/context.md) operator. A context has pluggable type, such
as a lookup table, GeoIP database, or a custom plugin. The
[`enrich`](./operators/enrich.md) places a context into a pipeline for
enrichment.

- Read more about [contexts](./contexts.md)
- [Manage](./operators/context.md) a context
- [Enrich](./operators/enrich.md) with a context

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

A [connector](#connector) that acquires bytes.

A loader is the dual to a [saver](#saver). It has a no input and only performs a
side effect that acquires bytes. Use a loader in the
[`from`](./operators/from.md) or [`load`](./operators/load.md) operators.

- Learn more about [pipelines](./pipelines.md)
- See [all connectors](./connectors.md)

## Node

A host for [pipelines](#pipeline) and storage reachable over the network.

The `tenzir-node` binary starts a node in a dedicated server process that
listens on TCP port 5158.

- [Deploy a node](./installation/deploy-a-node.md)
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

An operator is a [source](#source), [transformation](#transformation), or
[sink](#sink).

- See all available [operators](./operators.md)

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

A [format](#format) that translates bytes into events.

A parser is the dual to a [printer](#printer). Use a parser in the
[`from`](./operators/from.md) or [`read`](./operators/read.md) operators. You
can use the [`parse`](./operators/parse.md) operator to parse a single field
with a parser.

- Learn more about [pipelines](./pipelines.md)
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

- Understand [how pipelines work](./pipelines.md)
- Understand the [pipeline language](./language.md)
- [Run a pipeline](./usage/run-pipelines/README.md)

## Platform

Control plane for nodes and pipelines, accessible through [app](#app) at
[app.tenzir.com](https://app.tenzir.com).

## Printer

A [format](#format) that translates events into bytes.

A printer is the dual to a [parser](#parser). Use a parser in the
[`to`](./operators/to.md) or [`write`](./operators/write.md) operators.

- Learn more about [pipelines](./pipelines.md)
- See [all formats](./formats.md)

## Saver

A [connector](#connector) that emits bytes.

A saver is the dual to a [loader](#loader). It has a no output and only performs
a side effect that emits bytes. Use a saver in the [`to`](./operators/to.md) or
[`save`](./operators/save.md) operators.

- Learn more about [pipelines](./pipelines.md)
- See [all connectors](./connectors.md)

## Schema

A named record type describing the top-level structure of a data frame.

[Schemas](./data-model/schemas.md)

- [Show available schemas](./usage/show-available-schemas.md)

## Sink

An [operator](#operator) consuming input, without producing any output.

- Learn more about [pipelines](./pipelines.md)

## Source

An [operator](#operator) producing output, without consuming any input.

- Learn more about [pipelines](./pipelines.md)

## TQL

An acronym for *Tenzir Query Language*.

TQL is the language in which users write [pipelines](#pipeline).

- Learn more about the [language](./language.md)
- Understand the [syntax](./language/syntax.md)

## Transformation

An [operator](#operator) consuming both input and producing output.

- Learn more about [pipelines](./pipelines.md)
