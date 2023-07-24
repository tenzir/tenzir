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

## Connector

Manages chunks of raw bytes by interacting with a resource.

A connector is either a *loader* that acquires bytes or a *saver*
that sends bytes to an external resource. Examples include
[`file`](./connectors/file.md), [`kafka`](./connectors/kafka.md), and
[`nic`](./connectors/nic.md).

- See all [connectors](./connectors.md)

## Format

Translates between bytes and events.

A format is either a *parser* that converts bytes to events, or a *printer*
that converts events to bytes.

- See all [formats](./formats.md)

## Node

A host for [pipelines](#pipeline) and storage reachable over the network.

- [Deploy a node](./setup-guides/deploy-a-node/README.md)

## Operator

The atomic building block of a [pipeline](#pipeline).

An operator is a [source](#source), [transformation](#transformation), or
[sink](#sink)

## Partition

The horizontal scaling unit of the storage attached to a [node](#node).

A partition contaisn a [store](#store) and optionally a set of indexes.

## Pipeline

Combines a set of [operators](#operator) into a dataflow graph.

- Understand the [pipeline language](./language/pipelines.md)
- [Run a pipeline](./user-guides/run-a-pipeline/README.md)

## Sink

An [operator](#operator) consuming input, without producing any output.

- See all [sinks](./operators/sinks/README.md)

## Source

An [operator](#operator) producing output, without consuming any input.

- See all [sources](./operators/sources/README.md)

## Store

The raw data in a [partition](#partition) in Parquet or Feather.

## TQL

An acronym for *Tenzir Query Language*.

TQL is the language in which users write [pipelines](#pipeline).

- Learn more about the [language](./language/pipelines.md)

## Transformation

An [operator](#operator) consuming both input and producing output.

- See all [transformations](./operators/transformations/README.md)
