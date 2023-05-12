---
title: "Parquet & Feather: Enabling Open Investigations"
authors: 
  - mavam
  - dispanser
date: 2022-10-07
last_updated: 2023-01-10
tags: [arrow, parquet, feather]
---

[Apache Parquet][parquet] is the common denominator for structured data at rest.
The data science ecosystem has long appreciated this. But infosec? Why should
you care about Parquet when building a threat detection and investigation
platform? In this blog post series we share our opinionated view on this
question. In the next three blog posts, we

1. describe how VAST uses Parquet and its little brother [Feather][feather]
2. benchmark the two formats against each other for typical workloads
3. share our experience with all the engineering gotchas we encountered along
   the way

[parquet]: https://parquet.apache.org/
[feather]: https://arrow.apache.org/docs/python/feather.html

<!--truncate-->

:::info Parquet & Feather: 1/3
This is blog post is part of a 3-piece series on Parquet and Feather.

1. This blog post
2. [Writing Security Telemetry][parquet-and-feather-2]
3. [Data Engineering Woes][parquet-and-feather-3]

[parquet-and-feather-2]: /blog/parquet-and-feather-writing-security-telemetry/
[parquet-and-feather-3]: /blog/parquet-and-feather-data-engineering-woes/
:::

## Why Parquet and Feather?

Parquet is the de-facto standard for storing structured data in a format
conducive for analytics. Nearly all analytics engines support reading Parquet
files to load a dataset in memory for subsequent analysis.

The data science community has long built on this foundation, but the majority
of infosec tooling [does not build on an open
foundation](/docs/about/vision#the-soc-architecture-maze). Too many
products hide their data behind silos, either wrapped behind a SaaS with a thin
API, or in a custom format that requires cumbersome ETL pipelines. Nearly all
advanced use cases require full access to the data. Especially when
the goal is developing realtime threat detection and response systems.

Security is a data problem. But how should we represent that data? This is where
Parquet enters the picture. As a vendor-agnostic storage format for structured
and nested data, it decouples storage from analytics. This is where SIEM
monoliths fail: they offer a single black box that tightly couples data
acquisition and processing capabilities. Providing a thin "open" API is not really
open, as it prevents high-bandwidth data access that is needed for advanced
analytics workloads.

Open storage prevents vendor-lock-in. When any tool can work with the data, you
build a sustainable foundation for implementing future use cases. For example,
with Parquet's column encryption, you can offload fine-grained compliance use
cases to a dedicated application. Want to try out a new analytics engine? Just
point it to the Parquet files.

## Parquet's Little Brother

[Feather][feather] is Parquet's little brother. It emerged while building a
proof of concept for "fast, language-agnostic data frame storage for Python
(pandas) and R." The format is a thin layer on top of [Arrow
IPC](https://arrow.apache.org/docs/python/ipc.html#ipc), making it conducive for
memory mapping and zero-copy usage. On the spectrum of speed and
space-efficiency, think of it this way:

![Parquet vs. Feather](parquet-vs-feather.excalidraw.svg)

Before Feather existed, VAST had its own storage format that was 95% like
Feather, minus a thin framing. (We called it the *segment store*.)

Wait, but Feather is an in-memory format and Parquet an on-disk format. You
cannot compare them! Fair point, but don't forget transparent Zstd compression.
For some schemas, we barely notice a difference (e.g., PCAP), whereas for
others, Parquet stores boil down to a fraction of their Feather equivalent.

The [next blog post][parquet-and-feather-2] goes into these details. For now, we
want to stress that Feather is in fact a reasonable format for data at rest,
even when looking at space utilization alone.

## Parquet and Feather in VAST

VAST can store event data as Parquet or Feather. The unit of storage scaling is
a *partition*. In Arrow terms, a partition is a persisted form of an [Arrow
Table][arrow-table], i.e., a concatenation of [Record
Batches][arrow-record-batch]. A partition has thus a fixed schema. VAST's [store
plugin][store-plugin] determines how a partition writes its buffered record
batches to disk. The diagram below illustrates the architecture:

![Parquet Analytics](parquet-analytics.excalidraw.svg)

[arrow-table]: https://arrow.apache.org/docs/python/data.html#tables
[arrow-record-batch]: https://arrow.apache.org/docs/python/data.html#record-batches
[store-plugin]: /docs/VAST%20v3.0/understand/architecture/plugins#store

This architecture makes it easy to point an analytics application directly to
the store files, without the need for ETLing it into a dedicated warehouse, such
as Spark or Hadoop.

The event data thrown at VAST has quite some variety of schemas. During
ingestion, VAST first demultiplexes the heterogeneous stream of events into
multiple homogeneous streams, each of which has a unique schema. VAST buffers
events until the partition hits a pre-configured event limit (e.g., 1M) or until
a timeout occurs (e.g., 60m). Thereafter, VAST writes the partition in one shot
and persists it.

The buffering provides optimal freshness of the data, as it enables queries run
on not-yet-persisted data. But it also sets an upper bound on the partition
size, given that it must fit in memory in its entirety. In the future, we plan
to make this freshness trade-off explicit, making it possible to write out
larger-than-memory stores incrementally.

## Imbuing Domain Semantics

In a [past blog][blog-arrow] we described how VAST uses Arrow's extensible
type system to add richer semantics to the data. This is how the value of VAST
transcends through the analytics stack. For example, VAST has native IP address
types that you can show up in Python as [ipaddress][ipaddress] instance. This
avoids friction in the data exchange process. Nobody wants to spend time
converting bytes or strings into the semantic objects that are ultimately need
for the analysis.

[blog-arrow]: /blog/apache-arrow-as-platform-for-security-data-engineering
[ipaddress]: https://docs.python.org/3/library/ipaddress.html

Here's how [VAST's type system](/docs/understand/data-model/type-system) looks
like:

![Type System - VAST](type-system-vast.excalidraw.svg)

There exist two major classes of types: *basic*, stateless types with a static
structure and a-priori known representation, and *complex*, stateful types that
carry additional runtime information. We map this type system without
information loss to Arrow:

![Type System - Arrow](type-system-arrow.excalidraw.svg)

VAST converts enum, address, and subnet types to
[extension-types][arrow-extension-types]. All types are self-describing and part
of the record batch meta data. Conversion is bi-directional. Both Parquet and
Feather support fully nested structures in this type system. In theory. Our
third blog post in this series describes the hurdles we had to overcome to make
it work in practice.

[arrow-extension-types]: https://arrow.apache.org/docs/format/Columnar.html#extension-types

In the next blog post, we perform a quantitative analysis of the two formats: how
well do they compress the original data? How much space do they take up in
memory? How much CPU time do I pay for how much space savings? In the meantime,
if you want to learn more about Parquet, take a look at the [blog post
series][arrow-parquet-blog] from the Arrow team.

[arrow-parquet-blog]: https://arrow.apache.org/blog/2022/10/05/arrow-parquet-encoding-part-1/
