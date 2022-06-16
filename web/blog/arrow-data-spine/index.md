---
draft: true
description: How VAST leverages Apache Arrow for Security Data Engineering
authors: mavam
date: 2022-06-22
tags: [architecture, arrow, performance, query]
---

# Arrow Data Spine

VAST bets on [Apache Arrow][arrow] as the open interface to structured data. By
"bet," we mean that VAST does not work without Arrow. And we are not alone.
Influx's [IOx][iox], DataDog's [Husky][husky], Anyscale's [Ray][ray],
[TensorBase][tensorbase], and [others][arrow-projects] committed themselves to
making Arrow a corner stone of their system architecture. For us, Arrow was not
always a required dependency. We shifted to a tighter integration over the years
as the Arrow ecosystem matured. In this blog post we explain our journey of
becoming an Arrow-native engine, and what we are looking forward to in the
fast-growing Arrow ecosystem.

After having witnessed first-hand a commitment of [Ray][ray] to Arrow, we
started using Arrow as optional dependency for an alternative, column-oriented
representation of structured event data, next to a row-oriented
[MsgPack][msgpack] representation. The assumption was that a row-based data
representation matches more closely typical event data and therefore allows for
much higher ingestion rates, whereas a column-oriented representation lends
itself better for analytical workloads. Both representations implemented the
same data model that provides rich typing and corresponding operations (e.g.,
native representation of IPv4 and IPv6 addresses plus the ability to perform
top-k prefix search to answer subnet membership queries). Strong typing at the
core of the system allows for efficient data representation and enables
type-based optimizations, such as custom bitmap indexing structures. The data
model came first, then the representation. Arrow was only a representation:

![MsgPack & Arrow](msgpack-arrow.light.png#gh-light-mode-only)
![MsgPack & Arrow](msgpack-arrow.dark.png#gh-dark-mode-only)

Early use cases of VAST were limited to interactive, multi-dimensional search to
extract a subset of disparate records (= rows) in their entirety. The
row-oriented option worked well for this. But as security operations were
maturing, requirements extended to analytical processing of structured data,
making a columnar increasingly beneficial.

Today, the need to bring advanced security analytics and data engineering
together is stronger than ever, but there is a huge gap between the two fields.
We see Arrow as the vehicle to close this gap, by practicing *security data
engineering*. Security analytics should not only be easy and fast. It should
also allow the expert to act in their domain, end-to-end. This requires treating
central domain objects as first-class (e.g., IP addresses, subnets, and URLs)
and making working with them *easy*. This aspect is where our perception of
Arrow changed: the primary value proposition of Arrow is to *make data
interoperability easy*. With an open format specification at its core,
Arrow-based runtimes were mushrooming all over the place. Nowadays it requires
only a few lines of code to integrate Arrow data into the central logic of
applications.

Data interoperability is a sufficient, but not a necessary condition for
enabling security analytics. We also need to embed the data model of the
security domain. This is where Arrow's [extension types][extension-types] come
into play. They add *semantics* to otherwise generic types, e.g., by telling the
user "this is a transport-layer port" and not just a 16-bit unsigned integer, or
"this is a connection 4-tuple to represent a network flow" instead of "this is
a record with 4 fields of type string and unsigned integer". Extension types are
composable and allow for creating a rich typing layer with meaningful domain
objects on top of a standardized data representation. Since they are embedded in
the data, they do not have to be made available out-of-band when crossing the
boundaries of different tools. Now we have self-describing security data.

Interoperability plus support for a domain-specific data model makes Arrow a
solid *data plane*. But Arrow is much more than standardized data
representation. Arrow also comes with bag of tools for working with the
standardized data. In the diagram below, we show the various Arrow pieces that
power the architecture of VAST:

![Arrow Data Plane](arrow-data-plane.light.png#gh-light-mode-only)
![Arrow Data Plane](arrow-data-plane.dark.png#gh-dark-mode-only)

In the center we have highlighted the Arrow data plane that powers other
parts of the system. Orange pieces highlight Arrow building blocks that we use
today, and green pieces elements we plan to use in the future. There are several
aspects worth pointing out:

1. **Unified Data Plane**: When users ingest data into VAST, the
   parsing process converts the native data into Arrow. Similarly, a
   conversation boundary exists when data leaves the system, e.g., when a user
   wants a query result shown in JSON, CSV, or some custom format. Source and
   sink data formats are [exchangeable
   plugins](/docs/understand-vast/architecture/plugins).

2. **Read/Write Path Separation**: one design goal of VAST is a strict
   separation of read and write path, in order to scale them independently. The
   write path follows a horizontally scalable architecture where builders (one per
   schema) turn the in-memory record batches into a persistent representation.
   VAST currently has support for Parquet and Feather.

3. **Pluggable Query Engine**: VAST has live/continuous queries that simply run
   over the stream of incoming data, and historical queries that operate on
   persistent data. The harboring execution engine is something we are about to
   make pluggable. The reason is that VAST runs in extremely different
   environments, from cluster to edge. Query engines are usually optimized for a
   specific use case, so why not use the best engine for the job at hand? Arrow
   makes this possible. [DuckDB][duckdb] and [DataFusion][datafusion] are great
   example of embeddable query engines.

4. **Unified Control Plane**: to realize a pluggable query engine, we also need
   a standardized control plane. This is where [Substrait][substrait] and
   [Flight][flight] come into play. Flight for communication and Substrait as
   canonical query representation. We already experimented with Substrait,
   converting VAST queries into a logical query plan. In fact, VAST has a "query
   language" plugin to make it possible to translate security content. (For
   example, our Sigma plugin translates [Sigma rules][sigma] into VAST queries).
   In short: Substrait is to the control plane what Arrow is to the data plane.
   Both are needed to modularize the concept of a query engine.

Making our own query engine more suitable for analytical workloads has
received less attention in the past, as we prioritized high-performance data
acquisition, low-latency search, in-stream matching using [Compute][compute],
and expressiveness of the underlying domain data model. We did so because VAST
must run robustly in production on numerous appliances all over the world in a
security service provider setting, with confined processing and storage where
efficiency is key.

Moving forward, we are excited to bring more analytical horse power to the
system, while opening up the arena for third-party engines. With the bag of
tools from the Arrow ecosystem, plus all other embeddable Arrow engines that are
emerging, we have a modular architecture to can cover a very wide spectrum of
use cases.

[arrow]: https://arrow.apache.org
[compute]: https://arrow.apache.org/docs/cpp/compute.html
[extension-types]: https://arrow.apache.org/docs/format/Columnar.html#extension-types
[flight]: https://arrow.apache.org/docs/format/Flight.html
[substrait]: https://substrait.io/
[datafusion]: https://arrow.apache.org/datafusion/
[datafusion-c]: https://github.com/datafusion-contrib/datafusion-c
[msgpack]: https://msgpack.org/index.html
[iox]: https://github.com/influxdata/influxdb_iox
[husky]: https://www.datadoghq.com/blog/engineering/introducing-husky/
[ray]: https://github.com/ray-project/ray
[tensorbase]: https://github.com/tensorbase/tensorbase
[arrow-projects]: https://arrow.apache.org/powered_by/
[polars]: https://github.com/pola-rs/polars
[duckdb]: https://duckdb.org/
[sigma]: https://github.com/SigmaHQ/sigma
