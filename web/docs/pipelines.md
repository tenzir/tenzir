---
sidebar_position: 0
---

# Pipelines

A Tenzir **pipeline** is a chain of **operators** that represents a dataflow.
Operators are the atomic building blocks that produce, transform, or consume
data. Think of them as UNIX or Powershell commands where output from one command
is input to the next:

![Pipeline Chaining](pipeline-chaining.excalidraw.svg)

There exist three types of operators: **sources** that produce data, **sinks**
that consume data, and **transformations** that do both:

![Pipeline Structure](pipeline-structure.excalidraw.svg)

## Polymorphic Operators

Tenzir pipelines make one more distinction: the elements that the operators push
through the pipeline are *typed*. Every operator has an input and an output
type:

![Input and Output Types](operator-pieces.excalidraw.svg)

When composing pipelines out of operators, the type of adjacent operators have
to match. Otherwise the pipeline is malformed. Here's an example pipeline with
matching operators:

![Typed Pipeline](typed-pipeline.excalidraw.svg)

We call any void-to-void operator sequence a **closed pipeline**. Only closed
pipelines can execute. If a pipeline does not have a source and sink, it would
"leak" data. If a pipeline is open, the engine auto-completes a source/sink when
possible and rejects the pipeline otherwise. Auto-completion is context
dependent: on the command line we read JSON from stdin and write it stdout. In
the app we only auto-complete a missing sink with
[`serve`](operators/sinks/serve.md) to display the result in the browser.

Zooming out in the above type table makes the operator types apparent:

![Operator Types](operator-types.excalidraw.svg)

In fact, we can define the operator type as a function of its input and output types:

```
(Input x Output) → {Source, Sink, Transformation}
```

## Multi-Schema Dataflows

Tenzir dataflows are *multi-schema* in that a single pipeline can work
with heterogeneous types of events, each of which have a different schemas.
This allows you, for example, to perform aggregations across multiple events.
Multi-schema dataflows require automatic schema inference at parse time. Tenzir
parsers, such as [`json`](formats/json.md) support this out of the box.

This behavior is very different from execution engines that only work with
structured data, where the unit of computation is typically a fixed set of
tables. Schema-less systems, such as document-oriented databases, offer more
simplicity, at the cost of performance. In the spectrum of performance and ease
of use, Tenzir therefore [fills a gap](why-tenzir.md):

![Structured vs. Document-Oriented](structured-vs-document-oriented.excalidraw.svg)

:::info Eclectic & Super-structured Data
[Zed](https://amyousterhout.com/papers/zed_cidr23.pdf) has a type system similar
to Tenzir, with the difference that Zed associates types *with every single
value*. Unlike Zed, Tenzir uses a "data frame" abstraction and relies on
homogeneous Arrow record batches of up to 65,535 rows.
:::

If the schema in a pipeline changes, we simply create a new batch of events. The
worst case for Tenzir is a ordered stream of schema-switching events, with every
event having a new schema than the previous one. That said, even for those data
streams we can efficiently build homogeneous batches when the inter-event order
does not matter significantly. Similar to predicate pushdown, Tenzir operators
support "ordering pushdown" to signal to upstream operators that the event
order only matters intra-schema but not inter-schema. In this case we
transparently demultiplex a heterogeneous stream into *N* homogeneous streams,
each of which yields batches of up to 65k events. The `import` operator is an
example of such an operator, and it pushes its ordering upstream so that we can
efficiently parse, say, a diverse stream of NDJSON records, such as Suricata's
EVE JSON or Zeek's streaming JSON.

You could call multi-schema dataflows *multiplexed* and there exist dedicated
operators to demultiplex a stream. As of now, this is hard-coded per operator.
For example, [`to directory /tmp/dir write parquet`](connectors/directory.md)
demultiplexes a stream of events so that batches with the same schema go to the
same Parquet file.

The diagram below illustrates the multi-schema aspect of dataflows for schemas
A, B, and C:

![Multi-schema Example](multi-schema-example.excalidraw.svg)

Some operators only work with exactly one instance per schema internally, such
as [`write`](operators/transformations/write.md) when combined with the
[`parquet`](formats/parquet.md), [`feather`](formats/feather.md), or
[`csv`](formats/csv.md) formats. These formats cannot handle multiple input
schemas at once. A demultiplexing operator like `to directory .. write <format>`
removes this limitation by writing one file per schema instead.

We are having ideas to make this schema (de)multiplexing explicit with a
`per-schema` [operator modifier](operators/modifier.md) that you can write in
front of every operator. Similarly, we are going to add union types in the
future, making it possible to convert a heterogeneous stream of structured data
into a homogeneous one.

It's important to note that most of the time you don't have to worry about
schemas. They are there for you when you want to work with them, but it's often
enough to just specified the fields that you want to work with, e.g., `where
id.orig_h in 10.0.0.0/8`, or `select src_ip, dest_ip, proto`. Schemas are
inferred automatically in parsers, but you can also seed a parser with a schema
that you define explicitly.

## Unified Live Stream Processing and Historical Queries

At first, Tenzir may look like a pure streaming system. But we also offer a
native database engine under the hood, optionally usable at any Tenzir
node. The two primary actions supported are:

1. **Ingest**: to persist data at a node, create a pipeline that ends with the
   [`import`](operators/sinks/import.md) sink.
2. **Query**: to run a historical query, create a pipeline that begins with the
   [`export`](operators/sources/export.md) operator.

This light-weight engine is not a full-fledged database, but rather a thin
management layer over a set of Parquet/Feather files. The engine maintains an
additional layer of sparse indexes (sketch data structures, such as min-max
synopses, Bloom filters, etc.) to avoid full scans for every query.

Every node also comes with a *catalog* that tracks evolving schemas, performs
expression binding, and provides a transactional interface to add and replace
partitions during compaction.

The diagram below shows the main components of the database engine:

![Database Architecture](database-architecture.excalidraw.svg)

A historical query in a pipeline has the form `export | where <expression>`.
This declarative form suggest that the pipeline *first* exports everything, and
only *then* starts filtering, performing a full scan over the stored data. But
this is not what's happening. Our pipelines support **predicate pushdown** for
every operator. This means that `export` receives the filter expression when it
starts executing, and can pass it to the catalog to materialize only the subset
of needed partitions.

Because of this transparent optimization, you can just exchange the source of a
pipeline and switch between historical and streaming execution. For example, you
may perform some exploratory data analysis with a few `export` pipelines, but
then to deploy your pipeline on streaming data you'd switch to `from kafka`.

The difference between `import` and `from file <path> read parquet` (or `export`
and `to file <path> write parquet`) is that the storage engine has the extra
catalog and indexes, managing the complexity of dealing with a large set of raw
Parquet files.

:::info Delta, Iceberg, and Hudi?
We kept the catalog purposefully simple to iterate fast and gain experience in a
controlled system, rather than starting Lakehouse-grade with [Delta
Lake](https://delta.io/), [Iceberg](https://iceberg.apache.org/), or
[Hudi](https://hudi.apache.org/). We are looking forward to having the resources
to integrate with the existing lake management tooling.
:::

## Native Networking to Create Data Fabrics

Tenzir pipelines have built-in network communication, allowing you to create a
distributed fabric of dataflows to express intricate use cases. There are two
types of network connections: *implicit* and *explicit* ones:

![Implicit vs. Explicit](implicit-vs-explicit-networking.excalidraw.svg)

An implicit network connection exists, for example, when you use the `tenzir`
binary on the command line to run a pipeline that ends in
[`import`](operators/sinks/import.md):
pipeline illustrates the concept:

```bash
tenzir 'load gcs bkt/eve.json
       | read suricata
       | where #schema != "suricata.stats"
       | import
       '
```

This results in the following pipeline execution:

![Import Networking](import-networking.excalidraw.svg)

A historical query, like `export | where <expr> | to <connector>`, has the
network connection at the other end:

![Export Networking](export-networking.excalidraw.svg)

Tenzir pipelines are eschewing networking to minimize latency and maximize
throughput. So we generally transfer ownership of operators between processes as
late as possible to prefer local, high-bandwidth communication. For maximum
control over placement of computation, you can override the automatic operator
location with the `local` and `remote` [operator
modifiers](operators/modifier.md).

The above examples are implicit network connections because they're not visible
in the pipeline definition. An explicit network connection terminates a pipeline
as source or sink:

![Pipeline Fabric](pipeline-fabric.excalidraw.svg)

This fictive data fabric above consists of a heterogeneous set of technologies,
interconnected by pipelines. You can also turn any pipeline into an API using
the [`serve`](operators/sinks/serve.md) sink, effectively creating a dataflow
microservice that you can access with a HTTP client from the other side:

![Serve Operator](operators/sinks/serve.excalidraw.svg)

Because you have full control over the location where you run the pipeline, you
can push it all the way to the "last mile.". This helps especially when there
are compliance and data residency concerns that must be properly addressed.
