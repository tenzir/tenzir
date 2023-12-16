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

## Typed Operators

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
[`serve`](operators/serve.md) to display the result in the browser.

Because pipelines can transport the two types *bytes* and *events*, we can
cleanly model the data lifecycle as separate stages:

![Structuring](unstructured-to-structured.excalidraw.svg)

Data acquisition typically begins with a source operator that uses a
[loader](connectors.md), such as [`load`](operators/load.md), to acquire bytes
as a side effect. Then an operator, such as [`read`](operators/read.md),
transforms bytes into events with the help of a [parser](formats.md). This
effectively turns unstructured data into structured data. There exist numerous
[operators](operators.md) that bring structured data into the desired form for a
specific use case—a process we call *shaping*. After shaping yielded the desired
form, the data leaves the pipeline in reverse order. A [printer](formats.md)
creates bytes from events, e.g., using [`write`](operators/write.md). Finally, a
sink operator, such as [`save`](operators/save.md), uses a
[saver](connectors.md) to write the rendered bytes into a specific location.

## Multi-Schema Dataflows

Tenzir dataflows are *multi-schema* in that a single pipeline can work
with heterogeneous types of events, each of which have a different schemas.
This allows you, for example, to perform aggregations across multiple events.
Multi-schema dataflows require automatic schema inference at parse time. Tenzir
parsers, such as [`json`](formats/json.md) support this out of the box. This
behavior is different from engines that work with structured data where
operators typically work with fixed set of tables. While Schema-less systems,
such as document-oriented databases, offer more simplicity, their
one-record-at-a-time processing comes at the cost of performance.

If the schema in a pipeline changes, we simply create a new batch of events. The
worst case for Tenzir is a ordered stream of schema-switching events, with every
event having a new schema than the previous one. That said, even for those data
streams we can efficiently build homogeneous batches when the inter-event order
does not matter significantly. Similar to predicate pushdown, Tenzir operators
support "ordering pushdown" to signal to upstream operators that the event order
only matters intra-schema but not inter-schema. In this case we transparently
demultiplex a heterogeneous stream into *N* homogeneous streams, each of which
yields batches of up to 65k events. The [`import`](operators/import.md)
operator is an example of such an operator, and it pushes its ordering upstream
so that we can efficiently parse, say, a diverse stream of NDJSON records, such
as Suricata's EVE JSON or Zeek's streaming JSON.

You could call multi-schema dataflows *multiplexed* and there exist dedicated
operators to demultiplex a stream. As of now, this is hard-coded per operator.
For example, [`to directory /tmp/dir write parquet`](connectors/directory.md)
demultiplexes a stream of events so that batches with the same schema go to the
same Parquet file.

The diagram below illustrates the multi-schema aspect of dataflows for schemas
A, B, and C:

![Multi-schema Example](multi-schema-example.excalidraw.svg)

Some operators only work with exactly one instance per schema internally, such
as [`write`](operators/write.md) when combined with the
[`parquet`](formats/parquet.md), [`feather`](formats/feather.md), or
[`csv`](formats/csv.md) formats. These formats cannot handle multiple input
schemas at once. A demultiplexing operator like `to directory .. write <format>`
removes this limitation by writing one file per schema instead.

We are having ideas to make this schema (de)multiplexing explicit with a
`per-schema` [operator modifier](language/operator-modifiers.md) that you can
write in front of every operator. Similarly, we are going to add union types in
the future, making it possible to convert a heterogeneous stream of structured
data into a homogeneous one.

It's important to note that most of the time you don't have to worry about
schemas. They are there for you when you want to work with them, but it's often
enough to just specified the fields that you want to work with, e.g., `where
id.orig_h in 10.0.0.0/8`, or `select src_ip, dest_ip, proto`. Schemas are
inferred automatically in parsers, but you can also seed a parser with a schema
that you define explicitly.

## Unified Live Stream Processing and Historical Queries

Systems for stream processing and running historical queries have different
requirements, and combining them into a single system can be a daunting
challenge. But there is an architectural sweetspot at the right level of
abstraction where you can elegantly combine them. From a user persepctive, our
goal was to seamlessly exchange the beginning of a pipeline to select the source
of the data, be it a historical or continuous one:

![Unified Processing](unified-processing.excalidraw.svg)

Our desired user experience for interacting with historical looks like this:

1. **Ingest**: to persist data at a node, create a pipeline that ends with the
   [`import`](operators/import.md) sink.
2. **Query**: to run a historical query, create a pipeline that begins with the
   [`export`](operators/export.md) operator.

For example, to ingest JSON from a Kafka, you write `from kafka --topic foo |
import`. To query the stored data, you write `export | where file == 42`. The
latter example suggests that the pipeline *first* exports everything, and only
*then* starts filtering with `where`, performing a full scan over the stored
data. But this is not what's happening. Our pipelines support **predicate
pushdown** for every operator. This means that `export` receives the filter
expression before it starts executing, enabling index lookups or other
optimizations to efficiently execute queries with high selectivity where scans
would be sub-optimal.

The central insight here is to ensure that predicate pushdown (as well as other
forms of signalling) exist throughout the entire pipeline engine, and that the
engine can communicate this context to the storage engine.

Our own storage engine is not a full-fledged database, but rather a thin
indexing layer over a set of Parquet/Feather files. The sparse indexes (sketch
data structures, such as min-max synopses, Bloom filters, etc.) avoid full scans
for every query. The storage engine also has a *catalog* that tracks evolving
schemas, performs expression binding, and provides a transactional interface to
add and replace partitions during compaction.

The diagram below shows the main components of the database engine:

![Database Architecture](database-architecture.excalidraw.svg)

Because of this transparent optimization, you can just exchange the source of a
pipeline and switch between historical and streaming execution without degrading
performance. A typical use case begins some exploratory data analysis involving
a few `export` pipelines, but then would deploy the pipeline on streaming data
by exchanging the source with, say, `from kafka`.

The difference between `import` and `from file <path> read parquet` (or `export`
and `to file <path> write parquet`) is that the storage engine has the extra
catalog and indexes, managing the complexity of dealing with a large set of raw
Parquet files.

## Built-in Networking to Create Data Fabrics

Tenzir pipelines have built-in network communication, allowing you to create a
distributed fabric of dataflows to express intricate use cases. There are two
types of network connections: *implicit* and *explicit* ones:

![Implicit vs. Explicit](implicit-vs-explicit-networking.excalidraw.svg)

An implicit network connection exists, for example, when you use the `tenzir`
binary on the command line to run a pipeline that ends in
[`import`](operators/import.md):

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
modifiers](language/operator-modifiers.md).

The above examples are implicit network connections because they're not visible
in the pipeline definition. An explicit network connection terminates a pipeline
as source or sink:

![Pipeline Fabric](pipeline-fabric.excalidraw.svg)

This fictive data fabric above consists of a heterogeneous set of technologies,
interconnected by pipelines. You can also turn any pipeline into an API using
the [`serve`](operators/serve.md) sink, effectively creating a dataflow
microservice that you can access with a HTTP client from the other side:

![Serve Operator](operators/serve.excalidraw.svg)

Because you have full control over the location where you run the pipeline, you
can push it all the way to the "last mile." This helps especially when there
are compliance and data residency concerns that must be properly addressed.
