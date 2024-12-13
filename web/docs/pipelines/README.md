---
sidebar_position: 0
---

# Pipelines

A Tenzir **pipeline** is a chain of **operators** that represents a dataflow.
Operators are the atomic building blocks that produce, transform, or consume
data. Think of them as Unix or Powershell commands where the result from one
command is feeding into the next:

![Pipeline Chaining](operator-chaining.svg)

Our pipelines have 3 types of operators: **inputs** that produce data,
**outputs** that consume data, and **transformations** that do both:

![Pipeline Structure](operator-types.svg)

## Structured and Unstructured Dataflow

Tenzir pipelines make one more distinction: the elements that the operators push
through the pipeline are *typed*. An operator has an upstream and downstream
type:

![Upstream and Downstream Types](operator-table.svg)

When composing pipelines out of operators, the upstream/downstream type of
adjacent operators have to match. Otherwise the pipeline is malformed. We call
any void-to-void operator sequence a **closed pipeline**. Only closed pipelines
can execute. If a pipeline does not have a input and output operator, it would
"leak" data.

Operators can be *polymorphic* in that they can have more than a single upstream
and downstream type. For example, [`head`](../tql2/operators/head.md) accepts
both bytes and events, filtering either the first N bytes or events.

Many Tenzir pipelines use the [`from`](../tql2/operators/from.md) and
[`to`](../tql2/operators/to.md) operators to get data in and out, respectively.
For example, to load data from a local JSON file system, filter events where a
certain field matches a predicate, and store the result to an S3 bucket in
Parquet format, you can write the following pipeline:

```tql
from "/path/to/file.json"
where src_ip in 10.0.0.0/8
to "s3://bucket/dir/file.parquet"
```

This pipelines consists of three operators:

![Operator Composition Example 1](operator-composition-example-1.svg)

The operator [`from`](../tql2/operators/from.md) is a void-to-events
input operator, [`where`](../tql2/operators/where.md) an events-to-events
transformation operator, and [`to`](../tql2/operators/to.md) an events-to-void
output operator.

Other inputs provide bytes first and you need to interpret them in order to
transform them as events.

```tql
load_kafka "topic"
read_ndjson
select host, message
write_yaml
save_zmq "tcp://1.2.3.4"
```

![Operator Composition Example 2](operator-composition-example-2.svg)

With these building blocks in place, you can create all kinds of pipelines, as
long as they follow the two principal rules of (1) sequencing inputs,
transformations, and outputs, and (2) ensuring that operator upstream/downstream
types match. Here is an example of other valid pipeline instances:

![Operator Composition Examples](operator-composition-variations.svg)

:::note Auto-Completing Input and Output Operators
If a pipeline is open, Tenzir attempts to auto-complete an input and output
operators and rejects the pipeline otherwise. Auto-completion is context
dependent:

- **CLI**: On the command line we read JSON from stdin and write it to stdout.
  This means you could only write transformations on the command line, but we
  recommand making the parsing explicit.
- **App**: In the app we only auto-complete a missing output via the
  [`serve`](tql2/operators/serve.md) operator to display the result in the
  browser.
:::

## Multi-Schema Dataflows

Every event that flows through a pipeline is part of a *data frame* with a
schema. You expresses transformations on events by thinking
one-event-at-at-time, whereas the implementation internally works on data frames
(internally represented as Apache Arrow record batch), potentially of tens of
thousands of events at once. This batching is the reason why the pipelines
can achieve a high throughput.

Unique about Tenzir is that a single pipeline can run with *multiple schemas*,
even though the events are data frames internally. Tenzir parsers
(bytes-to-event operators) are capable of emitting events with changing schemas.
This behavior is different from other engines that work with data frames where
operators can only execute on a single schema. In this light, Tenzir combines
the performance of structured query engines with the flexibility of
document-oriented engines.

If an operator detects a schema changes, it creates a new batch of events. In
terms of performance, the worst case for Tenzir is a ordered stream of
schema-switching events, with every event having a new schema than the previous
one. But even for those scenarios operators can efficiently build homogeneous
batches when the inter-event order does not matter. Similar to predicate
pushdown, Tenzir operators support "ordering pushdown" to signal to upstream
operators that the event order only matters intra-schema but not inter-schema.
In this case the operator transparently "demultiplex" a heterogeneous event
stream into N homogeneous streams. The [`import`](tql2/operators/import.md)
operator is an example of such an operator; it pushes its ordering upstream,
allowing parsers to efficiently create multiple streams events.

![Multi-schema Example](multi-schema-example.svg)

Some operators only work with exactly one instance per schema internally, such
as [`write_csv`](tql2/operators/write_parquet.md), which first writes a header
and then all subsequent rows have to adhere to the emitted schema. Such
operators cannot handle events with changing schemas.

It's important to mention that most of the time you don't have to worry about
schemas. They are there for you when you want to work with them, but it's often
enough to just specified the fields that you want to work with, e.g., `where
id.orig_h in 10.0.0.0/8`, or `select src_ip, dest_ip, proto`. Schemas are
inferred automatically in parsers, but you can also seed a parser with a schema
that you define explicitly.

## Unified Live Stream Processing and Historical Queries

Engines for event stream processing and batch processing of historical data have
vastly different requirements. We believe that we found a sweetspot with our
language and accompanying execution engine that makes working with both types of
workloads incredibly easy: just pick a input operator at the beginning the a
pipeline that points to your data source, be it infinitely streaming or stored
dataset. Tenzir will figure out the rest.

![Unified Processing](unified-processing.svg)

Our desired user experience for interacting with historical data looks like
this:

1. **Ingest**: to store data at a node, create a pipeline that ends with
   [`import`](tql2/operators/import.md).
2. **Query**: to run a historical query over data at the node, create a pipeline
   that begins with [`export`](tql2/operators/export.md).

For example, to ingest JSON from a Kafka, you write `from "kafka://topic |
import`. To query the stored data, you write `export | where file == 42`.

The example with `export` suggests that the pipeline *first* exports everything,
and only *then* starts filtering with `where`, performing a full scan over the
stored data. But this is not what's happening. Pipelines support **predicate
pushdown** for every operator. This means that `export` receives the filter
expression before it starts executing, enabling index lookups or other
optimizations to efficiently execute queries with high selectivity where scans
would be sub-optimal.

The key insight here is to realize that optimizations like predicate pushdown
extend to the storage engine and do not only apply to the streaming executor.

The Tenzir native storage engine is not a full-fledged database, but rather a
catalog a thin indexing layer over a set of Parquet/Feather files. The sparse
indexes (sketch data structures, such as min-max synopses, Bloom filters, etc.)
avoid full scans for every query. The catalog tracks evolving schemas, performs
expression binding, and provides a transactional interface to add and replace
partitions during compaction.

The diagram below shows the main components of the storage engine:

![Database Architecture](storage-engine-architecture.svg)

Because of this transparent optimization, you can just exchange the input
operator of a pipeline and switch between historical and streaming execution
and everything works as expected. A typical use case begins some exploratory
data analysis involving a few `export` pipelines, but then would deploy the
pipeline on streaming data by exchanging the input with a Kafka stream.

## Built-in Networking to Create Data Fabrics

Tenzir pipelines have built-in network communication, allowing you to create a
distributed fabric of dataflows to express intricate use cases that go beyond
single-machine processing. There are two types of network connections:
*implicit* and *explicit* ones:

![Implicit vs. Explicit](implicit-vs-explicit-networking.svg)

An implicit network connection exists, for example, when you use the `tenzir`
binary on the command line to run a pipeline that ends in
[`import`](tql2/operators/import.md):

```tql
from "/file/eve.json"
where tag != "foo"
import
```
Or one that begins with [`export`](tql2/operators/export.md):

```tql
export
where src_ip in 10.0.0.0/8
to "/tmp/result.json"
```

Tenzir pipelines are eschewing networking to minimize latency and maximize
throughput, which results in the following operator placement for the above examples:

![Implicit Networking](implicit-networking.svg)

The executor generally transfers ownership of operators between
processes as late as possible to prefer local, high-bandwidth communication. For
maximum control over placement of computation, you can override the automatic
operator location with the [`local`](tql2/operators/local.md) and
[`remote`](tql2/operators/remote.md) operators.

The above examples are implicit network connections because they're not visible
in the pipeline definition. An explicit network connection terminates a pipeline
as with an input or output operator:

![Pipeline Fabric](pipeline-fabric.excalidraw.svg)

This fictive data fabric above consists of a heterogeneous set of technologies,
interconnected by pipelines. Because you have full control over the location
where you run the pipeline, you can push it all the way to the "last mile." This
helps especially when there are compliance and data residency concerns that must
be properly addressed.
