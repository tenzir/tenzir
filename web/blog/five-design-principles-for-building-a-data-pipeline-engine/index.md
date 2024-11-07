---
title: Five Design Principles for Building a Data Pipeline Engine
authors: [mavam]
date: 2023-10-17
last_updated: 2023-12-12
tags: [pipelines, design]
comments: true
---

One thing we are observing is that organizations are actively seeking out
solutions to better manage their security data operations. Until recently, they
have been aggressively repurposing common data and observability tools. I
believe that this is a stop-gap measure because there was no alternative. But
now there is a growing ecosystem of security data operations tools to support
the modern security data stack. Ross Haleliuk's [epic
article](https://ventureinsecurity.net/p/security-is-about-data-how-different)
lays this out at length.

In this article I am explaining the underlying design principles for developing
our own data pipeline engine, coming from the perspective of security teams that
are building out their detection and response architecture. These principles
emerged during design and implementation. Many times, we asked ourselves "what's
the right way of solving this problem?" We often went back to the drawing board
and started challenging existing approaches, such as what a data source is, or
what a connector should do. To our surprise, we found a coherent way to answer
these questions without having to make compromises. When things feel Just Right,
it is a good sign to have found the right solution for a particular problem.
What we are describing here are the lessons learned from studying other systems,
distilled as principles to follow for others.

![Five Design Principles](five-design-principles.excalidraw.svg)

<!--truncate-->

This article comes from the data engineering perspective. What makes a data
pipeline "security" is left for a future post. Technical SOC architects, CTOs,
or Principal Engineers our audience. Enjoy!

## A Value-First Approach to Data Pipelines

One way to think of it is that data pipelines offer **data as a service**. The
pipeline provides the data in the right shape, at the right time, at the right
place. As a data consumer, I do not want to bother with how it got there. I just
want to use it to solve my actual problem. That's the value.

But data has to jump through many hoops to get to this goldilocks state. The
*pyramid of data value* is an attempt to describe these hoops on a discrete
spectrum:

![Pyramid of Data Value](pyramid-of-data-value.excalidraw.svg)

The idea we want to convey is that you begin with a massive amount of
unstructured data, which makes up for the bulk of data in many organizations.
Once you lift it into a more digestible form by structuring (= parsing) it, you
invest cycles and time to make it easier to extract value from it. A byproduct
is often that you end up with less data afterwards. You can continue the process
to reshape the data to the exact form to solve a given problem, which often
accounts for just a tiny fraction of the original dataset. Data is often still
structured then, but you invest a lot of time in deduplicating, aggregating,
enriching, and more, just to get the needed information for the consumer. This
data massaging can take a lot of human effort!

It's incredibly costly for security teams to spend 80% of their time massaging
the data. They should spend their focus on executing their mission, which is
hunting threats to protect their constituency. It's certainly not building tools
to wrangle data.

We argue that making it easier for domain experts to work with data
fundamentally requires a solution that can seamlessly cross all layers of this
pyramid, as the boundaries are the places where efficiency leaks. The following
principles allowed us to get there.

## P1: Separation of Concerns

A central realization for us was that different types of data need different
abstractions that coherently fit together. When following the data journey
through the pyramid, we start with unstructured data. This means working with
raw bytes. You may get them over HTTP, Kafka, a file, or any other carrier
medium. The next step is translating bytes into structured data. Parsing gets
the data onto the "reshaping highway" where it can be manipulated at ease in a
rich data model.

In our nomenclature, [connectors](/connectors) are responsible for loading and
saving raw, unstructured bytes; [formats](/formats) translate unstructured bytes
into structured bytes; a rich set of computational [operators](/operators)
enable transformations on structured data.

The diagram below shows these key abstractions:

![Connectors, Formats, Operators](connectors-formats-operators.excalidraw.svg)

One thing we miss looking at existing systems is a *symmetry* in these
abstractions. Data acquisition is often looked just one-way: getting data in and
then calling it a day. No, please don't stop here! A structured data
representation is easy to work with, but throwing the output as JSON over the
fence is not a one-size-fits-all solution. I may need CSV or YAML. I may want a
Parquet file. I may need it in a special binary form. Deciding in what format to
consume that data is critical for flexibility and performance. This is why we
designed our connectors and formats to be symmetric: a **loader** takes bytes
in, and a **saver** sends bytes out. A **parser** translates bytes to events,
and a **printer** translates them back.

Here is an example that makes symmetric use of connectors, formats, and
operators:

```
load gcs bucket/in/my/cloud/logs.zstd
| decompress zstd
| read json
| where #schema == "ocsf.network_activity"
| select connection_info.protocol_name,
         src_endpoint.ip,
         src_endpoint.port,
         dst_endpoint.ip,
         dst_endpoint.port
| to s3 bucket/in/my/other/cloud write parquet
```

This pipeline starts by taking compressed, unstructured data, then makes the
data structured by parsing it as JSON, selects the connection 5-tuple, and
finally writes it as a Parquet file into an S3 bucket.

:::info Pipeline Languages Everywhere
New pipeline languages are mushrooming all over the place. Splunk started with
its own Search Processing Language (SPL) and now released SPL2 to make it
more pipeline-ish. And Elastic also doubled down as well with [their new
ES|QL](/blog/a-first-look-at-esql). When we designed the [Tenzir Query Language
(TQL)](/language), we drew a lot of inspiration from
[PRQL](https://prql-lang.org/), [Nu](https://www.nushell.sh/), and
[Zed](https://zed.brimdata.io/). These may sound esoteric to some, but they are
remarkably well designed evolutions of pipeline languages that are *not* SQL.
Don't get us wrong, we love SQL when we have our data engineering hat on, but
our users shouldn't have to wear that hat. Splunk gets a lot of flak for its
pricing, but one thing we admire is how well it caters to a broad audience of
users that are not data engineering wizards. Our slightly longer [FAQ
article](/faqs) elaborates on this topic.
:::

## P2: Typed Operators

In Tenzir [pipelines](/pipelines), we call the atomic building blocks
**operators**. They are sometimes also called "steps" or "commands". Data flows
between them:

![Pipeline Chaining](pipeline-chaining.excalidraw.svg)

Many systems, including Tenzir, distinguish three types of operators:
**sources** that produce data, **sinks** that consume data, and
**transformations** that do both.

![Pipeline Structure](pipeline-structure.excalidraw.svg)

Most pipeline engines support one type of data that flows between the operators.
If they exchange raw bytes, they'd be in the "unstructured" layer of the
pyramid. If they exchange JSON or data frames, they'd be in the "structured"
layer.

But in order to cut through the pyramid above, Tenzir pipelines make one more
distinction: the elements that operators push through the pipeline are
*typed*. Every operator has an input and an output type:

![Input and Output Types](operator-pieces.excalidraw.svg)

When composing pipelines, these types have to match. Otherwise the pipeline is
malformed. Let's take the above pipeline example and zoom out to just the
typing:

![Visualization](load-decompress-select-to.excalidraw.svg)

With the concept of input and output types in mind, the operator type become
more apparent:

![Operator Types](operator-types.excalidraw.svg)

This is quite powerful, because you can also *undulate* between bytes and events
within a pipeline before it ends in void. Consider this example:

```
load nic eth0
| read pcap
| decapsulate
| where src_ip !in [0.0.0.0, ::]
| write pcap
| zeek
| write parquet
| save file /tmp/zeek-logs.parquet
```

This pipeline starts with PCAPs, transforms the acquired packets to events,
[decapsulates](/next/operators/decapsulate) them to filter on some packet
headers, goes back to PCAP, runs Zeek[^1] on the filtered trace, and then writes
the log as Parquet file to disk.

[^1]: The `zeek` operator is [user-defined
    operator](/next/language/user-defined-operators) for `shell "zeek -r - …" |
    read zeek-tsv`. We wrote a [blog post on how you can use `shell` as escape
    hatch to integrate arbitrary
    tools](/blog/shell-yeah-supercharging-zeek-and-suricata-with-tenzir) in a
    pipeline.

Visually, this pipeline has the following operator typing:

![Undulating Pipeline](undulating-pipeline.excalidraw.svg)

## P3: Multi-Schema Dataflows

To further unlock value within the structured data layer of the pyramid, we made
our pipelines **multi-schema**: a single pipeline can process heterogeneous
types of events, each of which have their own schemas. Multi-schema dataflows
require automatic schema inference at parse time, which all our parsers support.

This behavior is different from engines that work with structured data where
operators typically work with fixed set of tables. While schema-less systems,
such as document-oriented databases, offer more simplicity, their
one-record-at-a-time processing comes at the cost of performance. In the
spectrum of performance and ease of use, Tenzir therefore fills a
gap:

![Structured vs.
Document-Oriented](structured-vs-document-oriented.excalidraw.svg)

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
support "ordering pushdown" to signal to upstream operators that the event order
only matters intra-schema but not inter-schema. In this case we transparently
demultiplex a heterogeneous stream into *N* homogeneous streams, each of which
yields batches of up to 65k events. The [`import`](/next/operators/import)
operator is an example of such an operator, and it pushes its ordering upstream
so that we can efficiently parse, say, a diverse stream of NDJSON records, such
as Suricata's EVE JSON or Zeek's streaming JSON.

You could call multi-schema dataflows *multiplexed* and there exist dedicated
operators to demultiplex a stream. As of now, this is hard-coded per operator.
For example, [`to directory /tmp/dir write parquet`](/connectors/directory)
demultiplexes a stream of events so that batches with the same schema go to the
same Parquet file.

The diagram below illustrates the multi-schema aspect of dataflows for schemas
A, B, and C:

![Multi-schema Example](multi-schema-example.excalidraw.svg)

Some operators only work with exactly one instance per schema internally, such
as [`write`](/next/operators/write) when combined with the
[`parquet`](/formats/parquet), [`feather`](/formats/feather), or
[`csv`](/formats/csv) formats. These formats cannot handle multiple input
schemas at once. A demultiplexing operator like `to directory .. write <format>`
removes this limitation by writing one file per schema instead.

We are having ideas to make this schema (de)multiplexing explicit with a
`per-schema` [operator modifier](/next/language/operator-modifiers) that you can
write in front of every operator. Similarly, we are going to add union types in
the future, making it possible to convert a heterogeneous stream of structured
data into a homogeneous one.

It's important to note that most of the time you don't have to worry about
schemas. They are there for you when you want to work with them, but it's often
enough to just specified the fields that you want to work with, e.g., `where
id.orig_h in 10.0.0.0/8`, or `select src_ip, dest_ip, proto`. Schemas are
inferred automatically in parsers, but you can also seed a parser with a schema
that you define explicitly.

## P4: Unified Live Stream Processing and Historical Queries

Systems for stream processing and running historical queries have different
requirements, and combining them into a single system can be a daunting
challenge. But there is an architectural sweetspot at the right level of
abstraction where you can elegantly combine them. From a user persepctive, our
goal was to seamlessly exchange the beginning of a pipeline to select the source
of the data, be it a historical or continuous one:

![Unified Processing](unified-processing.excalidraw.svg)

Our desired user experience for interacting with historical looks like this:

1. **Ingest**: to persist data at a node, create a pipeline that ends with the
   [`import`](/next/operators/import) sink.
2. **Query**: to run a historical query, create a pipeline that begins with the
   [`export`](/next/operators/export) operator.

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

:::info Delta, Iceberg, and Hudi?
We kept the catalog purposefully simple to iterate fast and gain experience in a
controlled system, rather than starting Lakehouse-grade with [Delta
Lake](https://delta.io/), [Iceberg](https://iceberg.apache.org/), or
[Hudi](https://hudi.apache.org/). We are looking forward to having the resources
to integrate with the existing lake management tooling.
:::

## P5: Built-in Networking to Create Data Fabrics

:::info Control Plane vs. Data Plane
The term *data fabric* is woven into many meanings. From a Tenzir perspective,
the set of interconnected pipelines through which data flows constitutes the
**data plane**, whereas the surrounding management platform at
[app.tenzir.com](https://app.tenzir.com) to control the nodes constitute the
**control plane**. When we refer to "data fabric" we mean to the data plane
aspect.
:::

Tenzir pipelines have built-in network communication, allowing you to create a
distributed fabric of dataflows to express intricate use cases. There are two
types of network connections: *implicit* and *explicit* ones:

![Implicit vs. Explicit](implicit-vs-explicit-networking.excalidraw.svg)

An implicit network connection exists, for example, when you use the `tenzir`
binary on the command line to run a pipeline that ends in
[`import`](/next/operators/import):

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
modifiers](/next/language/operator-modifiers).

The above examples are implicit network connections because they're not visible
in the pipeline definition. An explicit network connection terminates a pipeline
as source or sink:

![Pipeline Fabric](pipeline-fabric.excalidraw.svg)

This fictive data fabric above consists of a heterogeneous set of technologies,
interconnected by pipelines. You can also turn any pipeline into an API using
the [`serve`](/next/operators/serve) sink, effectively creating a dataflow
microservice that you can access with a HTTP client from the other side:

![Serve Operator](serve.excalidraw.svg)

Because you have full control over the location where you run the pipeline, you
can push it all the way to the "last mile." This helps especially when there
are compliance and data residency concerns that must be properly addressed.

## Summary

We've presented for design principles that we found to be key enabler to extract
value out of data pipelines:

1. Separating the different data processing concerns, it is possible to
   achieve high modularity and composability. Tenzir therefore has connectors,
   formats, and operators as central processing building blocks.
2. Typed operators make it possible to process multiple types of data in the
   same engine, avoiding the need to switch tools just because the pipeline
   engine has a narrow focus.
3. Multi-schema dataflows give us the best of structured and document-oriented
   engines. Coupled with schema inference, this creates a user experience where
   schemas are optional, but still can be applied when strict typing is needed.
4. Unifying live and historical data processing is the holy grail to covering a
   wide variety of workloads. Our engine offers a new way to combine the two
   with an intuitive language.
5. Built-in networking makes it possible to create data fabrics at ease.
   Spanning pipelines across multiple nodes, either implicitly or explicitly
   (via ZeroMQ, Kafka, AMQP, etc.), provides a powerful mechanism to meet the
   most intricate architectural requirements.

Tenzir pipelines embody all of these principles. Try it yourself with our free
Community Edition at [app.tenzir.com](https://app.tenzir.com).
