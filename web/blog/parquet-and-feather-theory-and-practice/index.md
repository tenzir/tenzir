---
title: "Parquet & Feather: Theory and Practice"
authors:
  - dispanser
  - mavam
date: 2022-10-24
image: image.jpg
tags: [arrow, parquet, feather]
---

[Apache Arrow](https://arrow.apache.org/) and Apache Parquet have become the
de-facto columnar formats for in-memory and on-disk representations when it
comes to structured data. Together, they provide data interoperability and
pollinate a diverse ecosystem of data tools. From an engineering perspective,
how well do they work together? This blog post reports on our experiences of
bridging the memory and storage gap for data using a complex type system.

<!--truncate-->

:::info Parquet & Feather: 3/3
This blog post is the last part of a 3-piece series on Parquet and Feather.

1. [Enabling Open Investigations][parquet-and-feather-1]
2. [Writing Security Telemetry][parquet-and-feather-2]
3. TBD (this blog post)

[parquet-and-feather-1]: /blog/parquet-and-feather-enabling-open-investigations/
[parquet-and-feather-2]: /blog/parquet-and-feather-writing-security-telemetry/
:::

In the previous blog post, we did a quantitative comparison of Parquet and
Feather on the write path. In this post, we look at the developer experience.

While our Feather implementation proved to be straight-forward, the Parquet
store implementation turned out to be more difficult. Recall that VAST has its
own [type system](https://vast.io/docs/understand-vast/data-model/type-system)
relying on [Arrow extension
types](https://arrow.apache.org/docs/format/Columnar.html#extension-types) to
express domain-specific concepts like IP addresses, subnets, or enumerations. We
hit a few places where the Arrow C++ implementation does not support all VAST
types directly. It's trickier than we thought, as we shall see next.

## Row Groups

In Apache Parquet, a [row group](https://parquet.apache.org/docs/concepts/) is a
subset of a Parquet file that's itself written in a columnar fashion. Smaller
row groups allow for higher granularity in reading parts of an individual file,
at the expense of a potentially increased file size due to less optimal
encoding. In VAST, we strive to send around [batches of
data](https://vast.io/docs/setup-vast/tune) that are considerably smaller than
what a recommended Parquet file size would look like. A typical parquet file
size recommendation is 1GB - which translates into 5-10GB of data in-memory when
reading the entire file. Our system recommends a table slice size of 64k events,
which translates into a few MBs. We planned to leverage individual row groups to
partition a Parquet file into smaller units, aligned with what our configured
table slices size.

However, attempting to read a Parquet file that was split into multiple row
groups doesn't work for some of our schemas, yielding:

```
NotImplemented: Nested data conversions not implemented for chunked array outputs
```

This appears to be related to
[ARROW-5030](https://issues.apache.org/jira/browse/ARROW-5030). Our current
workaround is to write a single row group, and split up the resulting Arrow
record batches into the desired size after reading. However, this increases
latency to first result, an important metric for some interactive use cases we
envision for VAST.

## Arrow → Parquet → Arrow Roundtrip Schema Mismatch

Parquet is a separate project which precedes Arrow, and has its own data types,
which don't exactly align with what Arrow provides. While it's possible to
instruct Arrow to also serialize [its own
schema](https://arrow.apache.org/docs/cpp/api/formats.html#_CPPv4N7parquet21ArrowWriterProperties7BuilderE)
into the Parquet file metadata, this doesn't seem to play well in concert with
extension types. As a result, a record batch written to and then read from a
Parquet file no longer adheres to the same schema! In particular, we found the
following things that don't work correctly:

### VAST Enumerations

VAST enumerations are represented as extension types wrapping an Arrow
dictionary of strings backed by unsigned 8-bit integers. On read, these 8-bit
index values become 32-bit values, which is not compatible with our extension
type definition, so the extension type wrapping is lost.

![Arrow Schema Conversion #width500](arrow-schema-conversion.light.png#gh-light-mode-only)
![Arrow Schema Conversion #width500](arrow-schema-conversion.dark.png#gh-dark-mode-only)

### Extension Types inside Maps

Both our address type and subnet type extensions are lost if they occur in
various nested structures. For example, a map from a VAST address to a VAST
enumeration of Arrow type:

```
map<extension<vast.address>,  extension<vast.enumeration>>
```

is not preserved. After reading it from a Parquet file, the resulting Arrow type
is:

```
map<fixed_size_binary[16], string>.
```

The key, an address type, has been replaced by its physical representation,
which is 16 bytes (allowing room for an IPv6 address). Interestingly, the
enumeration is replaced by a string instead of a dictionary as observed in the
previous paragraph. So the same type behaves differently depending on where in
the schema it occurs.

We created an issue in the Apache JIRA to track this:
[ARROW-17839](https://issues.apache.org/jira/browse/ARROW-17839).

To fix these 3 issues, we're post-processing the data after reading it from
Parquet. The workaround is a multi-step process:

1. Side-load the arrow schema from the Parquet metadata. This yields the actual
   schema, because it's in no way related to Parquet other than using its
   metadata capabilities to store it.

1. Load the actual Arrow table. This table has its own schema, which is not the
   same schema as the one derived from the Parquet metadata directly.

1. Finally, recursively walk the two schema trees with the associated data
   columns, and whenever there's a mismatch between the two, fix the data arrays
   by casting or transforming it, yielding a table that is aligned with the
   expected schema.

   - In the first case (dictionary vs vast.enumeration) we cast the int32 Arrow
     array of values into a uint8 Arrow array, and manually create the wrapping
     extension type and extension array. This is relatively cheap, as casting is
     cheap and the wrapping is done at the array level, not the value level.

   - In the second case (physical binary[16] instead of vast.address) we just
     wrap it in the appropriate extension type. Again, this is a cheap operation.

   - The most expensive fix-up we perform is when the underlying type has been
     changed from an enumeration to a string: we have to create the entire array
     from scratch after building a lookup table that translates the string values
     into their corresponding numerical representation.

## Apache Spark Support

So now VAST writes its data into a standardized, open format—we integrate
seamlessly with the entire big data ecosystem, for free, right? I can read my
VAST database with Apache Spark and analyze security telemetry data on a
200-node cluster? Nope. It’s not that standardized. Yet. Or rather, not every
tool or library supports every data type. In fact, as discussed above, writing a
Parquet file and reading it back *even with the same tool* doesn’t always
produce the data you started with.

We attempting to load a Parquet file with a single row, and a single field of
type VAST's `count` (64-bit unsigned integer) into Apache Spark v3.2, we are
greeted with:

```
org.apache.spark.sql.AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))
  at org.apache.spark.sql.errors.QueryCompilationErrors$.illegalParquetTypeError(QueryCompilationErrors.scala:1284)
```

Apache Spark v3.2 refuses to read the `import_time` field (a metadata column
added by VAST itself). It turns out that Spark v3.2 has a regression. Let’s try
with version v3.1 instead, which shouldn’t have this problem:

```
org.apache.spark.sql.AnalysisException: Parquet type not supported: INT64 (UINT_64)
```

We got past the timestamp issue, but it still doesn’t work: Spark only supports
signed integer types, and refuses to load our Parquet file with an unsigned 64
bit integer value. The related Spark JIRA Issue is marked as resolved, but
unfortunately the resolution is “a better error message”. However, this stack
overflow post has the solution: if we define an explicit schema, Spark happily
converts our column into a signed type.

```scala
val schema = StructType(
  Array(
    StructField("event",
      StructType(
        Array(
          StructField("c", LongType))))))
```

Finally, it works!

```
scala> spark.read.schema(schema).parquet(<file>).show()
+-----+
|event|
+-----+
| {13}|
+-----+
```

We were able to read VAST data in Spark, but it’s not an easy and "out of the
box" experience we were hoping for. It turns out that different tools don’t
always support all the data types, and additional effort is required to
integrate with the big players in the Parquet ecosystem.

## Conclusion

We love Apache Arrow—it's a cornerstone of our system, and we'd be in much
worse shape without it. We use it everywhere from the storage layer (using
Feather and Parquet) to the data plane (where we are passing around Arrow record
batches).

However, as VAST uses a few less common Arrow features we sometimes stumble over
some of the rougher edges. We're looking forward to fixing some of these things
upstream, but sometimes you just need a quick solution to help our users..

But the real reason why we wrote this blog post is to show how quickly the work
involved in data engineering can escalate. This is the long tail that nobody
wants to talk about when telling you to build your own security data lake. And
it quickly adds up! It’s also heavy-duty data wrangling, and not ideally
something you want your security team working on when they would be more useful
hunting threats. Even more reasons to use a purpose-built security data
technology like VAST.
