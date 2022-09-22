# Composable Pipelines

- **Status**: Accepted
- **Created**: Aug 19, 2022
- **Accepted**: Sep 21, 2022
- **Authors**:
  - [Matthias Vallentin](https://github.com/mavam)
- **Contributors**:
  - [Anthony Verez](https://github.com/netantho)
  - [Dominik Lohmann](https://github.com/dominiklohmann)
  - [Rémi Dettai](https://github.com/rdettai)
  - [Thomas Peiselt](https://github.com/dispanser)
  - [Tobias Mayer](https://github.com/tobim)
- **Discussion**: [PR #2511](https://github.com/tenzir/vast/pull/2511)

## Overview

This proposal ideates a new UX around VAST's pipelines. The fundamental idea is
that *everything is a pipeline* and build a composable UX around this concept.

## Problem Statement

Pipelines offer flexible data reshaping at [predefined customization
points](https://vast.io/docs/use-vast/transform). For example, they can be
triggered for specific events on import/export, or invoked by compaction.
However, it is impossible to run pipelines in an ad-hoc manner, for example as
a post-processing step after a query or for an ad-hoc ingest of a data set.

This RFC proposes dynamic pipelines that do not require static definition in
a configuration file, but rather allow users to specify them as an extension to
the expression language.

## Solution Proposal

Assume for a moment everything is a pipeline operator. Then we may write:

 ```bash
vast 'load s3://aws json |
      summarize count(dst) group-by src |
      store parquet /path/to/directory'
```

The UX would translate seamlessly to other languages, e.g., Python:

```python
import vast as v
await v.load_json("s3://aws")
       .group_by("src") # could also be merged with summarize()
       .summarize(v.count("dst"))
       .store_parquet("/path/to/directory")
```

The core of this proposal introduces pipeline operators as functions with input
and output types. The following overview introduces the pipeline building
blocks used throughout this proposal.

A pipeline operator has an input and output type, e.g., `load<In, Out>` takes as
input `In` and produces instances of `Out`. We consider the following valid
types at the logical level:

1. `Void`: no input or output, to represent a side effect
2. `Arrow`: table slices (i.e., record batches)

The majority of computation happens in `Arrow`, as this is the representation of
structured data that the user primarily wants to interact with. The other types
help in building a composable system to onboard and offboard data from the Arrow
data plane.

The central pipeline invariant is that composed operators must have a valid
combination of types. For example, this is a valid pipeline:

```
load<Void, Arrow> |
where<Arrow, Arrow> |
summarize<Arrow, Arrow>
```

The dataflow edges (`|`) in the middle produce valid type pairs:

- `<*, Void>`
- `<Arrow, Arrow>`
- `<Arrow, Arrow>`
- `<Arrow, *>`

The `*` character signals the open end of the pipeline. By collapsing the types
in the middle, the type pipeline of the pipeline becomes `<Void, Arrow>`. Since
`Void` does not require input to run, this pipeline could be "kicked off".
However, there is no consumer at the other end, as the pipeline ends in `Arrow`.
Adding another operator would make it complete, e.g.:

```
load<Void, Arrow> |
where<Arrow, Arrow> |
summarize<Arrow, Arrow> |
store<Arrow, Void>
```

This results in a `<Void, Void>` pipeline that does not "leak".

### Operator Overview

With this typing concept in mind, we can now take a closer look at the various
operators we propose.

#### I/O

The following operators exist for performing side effects that load and store
data.

| Operator | Input Type | Output Type | Description
| -------- | ---------- | ----------- | ------------------------------------------
| `load`   | `Void`     | `Arrow`     | Read data from a provided location
| `store`  | `Arrow`    | `Void`      | Write data into a provided location

These operators take two arguments:

1. **Carrier**: an URL like `s3://aws` or `-` for stdin
2. **Format**: `json`, `csv`, `feather`, `parquet`, etc.

#### Compute

The following operators have a fixed input and output type of `Arrow`. We can
group them into *filter* operators that do not change the schema, and *reshape*
operators where execution yields a new schema.

##### Filter

Filter operators have input and output type `Arrow`.

| Operator | Non-Blocking | Description
| -------- |:------------:| ------------------------------------------
| `head`   | ✅           | Retrieve first N records
| `tail`   | ❌           | Retrieve last N records
| `where`  | ✅           | Filter with expression
| `sort`   | ❌           | Sort according to field
| `uniq`   | ✅           | Produce deduplicated output

##### Reshape

Reshape operators have input and output type `Arrow`.

| Operator    | Non-Blocking | Description
| ----------- |:------------:| ------------------------------------------
| `put`       | ✅           | Select a set of columns (projection)
| `with`      | ✅           | Adds new fields with derived values
| `drop`      | ✅           | Remove a set of columns (projection)
| `rename`    | ✅           | Renames schema meta data (type & fields)
| `summarize` | ❌/✅[^1]    | Aggregate group and compute a summary function

[^1]: Depending on the aggregate function.

##### Matcher Plugin

| Operator | Input Type | Output Type | Description
| -------- | ---------- | ----------- | ------------------------------------------
| `build`  | `Arrow`    | `Matcher`   | Creates a matcher from a data stream
| `match`  | `Arrow`    | `Arrow`     | Matches input based on its state

#### Commands

The following table illustrates how the current CLI would change after this
proposal:

| Old Syntax                               | New Syntax
| ---------------------------------------- | -------------------------------------------
| `vast infer`                             | `vast exec 'load CARRIER [FORMAT] | infer"`
| `vast import FORMAT EXPR`                | `vast import FORMAT 'EXPR | op1 .. | op2 ..'`
| `vast export FORMAT EXPR`                | `vast export FORMAT 'EXPR | op1 .. | op2 ..'`
| `vast count [args] EXPR`                 | `vast export FORMAT 'EXPR | count [args]'`
| `vast pivot --format=FORMAT [args] EXPR` | `vast export FORMAT 'EXPR | pivot [args]'`
| `vast explore [args] EXPR`               | `vast export 'EXPR | explore [-A] [...]'`

In essence, we keep `import` and `export` and incrementally enhance the
expressiveness, by making it possible to append a pipeline after an expression.
That is, valid input has the form `EXPR | OPERATOR | OPERATOR | ...`. Note that
this is *not* a valid pipeline, because `EXPR` is not an operator. The output
type of a pipeline passed to `export` must have type `Arrow`, because query
execution today ends with a sink actor that receive `Arrow`. Likewise, a
pipeline for `import` must end with `Arrow`, because a source actor sends data
out via `Arrow`.

Another to look at this is that `export FORMAT EXPR | op1 | op2` creates two
pipelines:

1. `where EXPR | op1 | op2`
2. `store - FORMAT`

Pipeline (1) runs at the server and pipeline (2) at the client. (The dual holds
for `import`.)

Additionally, we add a new command `exec` that accepts a "pure" pipeline, i.e.,
just a composition of operators. Execution takes place locally, without any
implicit remote communication. For the remainder of the discussion, we focus
on `exec` only. All pipelines executed by `exec` must have input and output type
`Void`, because they represent a complete dataflow instance that must not leak.

### Local Execution

Given the new `exec` command, we can start processing data in situ. For example,
we can directly compute insights over telemetry:

```bash
vast exec 'load - zeek |
           summarize sum(orig_bytes), sum(resp_bytes) group-by id.orig_h, id.resp_h |
           sort |
           head -n 10 |
           store - json'
```

This pipeline answers the question "who are the top talkers in my network" by
summing originator and responder bytes per host pair. The dataflow happens as
follows:

1. Load Zeek data via stdin (`load`)
2. Aggregate data (`summarize`)
3. Sort the aggregate (`sort`)
4. Only consider the top 10 (`head`)
5. Write data as JSON to stdout (`store`)

Local pipeline execution opens the door for a lot of new modes of operations,
because a VAST server node is no longer required.

**This makes VAST a swiss army knife for security data.**

### Case Study: Matcher

The `matcher` plugin is an analyzer plugin that operates on the input stream by
matching an opaque state object against. We call this opaque state object the
"matcher."

#### Local execution

Let's assume that the following new pipeline operators:

- `build<Arrow, Matcher>`: consumes a data stream to produce a matcher
- `match<Arrow, Arrow>`: produces sightings for the active matchers

Now consider the use case of constructing a matcher from CSV data:

```bash
vast exec 'load file:///tmp/blacklist csv |
           build --extract=net.src.ip' > matcher.flatbuf
```

After having constructed a matcher, we can now use it in a pipeline for
matching:

```bash
vast exec 'load kafka -t /zeek/conn zeek |
           match --state matcher.flatbuf --on id.orig_h,id.resp_h
```

The `match` operator exposes the structured data to a specific matcher instance,
reshaping according to the positional arguments that are a list of extractors.
The pipeline writer is responsible for reshaping the data so that the matcher
can make sense of it.

A successful "match" wraps the corresponding event into a sighting envelope
record.

## Implementation

The following aspects require attention during the implementation:

- **Asynchronous Execution**: pipelines fundamentally support asynchronous
  execution by sequentializing dataflow within an operator, and parallelizing it
  across operators. Conceptually, this is very similar to the actor model
  architecture VAST exhibits, with operators mapping to actors.

  In the implementation, we may want to consider actors part of the solution,
  but will at first propose an independent architecture that consists of logical
  and physical pipeline representation.

- **Strong Typing**: pipelines are strongly typed with respect to their input
  and output types. This enables static type checking when instantiating a
  pipeline, e.g., an output type of `Arrow` with a downstream operator that has
  `Bytes` as input type describes an invalid pipeline.

  At the data plane, within Arrow, a "type" consists of a specific schema of a
  record batch. We should consider a mechanism to perform cross-operator type
  checking to simplify the implementation of individual operators.

- **Push vs. Pull**: implementation of a dataflow system can follow push-based
  execution where a passive pipeline only does work when some component feeds it
  with data, which then triggers a cascade of executions from the opt. The
  alternative is pull-based, where consumers request data at the sink. This
  request triggers request for additional results upstream, interleaved with
  computation to produce them. [Combining both models][shaikhha18] has shown
  promising results.

  Ideally we can find an API that allows for both execution modes so that we can
  optimize execution incrementally, without locking ourselves into one corner.

[shaikhha18]: https://doi.org/10.1017/s0956796818000102

### Logical vs. Physical Operators

The logical operators are user-facing and should provide an easy UX. The
physical operators are not exposed to the user and only relate to the
implementation.

To illustrate the logical-to-physical mapping, let's consider the example of
`load` and `store`.

We do not want to end up with M x N implementations for M carriers and N
formats, but rather M + N. To achieve this, we can separate the carrier and
format into dedicated operators. The carrier produces/consumes bytes, and the
format performs parsing/printing to interface with the `Arrow` type.

To combine carrier and format, we introduce an intermediate type `Bytes` that
bridges the physical operators. To implement `load`, we would use a combination
of `from` and `read`. To implement `store`, we would use `write` and `to`.

For example, the operator `from CARRIER FORMAT` would then map to the
physical implementation `from CARRIER | read FORMAT`. Similarly, `store CARRIER
FORMAT` would map to `write FORMAT | to CARRIER`.

The table below summarizes the signatures of the physical operators:

| Operator | Input Type | Output Type | Description
| -------- | ---------- | ----------- | ------------------------------------------
| `from`   | `Void`     | `Bytes`     | Loads bytes from a source as a side effect
| `read`   | `Bytes`    | `Arrow`     | Parses a specific format into Arrow
| `to`     | `Bytes`    | `Void`      | Writes bytes to a sink as a side effect
| `write`  | `Arrow`    | `Bytes`     | Prints input in a specific format

The implementation of `load` and `store` does not have to map to a combination
of `from|read` or `write|to`. It's not always possible to encapsulate data
acquisition in a two-step operation. For example, when using a C library that
yields structured objects, the parsing step already took place and it wouldn't
make sense to re-serialize them into `Bytes`. At this point, it makes more sense
to continue working with the objects and directly convert them to `Arrow`. In
other words, for some combinations of carrier and format, the logical operator
may dispatch to custom implementation for that particular pair.

Zeek's Broker library is a good example for a custom implementation of `load
broker://host:port`. Broker yields `broker::data` objects when subscribing to
data from a remote endpoint `host:port`. Note the absence of the format
argument, as the implementation would convert `broker::data` to Arrow directly.

## Alternatives

We considered the following ideas that we do not want to pursue further.

### Pipelines are only a query language construct

We may consider pipelines only in the context of executing queries at the
server. After all, this is where we are going to implement them first. However,
this limits us unnecessarily, given that pipeline can already be deployed at
various points in VAST today. We must consider them as general building block
that is easy to instantiate in various contexts.

### Commands are pipeline operators

To illustrate the composability of pipelines, we could make every VAST command
an operator:

```bash
vast load s3://aws json |
  vast 'summarize count(dst) group-by src' |
  vast store /path/to/file feather
```

As there is no clear underlying use case outside of testing and debugging, we
are not going to discuss this idea in more depth.
