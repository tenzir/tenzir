# Composable Pipelines

- **Status**: In-Review
- **Created**: Aug 19, 2022
- **ETA**: Sep 19, 2022
- **Authors**:
  - [Matthias Vallentin](https://github.com/mavam)
- **Contributors**:
  - [Anthony Verez](https://github.com/netantho)
  - [Dominik Lohmann](https://github.com/dominiklohmann)
  - [Rémi Dettai](https://github.com/rdettai)
  - [Thomas Peiselt](https://github.com/dispanser)
- **Discussion**: [PR #2511](https://github.com/tenzir/vast/pull/2511)

## Overview

This proposal ideates a new UX around VAST's pipelines. The fundamental idea is
that *everything is a pipeline* and build a composable UX around this concept.

## Problem Statement

Pipelines currently are only deployable at [predefined
points](https://vast.io/docs/use-vast/transform). For example, they can be
triggered for specific events on import/export, or invoked by compaction.

As we are bringing pipelines to the query language, we need to solve a UX
challenge: how do we make the data flow explicit and provide a unified
experience, especially in the presence of custom plugins?

## Solution Proposal

Assume for a moment everything is a pipeline operator. Then we may write:

 ```bash
vast 'from s3://aws |
      read json |
      summarize count(dst) group-by src |
      store parquet /path/to/directory'
```

The UX would translate seamlessly to other languages, e.g., Python:

```python
import vast as v
await v.from("s3://aws")
       .read_json()
       .group_by("src") # could also be merged with summarize()
       .summarize(v.count("dst"))
       .store_parquet("/path/to/directory")
```

### Synopsis

The core of this proposal introduces pipeline operators as function with input
and output types. The following overview introduces the pipeline building
blocks used throughout this proposal.

#### I/O

Input:

| Operator | Input Type | Output Type | Description
| -------- | ---------- | ----------- | ------------------------------------------
| `from`   | `Void`     | `Bytes`     | Loads bytes from a source as a side effect
| `read`   | `Bytes`    | `Arrow`     | Parses a specific format into Arrow
| `load`   | `Void`     | `Arrow`     | Performs `from` and `read` in one step

Output:

| Operator | Input Type | Output Type | Description
| -------- | ---------- | ----------- | ------------------------------------------
| `to`     | `Bytes`    | `Void`      | Writes bytes to a sink as a side effect
| `write`  | `Arrow`    | `Bytes`     | Prints input in a specific format
| `store`  | `Arrow`    | `Void`      | Performs `write` and `to` in one step

The reason where `load`/`store` exist, in addition to `from`/`to` and
`read`/`write`, is that it's not always possible to encapsulate data acquisition
in a two-step operation. For example, when using a C library that yields
structured objects, the parsing step already took place and it wouldn't make
sense to re-serialize them into `Bytes`. At this point, it makes more sense to
continue working with the objects and convert them to `Arrow`.

Zeek's Broker library is a good example for `load<Void, Arrow>`: it yields
`broker::data` objects when subscribing to data from a remote machine. Likewise,
Parquet is a good example for `store<Arrow, Void>`: since one Parquet file has a
fixed scheme, but `Arrow` is a heterogeneous stream, the implementation is
responsible for demultiplexing it and writing it to one file per schema.

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
| `drop`      | ✅           | Remove a set of columns (projection)
| `replace`   | ✅           | Replaces field described by extractors with values
| `extend`    | ✅           | Adds new fields with (initially fixed) values
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
| `vast infer`                             | `vast exec 'read [FORMAT] | infer"`
| `vast import FORMAT EXPR`                | `vast import FORMAT 'EXPR | op1 .. | op2 ..'`
| `vast export FORMAT EXPR`                | `vast export FORMAT 'EXPR | op1 .. | op2 ..'`
| `vast count [args] EXPR`                 | `vast export FORMAT 'EXPR | count [args]'`
| `vast pivot --format=FORMAT [args] EXPR` | `vast export FORMAT 'EXPR | pivot [args]'`
| `vast explore [args] EXPR`               | `vast export 'EXPR | explore [-A] [...]'`

In essence, we keep `import` and `export` and incrementally enhance the
expressiveness, by making it possible to append a pipeline after an expression.
That is, valid input has the form `EXPR | OPERATOR | OPERATOR | ...`. Note that
this is *not* valid pipeline, because `EXPR` is not an operator.

Additionally, we add a new command `exec` that accepts a "pure" pipeline, i.e.,
just a composition of operators. Execution takes place locally, without any
implicit remote communication. For the remainder of the discussion, we focus
on `exec` only.

### Local Execution

Given the new `exec` command, we can start processing data in situ. For example,
we can directly compute insights over telemetry:

```bash
vast exec 'from - |
           read zeek |
           summarize sum(orig_bytes), sum(resp_bytes) group-by id.orig_h, id.resp_h |
           sort |
           head -n 10 |
           write json |
           to -'
```

This pipeline answers the question "who are the top talkers in my network" by
summing originator and responder bytes per host pair. The dataflow happens as
follows:

1. Load data via stdin (`from`)
2. Push data into a parser (`read`)
3. Aggregate data (`summarize`)
4. Sort the aggregate (`sort`)
5. Only consider the top 10 (`head`)
6. Push data into a printer (`write`)
7. Write data to stdout (`to`)

Local pipeline execution opens the door for a lot of new modes of operations,
because a VAST server node is no longer required.

**This makes VAST a swiss army knife for security data.**

There are of course convenience defaults we can use, like assuming `from -` and
`to -` being the default. VAST can then easily used as a conversion utility
between different types of security data:

```bash
vast exec 'read zeek | write json'
# Obviates the need for:
vast exec 'convert zeek json'
```

## Implementation

We could implement the entire pipeline with Arrow IPC streams in the "interior"
operators, with only `from` and `to` taking raw input as byte stream. This
may be too restrictive, preventing a wide array of flexible operators. Let's
assume that input and output are dynamically typed. Operators must then define
their input and output types, e.g., `from<In, Out>` takes as input `In` and
produces instances of `Out`.

An pipeline *executor* orchestrates the execution of a pipeline, connecting
inputs and outputs from operators into a dataflow.

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
vast exec 'from file:///tmp/blacklist.csv |
           read csv |
           build --extract=net.src.ip' > matcher.flatbuf
```

After having constructed a matcher, we can now use it in a pipeline for
matching:

```bash
vast exec 'from kafka -t /zeek/conn |
           read zeek |
           match --state matcher.flatbuf --on id.orig_h,id.resp_h
```

The `match` operator exposes the structured data to a specific matcher instance,
reshaping according to the positional arguments that are a list of extractors.
The pipeline writer is responsible for reshaping the data so that the matcher
can make sense of it.

A successful "match" wraps the corresponding event into a sighting envelope
record.

## Alternatives

We already agreed that we want to broaden VASTQL to include a pipeline syntax.
The alternative would be doing just and make pipelines only applicable to
queries. As this limits us unnecessarily, we consider pipelines as general
building block that is deployable in various contexts.

To illustrate the composability of pipelines, we could make every VAST command
an operator:

```bash
vast from s3://aws |
  vast read json |
  vast 'summarize count(dst) group-by src' |
  vast write feather |
  vast to /path/to/file.feather
```

As there is no clear underlying use case outside of testing and debugging, we
are not going to discuss this idea in more depth.
