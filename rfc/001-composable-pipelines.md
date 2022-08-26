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
      write feather |
      to /path/to/file.feather'
```

Just to illustrate the composability, we could take this to an extreme when
performance doesn't matter:

```bash
vast from s3://aws |
  vast read json |
  vast 'summarize count(dst) group-by src' |
  vast write feather |
  vast to /path/to/file.feather
```

The UX would translate seamlessly to other languages, e.g., Python:

```python
import vast as v
await v.from("s3://aws")
       .read_json() # or read("json")
       .group_by("src") # could also be merged with summarize()
       .summarize(v.count("dst"))
       .write_feather() # or write("feather")
       .to("/path/to/file.feather")
```

### Operator Synopsis

The core of this proposal introduces pipeline operators as function with input
and output types. The following overview introduces the pipeline building
blocks used throughout this proposal.

#### I/O

| Operator | Input Type | Output Type | Description
| -------- | ---------- | ----------- | ------------------------------------------
| `from`   | `Void`     | `Bytes`     | Loads bytes from a source as a side effect
| `to`     | `Bytes`    | `Void`      | Writes bytes to a sink as a side effect
| `read`   | `Bytes`    | `Arrow`     | Parses a specific format into Arrow
| `write`  | `Arrow`    | `Bytes`     | Prints input in a specific format
| `pull`   | `Void`     | `Arrow`     | Loads Arrow from a remote source
| `push`   | `Arrow`    | `Void`      | Stores Arrow in a remote source

#### Computation

The following operators have a fixed input and output type of `Arrow`. We can
group them into "filter" operators that do not change the schema, and "reshape"
operators where execution yields a new schema.

##### Filter

| Operator | Non-Blocking | Description
| -------- |:------------:| ------------------------------------------
| `head`   | ✅           | Retrieve first N records
| `tail`   | ❌           | Retrieve last N records
| `where`  | ✅           | Filter with expression
| `sort`   | ❌           | Sort according to field
| `uniq`   | ✅           | Produce deduplicated output

##### Reshape

| Operator    | Non-Blocking | Description
| ----------- |:------------:| ------------------------------------------
| `put`       | ✅           | Select a set of columns (projection)
| `drop`      | ❌           | Remove a set of columns (projection)
| `replace`   | ✅           | Replaces field described by extractors with values
| `extend`    | ✅           | Adds new fields with (initially fixed) values
| `rename`    | ✅           | Renames schema meta data (type & fields)
| `summarize` | ❌/✅[^1]    | Aggregate group and compute a summary function

[^1]: Depending on the aggregate function.

### Pipeline Execution

Let's take a step back and just assume that we add a new `exec` command that
just executes a pipeline.

Then we can model the ingestion as follows: load data via stdin, push into a
parser, then send off to a remote VAST node. For example, we would rewrite
`vast import zeek` as:

```bash
vast exec 'from - | read zeek | push vast://1.2.3.4'
```

Likewise, `vast export json EXPRESSION` would become:

```bash
vast exec 'pull vast://1.2.3.4 |
           where EXPRESSION |
           write json |
           to -'
```

The beauty is that VAST nodes are now longer required. Pipelines provide value
locally as well:

```bash
vast exec 'from - |
           where EXPRESSION |
           write json |
           to -'
```

What would the role of the node be then? Basically a **pipeline manager** with
persistent state. A lot of things wouldn't change, e.g., starting a node:

```bash
vast -e 1.2.3.4 start
```

There are of course convenience defaults we can use, like assuming `from -` and
`to -` being the default. This would make the above look like this:

```bash
vast exec 'where EXPRESSION |
           write json
```

Side effect: we would get our envisioned convert operator almost for free:

```bash
vast exec 'read zeek | print csv'
# Obviates the need for:
vast exec 'convert zeek csv'
```

### Node Interaction

When considering pipelines in conjunction with VAST nodes, we may want to
improve the UX of working with pipelines. In the new model, VAST would be
an `Arrow` source or sink, making the primary interaction on the data plane
through `push` and `pull` operators.

Making `push` and `pull` top-level commands would streamline UX. For example,
consider this `vast export` pipeline:

```bash
# exec
vast exec 'pull vast://1.2.3.4 |
           where EXPRESSION |
           write json'
# pull
vast pull 'where EXPRESSION |
           write json
```

This assumes that `pull` tries the default VAST endpoint of 1.2.3.4 to connect.

An `import` degenerates to:

```bash
# exec
vast exec 'read zeek | where EXPRESSION | push vast:///'
# push
vast push 'read zeek | where EXPRESSION'
```

Note that this hoisting into commands only works because `pull` and `push` are
head/tail of the pipeline.

### Mutating existing data

Security telemetry data is often messy. After the data collection aspects have
been sorted out and analysts interact with the data, it becomes clear that some
part of the data needs to be cleaned. This matches ELT mindset of dumping
everything first so that the data is there, and then tweak as you go.

Example operations include, migrating data to a different schema, enriching
telemetry (e.g., with GeoIP data), and fixing invalid entries.

Mutation of data is non-trivial in that it requires exclusive access to the
data. Otherwise data races may occur. Consequently, we need transactional
interface (ACID) to support this operation.

As long as VAST owns the underlying data (i.e., only VAST is allowed to make
changes), VAST can already mutate of data at rest. [Spatial
compaction][mutate-at-rest] uses a pre-defined disk quota as trigger to apply
pipelines to a subset of to-be-transformed data. After VAST applies the
pipeline, VAST optionally removes the original data in an atomic fashion.

[mutate-at-rest]: https://vast.io/docs/use-vast/transform#modify-data-at-rest

Modeled after the `compaction` plugin, we may consider exposing a mutable
pipeline interface through a dedicate command. This command interacts with a
node, and could be seen as a combination of `pull` and `push` that executes
remotely. For example:

```bash
vast mutate --types=A,B,C 'where 10.0.0.0/8 | put orig.h resp.h'
```

This would apply the pipeline to filter and project in schema types `A`, `B`,
and `C`.

## Implementation

We could implement the entire pipeline with Arrow IPC streams in the "interior"
operators, with only `from` and `to` taking raw input as byte stream. But this
may be too restrictive. Let's assume that input and output are dynamically
typed. An operator must then define its input and output types, e.g., `from<In,
Out>` takes as input `In` and produces instances of `Out`.

### Matcher Example

This can come in handy for working with the matcher plugin. Consider the use
case of constructing a matcher from data. Consider this pipeline:

```bash
vast exec 'read csv |
           matcher build --extract=net.src.ip' > ips.flatbuffer
```

Here, we have:

- `read<Bytes, Arrow>`: reads `Bytes` and produces `Arrow` record batches
- `matcher build<Arrow, Flatbuffer>`: consumes `Arrow` and create a matcher
  state object.

Let `Void` be a valid type where the operator as only a side effect. Then we can
pre-load a matcher at a VAST node as follows:

```bash
vast exec 'from file:///ips.flatbuffer |
           matcher load --name=ips vast:///1.2.3.4'
```

Or more UNIX'ish, assuming an implicit `from -`:

```bash
vast exec 'matcher load --name=ips' < ips.flatbuffer
```

Here, `matcher load<Flatbuffer, Void>` is the operator that updates the remote
matcher state.

Finally, the `matcher` sub-command `match` does the actual matching:

```bash
vast pull 'where #type == "suricata.flow" |
           matcher match --on=:addr ips'
```

### Pipeline Management

The above is a locally running pipeline, but most likely we want this to run at
a node remotely, independent of where the client is. This begs the challenge:
how do we manage all these pipelines? As with all other VAST functions, there
could be a client command.

```bash
# Show all pipelines:
vast pipeline list
```

Create remote pipelines at ease:

```bash
vast pipeline create foo 'where #type == "suricata.flow" | matcher match ips'
vast pipeline list
# Output:
# foo: where #type == "suricata.flow" | matcher match ips
```

The idea would be that only clients can manipulate server state. This fixes
the mess of having server-side YAML that goes out of sync with the server
state when new things happen, e.g., new schemas arrive or new matchers get
spawned.

From now on, everything is client-side managed *only*. No more server-side
state configuration beyond options/settings. This is where you can
have your YAML for declarative ops:

```bash
vast pipeline load < pipelines.yaml
```

If you want the current server-side state for reproducing pipelines elsewhere,
just dump it (as YAML or JSON):

```bash
vast pipeline dump --json
vast pipeline dump --yaml
```

The output is not to be confused with the settings in `vast.yaml`. The pipeline
state is still persisted on the server, though, because it must survive
restarts. Consequently, the state changes must be atomic and be applied
in a WAL manner.

### REST API

In the future, it would also be nice to offer the same pipeline management
functionality through a REST API to make it easier to integrate with a
remote VAST node, e.g., build a web UI.)

## Alternatives

We already agreed that we want to broaden VASTQL to include a pipeline syntax.
The alternative would be doing just that: do not change the CLI UX.
