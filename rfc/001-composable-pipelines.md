# Composable Pipelines

- **Status**: In-Review
- **Created**: Aug 19, 2022
- **ETA**: Sep 19, 2022
- **Authors**:
  - [Matthias Vallentin](https://github.com/mavam)
- **Contributors**:
  - [Anthony Verez](https://github.com/netantho)
  - [Dominik Lohmann](https://github.com/dominiklohmann)
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
      group-by src |
      summarize count(dst) |
      write feather |
      to /path/to/file.feather'
```

Just to illustrate the composability, we could take this to an extreme when
performance doesn't matter:

```bash
vast from s3://aws |
  vast read json |
  vast group-by src |
  vast 'summarize count(dst)' |
  vast write feather |
  vast to /path/to/file.feather
```

The UX would translate seamlessly to other languages, e.g., Python:

```python
import vast as v
await v.from("s3://aws")
       .read_json() # or read("json")
       .group_by("src")
       .summarize(v.count("dst"))
       .write_feather() # or write("feather")
       .to("/path/to/file.feather")
```

### Pipeline Execution

Let's take a step back and just assume that we add a new `exec` command that
just executes a pipeline.

Then we can model the ingestion as follows: load data via stdin, push into a
parser, then send off to a remote VAST node. For example, we would rewrite
`vast import zeek` as:

```bash
vast exec 'from - | parse zeek | to vast://1.2.3.4'
```

Likewise, `vast export json EXPRESSION` would become:

```bash
vast exec 'from vast://1.2.3.4 |
           where EXPRESSION |
           write json |
           to -'
```

The beauty is that we can almost write the same without requiring a VAST node
and execute *in situ*:

```bash
vast exec 'from - |
           where EXPRESSION |
           write json |
           to -'
```

The only difference being `-` here.

The VAST node basically becomes a **pipeline manager** with persistent state. We
can keep everything as is, e.g., start a node:

```bash
vast -e 1.2.3.4 start
```

There are of course a bunch of convenience things we can do, like assuming `from
-` and `to -` being the default. This would make the above look like this:

```bash
vast exec 'where EXPRESSION |
           write json
```

Side effect: we would get our envisioned convert operator almost for free:

```bash
vast exec 'parse zeek | print csv'
# Obviates the need for:
vast exec 'convert zeek csv'
```

### Node Interaction

When considering pipelines in conjunction with VAST nodes, we may want to
improve the UX of working with pipelines.

One example would be specifying the VAST node as a source or sink of a pipeline.
To this end, we could have dedicated commands to prepend or append a pipeline
operator:

- `vast pull` → `from vast://...`
- `vast push` → `to vast://...`

An `export` would then become:

```bash
vast pull 'where EXPRESSION | print json'
# Same with exec:
vast exec 'from vast://.... | where EXPRESSION | print json'
# Same with export:
vast export json EXPRESSION
```

An `import` degenerates to:

```bash
vast push 'parse zeek | where EXPRESSION'
# Same with exec
vast exec 'parse zeek | where EXPRESSION | to vast://...'
# Same with import
vast import zeek EXPRESSION
```

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
  backend.

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
