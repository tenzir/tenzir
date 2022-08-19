# Composable Pipelines

- **Created**: Aug 19, 2022
- **Accepted**: TBA
- **Status**: WIP
- **Authors**:
  - [Matthias Vallentin](https://github.com/mavam)
- **Contributors**:
  - [Anthony Verez](https://github.com/netantho)
  - [Dominik Lohmann](https://github.com/dominiklohmann)

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
  vast group-by |
  vast summarize |
  vast write feather |
  vast to /path/to/file.feather
```

But let's take a step back and just assume that we add a new `exec` command that
just executes a pipeline.

Then we can model the ingestion as follows: load data via stdin, push into a
parser, then send off to a remote VAST node. For example, we would rewrite
`import zeek` as:

```bash
vast exec 'from - | parse zeek | to vast://1.2.3.4'
```

Likewise, `export json EXPRESSION` would become:

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
vast start -e 1.2.3.4
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

Another UX improvement would be assuming that we interact with a VAST node. Here
we could have dedicate commands the prepend or append a pipeline operator:

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

An `import` degenerates to

```bash
vast push 'parse zeek | where EXPRESSION'
# Same with exec
vast exec 'parse zeek | where EXPRESSION | to vast://...'
# Same with import
vast import zeek EXPRESSION
```

## Implementation

We could implement the entire pipeline with Arrow IPC streams in the middle.
It's clean and simple.

But this may be too restrictive. Let's assume for a moment that input and output
are dynamically typed. Then an operator must define its input and output types,
e.g., `from<In, Out>`. This can come in handy for working with matchers:

Here, we build a matcher, with read having the type `read<Bytes, Arrow>`:

```bash
vast exec 'read csv | matcher build --extract=net.src.ip' > ips.flatbuffer
```

The type of `matcher build` would be `<Arrow, Flatbuffer>`. Also assume `matcher
load` yields `<Flatbuffer, Void>` where `Void` is "no output, just a side
effect".

How do we pre-load a matcher?

```bash
vast exec 'from file:///ips.flatbuffer |
           matcher load --name=ips vast:///1.2.3.4'
```

Or more UNIX'ish, assuming an implicit `from -`:

```bash
vast exec 'matcher load --name=ips' < ips.flatbuffer
```

The `matcher` sub-command `match` does the actual matching:

```bash
vast pull 'where #type == "suricata.flow" |
           matcher match --on=:addr ips'
```

The above is a locally running pipeline, but most likely I want this to run at a
node. This begs the challenge: how do we manage all these pipelines? With a
client command (and ideally also a REST API):

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
spawned. From now on, everything is client-side managed. This is where you can
have your YAML for declarative ops:

```bash
vast pipeline load < pipelines.yaml
```

If you want the current server-side pipeline state for reproducing pipelines
elsewhere, just dump it (as YAML or JSON):

```bash
vast pipeline dump --json
vast pipeline dump --yaml
```

## Alternatives

We already agreed that we want to broaden VASTQL to include a pipeline syntax.
The alternative would be doing just that: do not change the CLI UX.
