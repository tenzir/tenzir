# Remote Pipelines

- **Status**: WIP
- **Created**: Sep 14, 2022
- **ETA**: Oct 14, 2022
- **Authors**:
  - [Matthias Vallentin](https://github.com/mavam)
- **Discussion**: [PR #2577](https://github.com/tenzir/vast/pull/2577)

## Overview

This proposal enhances the local pipeline execution with a remote component,
making it possible to connect pipeline across process boundaries.

## Problem Statement

As of RFC-001, VAST pipelines can execute locally. But a one-shot pipeline
execution is not sufficient. After this building block, natural questions arise:

- How can I connect a local and remote pipeline?
- How can I manage (add/update/remove) a set of continuously running pipelines?
- How do pipelines interact with VAST server nodes? Specifically, how can I send
  data into a VAST node, and how can I query data from a VAST node?

This proposal seeks an answer to these questions.

## Solution Proposal

The core of this proposal introduces a control plane that provides a global
namespace for pipelines and VAST nodes. The ability to interconnect pipelines
via unique names in location-independent manner enabeles a loosely coupled mesh
arcitecture.

The core primitives for establishing connections between pipelines are two
dedicated operators, `push` and `pull`:

| Operator | Input Type | Output Type | Description
| -------- | ---------- | ----------- | ------------------------------------------
| `push`   | `Arrow`    | `Void`      | Sends data to a named pipeline
| `pull`   | `Void`     | `Arrow`     | Attaches to a named pipeline

Arguments to these operators represent names from the global namespace.

### Example

Assuming an existing mechanism to bind a pipeline to name, the new operators
allow for interconnecting pipelines as follows.

Create a named pipeline `pipe1` at a VAST node and expose in the global control
plane:

```bash
vast spawn pipe1 'from - | read zeek | where orig_bytes > 1 GiB'
vast expose pipe1 tcp://1.2.3.4:5555 
```

Attach to `pipe1` to read from it locally:

```bash
vast exec 'pull pipe1 | where resp_bytes > 1 KiB'
```

Send data to the pipeline from a local vantage point:

```bash
vast exec 'from /tmp/conn.log | push pipe1'
```

Fuse two pipelines to create a joint dataset:

```bash
vast spawn pipe2 'from - | read suricata | where alert.severity == 2'
vast expose pipe2 tcp://1.2.3.4:5556
vast exec 'pull pipe1,pipe2 | store parquet /tmp/dataset'
```

### Treating storage as a pipeline

TBD

### Mutating existing data

How would we apply a pipeline (remotely) to mutate data at rest?

Security telemetry data is often messy. After the data collection aspects have
been sorted out and analysts interact with the data, it becomes clear that some
part of the data needs to be cleaned. This matches ELT mindset of dumping
everything first so that the data is there, and then tweak as you go. Example
operations include, migrating data to a different schema, enriching telemetry
(e.g., with GeoIP data), and fixing invalid entries.

Mutation of data is non-trivial in that it requires exclusive access to the
data. Otherwise data races may occur. Consequently, we need transactional
interface (ACID) to support this operation.

As long as VAST owns the underlying data (i.e., only VAST is allowed to make
changes), VAST can already mutate data at rest. [Spatial
compaction][mutate-at-rest] uses a pre-defined disk quota as trigger to apply
pipelines to a subset of to-be-transformed data. After VAST applies the
pipeline, VAST optionally removes the original data in an atomic fashion.

[mutate-at-rest]: https://vast.io/docs/use-vast/transform#modify-data-at-rest

Modeled after the `compaction` plugin, we may consider exposing a mutable
pipeline interface through a dedicated `mutate` command. In the
everything-is-a-pipeline mindset, the remote VAST storage node has a unique name
to reference the data at rest, e.g., `lake1`. Then we could mutate the data as
follows:

```bash
vast mutate lake1 'where 10.0.0.0/8 | put orig.h resp.h'
```





---

TODO: here be dragons; the following is verbatim copied from RFC-001 and the
content still needs to be adapted.


## Implementation

### Case study: remote matcher

TODO: still verbatim copied.

In the above example, we passed in a state file via `--state`. This is a
different pattern from today's matchers that run remotely. Assuming that we have
a mechanism for remote state management (specifically matcher state), we can
apply our existing UX pattern of remote matcher management.

Let's assume the matcher plugin provides the `matcher` command for state
management, just like today. To load the matcher state into a remote matcher, we
nothing would change:

```bash
vast matcher load foo < matcher.flatbuf
```

Assuming this operations updates the state of matcher `foo`, we can then
reference it in a remote pipeline:

```bash
vast spawn 'live | match --on=:addr foo'
```

Here, `live` is a dummy operator that represents the ingest path.

### Pipeline Management

In many cases we want to run pipelines remotely, independent of where the client
is. This begs the challenge: how do we manage pipelines? As with all other VAST
functions, there could be a client command.

```bash
# Show all pipelines:
vast pipeline list
```

Create remote pipelines:

```bash
vast pipeline create foo 'where #type == "suricata.flow"'
vast pipeline list
# Output:
# foo: where #type == "suricata.flow"
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

We did not consider any alternativs yet.
