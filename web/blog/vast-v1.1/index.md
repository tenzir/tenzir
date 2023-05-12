---
title: VAST v1.1
description: VAST v1.1 - Compaction & Query Language Frontends
authors: dominiklohmann
date: 2022-03-03
last_updated: 2022-07-15
tags: [release, compaction, query]
---

Dear community, we are excited to announce [VAST v1.1][github-vast-release],
which ships with exciting new features: *query language plugins* to exchange the
query expression frontend, and *compaction* as a mechanism for expressing
fine-grained data retention policies and gradually aging out data instead of
simply deleting it.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v1.1.0

<!--truncate-->

## Query Language Plugins

VAST features [a new query language plugin
type](https://vast.io/docs/VAST%20v3.0/understand/architecture/plugins#language)
that makes it possible to exchange the querying frontend, that is, replace the
language in which the user writes queries. This makes it easier to integrate
VAST into specific domains without compromising the policy-neutral system core.

The first instance of the query language plugin is the [`sigma`
plugin](https://github.com/tenzir/vast/tree/master/plugins/sigma), which make it
possible to pass [Sigma
rules](https://vast.io/docs/VAST%20v3.0/understand/language/frontends/sigma) as
input instead of a standard VAST query expression. Prior to this plugin, VAST
attempted to parse a query as Sigma rule first, and if that failed, tried to
parse it as a VAST expression. The behavior changed in that VAST now always
tries to interpret user input as VAST expression, and if that fails, goes
through all other loaded query language plugins.

Moving forward, we will make it easier for integrators to BYO query language and
leverage VAST as an execution engine. We have already
[experimented](https://github.com/tenzir/vast/pull/2075) with
[Substrait](https://substrait.io), a cross-language protobuf spec for query
plans. The vision is that users can easily connect *any* query language that
compiles into Substrait, and VAST takes the query plan as binary substrait blob.
Substrait is still a very young project, but if the Arrow integration starts to
mature, it has the potential to enable very powerful types of queries without
much heavy lifting on our end. We already use the Arrow Compute API to implement
generic grouping and aggregation during compaction, which allows us to avoid
hand-roll and optimize compute kernels for standard functions.

## Compaction Plugin

Compaction is a feature to perform fine-grained transformation of historical
data to manage a fixed storage budget. This gives operators full control over
shrinking data gradually—both from a temporal and spatial angle:

**Spatial**: Traditionally, reaching a storage budget triggers deletion of the
oldest (or least-recently-used) data. This is a binary decision to throw away a
subset of events. It does not differentiate the utility of data within an event.
What if you could only throw away the irrelevant parts and keep the information
that might still be useful for longitudinal investigations? What if you could
aggregate multiple events into a single one that captures valuable information?
Imagine, for example, halving the space utilization of events with network flow
information and keeping them 6 months longer; or imagine you could roll up a set
of flows into a traffic matrix that only captures who communicated with whom in
a given timeframe.

By incrementally elevating data into more space-efficient representations,
compaction gives you a much more powerful mechanism to achieve long retention
periods while working with high-volume telemetry.

**Temporal**: data residency regulations often come with compliance policies
with maximum retention periods, e.g., data containing personal data. For
example, a policy may dictate a maximum retention of 1 week for events
containing URIs and 3 months for events containing IP addresses related to
network connections. However, these retention windows could be broadened when
pseudonomyzing or anonymizing the relevant fields.

Compaction has a policy-based approach to specify these temporal constraints in
a clear, declarative fashion.

Compaction supersedes both the disk monitor and aging, being able to cover the
entire functionality of their behaviors in a more configurable way. The disk
monitor remains unchanged and the experimental aging feature is deprecated (see
below).

## Updates to Transform Steps

### Aggregate Step

:::info Transforms → Pipelines
In [VAST v2.2](/blog/vast-v2.2), we renamed *transforms* to *pipelines*, and
*transform steps* to *pipeline operators*. This caused several configuration key
changes. Additionally, we renamed the `aggregate` operator to
[`summarize`][summarize]. Please keep this in mind when reading the example
below and consult the
[documentation](/docs/VAST%20v3.0/understand/language/pipelines) for the
up-to-date syntax.
[summarize]: /docs/VAST%20v3.0/understand/language/operators/summarize
:::

The new `aggregate` transform step plugin allows for reducing data with an
aggregation operation over a group of columns.

Aggregation is a two-step process of first bucketing data in groups of values,
and then executing an aggregation function that computes a single value over the
bucket. The functionality is in line with what standard execution engines offer
via "group-by" and "aggregate".

Based on how the transformation is invoked in VAST, the boundary for determining
what goes into a grouping can be a table slice (e.g., during import/export) or
an entire partition (during compaction).

How this works is best shown on example data. Consider the following events
representing flow data that contain a source IP address, a start and end
timestamp, the number of bytes per flow, a boolean flag whether there is an
associated alert, and a unique identifier.

```json
{"source_ip": "10.0.0.1", "num_bytes": 87122, "start": "2022-02-22T10:36:40", "end": "2022-02-22T10:36:47", "alerted": false, "unique_id": 1}
{"source_ip": "10.0.0.2", "num_bytes": 62335, "start": "2022-02-22T10:36:43", "end": "2022-02-22T10:36:48", "alerted": false, "unique_id": 2}
{"source_ip": "10.0.0.1", "num_bytes": 640, "start": "2022-02-22T10:36:46", "end": "2022-02-22T10:36:47", "alerted": true, "unique_id": 3}
{"source_ip": "10.0.0.1", "num_bytes": 2162, "start": "2022-02-22T10:36:49", "end": "2022-02-22T10:36:51", "alerted": false, "unique_id": 4}
```

We can now configure a transformation that groups the events by their source IP
address, takes the sum of the number of bytes, the minimum of the start
timestamp, the maximum of the end timestamp, and the disjunction of the alerted
flag. Since the unique identifier cannot be aggregated in a meaningful manner,
it  is discarded.

```yaml
vast:
  transforms:
    example-aggregation:
      - aggregate:
          group-by:
            - source_ip
          sum:
            - num_bytes
          min:
            - start
          max:
            - end
          any:
            - alerted
```

After applying the transform, the resulting events will look like this:

```json
{"source_ip": "10.0.0.1", "num_bytes": 89924, "start": "2022-02-22T10:36:40", "end": "2022-02-02T10:36:51", "alerted": true}
{"source_ip": "10.0.0.2", "num_bytes": 62335, "start": "2020-11-06T10:36:43", "end": "2020-02-22T10:36:48", "alerted": false}
```

Unlike the built-in transform steps, `aggregate` is a separate open-source
plugin that needs to be manually enabled in your `vast.yaml` configuration to be
usable:

```yaml
vast:
  plugins:
    - aggregate
```

### Rename Step

The new `rename` transform step is a built-in that allows for changing the name
of the schema of data. This is particularly useful when a transformation changes
the shape of the data. E.g., an aggregated `suricata.flow` should likely be
renamed because it is of a different layout.

This is how you configure the transform step:

```yaml
rename:
  layout-names:
    - from: suricata.flow
      to: suricata.aggregated_flow
```

### Project and Select Steps

The built-in `project` and `select` transform steps now drop table slices where
no columns and rows match the configuration respectively instead of leaving the
data untouched.

## Deprecations

The `msgpack` encoding no longer exists. As we integrate deeper with Apache
Arrow, the `arrow` encoding is now the only option. Configuration options for
`msgpack` will be removed in an upcoming major release. On startup, VAST now
warns if any of the deprecated options are in use.

VAST’s *aging* feature never made it out of the experimental stage: it only
erased data without updating the index correctly, leading to unnecessary lookups
due to overly large candidate sets and miscounts in the statistics. Because
time-based compaction is a superset of the aging functionality (that also
updates the index correctly), we will remove aging in a future release. VAST now
warns on startup if it’s configured to run aging.
