---
sidebar_custom_props:
  operator:
    transformation: true
---

# batch

The `batch` operator controls the batch size of events.

:::warning Expert Operator
The `batch` operator is a lower-level building block that lets users explicitly
control batching, which otherwise is controlled automatically by Tenzir's
underlying pipeline execution engine. Use with caution!
:::

## Synopsis

```
batch [--timeout <duration>] [<limit>]
```

## Description

The `batch` operator takes its input and rewrites it into batches of up to the
desired size.

### `--timeout <duration>`

Specifies a maximum latency for events passing through the batch operator. When
unspecified, an infinite duration is used.

### `<limit>`

An unsigned integer denoting how many events to put into one batch at most.

Defaults to 65536.

## Examples

Write exactly one NDJSON object at a time to a Kafka topic.

```
batch 1 | to kafka -t topic write json -c
```
