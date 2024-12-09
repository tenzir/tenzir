---
sidebar_custom_props:
  operator:
    transformation: true
---

# measure

Replaces the input with metrics describing the input.

## Synopsis

```
measure [--real-time] [--cumulative]
```

## Description

The `measure` operator yields metrics for each received batch of events or bytes
using the following schema, respectively:

```title="Events Metrics"
type tenzir.metrics.events = record  {
  timestamp: time,
  schema: string,
  schema_id: string,
  events: uint64,
}
```

```title="Bytes Metrics"
type tenzir.metrics.bytes = record  {
  timestamp: time,
  bytes: uint64,
}
```

### `--real-time`

Emit metrics immediately with every batch, rather than buffering until the
upstream operator stalls, i.e., is idle or waiting for further input.

The `--real-time` option is useful when inspect should emit data without
latency.

### `--cumulative`

Emit running totals for the `events` and `bytes` fields rather than per-batch
statistics.

## Examples

Get the number of bytes read incrementally for a file:

```json {0} title="load file path/to/file.feather | measure | write json"
{"timestamp": "2023-04-28T10:22:10.192322", "bytes": 16384}
{"timestamp": "2023-04-28T10:22:10.223612", "bytes": 16384}
{"timestamp": "2023-04-28T10:22:10.297169", "bytes": 16384}
{"timestamp": "2023-04-28T10:22:10.387172", "bytes": 16384}
{"timestamp": "2023-04-28T10:22:10.408171", "bytes": 8232}
```

Get the number of events read incrementally from a file:

```json {0} title="from file path/to/file.feather | measure | write json"
{"timestamp": "2023-04-28T10:26:45.159885", "events": 65536, "schema": "suricata.dns", "schema_id": "d49102998baae44a"}
{"timestamp": "2023-04-28T10:26:45.812321", "events": 412, "schema": "suricata.dns", "schema_id": "d49102998baae44a"}
```

Get the total number of events in a file, grouped by schema:

```json {0} title="from file path/to/file.feather | measure | summarize events=sum(events) by schema"
{"events": 65948, "schema": "suricata.dns"}
```
