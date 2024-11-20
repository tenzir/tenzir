# measure

Replaces the input with metrics describing the input.

```tql
measure [real_time=bool, cumulative=bool]
```

## Description

The `measure` operator yields metrics for each received batch of events or bytes
using the following schema, respectively:

```text title="Events Metrics"
type tenzir.metrics.events = record{
  timestamp: time,
  schema: string,
  schema_id: string,
  events: uint64,
}
```

```text title="Bytes Metrics"
type tenzir.metrics.bytes = record{
  timestamp: time,
  bytes: uint64,
}
```

### `real_time = bool (optional)`

Whether to emit metrics immediately with every batch, rather than buffering
until the upstream operator stalls, i.e., is idle or waiting for further input.

The is especially useful when `measure` should emit data without latency.

### `cumulative = bool (optional)`

Whether to emit running totals for the `events` and `bytes` fields rather than
per-batch statistics.

## Examples

### Get the number of bytes read incrementally for a file

```tql
load_file "input.json"
measure
```

```
{timestamp: 2023-04-28T10:22:10.192322, bytes: 16384}
{timestamp: 2023-04-28T10:22:10.223612, bytes: 16384}
{timestamp: 2023-04-28T10:22:10.297169, bytes: 16384}
{timestamp: 2023-04-28T10:22:10.387172, bytes: 16384}
{timestamp: 2023-04-28T10:22:10.408171, bytes: 8232}
```

### Get the number of events read incrementally from a file

```tql
load_file "eve.json"
read_suricata
measure
```

```tql
{
  timestamp: 2023-04-28T10:26:45.159885,
  events: 65536,
  schema: "suricata.dns",
  schema_id: "d49102998baae44a"
}
{
  timestamp: 2023-04-28T10:26:45.812321,
  events: 412,
  schema: "suricata.dns",
  schema_id: "d49102998baae44a"
}
```

### Get the total number of events in a file, grouped by schema

```tql
load_file "eve.json"
read_suricata
measure
summarize schema, events=sum(events)
```

```tql
{schema: "suricata.dns", events: 65948}
```
