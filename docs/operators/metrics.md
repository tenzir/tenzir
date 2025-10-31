---
title: metrics
category: Node/Inspection
example: 'metrics "cpu"'
---

Retrieves metrics events from a Tenzir node.

```tql
metrics [name:string, live=bool, retro=bool]
```

## Description

The `metrics` operator retrieves metrics events from a Tenzir node. Metrics
events are collected every second.

:::tip[Retention Policy]
Set the `tenzir.retention.metrics` configuration option to change how long
Tenzir Nodes store metrics:

```yaml
tenzir:
  retention:
    metrics: 7d
```

:::

### `name: string (optional)`

Show only metrics with the specified name. For example, `metrics "cpu"` only shows
CPU metrics.

### `live = bool (optional)`

Work on all metrics events as they are generated in real-time instead of on
metrics events persisted at a Tenzir node.

### `retro = bool (optional)`

Work on persisted diagnostic events (first), even when `live` is given.

## Schemas

Tenzir collects metrics with the following schemas.

### `tenzir.metrics.api`

Contains information about all accessed API endpoints, emitted once per second.

```tql
{
  timestamp: time, // The time at which the API request was received.
  request_id: string, // The unique request ID assigned by the Tenzir Platform.
  method: string, // The HTTP method used to access the API.
  path: string, // The path of the accessed API endpoint.
  response_time: duration, // The time the API endpoint took to respond.
  status_code: uint64, // The HTTP status code of the API response.
  params: record, // The API endpoints parameters passed in.
}
```

The schema of the record `params` depends on the API endpoint used. Refer to the
[API documentation](/reference/node/api) to see the available parameters per endpoint.

### `tenzir.metrics.caf`

Contains metrics about the CAF (C++ Actor Framework) runtime system.

:::caution[Aimed at Developers]
CAF metrics primarily exist for debugging purposes. Actor names and other
details contained in these metrics are documented only in source code, and we
may change them without notice. Do not rely on specific actor names or metrics
in production systems.
:::

```tql
{
  system: { // Metrics about the CAF actor system.
    running_actors: int64, // Number of currently running actors.
    running_actors_by_name: [{ // Number of running actors, grouped by actor name.
      name: string, // Actor name.
      count: int64, // Number of actors with this name currently running.
    }],
    all_messages: { // Information about the total message metrics.
      processed: int64, // Number of processed messages.
      rejected: int64, // Number of rejected messages.
    },
    messages_by_actor: list[{ // List of metrics, grouped by actor.
      name: string, // Name of the receiving actor. This may be null for messages without an associated actor.
      processed: int64, // Number of processed messages.
      rejected: int64, // Number of rejected messages.
    }],
  },
  middleman: { // Metrics about CAF's network layer.
    inbound_messages_size: int64, // Size of received messages in bytes since last metric.
    outbound_messages_size: int64, // Size of sent messages in bytes since last metric.
    serialization_time: duration, // Time spent serializing messages since last metric.
    deserialization_time: duration, // Time spent deserializing messages since last metric.
  },
  actors: list[{ // Per-actor metrics for all running actors.
    name: string, // Name of the actor.
    processing_time: duration, // Time spent processing messages since last metric.
    mailbox_time: duration, // Time messages spent in mailbox since last metric.
    mailbox_size: int64, // Current number of messages in actor's mailbox.
  }],
}
```

### `tenzir.metrics.buffer`

Contains information about the `buffer` operator's internal buffer.

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time at which this metric was recorded.
  operator_id: uint64, // The ID of the `buffer` operator in the pipeline.
  used: uint64, // The number of events stored in the buffer.
  free: uint64, // The remaining capacity of the buffer.
  dropped: uint64, // The number of events dropped by the buffer.
}
```

### `tenzir.metrics.cpu`

Contains a measurement of CPU utilization.

```tql
{
  timestamp: time, // The time at which this metric was recorded.
  loadavg_1m: double, // The load average over the last minute.
  loadavg_5m: double, // The load average over the last 5 minutes.
  loadavg_15m: double, // The load average over the last 15 minutes.
}
```

### `tenzir.metrics.disk`

Contains a measurement of disk space usage.

```tql
{
  timestamp: time, // The time at which this metric was recorded.
  path: string, // The byte measurements below refer to the filesystem on which this path is located.
  total_bytes: uint64, // The total size of the volume, in bytes.
  used_bytes: uint64, // The number of bytes occupied on the volume.
  free_bytes: uint64, // The number of bytes still free on the volume.
}
```

### `tenzir.metrics.enrich`

Contains a measurement of the `enrich` operator, emitted once every second.

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time at which this metric was recorded.
  operator_id: uint64, // The ID of the `enrich` operator in the pipeline.
  context: string, // The name of the context the associated operator is using.
  events: uint64, // The amount of input events that entered the `enrich` operator since the last metric.
  hits: uint64, // The amount of successfully enriched events since the last metric.
}
```

### `tenzir.metrics.export`

Contains a measurement of the `export` operator, emitted once every second per
schema. Note that internal events like metrics or diagnostics do not emit
metrics themselves.

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time at which this metric was recorded.
  operator_id: uint64, // The ID of the `export` operator in the pipeline.
  schema: string, // The schema name of the batch.
  schema_id: string, // The schema ID of the batch.
  events: uint64, // The amount of events that were imported.
  queued_events: uint64, // The total amount of events that are enqueued in the export.
}
```

### `tenzir.metrics.import`

Contains a measurement the `import` operator, emitted once every second per
schema. Note that internal events like metrics or diagnostics do not emit
metrics themselves.

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time at which this metric was recorded.
  operator_id: uint64, // The ID of the `import` operator in the pipeline.
  schema: string, // The schema name of the batch.
  schema_id: string, // The schema ID of the batch.
  events: uint64, // The amount of events that were imported.
}
```

### `tenzir.metrics.ingest`

Contains a measurement of all data ingested into the database, emitted once per
second and schema.

```tql
{
  timestamp: time, // The time at which this metric was recorded.
  schema: string, // The schema name of the batch.
  schema_id: string, // The schema ID of the batch.
  events: uint64, // The amount of events that were ingested.
}
```

### `tenzir.metrics.lookup`

Contains a measurement of the `lookup` operator, emitted once every second.

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time at which this metric was recorded.
  operator_id: uint64, // The ID of the `lookup` operator in the pipeline.
  context: string, // The name of the context the associated operator is using.
  live: { // Information about the live lookup.
    events: uint64, // The amount of input events used for the live lookup since the last metric.
    hits: uint64, // The amount of live lookup matches since the last metric.
  },
  retro: { // Information about the retroactive lookup.
    events: uint64, // The amount of input events used for the lookup since the last metric.
    hits: uint64, // The amount of lookup matches since the last metric.
    queued_events: uint64, // The total amount of events that were in the queue for the lookup.
  },
  context_updates: uint64, // The amount of times the underlying context has been updated while the associated lookup is active.
}
```

### `tenzir.metrics.memory`

Contains statistics about allocated memory.

```tql
{
  timestamp: time, // The time at which this metric was recorded.
  system: { // Information about the systems memory state.
    total_bytes: int, // Total available memory in the system.
    used_bytes: int, // Amount of memory used on the system.
    free_bytes: int, // Amount of free memory on the system.
  },
  process: {
    peak_bytes: int, // Peak memory usage during the runtime of the process.
    current_bytes: int, // Current memory usage of the entire process.
    swap_bytes: int, // Swap space used by the process.
  },
  arrow: { // Information about memory allocated by Arrow buffers.
    bytes: {
      current: int, // Currently allocated bytes
      max: int, // Maximum allocated bytes during this run
      total: int, // Cumulative allocations during this run
    },
    allocations: {
      current: int, // Number of current allocations
      max: int, // Maximum number of allocations
      total: int, // Cumulative allocations during this run
    },
  },
  cpp: { /// Information about memory allocated by `operator new`
    bytes: {
      current: int, // Currently allocated bytes
      max: int, // Maximum allocated bytes during this run
      total: int, // Cumulative allocations during this run
    },
    allocations: {
      current: int, // Number of current allocations
      max: int, // Maximum number of allocations
      total: int, // Cumulative allocations during this run
    },
  },
  c: { /// Information about memory allocated `malloc` and other C/POSIX functions.
    bytes: {
      current: int, // Currently allocated bytes
      max: int, // Maximum allocated bytes during this run
      total: int, // Cumulative allocations during this run
    },
    allocations: {
      current: int, // Number of current allocations
      max: int, // Maximum number of allocations
      total: int, // Cumulative allocations during this run
    },
  },
}
```

:::note[Detailed Metrics Availability]
The `arrow`, `cpp` and `c` records in `tenzir.metrics.memory` are only collected
if Tenzir was build with allocator support (all are by default) **and** the environment
variable `TENZIR_ALLOC_STATS=true` is set. `TENZIR_ALLOC_STATS_<component>` can also
be used to enable only a specific component.
:::

### `tenzir.metrics.operator`

Contains input and output measurements over some amount of time for a single
operator instantiation.

:::caution[Deprecation Notice]
Operator metrics are deprecated and will be removed in a future release. Use
[pipeline metrics](#tenzirmetricspipeline) instead. While they offered great
insight into the performance of operators, they were not as useful as pipeline
metrics for understanding the overall performance of a pipeline, and were too
expensive to collect and store.
:::

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time when this event was emitted (immediately after the collection period).
  operator_id: uint64, // The ID of the operator inside the pipeline referenced above.
  source: bool, // True if this is the first operator in the pipeline.
  transformation: bool, // True if this is neither the first nor the last operator.
  sink: bool, // True if this is the last operator in the pipeline.
  internal: bool, // True if the data flow is considered to internal to Tenzir.
  duration: duration, // The timespan over which this data was collected.
  starting_duration: duration, // The time spent to start the operator.
  processing_duration: duration, // The time spent processing the data.
  scheduled_duration: duration, // The time that the operator was scheduled.
  running_duration: duration, // The time that the operator was running.
  paused_duration: duration, // The time that the operator was paused.
  input: { // Measurement of the incoming data stream.
    unit: string, // The type of the elements, which is `void`, `bytes` or `events`.
    elements: uint64, // Number of elements that were seen during the collection period.
    approx_bytes: uint64, // An approximation for the number of bytes transmitted.
    batches: uint64, // The number of batches included in this metric.
  },
  output: { // Measurement of the outgoing data stream.
    unit: string, // The type of the elements, which is `void`, `bytes` or `events`.
    elements: uint64, // Number of elements that were seen during the collection period.
    approx_bytes: uint64, // An approximation for the number of bytes transmitted.
    batches: uint64, // The number of batches included in this metric.
  },
}
```

### `tenzir.metrics.pipeline`

Contains measurements of data flowing through pipelines, emitted once every 10
seconds.

```tql
{
  timestamp: time, // The time at which this metric was recorded.
  pipeline_id: string, // The ID of the pipeline these metrics represent.
  ingress: { // Measurement of data entering the pipeline.
    duration: duration, // The timespan over which this data was collected.
    events: uint64, // Number of events that passed through during this period.
    bytes: uint64, // Approximate number of bytes that passed through.
    batches: uint64, // Number of batches that passed through.
    internal: bool, // True if the data flow is considered internal to Tenzir.
  },
  egress: { // Measurement of data exiting the pipeline.
    duration: duration, // The timespan over which this data was collected.
    events: uint64, // Number of events that passed through during this period.
    bytes: uint64, // Approximate number of bytes that passed through.
    batches: uint64, // Number of batches that passed through.
    internal: bool, // True if the data flow is considered internal to Tenzir.
  },
}
```

### `tenzir.metrics.platform`

Signals whether the connection to the Tenzir Platform is working from the node's
perspective. Emitted once per second.

```tql
{
  timestamp: time, // The time at which this metric was recorded.
  connected: bool, // The connection status.
}
```

### `tenzir.metrics.process`

Contains a measurement of the amount of memory used by the `tenzir-node` process.

:::caution[Deprecated]
`process` metrics are deprecated in favor of the `process` field in the
`memory` metrics. They will be removed in a future release.
:::

```tql
{
  timestamp: time, // The time at which this metric was recorded.
  current_memory_usage: uint64, // The memory currently used by this process.
  peak_memory_usage: uint64, // The peak amount of memory, in bytes.
  swap_space_usage: uint64, // The amount of swap space, in bytes. Only available on Linux systems.
  open_fds: uint64, // The amount of open file descriptors by the node. Only available on Linux systems.
}
```

### `tenzir.metrics.publish`

Contains a measurement of the `publish` operator, emitted once every second per
schema.

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time at which this metric was recorded.
  operator_id: uint64, // The ID of the `publish` operator in the pipeline.
  topic: string, // The topic name.
  schema: string, // The schema name of the batch.
  schema_id: string, // The schema ID of the batch.
  events: uint64, // The amount of events that were published to the `topic`.
}
```

### `tenzir.metrics.rebuild`

Contains a measurement of the partition rebuild process, emitted once every
second.

```tql
{
  timestamp: time, // The time at which this metric was recorded.
  partitions: uint64, // The number of partitions currently being rebuilt.
  queued_partitions: uint64, // The number of partitions currently queued for rebuilding.
}
```

### `tenzir.metrics.subscribe`

Contains a measurement of the `subscribe` operator, emitted once every second
per schema.

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time at which this metric was recorded.
  operator_id: uint64, // The ID of the `subscribe` operator in the pipeline.
  topic: string, // The topic name.
  schema: string, // The schema name of the batch.
  schema_id: string, // The schema ID of the batch.
  events: uint64, // The amount of events that were retrieved from the `topic`.
}
```

### `tenzir.metrics.tcp`

Contains measurements about the number of read calls and the received bytes per
TCP connection.

```tql
{
  pipeline_id: string, // The ID of the pipeline where the associated operator is from.
  run: uint64, // The number of the run, starting at 1 for the first run.
  hidden: bool, // Indicates whether the corresponding pipeline is hidden from the list of managed pipelines.
  timestamp: time, // The time at which this metric was recorded.
  operator_id: uint64, // The ID of the `publish` operator in the pipeline.
  native: string, // The native handle of the connection (unix: file descriptor).
  reads: uint64, // The number of attempted reads since the last metric.
  writes: uint64, // The number of attempted writes since the last metric.
  bytes_read: uint64, // The number of bytes received since the last metrics.
  bytes_written: uint64, // The number of bytes written since the last metrics.
}
```

## Examples

### Sort pipelines by total ingress in bytes

```tql
metrics "pipeline"
summarize pipeline_id, ingress=sum(ingress.bytes if not ingress.internal)
sort -ingress
```

```tql
{pipeline_id: "demo-node/m57-suricata", ingress: 59327586}
{pipeline_id: "demo-node/m57-zeek", ingress: 43291764}
```

### Show the CPU usage over the last hour

```tql
metrics "cpu"
where timestamp > now() - 1h
select timestamp, percent=loadavg_1m
```

```tql
{timestamp: 2023-12-21T12:00:32.631102, percent: 0.40478515625}
{timestamp: 2023-12-21T11:59:32.626043, percent: 0.357421875}
{timestamp: 2023-12-21T11:58:32.620327, percent: 0.42578125}
{timestamp: 2023-12-21T11:57:32.614810, percent: 0.50390625}
{timestamp: 2023-12-21T11:56:32.609896, percent: 0.32080078125}
{timestamp: 2023-12-21T11:55:32.605871, percent: 0.5458984375}
```

### Get the current memory usage

```tql
metrics "memory"
sort -timestamp
tail 1
select current_memory_usage
```

```tql
{current_memory_usage: 1083031552}
```

### Show the total pipeline ingress in bytes

Show the inggress for every day over the last week, excluding pipelines that run
in the Explorer:

```tql
metrics "operator"
where timestamp > now() - 1week
where source and not hidden
timestamp = floor(timestamp, 1day)
summarize timestamp, bytes=sum(output.approx_bytes)
```

```tql
{timestamp: 2023-11-08T00:00:00.000000, bytes: 79927223}
{timestamp: 2023-11-09T00:00:00.000000, bytes: 51788928}
{timestamp: 2023-11-10T00:00:00.000000, bytes: 80740352}
{timestamp: 2023-11-11T00:00:00.000000, bytes: 75497472}
{timestamp: 2023-11-12T00:00:00.000000, bytes: 55497472}
{timestamp: 2023-11-13T00:00:00.000000, bytes: 76546048}
{timestamp: 2023-11-14T00:00:00.000000, bytes: 68643200}
```

### Show the operators that produced the most events

Show the three operator instantiations that produced the most events in total
and their pipeline IDs:

```tql
metrics "operator"
where output.unit == "events"
summarize pipeline_id, operator_id, events=max(output.elements)
sort -events
head 3
```

```tql
{pipeline_id: "70a25089-b16c-448d-9492-af5566789b99", operator_id: 0, events: 391008694 }
{pipeline_id: "7842733c-06d6-4713-9b80-e20944927207", operator_id: 0, events: 246914949 }
{pipeline_id: "6df003be-0841-45ad-8be0-56ff4b7c19ef", operator_id: 1, events: 83013294 }
```

### Get the disk usage over time

```tql
metrics "disk"
sort timestamp
select timestamp, used_bytes
```

```tql
{timestamp: 2023-12-21T12:52:32.900086, used_bytes: 461834444800}
{timestamp: 2023-12-21T12:53:32.905548, used_bytes: 461834584064}
{timestamp: 2023-12-21T12:54:32.910918, used_bytes: 461840302080}
{timestamp: 2023-12-21T12:55:32.916200, used_bytes: 461842751488}
```

### Get the memory usage over time

```tql
metrics "memory"
sort timestamp
select timestamp, used_bytes
```

```tql
{timestamp: 2023-12-21T13:08:32.982083, used_bytes: 48572645376}
{timestamp: 2023-12-21T13:09:32.986962, used_bytes: 48380682240}
{timestamp: 2023-12-21T13:10:32.992494, used_bytes: 48438878208}
{timestamp: 2023-12-21T13:11:32.997889, used_bytes: 48491839488}
{timestamp: 2023-12-21T13:12:33.003323, used_bytes: 48529952768}
```

### Get inbound TCP traffic over time

```tql
metrics "tcp"
sort timestamp
select timestamp, port, handle, reads, bytes
```

```tql
{
  timestamp: 2024-09-04T15:43:38.011350,
  port: 10000,
  handle: "12",
  reads: 884,
  writes: 0,
  bytes_read: 10608,
  bytes_written: 0
}
{
  timestamp: 2024-09-04T15:43:39.013575,
  port: 10000,
  handle: "12",
  reads: 428,
  writes: 0,
  bytes_read: 5136,
  bytes_written: 0
}
{
  timestamp: 2024-09-04T15:43:40.015376,
  port: 10000,
  handle: "12",
  reads: 429,
  writes: 0,
  bytes_read: 5148,
  bytes_written: 0
}
```

## See Also

[`diagnostics`](/reference/operators/diagnostics)
