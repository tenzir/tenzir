---
sidebar_custom_props:
  operator:
    source: true
---

# metrics

Retrieves metrics events from a Tenzir node.

## Synopsis

```
metrics [--live] [--retro] [<name>]
```

## Description

The `metrics` operator retrieves metrics events from a Tenzir node. Metrics
events are collected every second.

### `--live`

Work on all metrics events as they are generated in real-time instead of on
metrics events persisted at a Tenzir node.

### `--retro`

Work on persisted diagnostic events (first), even when `--live` is given.

See [`export` operator](export.md#--retro) for more details.

### `<name>`

Show only metrics with the specified name. For example, `metrics cpu` only shows
CPU metrics.

## Schemas

Tenzir collects metrics with the following schemas.

### `tenzir.metrics.api`

Contains information about all accessed API endpoints, emitted once per second.

|Field|Type|Description|
|:-|:-|:-|
|`timestamp`|`time`|The time at which the API request was received.|
|`request_id`|`string`|The unique request ID assigned by the Tenzir Platform.|
|`method`|`double`|The HTTP method used to access the API.|
|`path`|`double`|The path of the accessed API endpoint.|
|`response_time`|`duration`|The time the API endpoint took to respond.|
|`status_code`|`uint64`|The HTTP status code of the API response.|
|`params`|`record`|The API endpoints parameters passed inused.|

The schema of the record `params` depends on the API endpoint used. Refer to the
[API documentation](/api) to see the available parameters per endpoint.

### `tenzir.metrics.cpu`

Contains a measurement of CPU utilization.

|Field|Type|Description|
|:-|:-|:-|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`loadavg_1m`|`double`|The load average over the last minute.|
|`loadavg_5m`|`double`|The load average over the last 5 minutes.|
|`loadavg_15m`|`double`|The load average over the last 15 minutes.|

### `tenzir.metrics.disk`

Contains a measurement of disk space usage.

|Field|Type|Description|
|:-|:-|:-|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`path`|`string`|The byte measurements below refer to the filesystem on which this path is located.|
|`total_bytes`|`uint64`|The total size of the volume, in bytes.|
|`used_bytes`|`uint64`|The number of bytes occupied on the volume.|
|`free_bytes`|`uint64`|The number of bytes still free on the volume.|

### `tenzir.metrics.enrich`

Contains a measurement the `enrich` operator, emitted once every second.

|Field|Type|Description|
|:-|:-|:-|
|`pipeline_id`|`string`|The ID of the pipeline where the associated operator is from.|
|`run`|`uint64`|The number of the run, starting at 1 for the first run.|
|`hidden`|`bool`|True if the pipeline is running for the explorer.|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`operator_id`|`uint64`|The ID of the `enrich` operator in the pipeline.|
|`context`|`string`|The name of the context the associated operator is using.|
|`events`|`uint64`|The amount of input events that entered the `enrich` operator since the last metric.|
|`hits`|`uint64`|The amount of successfully enriched events since the last metric.|

### `tenzir.metrics.export`

Contains a measurement the `export` operator, emitted once every second per
schema. Note that internal events like metrics or diagnostics to not emit
metrics themselves.

|Field|Type|Description|
|:-|:-|:-|
|`pipeline_id`|`string`|The ID of the pipeline where the associated operator is from.|
|`run`|`uint64`|The number of the run, starting at 1 for the first run.|
|`hidden`|`bool`|True if the pipeline is running for the explorer.|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`operator_id`|`uint64`|The ID of the `export` operator in the pipeline.|
|`schema`|`string`|The schema name of the batch.|
|`schema_id`|`string`|The schema ID of the batch.|
|`events`|`uint64`|The amount of events that were imported.|

### `tenzir.metrics.import`

Contains a measurement the `import` operator, emitted once every second per
schema. Note that internal events like metrics or diagnostics to not emit
metrics themselves.

|Field|Type|Description|
|:-|:-|:-|
|`pipeline_id`|`string`|The ID of the pipeline where the associated operator is from.|
|`run`|`uint64`|The number of the run, starting at 1 for the first run.|
|`hidden`|`bool`|True if the pipeline is running for the explorer.|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`operator_id`|`uint64`|The ID of the `import` operator in the pipeline.|
|`schema`|`string`|The schema name of the batch.|
|`schema_id`|`string`|The schema ID of the batch.|
|`events`|`uint64`|The amount of events that were imported.|

### `tenzir.metrics.lookup`

Contains a measurement of the `lookup` operator, emitted once every second.

|Field|Type|Description|
|:-|:-|:-|
|`pipeline_id`|`string`|The ID of the pipeline where the associated operator is from.|
|`run`|`uint64`|The number of the run, starting at 1 for the first run.|
|`hidden`|`bool`|True if the pipeline is running for the explorer.|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`operator_id`|`uint64`|The ID of the `lookup` operator in the pipeline.|
|`context`|`string`|The name of the context the associated operator is using.|
|`live`|`record`|Information about the live lookup.|
|`retro`|`record`|Information about the retroactive lookup.|
|`context_updates`|`uint64`|The amount of times the underlying context has been updated while the associated lookup is active.|

The record `live` has the following schema:

|Field|Type|Description|
|:-|:-|:-|
|`events`|`uint64`|The amount of input events used for the live lookup since the last metric.|
|`hits`|`uint64`|The amount of live lookup matches since the last metric.|

The record `retro` has the following schema:

|Field|Type|Description|
|:-|:-|:-|
|`events`|`uint64`|The amount of input events used for the lookup since the last metric.|
|`hits`|`uint64`|The amount of lookup matches since the last metric.|
|`queued_events`|`uint64`|The total amount of events that were in the queue for the lookup.|

### `tenzir.metrics.memory`

Contains a measurement of the available memory on the host.

|Field|Type|Description|
|:-|:-|:-|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`total_bytes`|`uint64`|The total available memory, in bytes.|
|`used_bytes`|`uint64`|The amount of memory used, in bytes.|
|`free_bytes`|`uint64`|The amount of free memory, in bytes.|

### `tenzir.metrics.operator`

Contains input and output measurements over some amount of time for a single
operator instantiation.

|Field|Type|Description|
|:-|:-|:-|
|`pipeline_id`|`string`|The ID of the pipeline where the associated operator is from.|
|`run`|`uint64`|The number of the run, starting at 1 for the first run.|
|`hidden`|`bool`|True if the pipeline is running for the explorer.|
|`timestamp`|`time`|The time when this event was emitted (immediately after the collection period).|
|`operator_id`|`uint64`|The ID of the operator inside the pipeline referenced above.|
|`source`|`bool`|True if this is the first operator in the pipeline.|
|`transformation`|`bool`|True if this is neither the first nor the last operator.|
|`sink`|`bool`|True if this is the last operator in the pipeline.|
|`internal`|`bool`|True if the data flow is considered to internal to Tenzir.|
|`duration`|`duration`|The timespan over which this data was collected.|
|`starting_duration`|`duration`|The time spent to start the operator.|
|`processing_duration`|`duration`|The time spent processing the data.|
|`scheduled_duration`|`duration`|The time that the operator was scheduled.|
|`running_duration`|`duration`|The time that the operator was running.|
|`paused_duration`|`duration`|The time that the operator was paused.|
|`input`|`record`|Measurement of the incoming data stream.|
|`output`|`record`|Measurement of the outgoing data stream.|

The records `input` and `output` have the following schema:

|Field|Type|Description|
|:-|:-|:-|
|`unit`|`string`|The type of the elements, which is `void`, `bytes` or `events`.|
|`elements`|`uint64`|Number of elements that were seen during the collection period.|
|`approx_bytes`|`uint64`|An approximation for the number of bytes transmitted.|

### `tenzir.metrics.platform`

Signals whether the connection to the Tenzir Platform is working from the node's
perspective. Emitted once per second.

|Field|Type|Description|
|:-|:-|:-|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`connected`|`bool`|The connection status.|

### `tenzir.metrics.process`

Contains a measurement of the amount of memory used by the `tenzir-node` process.

|Field|Type|Description|
|:-|:-|:-|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`current_memory_usage`|`uint64`|The memory currently used by this process.|
|`peak_memory_usage`|`uint64`|The peak amount of memory, in bytes.|
|`swap_space_usage`|`uint64`|The amount of swap space, in bytes. Only available on Linux systems.|
|`open_fds`|`uint64`|The amount of open file descriptors by the node. Only available on Linux systems.|

### `tenzir.metrics.publish`

Contains a measurement of the `publish` operator, emitted once every second per
schema.

|Field|Type|Description|
|:-|:-|:-|
|`pipeline_id`|`string`|The ID of the pipeline where the associated operator is from.|
|`run`|`uint64`|The number of the run, starting at 1 for the first run.|
|`hidden`|`bool`|True if the pipeline is running for the explorer.|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`operator_id`|`uint64`|The ID of the `publish` operator in the pipeline.|
|`topic`|`string`|The topic name.|
|`schema`|`string`|The schema name of the batch.|
|`schema_id`|`string`|The schema ID of the batch.|
|`events`|`uint64`|The amount of events that were published to the `topic`.|

### `tenzir.metrics.subscribe`

Contains a measurement of the `subscribe` operator, emitted once every second
per schema.

|Field|Type|Description|
|:-|:-|:-|
|`pipeline_id`|`string`|The ID of the pipeline where the associated operator is from.|
|`run`|`uint64`|The number of the run, starting at 1 for the first run.|
|`hidden`|`bool`|True if the pipeline is running for the explorer.|
|`timestamp`|`time`|The time at which this metric was recorded.|
|`operator_id`|`uint64`|The ID of the `subscribe` operator in the pipeline.|
|`topic`|`string`|The topic name.|
|`schema`|`string`|The schema name of the batch.|
|`schema_id`|`string`|The schema ID of the batch.|
|`events`|`uint64`|The amount of events that were retrieved from the `topic`.|

## Examples

Show the CPU usage over the last hour:

```c
metrics
| where #schema == "tenzir.metrics.cpu"
| where timestamp > 1 hour ago
| put timestamp, percent=loadavg_1m
```

<details>
<summary>Output</summary>

```json
{
  "timestamp": "2023-12-21T12:00:32.631102",
  "percent": 0.40478515625
}
{
  "timestamp": "2023-12-21T11:59:32.626043",
  "percent": 0.357421875
}
{
  "timestamp": "2023-12-21T11:58:32.620327",
  "percent": 0.42578125
}
{
  "timestamp": "2023-12-21T11:57:32.614810",
  "percent": 0.50390625
}
{
  "timestamp": "2023-12-21T11:56:32.609896",
  "percent": 0.32080078125
}
{
  "timestamp": "2023-12-21T11:55:32.605871",
  "percent": 0.5458984375
}
```
</details>

Get the current memory usage:

```c
metrics
| where #schema == "tenzir.metrics.memory"
| sort timestamp desc
| tail 1
| put current_memory_usage
```

<details>
<summary>Output</summary>

```json
{
  "current_memory_usage": 1083031552
}
```
</details>

Show the total pipeline ingress in bytes for every day over the last week,
excluding pipelines run in the Explorer:

```c
metrics
| where #schema == "tenzir.metrics.operator"
| where timestamp > 1 week ago
| where hidden == false and source == true
| summarize bytes=sum(output.approx_bytes) by timestamp resolution 1 day
```

<details>
<summary>Output</summary>

```json
{
  "timestamp": "2023-11-08T00:00:00.000000",
  "bytes": 79927223
}
{
  "timestamp": "2023-11-09T00:00:00.000000",
  "bytes": 51788928
}
{
  "timestamp": "2023-11-10T00:00:00.000000",
  "bytes": 80740352
}
{
  "timestamp": "2023-11-11T00:00:00.000000",
  "bytes": 75497472
}
{
  "timestamp": "2023-11-12T00:00:00.000000",
  "bytes": 55497472
}
{
  "timestamp": "2023-11-13T00:00:00.000000",
  "bytes": 76546048
}
{
  "timestamp": "2023-11-14T00:00:00.000000",
  "bytes": 68643200
}
```

</details>

Show the three operator instantiations that produced the most events in total
and their pipeline IDs:

```c
metrics
| where #schema == "tenzir.metrics.operator"
| where output.unit == "events"
| summarize events=max(output.elements) by pipeline_id, operator_id
| sort events desc
| head 3
```

<details>
<summary>Output</summary>

```json
{
  "pipeline_id": "70a25089-b16c-448d-9492-af5566789b99",
  "operator_id": 0,
  "events": 391008694
}
{
  "pipeline_id": "7842733c-06d6-4713-9b80-e20944927207",
  "operator_id": 0,
  "events": 246914949
}
{
  "pipeline_id": "6df003be-0841-45ad-8be0-56ff4b7c19ef",
  "operator_id": 1,
  "events": 83013294
}
```
</details>

Get the disk usage over time:

```c
metrics
| where #schema == "tenzir.metrics.disk"
| sort timestamp
| put timestamp, used_bytes
```

<details>
<summary>Output</summary>

```json
{
  "timestamp": "2023-12-21T12:52:32.900086",
  "used_bytes": 461834444800
}
{
  "timestamp": "2023-12-21T12:53:32.905548",
  "used_bytes": 461834584064
}
{
  "timestamp": "2023-12-21T12:54:32.910918",
  "used_bytes": 461840302080
}
{
  "timestamp": "2023-12-21T12:55:32.916200",
  "used_bytes": 461842751488
}
```
</details>

Get the memory usage over time:

```c
metrics
| where #schema == "tenzir.metrics.memory"
| sort timestamp
| put timestamp, used_bytes
```

<details>
<summary>Output</summary>

```json
{
  "timestamp": "2023-12-21T13:08:32.982083",
  "used_bytes": 48572645376
}
{
  "timestamp": "2023-12-21T13:09:32.986962",
  "used_bytes": 48380682240
}
{
  "timestamp": "2023-12-21T13:10:32.992494",
  "used_bytes": 48438878208
}
{
  "timestamp": "2023-12-21T13:11:32.997889",
  "used_bytes": 48491839488
}
{
  "timestamp": "2023-12-21T13:12:33.003323",
  "used_bytes": 48529952768
}
```
</details>
