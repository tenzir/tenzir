# Metrics

:::caution Experimental
This page describes the new, experimental metrics system. It should be
considered unstable and may change at any time. The documentation for the
legacy metrics system is available [here](metrics/legacy_metrics.md).
:::

Metrics are stored as internal events in the database. To access these events,
use `export --internal` followed by `where #schema == "<name>"`, where `<name>`
is one of the following:

## `tenzir.metrics.operator`

Contains input and output measurements over some amount of time for a single
operator instantiation.

|Field|Type|Description|
|-:|:-|-|
|`pipeline_id`|`string`|The ID of the pipeline where the associated operator is from.|
|`hidden`|`bool`|True if the pipeline is running for the explorer.|
|`operator_id`|`uint64`|The ID of the operator inside the pipeline referenced above.|
|`source`|`bool`|True if this is the first operator in the pipeline.|
|`transformation`|`bool`|True if this is neither the first nor the last operator.|
|`sink`|`bool`|True if this is the last operator in the pipeline.|
|`internal`|`bool`|True if the data flow is considered to internal to Tenzir.|
|`timestamp`|`time`|The time when this event was emitted (immediately after the collection period).|
|`duration`|`duration`|The timespan over which this data was collected.|
|`input`|`record`|Measurement of the incoming data stream.|
|`output`|`record`|Measurement of the outgoing data stream.|

The records `input` and `output` have the following schema:

|Field|Type|Description|
|-:|:-|-|
|`unit`|`string`|The type of the elements, which is `void`, `bytes` or `events`.|
|`elements`|`uint64`|Number of elements that were seen during the collection period.|
|`approx_bytes`|`uint64`|An approximation for the number of bytes transmitted.|

### Examples

Show the total pipeline ingress in bytes for every day over the last week,
excluding pipelines are only run for the explorer:

~~~c
export --internal |
where #schema == "tenzir.metrics.operator" |
where timestamp > 1 week ago |
where hidden == false && source == true  |
summarize bytes=sum(output.approx_bytes) by timestamp resolution 1 day
~~~

<details>
<summary>Output</summary>

~~~json
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
~~~
</details>

Show the three operator instantiations that produced the most events in total
and their pipeline:

~~~c
export --internal |
where #schema == "tenzir.metrics.operator" |
where output.unit == "events" |
summarize events=max(output.elements) by pipeline_id, operator_id |
sort events desc |
head 3
~~~

<details>
<summary>Output</summary>

~~~json
{
  "pipeline_id": "13",
  "operator_id": 0,
  "events": 391008694
}
{
  "pipeline_id": "12",
  "operator_id": 0,
  "events": 246914949
}
{
  "pipeline_id": "0",
  "operator_id": 1,
  "events": 83013294
}
~~~
</details>
