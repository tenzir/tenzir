# partitions

Retrieves metadata about events stored at a node.

```tql
partitions [predicate:expr]
```

## Description

The `partitions` operator shows a summary of candidate partitions at a node.

### `predicate: expr (optional)`

Show only partitions which would be considered for pipelines of the form
`export | where <expr>` instead of returning all data.

## Schemas

Tenzir emits partition information with the following schema:

### `tenzir.partition`

Contains detailed information about a partition.

|Field|Type|Description|
|:-|:-|:-|
|`uuid`|`string`|The unique ID of the partition in the UUIDv4 format.|
|`memusage`|`uint64`|The memory usage of the partition in bytes.|
|`diskusage`|`uint64`|The disk usage of the partition in bytes.|
|`events`|`uint64`|The number of events contained in the partition.|
|`min_import_time`|`time`|The time at which the first event of the partition arrived at the `import` operator.|
|`max_import_time`|`time`|The time at which the last event of the partition arrived at the `import` operator.|
|`version`|`uint64`|The version number of the internal partition storage format.|
|`schema`|`string`|The schema name of the events contained in the partition.|
|`schema_id`|`string`|A unique identifier for the physical layout of the partition.|
|`store`|`record`|Resource information about the partition's store.|
|`indexes`|`record`|Resource information about the partition's indexes.|
|`sketches`|`record`|Resource information about the partition's sketches.|

The records `store`, `indexes`, and `sketches` have the following schema:

|Field|Type|Description|
|:-|:-|:-|
|`url`|`string`|The URL of the resource.|
|`size`|`uint64`|The size of the resource.|

## Examples

Get an overview of the memory and disk requirements for all stored data sorted
by schema:

```tql
partitions
summarize schema,
  events=sum(events),
  diskusage=sum(diskusage),
  memusage=sum(memusage)
sort schema
```

Get an upper bound for the number of events that could contain the IP address
127.0.0.1:

```tql
partitions :ip == 127.0.0.1
summarize candidates=sum(events)
```

See how many partitions contain a non-null value for the field `hostname`:

```tql
partitions hostname != null
```
