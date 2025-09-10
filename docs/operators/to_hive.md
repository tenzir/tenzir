---
title: to_hive
category: Outputs/Events
example: 'to_hive "s3://…", partition_by=[x]'
---

Writes events to a URI using hive partitioning.

```tql
to_hive uri:string, partition_by=list<field>, format=string, [timeout=duration, max_size=int]
```

## Description

Hive partitioning is a partitioning scheme where a set of fields is used to
partition events. For each combination of these fields, a directory is derived
under which all events with the same field values will be stored. For example,
if the events are partitioned by the fields `year` and `month`, then the files
in the directory `/year=2024/month=10` will contain all events where
`year == 2024` and `month == 10`.

Files within each partition directory are named using UUIDv7 for guaranteed
uniqueness and natural time-based ordering. This prevents filename conflicts
when multiple processes write to the same partition simultaneously.

### `uri: string`

The base URI for all partitions.

### `partition_by = list<field>`

A list of fields that will be used for partitioning. Note that these fields will
be elided from the output, as their value is already specified by the path.

### `format = string`

The name of the format that will be used for writing, for example `json` or
`parquet`. This will also be used for the file extension.

### `timeout = duration (optional)`

The time after which a new file will be opened for the same partition group.
Defaults to `5min`.

### `max_size = int (optional)`

The total file size after which a new file will be opened for the same partition
group. Note that files will typically be slightly larger than this limit,
because it opens a new file when only after it is exceeded. Defaults to `100M`.

### `compression = string (optional)`

Compress the output files with the given compression algorithm. See docs for the
`compress` operator for supported compression algorithms.

## Examples

### Partition by a single field into local JSON files

```tql
from {a: 0, b: 0}, {a: 0, b: 1}, {a: 1, b: 2}
to_hive "/tmp/out/", partition_by=[a], format="json"
// This pipeline produces two files:
// -> /tmp/out/a=0/<uuid>.json:
//    {"b": 0}
//    {"b": 1}
// -> /tmp/out/a=1/<uuid>.json:
//    {"b": 2}
```

### Write a Parquet file into Azure Blob Store

Write as Parquet into the Azure Blob Filesystem, partitioned by year, month and
day.

```tql
to_hive "abfs://domain/bucket", partition_by=[year, month, day], format="parquet"
// -> abfs://domain/bucket/year=<year>/month=<month>/day=<day>/<uuid>.parquet
```

### Write partitioned JSON into an S3 bucket

Write JSON into S3, partitioned by year and month, opening a new file after
1 GB.

```tql
year = ts.year()
month = ts.month()
to_hive "s3://my-bucket/some/subdirectory",
  partition_by=[year, month],
  format="json",
  max_size=1G
// -> s3://my-bucket/some/subdirectory/year=<year>/month=<month>/<uuid>.json
```

## See Also

[`read_parquet`](/reference/operators/read_parquet),
[`write_bitz`](/reference/operators/write_bitz),
[`write_feather`](/reference/operators/write_feather),
[`write_parquet`](/reference/operators/write_parquet)
