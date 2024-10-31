# to_hive

Writes events to a URI using hive partitioning.

```tql
to_hive uri:str, partition_by=list<field>, format=str, [timeout=duration, max_size=int]
```

## Description

Hive partitioning is a partitioning scheme where a set of fields is used to
partition events. For each combination of these fields, a directory is derived
under which all events with the same field values will be stored. For example,
if the events are partitioned by the fields `year` and `month`, then the files
in the directory `/year=2024/month=10` will contain all events where
`year == 2024` and `month == 10`.

### `uri: str`

The base URI for all partitions.

### `partition_by = list<field>`

A list of fields that will be used for partitioning. Note that these fields will
be elided from the output, as their value is already specified by the path.

### `format = str`

The name of the format that will be used for writing, for example `json` or
`parquet`. This will also be used for the file extension.

### `timeout = duration (optional)`

The time after which a new file will be opened for the same partition group.
Defaults to `5min`.

### `max_size = int (optional)`

The total file size after which a new file will be opened for the same partition
group. Note that files will typically be slightly larger than this limit,
because it opens a new file when only after it is exceeded. Defaults to `100M`.

## Examples

### Partition by a single field into local JSON files

```tql
from [{a: 0, b: 0}, {a: 0, b: 1}, {a: 1, b: 2}]
to_hive "/tmp/out/", partition_by=[a], format="json"
// This pipeline produces two files:
// -> /tmp/out/a=0/1.json:
//    {"b": 0}
//    {"b": 1}
// -> /tmp/out/a=1/2.json:
//    {"b": 2}
```

### Write a Parquet file into Azure Blob Store

Write as Parquet into the Azure Blob Filesystem, partitioned by year, month and
day.

```tql
to_hive "abfs://domain/bucket", partition_by=[year, month, day], format="parquet"
// -> abfs://domain/bucket/year=<year>/month=<month>/day=<day>/<num>.parquet
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
// -> s3://my-bucket/some/subdirectory/year=<year>/month=<month>/<num>.json
```
