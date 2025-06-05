---
title: from_file
category: Inputs/Events
example: 'from_file "s3://data/**.json"'
---

:::warning[Under Active Development]
This operator is still under active development.
:::

Reads one or multiple files from a filesystem.

```tql
from_file url:string, [watch=bool, remove=bool, path_field=field] { … }
```

## Description

The `from_file` operator reads files from local filesystems or cloud storage,
with support for glob patterns, automatic format detection, and file
monitoring.

### `url: string`

URL or local filesystem path where data should be read from.

The characters `*` and `**` have a special meaning. `*` matches everything
except `/`. `**` matches everything including `/`. The sequence `/**/` can also
match nothing. For example, `foo/**/bar` matches `foo/bar`.

The URL can include additional options. For `s3://`, the options that can be
included in the URI as query parameters are `region`, `scheme`,
`endpoint_override`, `allow_bucket_creation`, and `allow_bucket_deletion`. For
`gs://`, the supported parameters are `scheme`, `endpoint_override`, and
`retry_limit_seconds`.

### `watch = bool (optional)`

In addition to processing all existing files, this option keeps the operator
running, watching for new files that also match the given URL. Currently, this
scans the filesystem up to every 10s.

Defaults to `false`.

### `remove = bool (optional)`

Deletes files after they have been read completely.

Defaults to `false`.

### `path_field = field (optional)`

This makes the operator insert the path to the file where an event originated
from before emitting it.

By default, paths will not be inserted into the outgoing events.

### `{ … } (optional)`

Pipeline to use for parsing the file. By default, this pipeline is derived from
the path of the file, and will not only handle parsing but also decompression if
applicable. This is using the same logic as [`from`](/reference/operators/from).

## Examples

### Read every `.csv` file from S3

```tql
from_file "s3://my-bucket/**.csv"
```

### Read every `.json` file in `/data` as Suricata EVE JSON

```tql
from_file "/data/**.json" {
  read_suricata
}
```

### Read all files from S3 continuously and delete them afterwards

```tql
from_file "s3://my-bucket/**", watch=true, remove=true
```

## See Also

[`from`](/reference/operators/from),
[`load_file`](/reference/operators/load_file)
