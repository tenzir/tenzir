# from_file

:::warning
This operator is still under active development.
:::

Reads one or multiple files using from a filesystem.

```tql
from_file url:string, [watch=bool, remove=bool, path_field=field { … }]
```

## Description

### `url: string`

URL or local filesystem path where data should be read from.

The characters `*` and `**` have a special meaning. `*` matches everything
except `/`. `**` matches everything including `/`. The sequence `/**/` can also
match nothing. For example, `foo/**/bar` matches `foo/bar`.

### `watch = bool (optional)`

In addition to processing all existing files, this option keeps the operator
running, watching for new files that also match the given URL.

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
applicable. This is using the same logic as [`from`](from.md).

## Examples

### Read every `.csv` file from S3

```tql
from_file `s3://my-bucket/**.csv`
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
