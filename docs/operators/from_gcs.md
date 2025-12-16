---
title: from_gcs
category: Inputs/Events
example: 'from_gcs "gs://my-bucket/data/**.json"'
---

Reads one or multiple files from Google Cloud Storage.

```tql
from_gcs url:string, [anonymous=bool, watch=bool, remove=bool,
  rename=string->string, path_field=field, max_age=duration] { … }
```

## Description

The `from_gcs` operator reads files from Google Cloud Storage, with support for
glob patterns, automatic format detection, and file monitoring.

By default, authentication is handled by Google's Application Default
Credentials (ADC) chain, which may read from multiple sources:

- `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account key file
- User credentials from `gcloud auth application-default login`
- Service account attached to the compute instance (Compute Engine, GKE)
- Google Cloud SDK credentials

### `url: string`

URL identifying the Google Cloud Storage location where data should be read from.

The characters `*` and `**` have a special meaning. `*` matches everything
except `/`. `**` matches everything including `/`. The sequence `/**/` can also
match nothing. For example, `bucket/**/data` matches `bucket/data`.

The syntax is `gs://<bucket-name>/<full-path-to-object>(?<options>)`. The
`<options>` are query parameters. Per the [Arrow
documentation](https://arrow.apache.org/docs/r/articles/fs.html#connecting-directly-with-a-uri),
the following options exist:

> For GCS, the supported parameters are `scheme`, `endpoint_override`, and
> `retry_limit_seconds`.

### `anonymous = bool (optional)`

Use anonymous credentials instead of any configured authentication. This only
works for publicly readable buckets and objects.

Defaults to `false`.

### `watch = bool (optional)`

In addition to processing all existing files, this option keeps the operator
running, watching for new files that also match the given URL. Currently, this
scans the filesystem up to every 10s.

Defaults to `false`.

### `remove = bool (optional)`

Deletes files after they have been read completely.

Defaults to `false`.

### `rename = string -> string (optional)`

Renames files after they have been read completely. The lambda function receives
the original path as an argument and must return the new path.

If the target path already exists, the operator will overwrite the file.

The operator automatically creates any intermediate directories required for the
target path. If the target path ends with a trailing slash (`/`), the original
filename will be automatically appended to create the final path.

### `path_field = field (optional)`

This makes the operator insert the path to the file where an event originated
from before emitting it.

By default, paths will not be inserted into the outgoing events.

### `max_age = duration (optional)`

Only process files that were modified within the specified duration from the
current time. Files older than this duration will be skipped.

### `{ … } (optional)`

Pipeline to use for parsing the file. By default, this pipeline is derived from
the path of the file, and will not only handle parsing but also decompression if
applicable.

## Examples

### Read every JSON file from a bucket

```tql
from_gcs "gs://my-bucket/data/**.json"
```

### Read CSV files from a public bucket

```tql
from_gcs "gs://public-dataset/data.csv", anonymous=true
```

### Read Zeek logs continuously

```tql
from_gcs "gs://logs/zeek/**.log", watch=true {
  read_zeek_tsv
}
```

### Add source path to events

```tql
from_gcs "gs://data-bucket/**.json", path_field=source_file
```

### Read Suricata EVE JSON logs with custom parsing

```tql
from_gcs "gs://security-logs/suricata/**.json" {
  read_suricata
}
```
