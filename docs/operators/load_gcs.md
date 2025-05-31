---
title: load_gcs
category: Inputs/Bytes
example: 'load_gcs "gs://bucket/object.json"'
---
Loads bytes from a Google Cloud Storage object.

```tql
load_gcs uri:string, [anonymous=bool]
```

## Description

The `load_gcs` operator connects to a GCS bucket to acquire raw bytes from a GCS object.

The connector tries to retrieve the appropriate credentials using Google's
[Application Default Credentials](https://google.aip.dev/auth/4110).

### `uri: string`

The path to the GCS object.

The syntax is `gs://<bucket-name>/<full-path-to-object>(?<options>)`. The
`<options>` are query parameters. Per the [Arrow
documentation](https://arrow.apache.org/docs/r/articles/fs.html#connecting-directly-with-a-uri),
the following options exist:

> For GCS, the supported parameters are `scheme`, `endpoint_override`, and
> `retry_limit_seconds`.

### `anonymous = bool (optional)`

Ignore any predefined credentials and try to use anonymous
credentials.

## Examples

Read JSON from an object `log.json` in the folder `logs` in `bucket`.

```tql
load_gcs "gs://bucket/logs/log.json"
read_json
```

## See Also

[`save_gcs`](/reference/operators/save_gcs)
