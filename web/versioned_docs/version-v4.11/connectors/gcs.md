---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# gcs

Loads bytes from a Google Cloud Service object. Saves bytes to a Google Cloud
Service object.

## Synopsis

Loader:

```
gcs [--anonymous] <object>
```

Saver:

```
gcs [--anonymous] <object>
```

## Description

The `gcs` loader connects to a GCS bucket to acquire raw bytes from a GCS
object. The `gcs` saver writes bytes to a GCS object in a GCS bucket.

The connector tries to retrieve the appropriate credentials using Google's
[Application Default Credentials](https://google.aip.dev/auth/4110).

### `<object>` (Loader, Saver)

The path to the GCS object.

The syntax is `gs://<bucket-name>/<full-path-to-object>(?<options>)`. The
`<options>` are query parameters. Per the [Arrow
documentation](https://arrow.apache.org/docs/r/articles/fs.html#connecting-directly-with-a-uri), the following options exist:

> For GCS, the supported parameters are `scheme`, `endpoint_override`, and
> `retry_limit_seconds`.

### `--anonymous` (Loader, Saver)

Ignore any predefined credentials and try to load/save with anonymous
credentials.

## Examples

Read JSON from an object `log.json` in the folder `logs` in `bucket`.
Note how the `gcs` loader is used automatically, when the URL scheme is `gs://`.

```
from gcs gs://bucket/logs/log.json
from gs://bucket/logs/log.json
```

Read JSON from an object `test.json` in `bucket`, but using a different
GCS-compatible endpoint:

```
from gcs gs://bucket/test.json?endpoint_override=gcs.mycloudservice.com
```
