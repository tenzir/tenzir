# Cloud Storage

[Cloud Storage](https://cloud.google.com/storage) is Google's object storage
service. Tenzir can treat it like a local filesystem to read and write files.

![Google Cloud Storage](gcs.svg)

:::tip URL Support
The URL scheme `gs://` dispatches to
[`load_gcs`](../../../tql2/operators/load_gcs.md) and
[`save_gcs`](../../../tql2/operators/save_gcs.md) for seamless URL-style use via
[`from`](../../../tql2/operators/from.md) and
[`to`](../../../tql2/operators/to.md).
:::

## Configuration

You need to configure appropriate credentials using Google's [Application
Default Credentials](https://google.aip.dev/auth/4110).

## Examples

### Write an event to a file in a bucket

```tql
from {foo: 42}
to "gs://bucket/path/to/file.json"
```

### Read events from a file in a bucket

```tql
from "gs://bucket/path/to/file.json"
```

```tql
{foo: 42}
```
