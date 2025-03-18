# S3

[Amazon Simple Storage Service (S3)](https://aws.amazon.com/s3/) is an object
storage service. Tenzir can treat it like a local filesystem to read and write
files.

![S3](s3.svg)

:::tip URL Support
The URL scheme `s3://` dispatches to
[`load_s3`](../../../tql2/operators/load_s3.md) and
[`save_s3`](../../../tql2/operators/save_s3.md) for seamless URL-style use via
[`from`](../../../tql2/operators/from.md) and
[`to`](../../../tql2/operators/to.md).
:::

## Configuration

Follow the [standard configuration instructions](../README.md) to authenticate
with your AWS credentials.

## Examples

### Write to an S3 bucket

```tql
from {foo: 42}
to "s3://my-bucket/path/to/file.json.gz"
```

### Read from an S3 bucket

```tql
from "s3://my-bucket/path/to/file.json.gz"
```

```tql
{foo: 42}
```
