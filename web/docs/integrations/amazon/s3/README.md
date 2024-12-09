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

The connector tries to retrieve the appropriate credentials using AWS's
[default credentials provider
chain](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

Make sure to configure AWS credentials for the same user account that runs
`tenzir` and `tenzir-node`. The AWS CLI creates configuration files for the
current user under `~/.aws`, which can only be read by the same user account.

The `tenzir-node` systemd unit by default creates a `tenzir` user and runs as
that user, meaning that the AWS credentials must also be configured for that
user. The directory `~/.aws` must be readable for the `tenzir` user.

If a config file `<prefix>/etc/tenzir/plugin/s3.yaml` or
`~/.config/tenzir/plugin/s3.yaml` exists, it is always preferred over the
default AWS credentials. The configuration file must have the following format:

```yaml
access-key: your-access-key
secret-key: your-secret-key
session-token: your-session-token (optional)
```

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
