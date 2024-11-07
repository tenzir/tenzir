# save_s3

Saves bytes to an Amazon S3 object.

```tql
save_s3 uri:str, [anonymous=bool]
```

## Description

The `save_s3` operator writes bytes to an S3 object in an S3 bucket.

The connector tries to retrieve the appropriate credentials using AWS's
[default credentials provider
chain](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

:::info
Make sure to configure AWS credentials for the same user account that runs
`tenzir` and `tenzir-node`. The AWS CLI creates configuration files for the
current user under `~/.aws`, which can only be read by the same user account.

The `tenzir-node` systemd unit by default creates a `tenzir` user and runs as
that user, meaning that the AWS credentials must also be configured for that
user. The directory `~/.aws` must be readable for the `tenzir` user.
:::

If a config file `<prefix>/etc/tenzir/plugin/s3.yaml` or
`~/.config/tenzir/plugin/s3.yaml` exists, it is always preferred over the
default AWS credentials. The configuration file must have the following format:

```yaml
access-key: your-access-key
secret-key: your-secret-key
session-token: your-session-token (optional)
```

### `uri: str`

The path to the S3 object.

The syntax is
`s3://[<access-key>:<secret-key>@]<bucket-name>/<full-path-to-object>(?<options>)`.

Options can be appended to the path as query parameters, as per
[Arrow](https://arrow.apache.org/docs/r/articles/fs.html#connecting-directly-with-a-uri):

> For S3, the options that can be included in the URI as query parameters are
> `region`, `scheme`, `endpoint_override`, `allow_bucket_creation`, and
> `allow_bucket_deletion`.

### `anonymous = bool (optional)`

If to ignore any predefined credentials and try to save with anonymous
credentials.

## Examples

Read CSV from an object `obj.csv` in the bucket `examplebucket` and save it as
YAML to another bucket `examplebucket2`:

```tql
load_s3 "s3://examplebucket/obj.csv"
read_csv
write_yaml
load_s3 "s3://examplebucket2/obj.yaml"
```
