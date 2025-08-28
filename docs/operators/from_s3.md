---
title: from_s3
category: Inputs/Events
example: 'from_s3 "s3://my-bucket/data/**.json"'
---

Reads one or multiple files from Amazon S3.

```tql
from_s3 url:string, [anonymous=bool, access_key=string, secret_key=string,
  session_token=string, role=string, external_id=string, watch=bool,
  remove=bool, rename=string->string, path_field=field] { … }
```

## Description

The `from_s3` operator reads files from Amazon S3, with support for glob
patterns, automatic format detection, and file monitoring.

By default, authentication is handled by AWS's default credentials provider
chain, which may read from multiple environment variables and credential files:

- `~/.aws/credentials` and `~/.aws/config`
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`
- EC2 instance metadata service
- ECS container credentials

### `url: string`

URL identifying the S3 location where data should be read from.

The characters `*` and `**` have a special meaning. `*` matches everything
except `/`. `**` matches everything including `/`. The sequence `/**/` can also
match nothing. For example, `bucket/**/data` matches `bucket/data`.

Supported URI format:
`s3://[<access-key>:<secret-key>@]<bucket-name>/<full-path-to-object>(?<options>)`

Options can be appended to the path as query parameters:
- `region`: AWS region (e.g., `us-east-1`)
- `scheme`: Connection scheme (`http` or `https`)
- `endpoint_override`: Custom S3-compatible endpoint
- `allow_bucket_creation`: Allow creating buckets if they don't exist
- `allow_bucket_deletion`: Allow deleting buckets

### `anonymous = bool (optional)`

Use anonymous credentials instead of any configured authentication.

Defaults to `false`.

### `access_key = string (optional)`

AWS access key ID for authentication.

### `secret_key = string (optional)`

AWS secret access key for authentication. Required if `access_key` is provided.

### `session_token = string (optional)`

AWS session token for temporary credentials.

### `role = string (optional)`

IAM role to assume when accessing S3.

### `external_id = string (optional)`

External ID to use when assuming the specified `role`.

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

### `{ … } (optional)`

Pipeline to use for parsing the file. By default, this pipeline is derived from
the path of the file, and will not only handle parsing but also decompression if
applicable.

## Examples

### Read every JSON file from a bucket

```tql
from_s3 "s3://my-bucket/data/**.json"
```

### Read CSV files using explicit credentials

```tql
from_s3 "s3://my-bucket/data.csv",
  access_key=secret("AWS_ACCESS_KEY"),
  secret_key=secret("AWS_SECRET_KEY")
```

### Read from S3-compatible service with custom endpoint

```tql
from_s3 "s3://my-bucket/data/**.json?endpoint_override=minio.example.com:9000&scheme=http"
```

### Read files continuously and assume IAM role

```tql
from_s3 "s3://logs/application/**.json", watch=true, role="arn:aws:iam::123456789012:role/LogReaderRole"
```

### Process files and move them to an archive bucket

```tql
from_s3 "s3://input-bucket/**.json",
  rename=(path => "archive/" + path)
```

### Add source path to events

```tql
from_s3 "s3://data-bucket/**.json", path_field=source_file
```

### Read Zeek logs with anonymous access

```tql
from_s3 "s3://public-bucket/zeek/**.log", anonymous=true {
  read_zeek_tsv
}
```

## See Also

[`from_file`](/reference/operators/from_file),
[`load_s3`](/reference/operators/load_s3),
[`save_s3`](/reference/operators/save_s3)
