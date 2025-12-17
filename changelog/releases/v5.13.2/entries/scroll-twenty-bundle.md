---
title: "`from_s3` operator"
type: feature
author: raxyte
created: 2025-08-28T23:27:47Z
pr: 5449
---

The `from_s3` operator reads files from Amazon S3 with support for glob
patterns, automatic format detection, and file monitoring.

```tql
from_s3 "s3://my-bucket/data/**.json"
```

The operator supports multiple authentication methods including default AWS
credentials, explicit access keys, IAM role assumption, and anonymous access
for public buckets:

```tql
from_s3 "s3://my-bucket/data.csv",
  access_key=secret("AWS_ACCESS_KEY"),
  secret_key=secret("AWS_SECRET_KEY")
```

For S3-compatible services, specify custom endpoints via URL parameters:

```tql
from_s3 "s3://my-bucket/data/**.json?endpoint_override=minio.example.com:9000&scheme=http"
```

Additional features include file watching for continuous ingestion, automatic
file removal or renaming after processing, and path field injection to track
source files in events.
