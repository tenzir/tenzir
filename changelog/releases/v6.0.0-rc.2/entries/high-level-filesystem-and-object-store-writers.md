---
title: High-level filesystem and object store writers
type: feature
author: raxyte
pr: 6053
created: 2026-04-30T13:00:01.571739Z
---

Four new high-level writer operators serialize events to local filesystems
and cloud object stores with rotation, hive-style partitioning, and
per-partition unique filenames:

- `to_file` writes to a local filesystem.
- `to_s3` writes to Amazon S3.
- `to_azure_blob_storage` writes to Azure Blob Storage.
- `to_google_cloud_storage` writes to Google Cloud Storage.

Each takes a printing subpipeline, a URL with optional `**` and `{uuid}`
placeholders, and rotation parameters. The `**` placeholder expands into a
hive partitioning hierarchy based on `partition_by`, and `{uuid}` ensures
each partition gets unique destination names:

```tql
subscribe "events"
to_s3 "s3://my-bucket/year=**/month=**/{uuid}.json",
  partition_by=[year, month] {
  write_ndjson
}
```

Files rotate automatically when the configured `max_size` or `timeout` is
reached, so long-running pipelines do not produce single huge objects.
