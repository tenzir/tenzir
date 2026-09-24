---
title: Efficient Parquet reads from files and object stores
type: change
authors:
  - mavam
created: 2026-09-23T07:04:37.07789Z
---

Reading Parquet from local files and object stores no longer downloads and buffers the entire file. `read_parquet` now reads the file's footer first, then fetches only the columns and row groups that the rest of the pipeline needs, and stops as soon as it has enough events.

For example, this pipeline reads two fields of the first 100 events from a large Parquet object on S3:

```tql
from_s3 "s3://logs/2026/events.parquet" {
  read_parquet
}
select timestamp, message
head 100
```

Instead of the whole object, Tenzir fetches the footer plus the `timestamp` and `message` columns of the first row groups, until it has 100 events. For wide files, or when you need only a few events, this cuts data transfer, memory use, and latency substantially. Full reads benefit as well: memory use stays bounded by a couple of row groups instead of growing with the file, so you can read Parquet files larger than the available memory.

This applies to `from_file`, `from_s3`, `from_google_cloud_storage`, and `from_azure_blob_storage` when `read_parquet` comes first in the nested pipeline. After a byte transformation such as `decompress_gzip`, `read_parquet` still buffers the full input.
