---
title: Faster and leaner Parquet reads
type: change
authors:
  - mavam
created: 2026-09-23T07:04:37.07789Z
---

`read_parquet` no longer buffers the entire file when it comes first in `from_file`, `from_s3`, `from_google_cloud_storage`, or `from_azure_blob_storage`. It reads the footer, then fetches only the columns and row groups the pipeline needs, and stops once it has enough events. Memory use stays bounded by a few row groups, so you can read files larger than the available memory.

Reads also do less work: `read_parquet` decodes only the nested fields the pipeline uses, skips row groups whose statistics show they cannot match a subsequent `where`, and avoids materializing columns that hold a single value or only nulls.
