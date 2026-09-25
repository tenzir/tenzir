---
title: Faster Parquet reads of columns with repeated values
type: change
authors:
  - mavam
created: 2026-09-25T13:40:14.147031Z
---

`read_parquet` now reads columns that hold the same value throughout a row group much faster. Such columns are common in normalized data: in OCSF events, fields like `metadata.product.name` or `metadata.version` often repeat one value for millions of events. Instead of materializing that value once per event, Tenzir now keeps a single copy for the whole batch.

In a benchmark of OCSF network activity events, this reduced the time of a full read by about 16%.
