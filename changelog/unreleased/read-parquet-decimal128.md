---
title: Read Decimal128 values from Parquet files
type: bugfix
authors:
  - raxyte
created: 2026-08-20T00:00:00.000000Z
---

The `read_parquet` operator now reads Arrow `Decimal128` values as strings by
default. Set `decimal_format="float"` to read them as potentially lossy real
values instead. Decimal values in maps remain unsupported in float mode.
