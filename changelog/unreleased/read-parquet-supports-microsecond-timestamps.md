---
title: read_parquet supports microsecond timestamps
type: bugfix
authors:
  - zedoraps
  - claude
prs:
  - 6426
created: 2026-07-08T20:22:43.899546Z
---

Reading Parquet files written by other systems crashed when they contained timestamp columns with non-nanosecond precision, such as files written by Apache Spark or to Apache Iceberg tables, which mandate microseconds. `read_parquet` now converts such columns to Tenzir's nanosecond precision on read.
