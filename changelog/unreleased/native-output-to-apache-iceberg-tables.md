---
title: Native output to Apache Iceberg tables
type: feature
authors:
  - zedoraps
  - claude
prs:
  - 6426
created: 2026-07-06T11:42:50.790809Z
---

The new `to_iceberg` operator writes events into an Apache Iceberg table
through a REST catalog, replacing the previous recipe of partitioned Parquet
via `to_s3`, hand-authored table metadata, and a scheduled PyIceberg commit
job:

```tql
subscribe "ocsf"
ocsf::cast
to_iceberg "security.ocsf", catalog="https://catalog.example.com",
  partition_by=[class_uid, day(time)]
```

The operator creates missing tables from the schema of the first arriving
events (`mode` selects between `create_append`, `create`, and `append`) and
evolves the table schema continuously: fields the table does not have yet —
at any nesting depth — are added through a metadata-only schema update before
any data file carries them, so heterogeneous streams like OCSF converge into
one wide table without name mappings or manual `ALTER TABLE` steps. Existing
columns too narrow for the incoming values widen in place where the Iceberg
spec allows it (`int` to `long`, `float` to `double`); individual values that
still cannot convert land as null without affecting neighboring rows.

Tables partition via Iceberg's hidden partitioning: `partition_by` accepts
field paths and the symbolic transforms `year`, `month`, `day`, `hour`,
`bucket`, and `truncate`, without materializing helper columns. Data files
are zstd-compressed Parquet with field IDs and per-column metrics, rotated by
`max_size` and `timeout`, and committed as verified snapshots that retry on
top of concurrent updates. Partitions buffer in memory under a shared
`buffer_size` budget before opening data files, so high partition
cardinality degrades file sizes gracefully instead of thrashing open
writers. S3 and S3-compatible object stores are supported
via `aws_iam`, `s3_endpoint`, and `s3_path_style`; catalogs taking bearer
tokens authenticate via `token`.

The operator ships as experimental: delivery is at-least-once, upgrading to
exactly-once automatically once pipeline checkpointing lands.
