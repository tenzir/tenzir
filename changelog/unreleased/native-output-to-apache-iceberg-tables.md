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

The new `to_iceberg` operator writes events to Apache Iceberg tables through a
REST catalog:

```tql
subscribe "ocsf"
ocsf::cast
to_iceberg "security.ocsf",
  catalog="https://catalog.example.com",
  partition_by=[class_uid, day(time)]
```

The operator creates missing tables from the first arriving events (`mode`
selects between `create_append`, `create`, and `append`) and evolves the table
schema continuously. It adds new fields at any nesting depth through a
metadata-only schema update before writing data files that carry them, so
heterogeneous streams like OCSF converge into one wide table without name
mappings or manual `ALTER TABLE` steps. Existing columns widen in place where
the Iceberg spec allows it (`int` to `long`, `float` to `double`). Values that
still do not fit are written as null with a warning; for required columns the
operator fails with an error instead, so data is never lost silently.

`partition_by` uses Iceberg's hidden partitioning: it accepts field paths and
the transforms `year`, `month`, `day`, `hour`, `bucket`, and `truncate`,
without materializing helper columns. Data files are zstd-compressed Parquet
with field IDs and per-column metrics, rotated by `max_size` and `timeout` and
committed as Iceberg snapshots that retry on top of concurrent updates.
Partitions buffer under a shared `buffer_size` budget, so high partition
cardinality produces fewer, larger files instead of many small ones.

The operator connects to S3 and S3-compatible object stores (`aws_iam`,
`s3_endpoint`, `s3_path_style`), catalogs taking bearer tokens (`token`), AWS
Glue and Amazon S3 Tables (`catalog_aws_service`), and Google BigLake with
`gs://` storage (`gcp_service_account_key`, or `gcp_auth=true` for Application
Default Credentials).
