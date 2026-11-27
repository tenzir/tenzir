---
title: Native input from Apache Iceberg tables
type: feature
authors:
  - mavam
created: 2026-11-25T11:46:07.816555Z
---

The new experimental `from_iceberg` operator reads events from Apache Iceberg
tables through a REST catalog, using the same catalog, storage, and
authentication options as `to_iceberg`:

```tql
from_iceberg "security.ocsf",
  catalog="https://catalog.example.com",
  token=secret("iceberg_token")
where class_uid == 1001 and time >= 2026-07-01
select time, src_endpoint, dst_endpoint
head 100
```

`from_iceberg` reads Parquet and Avro data files with schema evolution (column
additions, field renames, and type promotions) and Merge-on-Read support (v2
position and equality delete files). Pass `decimal_format="float"` to decode
Iceberg `decimal` columns as floating-point numbers instead of the default
exact `"string"` representation.

Downstream `where`, `select`, and `head` stages push hints into the operator:
conservative hidden-partition and column-metrics pruning skip non-matching
manifests and data files while exact TQL predicates still run locally, column
projection reads only required columns (including a snapshot-metadata fast path
for unfiltered `summarize count()` queries when the manifest list confirms the
row count and zero delete files exist, as well as per-file manifest counting
for whole-file tasks without delete files), and `head` limits stop reading data
files as soon as enough matching rows are emitted.

To keep throughput high on tables with many files in object storage,
`from_iceberg` plans manifests in parallel and prefetches upcoming data files
concurrently while emitting files in deterministic order, preserving exact
checkpoint recovery across restarts.

The operator requests all table snapshots (`snapshot-loading-mode=ALL`) so
point-in-time reads (`snapshot_id`, `as_of`, and named branches or tags via
`ref`), bounded incremental append scans (`from_snapshot_id`,
`to_snapshot_id`), and continuous tailing of new appends (`live=true` with
`poll_interval`, defaulting to `10s`) work across catalogs such as Google
BigLake that default to loading only referenced snapshots:

```tql
from_iceberg "security.ocsf",
  catalog="https://catalog.example.com",
  live=true,
  poll_interval=30s
```
