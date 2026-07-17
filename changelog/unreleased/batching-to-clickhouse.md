---
title: Improved `to_clickhouse`
type: feature
authors:
  - IyeOnline
prs:
  - 6445
created: 2026-07-13T20:24:29+00:00
---

The `to_clickhouse` operator now re-batches events internally before inserting
them. Instead of sending one INSERT per incoming table slice, it accumulates
events per target table and flushes them once they reach `max_batch_rows` (default `65536`)
or have waited `batch_timeout` (default `1s`). This coalesces the tiny slices
produced by heterogeneous input, such as OCSF, into far larger and more efficient
inserts.

`to_clickhouse` can now also write `string` values directly into a ClickHouse
`JSON` column, as long as the string is a JSON object. This lets you serialize
heterogeneous events to JSON yourself and collapse them into a single schema for
maximum insert throughput.
