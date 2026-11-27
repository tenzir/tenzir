---
title: to_clickhouse compresses inserted data by default
type: change
authors:
  - jachris
created: 2026-11-26T11:05:49.638494Z
---

The `to_clickhouse` operator now compresses the data it inserts with ZSTD, which reduces network traffic to ClickHouse at a moderate CPU cost. ClickHouse decompresses the data transparently, so existing pipelines keep working without changes.

Use `compress=false` to send uncompressed data, for example when Tenzir and ClickHouse run on the same host:

```tql
to_clickhouse table="events", compress=false
```
