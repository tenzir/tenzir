---
title: Support for ClickHouse JSON columns in to_clickhouse
type: feature
authors:
  - IyeOnline
  - claude
prs:
  - 6381
created: 2026-06-24T17:05:29.099901Z
---

The `to_clickhouse` operator can now send data to existing `JSON` columns on the
ClickHouse server. Each event's field is serialized to JSON text and inserted, so
records, lists, and nested structures land in the column as JSON:

```tql
from {id: 0, payload: {a: 1, b: [2, 3]}}
to_clickhouse table="events", mode="append"
```

Null or absent values are written as `{}`. Tenzir does not create `JSON` columns
itself; the column must already exist in the target table.
