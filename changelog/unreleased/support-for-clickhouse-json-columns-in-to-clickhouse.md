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

The `to_clickhouse` operator can now send data to existing `JSON` and
`Nullable(JSON)` columns on the ClickHouse server. Each event's field is
serialized to JSON text and inserted, so records and their nested structures land
in the column as JSON:

```tql
from {id: 0, payload: {a: 1, b: [2, 3]}}
to_clickhouse table="events", mode="append"
```

Absent values are written as `{}`. A null value becomes `{}` for a plain `JSON`
column and SQL `NULL` for a `Nullable(JSON)` column. Because ClickHouse `JSON`
columns only accept objects, non-record values are written as `{}` and a warning
is emitted.

When creating a table, the new `json` argument selects top-level fields to create
as `JSON` columns instead of the inferred type. It accepts a single field or a
list of fields, and the columns are created even if the field is absent from the
first event used for creation:

```tql
from {id: 0, payload: {a: 1, b: [2, 3]}}
to_clickhouse table="events", primary=id, json=[payload, raw]
```
