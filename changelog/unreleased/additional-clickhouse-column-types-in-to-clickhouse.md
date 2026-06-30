---
title: Additional ClickHouse column types in `to_clickhouse`
type: feature
authors:
  - IyeOnline
  - claude
prs:
  - 6396
created: 2026-06-30T08:45:33.215753Z
---

The `to_clickhouse` operator can now append to existing tables whose columns use
`LowCardinality(...)` or a `DateTime64` of any scale and timezone. Previously,
both were rejected with an `unsupported ClickHouse type` error even though the
data could be written without loss.

`LowCardinality` columns accept the plain inner value (for example, a `string`
into `LowCardinality(String)`), and `DateTime64(N[, 'tz'])` columns accept Tenzir
`time` values, truncated to the column's precision:

```tql
// Table: CREATE TABLE events (
//   id Int64,
//   name LowCardinality(String),
//   ts DateTime64(3, 'UTC')
// ) ENGINE = MergeTree ORDER BY id
from {id: 0, name: "alpha", ts: 2024-01-01T12:00:00.123456Z}
to_clickhouse table="events", host="localhost", mode="append"
// stored as name="alpha", ts="2024-01-01 12:00:00.123"
```

This also covers nullable and nested forms such as
`LowCardinality(Nullable(String))` and `Nullable(DateTime64(6))`.

The new `low_cardinality=` argument lets `to_clickhouse` *create*
`LowCardinality(String)` columns, analogous to `json=`. Listed fields are
created as `LowCardinality` instead of plain `String`:

```tql
from {id: 0, name: "alpha"}
to_clickhouse table="events", primary=id, mode="create", low_cardinality=name
// creates `name` as LowCardinality(Nullable(String))
```

Unlike `json`, every listed field must be present in the first event (its inner
type is inferred from the data). `low_cardinality` is only supported for string
columns.
