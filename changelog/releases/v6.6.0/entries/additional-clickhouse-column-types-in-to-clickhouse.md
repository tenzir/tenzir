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

The `to_clickhouse` operator now supports more ClickHouse column types.

When appending to an existing table, it can now write to `LowCardinality(String)`
columns and to `DateTime64(N[, 'tz'])` columns of any scale and timezone.
Previously both were rejected with an `unsupported ClickHouse type` error even
though the data could be written without loss. A `string` is written into a
`LowCardinality(String)` column (the server adds the `LowCardinality` wrapper),
and `time` values are written into `DateTime64(N)` columns, truncated to the
column's precision. This also covers the nullable forms
`LowCardinality(Nullable(String))` and `Nullable(DateTime64(N))`.

The new `low_cardinality=` argument creates `LowCardinality(String)` columns,
analogous to `json=`. Listed fields are created as `LowCardinality` instead of
plain `String`:

```tql
from {id: 0, name: "alpha"}
to_clickhouse table="events", primary=id, mode="create", low_cardinality=name
// creates `name` as LowCardinality(Nullable(String))
```

Every listed field must be present in the first event, because its inner type is
inferred from the data.

`LowCardinality` support is limited to string inner types: the bundled ClickHouse
client library does not support `LowCardinality` over numeric or other fixed-size
types.
