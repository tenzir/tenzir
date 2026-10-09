---
title: Read from and write to DuckDB databases
type: feature
authors:
  - mavam
created: 2026-10-01T18:22:18.933266Z
---

The new [`from_duckdb`](https://tenzir.com/docs/reference/operators/from_duckdb) and [`to_duckdb`](https://tenzir.com/docs/reference/operators/to_duckdb) operators read and write DuckDB databases. Use a file path for a local database, `:memory:` for an in-memory database, or a `quack:` URI for a remote server.

Read a table, run a query with `sql`, or follow new rows with `live=true`:

```tql
from_duckdb "events.duckdb", table="alerts"
```

When reading a table, `from_duckdb` hands downstream filters and limits to DuckDB, so that only the matching rows leave the database:

```tql
from_duckdb "events.duckdb", table="alerts"
where severity >= 3 and rule.starts_with("ET ")
head 100
```

Where DuckDB's semantics differ from TQL, as for `NaN`, arithmetic, struct fields, and strings under a collation, the query spells out TQL's semantics. Predicates that DuckDB cannot evaluate like TQL, such as comparisons on `DECIMAL` columns, run in the pipeline instead.

Excel workbooks can be read offline without downloading or loading an extension:

```tql
from_duckdb ":memory:",
  sql="SELECT * FROM read_xlsx('assets.xlsx', sheet='Assets', header=true)"
```

Use `sheet` to select a worksheet and `header=true` to turn its first row into field names.

Write events to a remote table, creating it if needed:

```tql
subscribe "alerts"
to_duckdb "quack:analytics.example.com:443",
  token=secret("duckdb-token"), table="alerts"
```

Remote SQL runs on the server. TLS is enabled by default except for loopback hosts. Quack is beta: failed writes can leave partial results.
