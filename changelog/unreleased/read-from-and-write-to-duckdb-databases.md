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

Write events to a remote table, creating it if needed:

```tql
subscribe "alerts"
to_duckdb "quack:analytics.example.com:443",
  token=secret("duckdb-token"), table="alerts"
```

Remote SQL runs on the server. TLS is enabled by default except for loopback hosts. Quack is beta: failed writes can leave partial results.
