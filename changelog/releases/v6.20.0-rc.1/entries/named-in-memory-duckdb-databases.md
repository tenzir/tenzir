---
title: Named in-memory DuckDB databases
type: feature
authors:
  - mavam
created: 2026-10-01T15:59:16.637203Z
---

The `from_duckdb` and `to_duckdb` operators now support named in-memory databases such as `:memory:alerts`. Pipelines in the same node that use the same name share a database, so you can read data that another running pipeline writes without creating a file:

```tql
from_duckdb ":memory:alerts", table="alerts"
```

Start the reader after a writer has created the table. The database remains available while at least one operator holds it open. Closing the last operator discards its data; reopening that name starts empty. Different names and separate processes remain isolated. Exactly `:memory:` and an empty path still create private databases. SQL queries against named databases remain read-only in `from_duckdb`.
