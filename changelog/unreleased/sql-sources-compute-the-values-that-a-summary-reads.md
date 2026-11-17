---
title: SQL sources compute the values that a summary reads
type: change
authors:
  - mavam
created: 2026-11-15T18:08:56.210266Z
---

SQL sources now take over a `summarize` that reads computed values, including values that a `set` before it assigns. `from_duckdb`, `from_mysql`, `from_microsoft_sql`, `from_clickhouse`, and `from_hydrolix` then compute those values with the semantics of the database:

```tql
from_duckdb "events.duckdb", table="events"
level = severity * 2
summarize src_ip, worst=max(level)
```

DuckDB computes `worst` as `MAX(("severity" * 2))`. An assignment that the summary does not read keeps the operators in the pipeline if it may fail, such as a function call.
