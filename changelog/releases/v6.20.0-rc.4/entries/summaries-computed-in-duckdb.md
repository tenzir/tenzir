---
title: Summaries computed in DuckDB
type: feature
authors:
  - mavam
created: 2026-10-12T11:10:44.79994Z
---

`from_duckdb` now computes a `summarize` that directly follows it in the database, so DuckDB returns one row per group instead of every matching row:

```tql
from_duckdb "events.duckdb", table="alerts"
where severity >= 3
summarize src_ip, alerts=count(), last=max(ts)
```

This applies to table reads whose summary groups by boolean, integer, string, or time fields and uses `count`, `min`, or `max`, including `top` and `rare`. All other summaries, and summaries after a filter that stays in the pipeline, still run in Tenzir with the same result. The groups then arrive in the order in which DuckDB returns them rather than in the order of their first row, which a table does not define anyway. Use `sort` when you need a particular order.
