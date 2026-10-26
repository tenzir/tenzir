---
title: Summaries computed in ClickHouse, MySQL, and SQL Server
type: feature
authors:
  - mavam
created: 2026-10-12T15:54:30.943967Z
---

`from_clickhouse`, `from_mysql`, and `from_microsoft_sql` now compute a `summarize` that directly follows them in the database, so the database returns one row per group instead of every matching row:

```tql
from_mysql table="events", host="db.example.com", database="soc"
where severity >= 3
summarize source, alerts=count(), first=min(id)
```

This applies to table reads whose summary uses `count`, `min`, or `max`, and groups by fields that the database compares exactly like TQL: integers and text in all three, plus booleans, UUIDs, and time values in ClickHouse and SQL Server, and enums and IP addresses in ClickHouse. Text groups by its bytes, so values that differ only in case or trailing spaces stay apart, as in TQL. This includes `top` and `rare`. All other summaries, and summaries after a filter that stays in the pipeline, still run in Tenzir with the same result. The groups arrive in the order in which the database returns them. Use `sort` when you need a particular order.
