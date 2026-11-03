---
title: Filters, sorting, and limits after summaries in SQL databases
type: feature
authors:
  - mavam
created: 2026-11-02T20:27:39.323684Z
---

`from_duckdb`, `from_clickhouse`, `from_mysql`, and `from_microsoft_sql` now also run the `where`, `sort`, and `head` that follow a summary they compute, so the database returns only the groups that the pipeline keeps:

```tql
from_clickhouse table="events", host="clickhouse.example.com", tls=false
where severity >= 3
summarize src_ip, events=count()
where events >= 10
sort -events, src_ip
head 20
```

The filter applies to the groups after grouping, and the limit counts groups. This also makes `top` and `rare` followed by `head` return just the top or bottom values from the database. The database runs these steps with its own semantics: text compares and sorts under the collation of its column, and the database decides where `null` sorts, for example first in ascending order in MySQL and SQL Server, and last in DuckDB and ClickHouse. Filters with functions or with constants other than numbers, strings, booleans, and `null` still run in Tenzir, and so does everything after them.
