---
title: Pushed-down aggregations follow the target's semantics
type: change
authors:
  - mavam
created: 2026-11-20T18:58:38.047106Z
---

The aggregations and functions that `from_azure_log_analytics` and `from_microsoft_defender` compute in KQL now follow KQL's semantics instead of imitating TQL's. For example, `summarize total=sum(x)` now runs as `sum(['x'])`, which yields `0` without values, and `mean` runs as `avg`, which yields `NaN`. `sqrt` and `log` yield `NaN` and `-Infinity` outside their domain, `count_distinct` counts empty strings as values, and `count(x)` counts the values that are not `null`, including empty strings.

The same holds for ClickHouse, DuckDB, and SQL Server: without values, ClickHouse's `min`, `max`, and `sum` yield the default value of their type, `all` and `any` yield `null` in ClickHouse and DuckDB, and DuckDB and SQL Server fail a query that takes the root of a negative number or the logarithm of zero. Use `pushdown=false` to compute these in Tenzir instead.
