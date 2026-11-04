---
title: Native DuckDB semantics for operators after from_duckdb
type: change
authors:
  - mavam
created: 2026-11-03T16:58:13.80077Z
---

`from_duckdb` now hands DuckDB the operators that follow it in a `table` read, as long as DuckDB can spell them: `where`, `select`, `drop`, `summarize`, `sort`, `head`, and `top` or `rare` followed by `head`. Those operators leave the pipeline and run in DuckDB with DuckDB's semantics. For example, `name != "alpha"` no longer keeps rows where `name` is `null`, text compares under the collation of its column, and `null` sorts last:

```tql
from_duckdb "events.duckdb", table="events"
where severity >= 3 and src_ip == 10.0.0.1
select src_ip, host=meta.host
sort -src_ip
head 100
```

Before, `from_duckdb` pushed only the filters and summaries that DuckDB computes exactly as TQL does, and computed the rest itself. A `live` read hands DuckDB its filters only.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics. Column pruning still applies:

```tql
from_duckdb "events.duckdb", table="events", pushdown=false
where name != "alpha"
```

A query that DuckDB rejects now fails the pipeline, for example a `select` of a column that the table lacks.
