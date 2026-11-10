---
title: Native ClickHouse semantics for operators after from_clickhouse
type: change
authors:
  - mavam
  - zedoraps
created: 2026-11-09T11:12:24.158597Z
---

`from_clickhouse` now hands ClickHouse the operators that follow it in a `table` read, as far as ClickHouse can spell them: `where`, `select`, `drop`, `summarize` with `count`, `min`, and `max`, `sort`, `head`, and `top` or `rare` followed by `head`, including the operators after a `summarize`. Those operators leave the pipeline and run in ClickHouse with ClickHouse's semantics. For example, a comparison with `NULL` yields `NULL`, so `not (n == 1)` and `s != "a"` no longer keep rows where the field is `null`, and `null` sorts last:

```tql
from_clickhouse table="events", host="clickhouse.example.com"
where severity >= 3
summarize src_ip, events=count()
sort -events
head 10
```

Queries for the latest events, such as `sort -time | head 100`, also run in ClickHouse, which then returns only the rows that the pipeline keeps.

A query that ClickHouse rejects fails the pipeline, for example when it compares a column with a value of another type, such as a string column with an IP address, or names a column that the table lacks. Fields below a `JSON` column compare in ClickHouse only where all their values share a type. Where such a field holds an object, `x != null` holds and `x == null` does not, but a comparison with a value, such as `x == 5`, never matches. ClickHouse keeps no trace of an empty object, which reads like a missing field. A sort by such a field fails the read if a later operator needs its order, as in `sort x | head 1 | sort y`. A field `a.b` names the element `b` of the tuple `a`, while `this["a.b"]` names the column `a.b`.

For a table with a catch-all column, ClickHouse runs `where`, `sort`, and `head` on the fields of the reconstructed events, as long as each field comes from a single column. A `select`, `drop`, or `summarize` on such a table fails the read before it runs any query, and so does a field that spans several columns, an `ALIAS` or `MATERIALIZED` column, or a column that `from_clickhouse` cannot read. The error suggests `pushdown=false`.

A `table` read now fails instead of returning a partial result when it exceeds a limit of the session, such as `max_result_rows` with `result_overflow_mode = 'break'`, also with `pushdown=false`. A read with `sql` keeps the session's settings.

Stopping a pipeline now also stops a `from_clickhouse` or `from_hydrolix` read whose query is still sorting or summarizing on the server, instead of waiting until the server sends its first result.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics:

```tql
from_clickhouse table="events", host="clickhouse.example.com", pushdown=false
where not (severity == 3)
```

`from_hydrolix` keeps its previous behavior.
