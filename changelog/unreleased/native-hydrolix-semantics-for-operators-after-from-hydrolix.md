---
title: Native Hydrolix semantics for operators after from_hydrolix
type: change
authors:
  - mavam
created: 2026-11-10T08:44:28.584453Z
---

`from_hydrolix` now hands Hydrolix the operators that follow it in a `table` read, as `from_clickhouse` does for ClickHouse: `where`, `select`, `drop`, `summarize` with `count`, `min`, and `max`, `sort`, `head`, and `top` or `rare` followed by `head`, including the operators after a `summarize`. These operators leave the pipeline and run in Hydrolix with the semantics of its ClickHouse engine. For example, a comparison with `NULL` yields `NULL`, so `not (n == 1)` and `s != "a"` no longer keep rows where the field is `null`. Before, `from_hydrolix` added some filters and summaries to its query and ran everything else in Tenzir.

A query that Hydrolix rejects fails the pipeline with Hydrolix's error, for example when it compares a column with a value of another type. A field that the table lacks in the expected shape, such as an element of a column that is no tuple, fails the read before it runs any query. Both errors suggest `pushdown=false`, which keeps every operator in the pipeline with TQL semantics:

```tql
from_hydrolix "https://acme.hydrolix.live", token=secret("hydrolix-token"), table="logs.events", pushdown=false
where not (severity == 3)
```

Hydrolix reads only the partitions within the time range that a query sets on the table's primary timestamp. Put a filter on that timestamp directly after `from_hydrolix`, so that it constrains the table itself:

```tql
let $since = now() - 1h
from_hydrolix "https://acme.hydrolix.live", token=secret("hydrolix-token"), table="logs.events"
where timestamp > $since
summarize src_ip, events=count()
```

A filter after a `head` or a `summarize` reads the result of that operator and does not restrict the time range. With `pushdown=false`, no filter reaches the query, so a cluster that requires a time range rejects it. In that case, read with `sql` and a query that sets the time range itself.

A field `a.b` now names the element `b` of the tuple `a`, while `this["a.b"]` names the column `a.b`. For a table with a catch-all column, Hydrolix runs `where`, `sort`, and `head` on the fields of the reconstructed events, as long as each field comes from a single column, but a `select`, `drop`, or `summarize` fails the read and suggests `pushdown=false`.

A field below a map column reads the value of its key in Hydrolix. For example, `where catchall.class_uid == "1001"` reads `catchall['class_uid']` from the `catchall` column of the type `Map(String, Nullable(String))`, which keeps the fields that Hydrolix's default transform does not map. A missing key yields the default value of the map's values: `null` for nullable values, so `catchall.missing == null` holds, but an empty string or `0` otherwise, where Tenzir would see no field. Only maps with string keys have fields that operators can address.

Hydrolix may reject a query that reads a field below a `JSON` column in one of the subqueries that `from_hydrolix` generates, for example for a `select` or for the operators after a `head`. Use `pushdown=false` in that case, or write the query yourself with `sql`, including any `SETTINGS` that your cluster needs. A query with `sql` runs as you wrote it, with the settings of your session.
