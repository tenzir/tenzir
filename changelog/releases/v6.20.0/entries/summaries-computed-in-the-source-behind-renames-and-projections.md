---
title: Summaries computed in the source behind renames and projections
type: feature
authors:
  - mavam
created: 2026-10-13T20:59:05.030999Z
---

Sources that compute a `summarize` in their database or query engine now also do so when renames, assignments, or projections sit between the source and the `summarize`:

```tql
from_duckdb "events.duckdb", table="alerts"
select ip=src_ip, ts, severity
where severity >= 3
summarize ip, alerts=count(), last=max(ts)
```

This applies to `from_duckdb`, `from_clickhouse`, `from_mysql`, `from_microsoft_sql`, and `from_sentinelone_data_lake`. The summary may sit behind `select` and `drop` that keep the fields it reads, and behind `select`, `set`, and plain assignments that copy fields, such as `select ip=src_ip` or `ip = src_ip`. The source then groups and aggregates its own fields, and the result keeps the new names. When the source finds that it cannot compute the summary after all, it runs these operators and the `summarize` on its events instead, with the same result. Assignments to fields that the summary does not read do not matter, unless they call a function, which could fail the pipeline. A computed value for a field that the summary reads, such as `ip = src_ip.string()`, an assignment of anything but a record literal to `this`, `head`, and filters that stay in the pipeline still keep the summary in Tenzir.
