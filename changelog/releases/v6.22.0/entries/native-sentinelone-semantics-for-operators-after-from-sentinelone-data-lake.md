---
title: Native SentinelOne semantics for operators after from_sentinelone_data_lake
type: change
authors:
  - mavam
created: 2026-11-05T17:40:02.738076Z
---

`from_sentinelone_data_lake` now hands SentinelOne the operators that follow it, as long as PowerQuery can spell them: `where`, `select`, `summarize` with `count()` and `count(x)`, `sort` after a `select` or `summarize`, `head`, and `top` or `rare`. Those operators leave the pipeline and run in SentinelOne with SentinelOne's semantics:

```tql
from_sentinelone_data_lake "https://example.sentinelone.net",
  token=secret("sentinelone-token"),
  start=now() - 1h
where event.type == "DNS Resolved"
summarize endpoint.name, lookups=count()
sort -lookups
head 10
```

- **Filters:** `where` becomes a `filter` that compares case-sensitively. `!=` also keeps events without the field, numbers compare as doubles, and before a `summarize`, a numeric string may compare equal to a number. Filters on `timestamp` also narrow the request window.
- **Reads:** filters and a `head` on whole events become a LOG query, which pages through complete events without a cap. All other reads become a PowerQuery, which now ends in `| limit 1600000` unless it ends in a `head`, so that SentinelOne's default cap of 1,000 rows no longer applies.
- **Summaries:** SentinelOne groups events without the field under `null`, and sorts `null` first. A field that exists only as nested fields, such as `endpoint` for `endpoint.name`, is `null` in SentinelOne. Counts are integers also with `raw=true`.
- **Queries:** the operators after `query` now also run in SentinelOne, appended to your query, and see its result. A `query` without operators after it goes out unchanged, with SentinelOne's default cap. If your query ends in `|` or inside a comment or string, the operators stay in the pipeline.

Before, `from_sentinelone_data_lake` only preselected events in SentinelOne and filtered them again in Tenzir, and computed summaries in SentinelOne only where it could verify the result, with further queries otherwise. It now sends a single query. A query that SentinelOne rejects fails the pipeline. Results that SentinelOne reports as incomplete, such as the default cap of 1,000 rows or omitted events, now produce a warning instead of an error, as does a PowerQuery result with 1,600,000 rows.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics:

```tql
from_sentinelone_data_lake "https://example.sentinelone.net",
  token=secret("sentinelone-token"),
  start=now() - 1h,
  pushdown=false
where event.type == "DNS Resolved"
```
