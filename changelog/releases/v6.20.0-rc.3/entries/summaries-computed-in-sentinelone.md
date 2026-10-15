---
title: Summaries computed in SentinelOne
type: feature
authors:
  - mavam
created: 2026-10-13T12:00:00Z
---

`from_sentinelone_data_lake` now lets SentinelOne compute a `summarize` that directly follows it, so the Data Lake returns one row per group instead of every event of the time window:

```tql
from_sentinelone_data_lake "https://example.sentinelone.net",
  token=secret("sentinelone-token"),
  start=now() - 1d
summarize event.type, dataSource.name, events=count()
```

This applies to summaries that group by fields and count events with `count()`, including `top` and `rare`. Use `start` and `end` to choose the time window: a `where` before the `summarize`, including one on `timestamp`, makes the summary run in Tenzir.

Tenzir checks every result from SentinelOne and summarizes the events itself when the result could differ from what TQL computes. For events without the group key, Tenzir reads just those events to confirm that the key is `null` in TQL as well, and not a record built from fields such as `endpoint.name`. Tenzir also summarizes locally when type inference turns different text such as `"true"` and `true` into the same key, or when SentinelOne omits events because of its limits. The result is the same either way. The groups arrive in the order in which SentinelOne returns them. Use `sort` when you need a particular order.
