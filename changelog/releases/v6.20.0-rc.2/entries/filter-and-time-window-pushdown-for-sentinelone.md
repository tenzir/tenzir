---
title: Filter and time-window pushdown for SentinelOne
type: change
authors:
  - mavam
created: 2026-10-05T11:23:24.945495Z
---

You can now query SentinelOne Data Lake entirely in TQL, without writing PowerQuery or passing `query`, `start`, or `end` to `from_sentinelone_data_lake`:

```tql
let $end = now()
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-console-token")
where timestamp >= $end - 1h and timestamp < $end
where severity > 3 and message.starts_with("alert")
head 100
```

The operator builds the remote query from supported filters, timestamp bounds, and limits. Exploratory pipelines return complete parsed events, including fields you never named. Restrictive projections such as `select event.type, account.id` can use efficient column retrieval instead. Selection is automatic: no retrieval flag or separate schema-discovery step is needed. Whole-event inspection, dynamic field access, and whole-record selections remain supported.

TQL bounds supply omitted time arguments, including historical windows outside the default past 24 hours. Every original filter still runs locally, and `head` counts events after that filtering. Generated reads continue beyond the default query result cap when needed, including across events with identical timestamps. All requests share the operator's timeout; a later-page failure marks the run incomplete even if earlier events have already been emitted. Filters that exceed the remote query's size limit stay local without preventing smaller filters from contributing to the request.

If no supported constraint reaches the source, the operator reports a runtime error before contacting SentinelOne. A rejected generated query fails rather than falling back to an unrestricted scan. You can still provide `query`, `start`, and `end` as explicit overrides. An explicit PowerQuery and its request window stay unchanged; downstream TQL filters and limits run locally to preserve the rows selected by SentinelOne's result cap.
