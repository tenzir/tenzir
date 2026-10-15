---
title: Read events from Hydrolix
type: feature
authors:
  - mavam
created: 2026-10-14T10:00:50.000000Z
---

The new [`from_hydrolix`](https://tenzir.com/docs/reference/operators/from_hydrolix) operator reads events from [Hydrolix](https://hydrolix.io) tables and SQL queries, with the same URL and token as `to_hydrolix`:

```tql
let $since = now() - 1h
from_hydrolix "https://acme.hydrolix.live",
  token=secret("hydrolix-token"),
  table="security.events"
where time > $since and severity_id >= 4
summarize class_uid, events=count()
```

The operator queries the native interface of the Hydrolix query engine. When it reads a table, Hydrolix evaluates the `where`, `select`, and `head` operators that follow, and a `summarize` that only uses `count`, `min`, and `max`, so that it reads only the partitions and columns that the pipeline needs.
