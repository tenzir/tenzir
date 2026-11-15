---
title: Truncated Log Analytics results warn instead of failing
type: change
authors:
  - mavam
created: 2026-11-12T16:35:46.815583Z
---

When a query result exceeds the Log Analytics record count or data size limit,
by default 500,000 rows or 64 MiB, the service returns the rows it kept
together with a partial error. `from_azure_log_analytics` now emits these rows
with a single warning instead of failing the pipeline:

```
warning: the result is incomplete
  = note: Log Analytics truncates a result beyond its record count or data size limit, by default 500,000 rows or 64 MiB
  = hint: narrow the query or its time range, return fewer columns, or limit or aggregate rows in the query, for example with `take` or `summarize`
```

Other partial errors, such as overflows or queries that exceed their memory
budget, still fail the pipeline because their rows may be incorrect.

Results with more than about two million values no longer fail with "result
exceeds the decoded value limit". Every result within the service's limits
now decodes.
