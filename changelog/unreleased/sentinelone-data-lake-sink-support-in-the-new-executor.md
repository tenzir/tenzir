---
title: SentinelOne Data Lake sink support in the new executor
type: bugfix
authors:
  - mavam
  - codex
pr: 6081
created: 2026-04-24T18:42:39.346763Z
---

The `to_sentinelone_data_lake` operator now works in pipelines that run on the new executor. Previously, using it there failed before the pipeline could send events.

```tql
from {message: "hello"}
to_sentinelone_data_lake "https://example.com", token="TOKEN"
```
