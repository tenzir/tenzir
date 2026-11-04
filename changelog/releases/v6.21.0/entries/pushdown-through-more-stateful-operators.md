---
title: Pushdown through more stateful operators
type: change
authors:
  - mavam
created: 2026-11-02T19:48:28.000000Z
---

Pipeline optimization now carries field selection through `unroll`, `sample`, `reorder`, `delay`, `throttle`, `assert`, `assert_throughput`, `ai_prompt`, and `ai_decide`, so sources and readers such as `read_parquet` materialize only the fields that downstream operators and these operators use.

`unroll` also lets filters on other fields run before it. `delay` and `throttle` without `drop` pass a downstream `head` upstream, so readers can stop early. `ai_prompt` and `ai_decide` let filters on fields other than their result run before them, so they send no requests for the events that these filters drop. Without a filter on the result, they also pass a downstream `head` upstream, so sources that support limits, such as `read_parquet`, stop early.
