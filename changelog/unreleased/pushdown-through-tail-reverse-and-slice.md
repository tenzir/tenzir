---
title: Pushdown through tail, reverse, and slice
type: change
authors:
  - mavam
created: 2026-09-24T15:15:10.331493Z
---

Pipeline optimization now carries field selection through `tail`, `reverse`, and `slice`, so sources and readers such as `read_parquet` materialize only the fields that downstream operators use.

`slice` with non-negative `begin` and `end` and a positive `stride` also bounds how many events upstream must produce, including when followed by `head`. For example, `slice end=100` or `slice begin=10, stride=2 | head 5` lets a reader stop early. `tail 0` no longer reads any input.
