---
title: Pushdown through more operators
type: change
authors:
  - mavam
created: 2026-09-24T15:15:10.331493Z
---

Pipeline optimization now carries field selection through `tail`, `reverse`, `slice`, `repeat`, and `deduplicate`, so readers such as `read_parquet` materialize only the fields that downstream operators use. `slice` and `repeat` also pass row limits upstream, so a pipeline like `repeat 3 | head 10` lets the reader stop early, and `repeat 0` and `tail 0` no longer read any input.
