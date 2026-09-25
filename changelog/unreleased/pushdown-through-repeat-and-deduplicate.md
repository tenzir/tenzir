---
title: Pushdown through repeat and deduplicate
type: change
authors:
  - mavam
created: 2026-09-24T17:52:40.304442Z
---

Pipeline optimization now carries field selection through `repeat` and `deduplicate`, so sources and readers such as `read_parquet` materialize only the fields that downstream operators and the deduplication keys use.

`repeat` also passes filters and row limits upstream. For example, `repeat 3 | head 10` lets a reader stop after 10 events, and `repeat 0` no longer reads any input.
