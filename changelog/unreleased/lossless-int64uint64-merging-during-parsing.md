---
title: Lossless int64/uint64 merging during parsing
type: change
authors:
  - IyeOnline
  - claude
created: 2026-05-05T16:34:33.815786Z
---

Parsing data that mixes `int64` and `uint64` values in the same field no longer
produces unnecessary table-slice splits, improving batching performance. Fields
like `flow_id` that are always non-negative but occasionally exceed the signed
integer limit of  `2^63 − 1` are now merged into a single `uint64` column where
possible, instead of being emitted as separate slices.
