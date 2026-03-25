---
title: Optimize `in` operator and fix eq/neq null semantics
type: bugfix
authors:
  - jachris
pr: 5899
created: 2026-03-12T00:00:00Z
---

The `in` operator for list expressions is up to 33x faster. Previously it
created and finalized entire Arrow arrays for every element comparison, causing
severe overhead for expressions like `EventID in [5447, 4661, ...]`.

Additionally, comparing a typed null value with `==` now returns `false` instead
of `null`, and `!=` returns `true`, fixing a correctness issue with null
handling in equality comparisons.
