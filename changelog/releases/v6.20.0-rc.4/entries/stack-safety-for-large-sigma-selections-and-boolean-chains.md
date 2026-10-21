---
title: Stack safety for large Sigma selections and boolean chains
type: bugfix
authors:
  - mavam
created: 2026-10-16T04:50:53.841854Z
---

Large Sigma selection lists and long `and` and `or` chains no longer exhaust the stack during evaluation. Previously, a Sigma rule with around 185 values in a selection could crash the entire pipeline.
