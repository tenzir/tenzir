---
title: Sigma processing metrics
type: feature
authors:
  - mavam
created: 2026-08-13T22:06:57.243075Z
---

The `sigma` operator now emits lightweight `tenzir.metrics.sigma` events for
each processed batch. The metrics report the number of input events, rule
evaluations, and matches, making it possible to monitor Sigma processing load
and match rates.
