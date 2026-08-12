---
title: '`quantile` keeps its configuration across list rows'
type: bugfix
authors:
  - mavam
  - claude
created: 2026-08-12T05:16:22.000000Z
---

Calling `quantile` on lists no longer degrades to the minimum after the first
row. The per-row evaluation reset the aggregation state between rows and
incorrectly cleared the configured quantile along with it, so
`xs.quantile(q=0.5)` returned the median for the first event and the minimum
for every subsequent one.

The `quantile` aggregation now also participates in pipeline snapshots:
restoring a checkpoint preserves the accumulated t-digest state instead of
failing the restore.
