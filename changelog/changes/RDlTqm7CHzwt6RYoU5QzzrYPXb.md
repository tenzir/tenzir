---
title: Add "-total" metric keys for schema-dependent metrics
type: feature
authors: Dakostu
pr: 2682
---

VAST has three new metrics: `catalog.num-partitions-total`,
`catalog.num-events-total`, and `ingest-total` that sum up all schema-based
metrics by their respective schema-based metric counterparts.
