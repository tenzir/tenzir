---
title: "Implement `--limit` flag for the `chart` operator"
type: feature
authors: IyeOnline
pr: 4757
---

The `--limit` option for the TQL1 `chart` operator controls the previously
hardcoded upper limit on the number of events in a chart. The option defaults
to 10,000 events.
