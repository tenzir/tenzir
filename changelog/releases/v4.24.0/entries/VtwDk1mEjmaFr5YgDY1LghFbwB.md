---
title: "Implement `--limit` flag for the `chart` operator"
type: feature
author: IyeOnline
created: 2024-11-29T12:34:08Z
pr: 4757
---

The `--limit` option for the TQL1 `chart` operator controls the previously
hardcoded upper limit on the number of events in a chart. The option defaults
to 10,000 events.
