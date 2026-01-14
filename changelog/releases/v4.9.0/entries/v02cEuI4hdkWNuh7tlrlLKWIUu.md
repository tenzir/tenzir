---
title: "Implement more malleable lookup data for contexts"
type: feature
author: Dakostu
created: 2024-02-19T11:58:00Z
pr: 3920
---

The context match events now contain a new field `mode` that states the lookup
mode of this particular match.

The `enrich` operator gained a `--filter` option, which causes it to exclude
enriched events that do not contain a context.
