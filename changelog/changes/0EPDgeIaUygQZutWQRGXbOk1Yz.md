---
title: "Delay catalog lookups until catalog is ready"
type: bugfix
authors: dominiklohmann
pr: 5156
---

The `export`, `metrics`, `diagnostics` and `partitions` operators returned an
empty result when used before the node had successfully loaded its persisted
data. They now wait correctly.
