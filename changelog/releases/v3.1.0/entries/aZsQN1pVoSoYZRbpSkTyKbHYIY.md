---
title: "Fix remaining partitions counter in the rebuilder"
type: bugfix
author: dominiklohmann
created: 2023-05-12T09:17:27Z
pr: 3147
---

The `rebuilder.partitions.remaining` metric sometimes reported wrong values when
partitions for at least one schema did not need to be rebuilt. We aligned the
metrics with the actual functionality.
