---
title: "Prefer recent partitions for retro lookups"
type: change
authors: dominiklohmann
pr: 4636
---

The `lookup` operator now prefers recent data in searches for lookups against
historical data instead of using the order in which context updates arrive.
