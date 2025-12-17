---
title: "Prefer recent partitions for retro lookups"
type: change
author: dominiklohmann
created: 2024-10-02T14:38:52Z
pr: 4636
---

The `lookup` operator now prefers recent data in searches for lookups against
historical data instead of using the order in which context updates arrive.
