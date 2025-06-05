---
title: "Clear failed partitions from the cache"
type: bugfix
authors: lava
pr: 2642
---

VAST now ejects partitions from the LRU cache if they fail to load with an I/O error.
