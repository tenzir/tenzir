---
title: "Clear failed partitions from the cache"
type: bugfix
author: lava
created: 2022-10-20T10:52:56Z
pr: 2642
---

VAST now ejects partitions from the LRU cache if they fail to load with an I/O error.
