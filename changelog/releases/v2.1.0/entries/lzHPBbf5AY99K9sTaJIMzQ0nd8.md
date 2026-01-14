---
title: "Write homogenous partitions from the partition transformer"
type: change
author: lava
created: 2022-06-02T16:08:36Z
pr: 2277
---

Partition transforms now always emit homogenous partitions, i.e., one schema per
partition. This makes compaction and aging more efficient.
