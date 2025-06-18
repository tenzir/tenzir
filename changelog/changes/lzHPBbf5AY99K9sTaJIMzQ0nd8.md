---
title: "Write homogenous partitions from the partition transformer"
type: change
authors: lava
pr: 2277
---

Partition transforms now always emit homogenous partitions, i.e., one schema per
partition. This makes compaction and aging more efficient.
