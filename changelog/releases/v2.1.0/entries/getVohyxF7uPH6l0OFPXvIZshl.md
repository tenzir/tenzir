---
title: "Add index metric for created active partitions"
type: feature
author: lava
created: 2022-06-13T17:18:05Z
pr: 2302
---

VAST emits the new metric `partition.events-written` when writing a partition to
disk. The metric's value is the number of events written, and the
`metadata_schema` field contains the name of the partition's schema.
