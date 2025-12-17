---
title: "PRs 2360-2363"
type: feature
author: dominiklohmann
created: 2022-06-19T13:32:52Z
pr: 2360
---

The output `vast status --detailed` now shows metadata from all partitions under
the key `.catalog.partitions`. Additionally, the catalog emits metrics under the
key `catalog.num-events` and `catalog.num-partitions` containing the number of
events and partitions respectively. The metrics contain the schema name in the
field `metadata_schema` and the (internal) partition version in the field
`metadata_partition-version`.
