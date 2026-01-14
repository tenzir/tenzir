---
title: "Use dedicated partitions for each layout"
type: feature
author: tobim
created: 2022-03-21T09:26:37Z
pr: 2096
---

VAST now creates one active partition per layout, rather than having a single
active partition for all layouts.

The new option `vast.active-partition-timeout` controls the time after which an
active partition is flushed to disk. The timeout may hit before the partition
size reaches `vast.max-partition-size`, allowing for an additional temporal
control for data freshness. The active partition timeout defaults to 1 hour.
