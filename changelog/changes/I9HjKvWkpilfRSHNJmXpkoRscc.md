---
title: "Rebatch undersized batches when rebuilding partitions"
type: feature
authors: dominiklohmann
pr: 2583
---

Rebuilding partitions now additionally rebatches the contained events to
`vast.import.batch-size` events per batch, which accelerates queries against
partitions that previously had undersized batches.
