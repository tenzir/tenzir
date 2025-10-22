---
title: "Dynamic clean up of expired keys in `deduplicate`"
type: change
authors: raxyte
pr: 5534
---

The `deduplicate` operator now also considers the timeouts set when calculating
frequency of cleaning up expired state. This resuts in lower memory usage if
a timeout is under `15min`.
