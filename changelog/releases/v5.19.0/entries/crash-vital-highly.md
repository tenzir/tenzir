---
title: "Dynamic clean up of expired keys in `deduplicate`"
type: change
author: raxyte
created: 2025-10-22T12:02:00Z
pr: 5534
---

The `deduplicate` operator now also considers the timeouts set when calculating
frequency of cleaning up expired state. This resuts in lower memory usage if
a timeout is under `15min`.
