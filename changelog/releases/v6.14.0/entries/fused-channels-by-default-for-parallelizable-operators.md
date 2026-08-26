---
title: Fuse channels by default
type: change
authors:
  - aljazerzen
created: 2026-08-26T12:22:41.957594Z
---

By default, most operators will now not run concurrently. This change will
significantly reduce operating memory usage and memory usage under full
backpressure. It can also lead to 40% lower maximum pipeline troughput.

Pipelines that need the higher throughput ceiling can opt back into it
with an explicit directive, e.g.:

```tql
// parallelism: 4
```
