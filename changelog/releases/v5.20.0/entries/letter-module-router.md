---
title: "Improved memory allocation & metrics"
type: feature
author: IyeOnline
created: 2025-12-17T09:23:13Z
pr: [5512,5544]
---

We have expanded the `metrics "memory"` the application provides. When enabled,
you can now see metrics for memory allocated via different means. Since this
metrics collection affects every single allocation, it is currently disabled
by default. To enable collection of these statistics, you can set an
environment variable `TENZIR_ALLOC_STATS=true`.

:::info[Changed metric layout]
This change also changes the structure of `tenzir.metrics.memory`. The
system-wide stats `total_bytes`, `free_bytes` and `used_bytes` are now grouped
under a `system` key to differentiate them from the `process` memory usage and
per-component memory usage added in this release. These process metrics were
previously only found in `metrics.process`.
:::

We also switched the default memory allocator used on all
platforms to [mimalloc](https://github.com/microsoft/mimalloc), which may
reduce the effective memory usage of Tenzir.
