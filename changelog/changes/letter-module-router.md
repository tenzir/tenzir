---
title: "Improved memory allocation & metrics"
type: feature
authors: IyeOnline
pr: [5512,5544]
---

We have expanded the `metrics "memory"` the application provides. When enabled,
you can now see metrics for memory allocated via different means. Since this
metrics collection affects every single allocation, it is currently disabled
by default. To enable collection of these statistics, you can set an
environment variable `TENZIR_ALLOC_STATS=true`.

With this change, we also switched the default memory allocator used on all
platforms to [mimalloc](https://github.com/microsoft/mimalloc), which may
reduce the effective memory usage of Tenzir.
