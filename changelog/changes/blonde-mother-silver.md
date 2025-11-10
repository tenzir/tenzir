---
title: "Improved Memory Usage"
type: bugfix
authors: [IyeOnline,jachris,raxyte]
pr: 5561
---

We have switched the allocator back to the system/glibc allocator, as the `mimalloc`
setup ran into issues on some systems.

With this, we also added many more detailed metrics to `metrics "memory"` that
should help us track down the cause of high memory usage.
