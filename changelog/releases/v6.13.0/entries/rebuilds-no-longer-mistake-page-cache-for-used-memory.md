---
title: Rebuilds no longer mistake page cache for used memory
type: bugfix
authors:
  - tobim
  - claude
created: 2026-08-12T07:51:45.597822Z
---

Rebuilds running in a memory-limited cgroup now make full progress per batch
instead of abandoning most of the partitions they selected.

A node computed the memory available to it from the cgroup's limit minus its
current charge. Under cgroup v2 that charge includes clean page cache, so
reading files inflated it even though the kernel reclaims those pages on
demand. Rebuilds read every partition they merge, so they appeared to exhaust
their own memory budget and stopped early, logging:

```text
stops loading transform input before partition <uuid> after 12 partition(s);
live memory budget is full
```

Reclaimable page cache and reclaimable slab no longer count towards a cgroup's
consumed memory. Memory that needs writeback or swap first — dirty pages, pages
under writeback, and shared memory — still counts. Readings taken from
`/proc/meminfo` were already correct and are unchanged.

Because rebuilds now see the memory they actually have, each batch admits more
partitions and takes correspondingly longer to complete. Raise
`tenzir.automatic-rebuild` to run more batches concurrently and shrink each one.
