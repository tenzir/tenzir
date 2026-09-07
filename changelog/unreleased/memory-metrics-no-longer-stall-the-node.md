---
title: Memory metrics no longer stall the node
type: bugfix
authors:
  - tobim
created: 2026-09-04T12:30:00.000000Z
---

The `memory` metrics collector no longer reads `/proc/self/smaps_rollup` every
second. Producing that file makes the kernel walk every page table entry of the
process while holding its memory-map lock. On nodes with a large resident set
each read took a substantial fraction of a second, and during that time every
concurrent `mmap`, `munmap`, page fault, and allocator purge in the process had
to wait. Under load this serialized the whole node behind a single thread,
leaving all but one core idle while throughput collapsed.

The `procfs.smaps` record of `tenzir.metrics.memory` is gone. Its resident,
anonymous, and swap totals duplicated the `vm_rss_bytes`, `rss_anon_bytes`,
and `vm_swap_bytes` counters that `procfs.status` already reports from
`/proc/self/status`, which the kernel maintains incrementally and which cost
microseconds to read. The remaining value, `hugetlb_bytes`, now lives in
`procfs.status` as well. The fields `pss_bytes`, `private_clean_bytes`, and
`private_dirty_bytes` have no cheap source and were removed without
replacement.
