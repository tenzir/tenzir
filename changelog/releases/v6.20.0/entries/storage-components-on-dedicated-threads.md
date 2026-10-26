---
title: Storage components on dedicated threads
type: change
authors:
  - tobim
created: 2026-10-07T08:30:00Z
---

Catalog lookups and the importer now run on dedicated threads instead of the
shared actor scheduler. Previously, a lookup over a large catalog could occupy
the scheduler for its whole duration, which delayed ingest, held back metrics
for minutes at a time, and left retro exports such as `diagnostics` or `export`
waiting for data. The threads are named `tnz.importer` and `tnz.lookup-<n>`,
next to the existing `tnz.index` and `tnz.catalog`, so `top -H` and thread
dumps show which component is busy.

The node also exposes USDT probes for the storage engine, usable with
`bpftrace` or `perf` without restarting the node. A per-second `*_tick`
probe for the importer, index, and catalog reports how late each component's
periodic work ran and how many messages wait in its mailbox, which shows
directly which component is overloaded. Further probes mark catalog lookups,
leases, merges, deferred and forced partition deletions, rebuild runs and
batches, partition transforms, index persistence and backpressure, and
importer backpressure. For example, to watch the importer's mailbox:

```sh
bpftrace -e 'usdt:/path/to/libtenzir.so:tenzir:importer_tick { printf("lag %d us, mailbox %d\n", arg0, arg1); }'
```

The log message that reports how long a catalog lookup took is emitted at the
verbose level again.
