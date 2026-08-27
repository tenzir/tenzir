---
title: The catalog owns storage maintenance
type: change
authors:
  - tobim
created: 2026-08-27T14:20:00.000000Z
---

Rebuilds, compaction, and disk-budget eviction now run inside the catalog
rather than in the separate rebuilder, compactor, and disk-monitor actors,
which are gone. Each of those actors used to ask the catalog which partitions
existed and then work through the answer, but the answer went stale the moment
it was handed over: partitions merge, get erased, and become inputs to other
transforms while a run is under way, and each actor found out only when the
work failed. The catalog decides one partition at a time against its own live
state, so nothing it decides can go stale before it acts.

Two long-standing behaviors improve as a result. Eviction now orders
partitions by the import time of the events they hold, where the disk monitor
ordered by the partition file's modification time — which a rebuild resets, so
rewriting an old partition made it look freshly written and sent it to the
back of the queue. And a partition that is an input to a running transform is
no longer selected for erasure, instead of being erased and having its data
reappear through the transform's output.

All existing settings keep working unchanged, including
`tenzir.start.disk-budget-*`, `tenzir.automatic-rebuild`, and
`tenzir.rebuild-interval`. The compaction plugin's
`plugins.compaction.space.*` keys now alias the corresponding
`tenzir.start.disk-budget-*` settings, and take precedence where both are set.
`tenzir.compaction-slots` is new: it bounds how many compaction pipelines run
at once, independently of rebuild parallelism, so that a slow pipeline cannot
stall a rebuild.

`tenzir-ctl rebuild {start,stop,show}` and `tenzir-ctl compaction
{list,apply,run}` are unchanged for callers, except that `compaction run` now
rejects an unknown rule name instead of reporting success for a no-op.

Two reporting details changed. `rebuild show` returns the catalog's status
record rather than the rebuilder's, so anything parsing that output needs
updating. And `tenzir.metrics.rebuild`'s `queued_partitions` is now always
zero: the catalog selects one batch at a time against live state, so there is
no queue to report.
