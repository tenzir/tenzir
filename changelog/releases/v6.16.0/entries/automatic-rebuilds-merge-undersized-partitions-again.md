---
title: Automatic rebuilds merge undersized partitions again
type: bugfix
authors:
  - tobim
created: 2026-09-04T08:13:32.658784Z
---

Automatic rebuilds consolidate undersized partitions again. Since v6.15.0,
the rebuilder silently skipped every batch whose merged result would still
fall below 80% of `tenzir.max-partition-size`. On nodes where the rebuild
memory budget limits how many partitions fit into one batch, this stopped
consolidation entirely: a run would report tens of thousands of candidates,
rebuild only a handful of partitions, and rescan the same candidates on
every subsequent run. The rebuilder now merges every batch that contains
more than one partition, and only skips lone partitions that have nothing
to merge with.
