---
title: Rebuild no longer trips over concurrently deleted partitions
type: bugfix
authors:
  - tobim
  - claude
prs:
  - 6433
created: 2026-07-10T00:00:00.000000Z
---

The index no longer maintains its own set of persisted partitions alongside
the catalog. This bookkeeping could drift from the catalog's view when
partitions were deleted while a rebuild was in flight, causing repeated
`index skips unknown partition` warnings and needless retries of partitions
that no longer existed.

Partition transforms now detect inputs that vanished from disk—for example
because compaction or the disk monitor deleted them concurrently—skip them
gracefully, and report them back, so rebuild drops them from the run instead
of retrying them. As part of this cleanup the index also stops writing the
vestigial `index.bin` state file, which no released version reads back.
