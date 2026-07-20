---
title: Gracefully handle rebuild and compaction failures
type: bugfix
authors:
  - IyeOnline
  - claude
prs:
  - 6452
created: 2026-07-17T12:00:00.000000Z
---

Corrupt partitions could previously bring down more than just the rebuild
that touched them. The compactor now skips partitions that failed to compact
instead of retrying them on every run. A catalog lookup over a damaged partition
synopsis no longer terminates the node, and neither does importing events whose
record batches fail to concatenate — the affected buffer is dropped with an error.

A corrupt or truncated partition or store file could also crash a node
repeatedly and stall reads across the whole node, even though `import` kept
working. The node now detects such a file, renames it aside by appending
`.broken` so `rebuild` no longer picks it up again, and continues rebuilding
all other partitions normally. Each quarantine is logged with the file and
error, tracked in the rebuilder's status, and reported through a new
`tenzir.metrics.rebuild_quarantine` event, emitted only when a partition is
actually quarantined.
