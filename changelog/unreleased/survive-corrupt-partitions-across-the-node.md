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
