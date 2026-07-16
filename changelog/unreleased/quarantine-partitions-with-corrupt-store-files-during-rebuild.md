---
title: Quarantine partitions with corrupt store files during rebuild
type: bugfix
authors:
  - IyeOnline
  - claude
prs:
  - 6452
created: 2026-07-16T12:04:37.577917Z
---

A corrupt or truncated partition store file could crash a node repeatedly and
stall reads across the whole node, even though `import` kept working. The
node now detects such a partition, quarantines it so `rebuild` no longer
retries it forever, and continues rebuilding all other partitions normally.

Quarantined partitions are reported through the rebuilder's status and
through a new `tenzir.metrics.rebuild_quarantine` metric, so operators can
see how many partitions were quarantined without digging through logs.
