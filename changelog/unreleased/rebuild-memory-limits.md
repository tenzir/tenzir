---
title: Rebuild memory limits
type: bugfix
authors:
  - tobim
  - codex
prs:
  - 6216
created: 2026-06-02T11:17:41.584713Z
---

Rebuilds now limit how much partition data they load into memory at once.
This reduces the risk that automatic rebuilds or `tenzir-ctl rebuild start`
cause the node to run out of memory when many partitions are selected.

When memory is scarce, rebuilds load fewer partitions in one batch and retry the
remaining partitions later instead of materializing the full selected input set
up front.
