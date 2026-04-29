---
title: Partition rebuild completion
type: bugfix
authors:
  - tobim
  - codex
pr: 6059
created: 2026-04-20T13:38:16.55446Z
---

Partition rebuilds now finish after persisting rebuilt partitions. Previously, rebuild jobs could remain stuck indefinitely even though the replacement partitions were written successfully.
