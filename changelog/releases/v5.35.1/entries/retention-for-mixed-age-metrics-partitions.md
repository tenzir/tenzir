---
title: Retention for mixed-age metrics partitions
type: bugfix
pr: 6086
authors:
  - tobim
  - codex
created: 2026-04-28T17:18:58.524365Z
---

Default retention policies now continue deleting metrics and diagnostics as their timestamps age into the retention window, even when older and newer events share a partition.

Previously, a partition that still contained newer events after retention could be skipped by later retention runs, leaving those events behind after they expired.
