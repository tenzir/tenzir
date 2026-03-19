---
title: Fix over-reservation in partition_array for string/blob types
type: bugfix
authors:
  - jachris
pr: 5899
created: 2026-03-12T00:00:00Z
---

Splitting Arrow arrays for string and blob types no longer over-reserves memory.
Previously both output builders reserved the full input size each, using up to
twice the necessary memory.
