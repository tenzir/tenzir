---
title: Recursive record sorting
type: feature
authors:
  - mavam
  - codex
created: 2026-05-28T15:46:20Z
---

The `sort` function now recursively sorts record fields wherever they occur in
the sorted value, including records inside lists. Nested list element order is
preserved unless the list itself is being sorted.
