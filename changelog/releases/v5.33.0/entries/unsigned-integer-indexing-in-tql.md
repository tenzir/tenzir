---
title: Unsigned integer indexing in TQL
type: bugfix
authors:
  - mavam
  - codex
created: 2026-04-22T15:03:27.225491Z
---

Both list and record indexing in TQL now work with signed and unsigned integer indices.
This also applies to record field-position indexing and to the `get` function for records and lists.
