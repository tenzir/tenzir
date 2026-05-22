---
title: Faster drop_null_fields on heterogeneous data
type: bugfix
authors:
  - jachris
  - mavam
  - codex
pr: 5963
created: 2026-03-31T10:34:08.17118Z
---

The `drop_null_fields` operator is now much faster on heterogeneous input with many changing null patterns.
