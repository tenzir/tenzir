---
title: Faster drop_null_fields on heterogeneous data
type: bugfix
authors:
  - jachris
  - codex
created: 2026-03-31T10:34:08.17118Z
---

The `drop_null_fields` operator now avoids severe slowdowns on heterogeneous input with many null-shape changes by processing rows in larger groups with the same dropped-field pattern. This keeps pipelines that use `drop_null_fields` responsive on wide or highly variable event streams.
