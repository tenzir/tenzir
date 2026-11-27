---
title: Empty string partition values in to_iceberg
type: bugfix
authors:
  - zedoraps
created: 2026-11-26T11:44:47.111894Z
---

Readers of a table that `to_iceberg` writes now find the rows of a string partition when one of its values is the empty string. Previously, such a value removed the lower bound of the partition from the manifest list. Readers such as PyIceberg then skipped the manifest, so a filter like `k = 'a'` returned no rows, even for values other than the empty string.
