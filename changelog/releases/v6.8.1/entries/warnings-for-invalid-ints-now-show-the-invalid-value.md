---
title: Warnings for invalid ints now show the invalid value
type: bugfix
authors:
  - zedoraps
  - claude
prs:
  - 6465
created: 2026-07-22T13:45:34.279251Z
---

Warnings from `int()` and `uint()` now show the invalid value:

```text
= note: tried to convert: invalid
```
