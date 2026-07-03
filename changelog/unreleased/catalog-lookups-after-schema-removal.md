---
title: Catalog lookups after schema removal
type: bugfix
authors:
  - tobim
  - codex
prs:
  - 6411
created: 2026-07-03T09:34:36.758056Z
---

Tenzir nodes no longer terminate with an internal error when catalog maintenance removes the last partition for a schema while lookups continue. Queries now treat the removed schema as absent.
