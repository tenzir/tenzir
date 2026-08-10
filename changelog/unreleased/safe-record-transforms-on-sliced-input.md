---
title: Safe record transforms on sliced input
type: bugfix
authors:
  - mavam
  - codex
created: 2026-08-10T13:49:26.625186Z
---

The `select_matching()` and `drop_matching()` functions and the `unroll`
operator no longer crash when they transform nested records from a sliced
multi-row batch. These operations now preserve record values and nulls after
slicing.
