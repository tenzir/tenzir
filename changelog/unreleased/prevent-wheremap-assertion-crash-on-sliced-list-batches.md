---
title: Prevent where/map assertion crash on sliced list batches
type: bugfix
authors:
  - IyeOnline
  - codex
pr: 5886
created: 2026-03-10T13:41:11.708137Z
---

Pipelines using chained list transforms such as `xs.where(...).map(...).where(...)` no longer trigger an internal assertion on sliced input batches.
