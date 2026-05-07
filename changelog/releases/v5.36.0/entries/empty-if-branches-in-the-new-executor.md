---
title: Empty if branches in the new executor
type: bugfix
authors:
  - mavam
  - codex
prs:
  - 6128
created: 2026-05-06T08:58:37.14077Z
---

Empty `if` branches no longer crash when running pipelines with the new executor. For example, `if false {}` now behaves like an empty pass-through branch instead of triggering an internal assertion failure.
