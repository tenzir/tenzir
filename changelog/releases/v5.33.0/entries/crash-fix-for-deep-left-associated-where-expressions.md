---
title: Crash fix for deep left-associated where expressions
type: bugfix
authors:
  - tobim
  - codex
pr: 6068
created: 2026-04-23T10:04:04.944386Z
---

Tenzir no longer segfaults on some very deep left-associated boolean
expressions in `where` clauses due to source-location handling.
