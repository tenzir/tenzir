---
title: Recursive files traversal of unreadable directories
type: bugfix
authors:
  - mavam
  - codex
created: 2026-04-24T07:09:11.344154Z
---

The `files` operator now skips unreadable child directories during recursive traversal, emits a warning for each skipped directory, and continues listing accessible siblings. The initial directory still must be readable unless `skip_permission_denied=true` is set.
