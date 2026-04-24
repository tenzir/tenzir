---
title: Recursive files traversal of unreadable directories
type: bugfix
authors:
  - mavam
  - codex
created: 2026-04-24T07:09:11.344154Z
---

The `files` operator now skips unreadable child directories during recursive traversal, emits a warning for each skipped directory by default, and continues listing accessible siblings. Set `skip_permission_denied=true` to ignore permission-denied paths silently: this suppresses warnings for skipped child directories and still makes an unreadable initial directory produce no events instead of an error. Non-permission filesystem errors continue to fail the pipeline.
