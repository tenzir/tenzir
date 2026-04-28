---
title: Static musl builds no longer crash on deep TQL expressions
type: bugfix
authors:
  - tobim
  - codex
pr: 6082
created: 2026-04-27T13:26:45.949058Z
---

Static musl builds of `tenzir` no longer crash on deeply nested generated TQL
expressions.

This affected generated pipelines with deeply nested expressions, for example
rules or transformations that expand into long left-associated operator chains.

The `tenzir` binary now links with a larger default thread stack size on musl,
which brings its behavior in line with non-static builds for these pipelines.
