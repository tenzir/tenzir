---
title: Diagnostics for lambda-valued let bindings
type: bugfix
authors:
  - mavam
  - codex
pr: 6241
created: 2026-05-31T06:17:30.259084Z
---

Invalid lambda-valued `let` bindings now produce a focused diagnostic instead of cascading into a generic evaluation warning and an internal error.

For example, `let $f = (x => x + 1)` now points at the lambda and suggests inlining it at the use site.
