---
title: Large unroll output stability
type: bugfix
authors:
  - mavam
  - codex
created: 2026-04-24T08:22:17.329074Z
---

The `unroll` operator no longer crashes when expanding very large lists into output that exceeds Arrow's per-array capacity.
