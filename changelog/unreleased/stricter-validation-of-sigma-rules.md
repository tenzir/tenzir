---
title: Stricter validation of Sigma rules
type: change
authors:
  - mavam
prs:
  - 106
created: 2026-08-12T16:32:43.339738Z
---

The `sigma` operator now validates rules before executing them and rejects invalid rules with actionable diagnostics instead of silently misinterpreting them. Rules using an unknown or unsupported modifier (for example `a|frobnicate: 1`) are now rejected instead of the modifier being silently ignored, conditions referencing an unknown search identifier report the identifier by name, and quantifier patterns such as `1 of selection_*` that match no search identifier are rejected instead of never matching. Rules declaring `sigma-version` with an unsupported major version now fail explicitly rather than being interpreted with older semantics.
