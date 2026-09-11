---
title: Robust Base64 decoding
type: bugfix
authors:
  - zedoraps
created: 2026-09-09T11:36:17.336358Z
---

Tenzir now handles edge cases in Base64-encoded field values without crashing
the node, allowing affected pipelines to continue processing.
