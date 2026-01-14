---
title: Length mismatch in expression evaluation for heterogeneous data
type: bugfix
authors:
  - raxyte
  - codex
created: 2025-12-31T12:58:04.073249Z
---

Expression evaluation could produce a length mismatch when processing heterogeneous data, potentially causing assertion failures. This affected various operations including binary and unary operators, field access, indexing, and aggregation functions.
