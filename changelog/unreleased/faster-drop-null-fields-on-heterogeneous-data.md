---
title: Faster drop_null_fields on heterogeneous data
type: bugfix
authors:
  - jachris
  - mavam
  - codex
pr: 5963
created: 2026-03-31T10:34:08.17118Z
---

The `drop_null_fields` operator is now much faster on heterogeneous input with many changing null patterns. In local 50,000-row benchmarks with 80 fields, `drop_null_fields | discard` improved from about 25–30 seconds to about 0.5 seconds. When unordered output is allowed, pipelines that write JSON after dropping fields improved from about 7–8 seconds to about 1 second.
