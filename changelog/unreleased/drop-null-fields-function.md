---
title: '`drop_null_fields` function'
type: feature
authors:
  - zedoraps
  - claude
created: 2026-06-05T19:42:25Z
---

The new `drop_null_fields` function strips fields whose value is `null` from a
record, mirroring the existing `drop_null_fields` operator. This lets you clean
optional fields inline in expressions, for example
`from_http "...", body=drop_null_fields({license: $license, version: 1, mapping: $mapping})`.
