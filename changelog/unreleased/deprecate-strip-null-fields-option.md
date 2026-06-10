---
title: Deprecate the `strip_null_fields` option
type: change
authors:
  - lava
  - claude
created: 2026-06-10T00:00:00Z
---

The `strip_null_fields` option of the `print_json` and `print_ndjson`
functions and the `write_json`, `write_ndjson`, and `write_tql` operators is
now deprecated. Use the `drop_null_fields` function or operator instead, for example
`x.drop_null_fields().print_ndjson()` instead of
`x.print_ndjson(strip_null_fields=true)`, or the `drop_null_fields` operator
before `write_json` instead of `write_json strip_null_fields=true`. Using
`strip_null_fields` still works but emits a deprecation warning.
