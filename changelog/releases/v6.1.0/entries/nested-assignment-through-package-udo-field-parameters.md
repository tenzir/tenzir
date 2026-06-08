---
title: Nested assignment through package UDO field parameters
type: bugfix
authors:
  - mavam
  - codex
prs:
  - 6264
created: 2026-06-08T17:36:00.733767Z
---

Package UDO field parameters now support assignment through field accesses. This lets package operators write to nested fields of a field argument using dot syntax, string-literal bracket syntax, or dynamic index expressions.

For example, this package UDO body is now valid:

```tql
$field.subfield = $value
$field["quoted-field"] = $value
$field[$key] = $value
```

This is useful for mapping operators that accept an `event=` target and need to keep temporary state under that target instead of creating top-level scratch fields.
