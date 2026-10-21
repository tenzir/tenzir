---
title: Equality is total
type: change
authors:
  - aljazerzen
---

The `==` and `!=` operators now always answer and never warn. Operands that cannot be equal, including operands of different types, are simply not equal:

```tql
from {
  mismatch: "x" == 1d,
  mismatch_neq: "x" != 1d,
  with_null: null == 1d,
}
```

```tql
{
  mismatch: false,
  mismatch_neq: true,
  with_null: false,
}
```

Previously, only a `null` operand followed this rule, while two differently typed non-null operands emitted a warning and evaluated to `null`.

This also makes `in` consistent with the `==` it is defined in terms of, so `"x" in [1d]` evaluates to `false` and `"x" not in [1d]` evaluates to `true`. Denylists such as `where src not in bad` now keep events whose type does not occur in the list instead of silently dropping them.

The ordered comparisons `<`, `>`, `<=`, and `>=` remain partial and still evaluate to `null` for operands that have no shared order.
