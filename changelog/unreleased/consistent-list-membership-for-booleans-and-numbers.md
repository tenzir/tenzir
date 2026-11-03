---
title: Consistent list membership for booleans and numbers
type: bugfix
authors:
  - mavam
created: 2026-11-02T14:57:12.342893Z
---

The `in` operator no longer matches booleans with numbers. It now holds
exactly where `==` holds for some element of the list, so `false` no longer
matches `0`, and `true` no longer matches `1`:

```tql
from {x: false}, {x: 0}, {x: 5}
where x in [0, 5]
```

```tql
{x: 0}
{x: 5}
```

Previously, the filter also kept `{x: false}`, although `false == 0` is
`false`. Numbers still match across their types, so `5 in [5.0]` remains
`true`. As with `==`, a comparison with a secret is now `null` with a
warning rather than `false`.
