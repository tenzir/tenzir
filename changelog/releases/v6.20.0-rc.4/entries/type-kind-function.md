---
title: The `type_kind` function
type: feature
authors:
  - IyeOnline
created: 2026-10-21T00:00:00.000000Z
---

The new `type_kind` function returns the kind of a value's type as a string,
such as `int`, `string`, `list`, or `record`:

```tql
from {x: 1}, {x: "a"}, {x: null}
kind = x.type_kind()
```

```tql
{x: 1, kind: "int"}
{x: "a", kind: "string"}
{x: null, kind: "null"}
```

A `null` value always has the kind `null`. Lists and records report `list` and
`record`, regardless of their elements and fields.

The `type_of` function is now deprecated. Replace `type_of(x).kind` with
`type_kind(x)`.
