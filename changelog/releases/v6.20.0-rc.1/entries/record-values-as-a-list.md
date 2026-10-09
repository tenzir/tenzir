---
title: Record values as a list
type: feature
authors:
  - mavam
created: 2026-09-30T16:09:37.613635Z
---

The new `values` function returns the values of a record as a list, in field order:

```tql
from {
  user: {
    name: "alice",
    age: 42,
  },
}
select values=user.values()
```

```tql
{
  values: ["alice", 42],
}
```

Each value keeps its original type, so one list can mix strings, numbers, `null`, lists, and records. Only top-level fields contribute values, and nested records and lists stay intact. An empty record produces `[]`, and a null input produces `null`.

Together with `keys`, the function is the inverse of the two-argument form of `collect_record`: `collect_record(x.keys(), x.values())` equals `x`.
