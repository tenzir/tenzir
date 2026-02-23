---
title: Enhance `sort` function with `desc` and `cmp` parameters
type: feature
authors:
  - mavam
  - codex
pr: 5767
created: 2026-02-17T06:37:14.758079Z
---

The `sort` function now supports two new parameters: `desc` for controlling
sort direction and `cmp` for custom comparison logic via binary lambdas.

**Sort in descending order:**

```tql
from {xs: [3, 1, 2]}
select ys = sort(xs, desc=true)
```

```tql
{ys: [3, 2, 1]}
```

**Sort records by a specific field using a custom comparator:**

```tql
from {xs: [{v: 2, id: "b"}, {v: 1, id: "a"}, {v: 2, id: "c"}]}
select ys = sort(xs, cmp=(left, right) => left.v < right.v)
```

```tql
{
  ys: [
    {v: 1, id: "a"},
    {v: 2, id: "b"},
    {v: 2, id: "c"},
  ],
}
```

The `cmp` lambda receives two elements and returns a boolean indicating whether
the first element should come before the second. Both parameters can be combined
to reverse a custom comparison.
