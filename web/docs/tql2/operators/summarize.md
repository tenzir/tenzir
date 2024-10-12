---
sidebar_class_name: hidden
---

# summarize

```tql
summarize ...
```

## Description

## Examples

```tql
from [{x: 1}, {x: 2}]
summarize y=sum(x)
// this == {y: 3}
```

```tql
from [
  {a: 0, b: 0, c: 1},
  {a: 1, b: 1, c: 2},
  {a: 1, b: 1, c: 3},
]
summarize a, b=sum(b)
// {a: 0, b: 0}
// {a: 1, b: 2}
```
