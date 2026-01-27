---
title: "Grouped enumeration"
type: feature
author: raxyte
created: 2025-09-16T09:50:59Z
pr: 5475
---

The `enumerate` operator now supports a `group` option to enumerate events
separately based on a value.

For example, to have a field act as a counter for a value, use the following
pipeline:

```tql
from {x: 1}, {x: 2}, {x: "1"}, {x: 2}
enumerate count, group=x
count = count + 1
```

```tql
{
  count: 1,
  x: 1,
}
{
  count: 1,
  x: 2,
}
{
  count: 1,
  x: "1",
}
{
  count: 2,
  x: 2,
}
```
