---
title: count_if
---

Counts the events or non-null grouped values matching a given predicate.

```tql
count_if(xs:list, predicate:any => bool) -> int
```

## Description

The `count_if` function returns the number of non-null values in `xs` that
satisfy the given `predicate`.

### `xs: list`

The values to count.

### `predicate: any => bool`

The predicate to apply to each value to check whether it should be counted.

## Examples

### Count the number of values greater than 1

```tql
from {x: 1}, {x: null}, {x: 2}
summarize total=x.count_if(x => x > 1)
```

```tql
{total: 1}
```

## See Also

[`count`](/reference/functions/count)
