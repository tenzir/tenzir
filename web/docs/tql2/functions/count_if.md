# count_if

Counts the events or non-null grouped values.

```tql
count_if(xs:list, predicate:any => bool) -> int
```

## Description

The `count_if` function returns the number of non-null values in `xs` that
satisfy the given `predicate`.

### `xs: list`

The values to count.

### `predicate: any => bool`

The values to count.

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

[`count`](count.md)
