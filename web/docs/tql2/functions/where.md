# all

Removes list elements based on a predicate.

```tql
map(list:list, capture:field, predicate:bool) -> list
```

## Description

The `where` function removes all elements of a list for which a predicate
evaluates to `false`.

### `list : list`

A list of values.

### `capture : field`

The name of each list element in each predicate.

### `predicate : bool`

The predicate evaluated for each list element.

## Examples

### Remove all list elements smaller than 3

```tql
from {
  xs: [1, 2, 3, 4, 5]
}
xs = xs.where(x, x >= 3)
```

```tql
{xs: [3, 4, 5]}
```

## See Also

[`map`](map.md)
