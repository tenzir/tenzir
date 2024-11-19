# concatenate

Merges two lists.

```tql
concatenate(list1:list, list2:list) -> list
```

## Description

The `concatenate` function returns a list containing all elements from `list1`
and `list2` in order. `concatenate(xs, ys)` is equivalent to `[...xs, ...ys]`.

## Examples

### Concatenate two lists

```tql
from {xs: [1, 2], ys: [3, 4]}
zs = concatenate(xs, ys)
```

```tql
{
  xs: [1, 2],
  ys: [3, 4],
  zs: [1, 2, 3, 4]
}
```

## See Also

[`append`](append.md), [`prepend`](prepend.md)

