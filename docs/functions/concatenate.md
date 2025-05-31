---
title: concatenate
category: List
example: 'concatenate(xs, ys)'
---
Merges two lists.

```tql
concatenate(xs:list, ys:list) -> list
```

## Description

The `concatenate` function returns a list containing all elements from the lists
`xs` and `ys` in order. The expression `concatenate(xs, ys)` is equivalent to
`[...xs, ...ys]`.

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

[`append`](/reference/functions/append),
[`merge`](/reference/functions/merge),
[`prepend`](/reference/functions/prepend),
[`zip`](/reference/functions/zip)
