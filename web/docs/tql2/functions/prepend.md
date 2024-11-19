# prepend

Inserts an element at the start of a list.

```tql
prepend(list:list, element:any) -> list
```

## Description

The `prepend` function returns the `list` with `element` inserted at the front.
`xs.prepend(y)` is equivalent to `[y, ...xs]`.

## Examples

### Prepend a number to a list

```tql
from {xs: [1, 2]}
xs = xs.prepend(3)
```

```tql
{xs: [3, 1, 2]}
```

## See Also

[`append`](append.md), [`concatenate`](concatenate.md)

