# append

Inserts an element at the back of a list.

```tql
append(list:list, element:any) -> list
```

## Description

The `append` function returns the `list` with `element` inserted at the end.
`xs.append(y)` is equivalent to `[...xs, y]`.

## Examples

### Append a number to a list

```tql
from {xs: [1, 2]}
xs = xs.append(3)
```

```tql
{xs: [1, 2, 3]}
```

## See Also

[`prepend`](prepend.md), [`concatenate`](concatenate.md)

