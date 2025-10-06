---
title: prepend
category: List
example: "xs.prepend(y)"
---

Inserts an element at the start of a list.

```tql
prepend(xs:list, x:any) -> list
```

## Description

The `prepend` function returns the list `xs` with `x` inserted at the front.
The expression `xs.prepend(y)` is equivalent to `[x, ...xs]`.

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

[`add`](/reference/functions/add),
[`append`](/reference/functions/append),
[`concatenate`](/reference/functions/concatenate),
[`remove`](/reference/functions/remove)
