---
title: remove
category: List
example: "xs.remove(y)"
---

Removes all occurrences of an element from a list.

```tql
remove(xs:list, x:any) -> list
```

## Description

The `remove` function returns the list `xs` with all occurrences of `x` removed.
If `x` is not present in the list, the original list is returned unchanged.

### `xs: list`

A list to remove elements from.

### `x: any`

The value to remove from the list.

## Examples

### Remove an element from a list

```tql
from {xs: [1, 2, 3, 2, 4]}
xs = xs.remove(2)
```

```tql
{xs: [1, 3, 4]}
```

### Remove a non-existent element

```tql
from {xs: [1, 2, 3]}
xs = xs.remove(5)
```

```tql
{xs: [1, 2, 3]}
```

### Remove from a list with strings

```tql
from {xs: ["apple", "banana", "apple", "orange"]}
xs = xs.remove("apple")
```

```tql
{xs: ["banana", "orange"]}
```

## See Also

[`add`](/reference/functions/add),
[`append`](/reference/functions/append),
[`prepend`](/reference/functions/prepend)
[`distinct`](/reference/functions/distinct)
