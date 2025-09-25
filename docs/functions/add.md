---
title: add
category: List
example: "xs.add(y)"
---

Inserts an element into a list if it doesn't already exist (set-insertion).

```tql
add(xs:list, x:any) -> list
```

## Description

The `add` function returns the list `xs` with `x` inserted at the end, but only
if `x` is not already present in the list. This performs a set-insertion
operation, ensuring no duplicate values in the resulting list.

### `xs: list`

The list to add to.

### `x: any`

An element to add to the list. If this is of a type incompatible with the list,
it will be considered as `null`.

## Examples

### Add a new element to a list

```tql
from {xs: [1, 2, 3]}
xs = xs.add(4)
```

```tql
{xs: [1, 2, 3, 4]}
```

### Try to add an existing element

```tql
from {xs: [1, 2, 3]}
xs = xs.add(2)
```

```tql
{xs: [1, 2, 3]}
```

### Add to an empty list

```tql
from {xs: []}
xs = xs.add("hello")
```

```tql
{xs: ["hello"]}
```

## See Also

[`append`](/reference/functions/append),
[`prepend`](/reference/functions/prepend),
[`remove`](/reference/functions/remove)
