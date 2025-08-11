---
title: is_empty
category: Utility
example: '"".is_empty()'
---

Checks whether a value is empty.

```tql
is_empty(x:string|list|record) -> bool
```

## Description

The `is_empty` function returns `true` if the input value is empty and `false`
otherwise.

### `x: string|list|record`

The value to check for emptiness.

The function works on three types:

- **Strings**: Returns `true` for empty strings (`""`)
- **Lists**: Returns `true` for empty lists (`[]`)
- **Records**: Returns `true` for empty records (`{}`)

For `null` values, the function returns `null`. For unsupported types, the
function emits a warning and returns `null`.

## Examples

### Check if a string is empty

```tql
from {
  empty: "".is_empty(),
  not_empty: "hello".is_empty(),
}
```

```tql
{
  empty: true,
  not_empty: false,
}
```

### Check if a list is empty

```tql
from {
  empty: [].is_empty(),
  not_empty: [1, 2, 3].is_empty(),
}
```

```tql
{
  empty: true,
  not_empty: false,
}
```

### Check if a record is empty

```tql
from {
  empty: {}.is_empty(),
  not_empty: {a: 1, b: 2}.is_empty(),
}
```

```tql
{
  empty: true,
  not_empty: false,
}
```

### Null handling

```tql
from {
  result: null.is_empty(),
}
```

```tql
{
  result: null,
}
```

## See Also

[`length`](/reference/functions/length),
[`has`](/reference/functions/has)
