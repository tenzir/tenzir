---
title: contains_null
category: Utility
example: '{x: 1, y: null}.contains_null() == true'
---

Checks whether the input contains any `null` values.

```tql
contains_null(x:any) -> bool
```

## Description

The `contains_null` function checks if the input contains any `null` values
recursively.

### `x: any`

The input to check for `null` values.

## Examples

### Check if list has null values

```tql
from {x: [{a: 1}, {}]}
contains_null = x.contains_null()
```

```tql
{
  x: [
    {
      a: 1,
    },
    {
      a: null,
    },
  ],
  contains_null: true,
}
```

### Check a record with null values

```tql
from {x: "foo", y: null}
contains_null = this.contains_null()
```

```tql
{
  x: "foo",
  y: null,
  contains_null: true,
}
```

## See Also

[`has`](/reference/functions/has),
[`is_empty`](/reference/functions/is_empty)
[`contains`](/reference/functions/contains)
