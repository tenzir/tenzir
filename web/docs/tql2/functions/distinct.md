# distinct

Creates a sorted list without duplicates of non-null grouped values.

```tql
distinct(xs:list) -> list
```

## Description

The `distinct` function returns a sorted list containing unique, non-null values
in `xs`.

### `xs: list`

The values to deduplicate.

## Examples

### Get distinct values in a list

```tql
from [
  {x: 1},
  {x: 2},
  {x: 2},
  {x: 3},
]
summarize unique=distinct(x)
```

```tql
{unique: [1, 2, 3]}
```

## See Also

[`collect`](collect.md), [`count_distinct`](count_distinct.md)
