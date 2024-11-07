# collect

Creates a list of all non-null grouped values, preserving duplicates.

```tql
collect(xs:list) -> list
```

## Description

The `collect` function returns a list of all non-null values in `xs`, including
duplicates.

### `xs: list`

The values to collect.

## Examples

### Collect values into a list

```tql
from [
  {x: 1},
  {x: 2},
  {x: 2},
  {x: 3},
]
summarize values=collect(x)
```

```tql
{values: [1, 2, 2, 3]}
```

## See Also

[`distinct`](distinct.md), [`sum`](sum.md)
