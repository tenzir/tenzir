# max

Computes the maximum of all grouped values.

```tql
max(xs:list) -> number
```

## Description

The `max` function returns the largest numeric value in `xs`.

### `xs: list`

The values to evaluate.

## Examples

### Find the maximum value

```tql
from [
  {x: 1},
  {x: 2},
  {x: 3},
]
summarize max_value=max(x)
```

```tql
{max_value: 3}
```

## See Also

[`min`](min.md), [`mean`](mean.md), [`sum`](sum.md)
