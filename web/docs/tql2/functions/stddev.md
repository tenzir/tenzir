# stddev

Computes the standard deviation of all grouped values.

```tql
stddev(xs:list) -> float
```

## Description

The `stddev` function returns the standard deviation of all numeric values in
`xs`.

### `xs: list`

The values to evaluate.

## Examples

### Compute the standard deviation of values

```tql
from [
  {x: 1},
  {x: 2},
  {x: 3},
]
summarize stddev_value=stddev(x)
```

```tql
{stddev_value: 0.816}
```

## See Also

[`variance`](variance.md), [`mean`](mean.md)
