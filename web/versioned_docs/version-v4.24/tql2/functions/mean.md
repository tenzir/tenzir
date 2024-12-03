# mean

Computes the mean of all grouped values.

```tql
mean(xs:list) -> float
```

## Description

The `mean` function returns the average of all numeric values in `xs`.

### `xs: list`

The values to average.

## Examples

### Compute the mean value

```tql
from [
  {x: 1},
  {x: 2},
  {x: 3},
]
summarize avg=mean(x)
```

```tql
{avg: 2.0}
```

## See Also

[`median`](median.md), [`sum`](sum.md)
