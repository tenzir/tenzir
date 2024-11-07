# sum

Computes the sum of all values.

```tql
sum(xs:list) -> int
```

## Description

The `sum` function computes the total of all number values.

### `xs: list`

The values to aggregate.

## Examples

### Compute a sum over a group of events

```tql
from [
  {x: 1},
  {x: 2},
  {x: 3},
]
summarize n=sum(x)
```

```tql
{n: 6}
```

## See Also

[`mean`](mean.md), [`min`](min.md), [`max`](max.md)
