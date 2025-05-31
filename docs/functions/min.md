---
title: min
category: Aggregation
example: 'min([1,2,3])'
---

Computes the minimum of all grouped values.

```tql
min(xs:list) -> number
```

## Description

The `min` function returns the smallest numeric value in `xs`.

### `xs: list`

The values to evaluate.

## Examples

### Find the minimum value

```tql
from {x: 1}, {x: 2}, {x: 3}
summarize min_value=min(x)
```

```tql
{min_value: 1}
```

## See Also

[`max`](/reference/functions/max),
[`mean`](/reference/functions/mean),
[`sum`](/reference/functions/sum)
