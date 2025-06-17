---
title: entropy
category: Aggregation
example: "entropy([1,1,2,3])"
---

Computes the Shannon entropy of all grouped values.

```tql
entropy(xs:list, [normalize=bool]) -> float
```

## Description

The `entropy` function calculates the Shannon entropy of the values in `xs`,
which measures the amount of uncertainty or randomness in the data. Higher
entropy values indicate more randomness, while lower values indicate more
predictability.

The entropy is calculated as: `H(x) = -sum(p(x[i]) * log(p(x[i])))`, where
`p(x[i])` is the probability of each unique value.

### `xs: list`

The values to evaluate.

### `normalize: bool (optional)`

Optional parameter to normalize the entropy between 0 and 1. When `true`, the
entropy is divided by `log(number of unique values)`. Defaults to `false`.

## Examples

### Compute the entropy of values

```tql
from {x: 1}, {x: 1}, {x: 2}, {x: 3}
summarize entropy_value=entropy(x)
```

```tql
{
  entropy_value: 1.0397207708399179,
}
```

### Compute the normalized entropy

```tql
from {x: 1}, {x: 1}, {x: 2}, {x: 3}
summarize normalized_entropy=entropy(x, normalize=true)
```

```tql
{
  normalized_entropy: 0.946394630357186,
}
```

## See Also

[`mode`](/reference/functions/mode),
[`value_counts`](/reference/functions/value_counts)
