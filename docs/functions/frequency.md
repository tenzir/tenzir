---
title: frequency
category: Aggregation
example: 'frequency(["apple","banana","apple"])'
---

Computes the relative frequency distribution of grouped values.

```tql
frequency(xs:list) -> list
```

## Description

The `frequency` function calculates the relative frequency (proportion) of each
unique value in `xs`. It returns a list of records containing the value and its
frequency as a decimal between 0 and 1.

The results are sorted by frequency in descending order, with the most frequent
values appearing first.

### `xs: list`

The values to analyze.

## Examples

### Compute frequency distribution of strings

```tql
from {x: "a"}, {x: "b"}, {x: "a"}, {x: "c"}, {x: "a"}, {x: "b"}
summarize freq_dist=frequency(x)
```

```tql
{
  freq_dist: [
    {
      value: "a",
      frequency: 0.5,
    },
    {
      value: "b",
      frequency: 0.3333333333333333,
    },
    {
      value: "c",
      frequency: 0.16666666666666666,
    },
  ],
}
```

### Frequency with null values

```tql
from {x: 1}, {x: null}, {x: 1}, {x: 2}, {x: null}
summarize freq_with_null=frequency(x)
```

```tql
{
  freq_with_null: [
    {
      value: 1,
      frequency: 0.6666666666666666,
    },
    {
      value: 2,
      frequency: 0.3333333333333333,
    },
  ],
}
```

Note: null values are excluded from the frequency calculation.

### Count null vs non-null values

```tql
from {xs: [1, null, 2, null, null, 3, 4]}
select nulls = xs.map(x => x != null).frequency()
```

```tql
{
  nulls: [
    {
      value: false,
      frequency: 0.6,
    },
    {
      value: true,
      frequency: 0.4,
    },
  ],
}
```

## See Also

[`value_counts`](/reference/functions/value_counts),
[`mode`](/reference/functions/mode),
[`entropy`](/reference/functions/entropy)
