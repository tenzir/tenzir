---
title: distinct
category: Aggregation
example: 'distinct([1,2,2,3])'
---

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
from {x: 1}, {x: 2}, {x: 2}, {x: 3}
summarize unique=distinct(x)
```

```tql
{unique: [1, 2, 3]}
```

## See Also

[`collect`](/reference/functions/collect),
[`count_distinct`](/reference/functions/count_distinct),
[`value_counts`](/reference/functions/value_counts)
