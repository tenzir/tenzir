---
title: count_distinct
category: Aggregation
example: 'count_distinct([1,2,2,3])'
---

Counts all distinct non-null grouped values.

```tql
count_distinct(xs:list) -> int
```

## Description

The `count_distinct` function returns the number of unique, non-null values in
`xs`.

### `xs: list`

The values to count.

## Examples

### Count distinct values

```tql
from {x: 1}, {x: 2}, {x: 2}, {x: 3}
summarize unique=count_distinct(x)
```

```tql
{unique: 3}
```

## See Also

[`count`](/reference/functions/count),
[`distinct`](/reference/functions/distinct)
