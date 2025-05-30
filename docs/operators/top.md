---
title: top
---

Shows the most common values. The dual to [`rare`](/reference/operators/rare).

```tql
top x:field
```

## Description

Shows the most common values for a given field. For each value, a new event
containing its count will be produced. In general, `top x` is equivalent to:

```tql
summarize x, count=count()
sort -count
```

:::note[Potentially High Memory Usage]
Use caution when applying this operator to large inputs. It currently buffers
all data in memory. Out-of-core processing is on our roadmap.
:::

### `x: field`

The field to find the most common values for.

## Examples

### Find the most common values

```tql
from {x: "B"}, {x: "A"}, {x: "A"}, {x: "B"}, {x: "A"}, {x: "D"}, {x: "C"}, {x: "C"}
top x
```

```tql
{x: "A", count: 3}
{x: "B", count: 2}
{x: "C", count: 2}
{x: "D", count: 1}
```

### Show the 5 top-most values

```tql
top id.orig_h
head 5
```

## See Also

[`summarize`](/reference/operators/summarize),
[`rare`](/reference/operators/rare),
[`sort`](/reference/operators/sort)
