# rare

Shows the least common values. The dual to [`top`](top.md).

```tql
rare x:field
```

## Description

Shows the least common values for a given field. For each unique value, a new
event containing its count will be produced. In general, `rare x` is equivalent
to:

```tql
summarize x, count=count()
sort count
```

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `x: field`

The name of the field to find the least common values for.

## Examples

### Find the least common values

```tql
from [
  {x: "B"},
  {x: "A"},
  {x: "A"},
  {x: "B"},
  {x: "A"},
  {x: "D"},
  {x: "C"},
  {x: "C"},
]
rare x
```

```tql
{x: "D", count: 1}
{x: "C", count: 2}
{x: "B", count: 2}
{x: "A", count: 3}
```

### Show the five least common values for `id.orig_h`

```tql
rare id.orig_h
head 5
```
