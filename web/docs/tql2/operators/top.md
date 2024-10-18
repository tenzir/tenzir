# top

Shows the most common values. The dual to [`rare`](rare.md).

```tql
top x:field
```

## Description

Shows the most common values for a given field. For each value, a new event
containing its count will be produced.

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `x: field`

The field to find the most common values for.

## Examples

Find the most common values for `x`.

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
top x
――――――――――――――――――
{x: "A", count: 3}
{x: "B", count: 2}
{x: "C", count: 2}
{x: "D", count: 1}
```

Show the five most common values for `id.orig_h`:

```tql
top id.orig_h
head 5
```
