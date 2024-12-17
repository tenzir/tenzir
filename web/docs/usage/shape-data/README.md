---
sidebar_position: 1
---

# Shape data

Tenzir comes with numerous transformation [operators](../../operators.md) that
do change the the shape of their input and produce a new output. Here is a
visual overview of transformations that you can perform over a data frame:

![Shaping Overview](shaping.svg)

We'll walk through examples for each depicted operator, using the
[M57](../../installation.md) dataset. All examples assume that you have imported
the M57 sample data into a node, as explained in the
[quickstart](../../tutorials/quickstart/README.md). We therefore start every pipeline with
[`export`](../../operators/export.md).

## Filter events with `where`

Use [`where`](../../tql2/operators/where.md) to filter events in the
input with an [expression](../../tql2/language/expressions.md):

```tql
from {x: 1, y: "foo"},
     {x: 2, y: "bar"},
     {x: 3, y: "baz"}
where x != 2 and y.starts_with("b")
```

```tql
{x: 3, y: "baz"}
```

## Slice events with `head`, `tail`, and `slice`

Use the [`head`](../../tql2/operators/head.md) and
[`tail`](../../tql2/operators/tail.md) operators to get the first or
last N records of the input.

Get the first event:

```tql
from {x: 1, y: "foo"},
     {x: 2, y: "bar"},
     {x: 3, y: "baz"}
head 1
```

```tql
{x: 1, y: "foo"}
```

Get the last two events:

```tql
from {x: 1, y: "foo"},
     {x: 2, y: "bar"},
     {x: 3, y: "baz"}
tail 2
```

```tql
{x: 2, y: "bar"}
{x: 3, y: "baz"}
```
:::caution `tail` is blocking
The `tail` operator must wait for its entire input, whereas `head N` terminates
immediately after the first `N` records have arrived. Use `head` for the
majority of use cases and `tail` only when you have to.
:::

The [`slice`](../../tql2/operators/slice.md) operator generalizes `head` and
`tail` by allowing for more flexible slicing. For example, to return every
other event starting from the third:

```tql
from {x: 1, y: "foo"},
     {x: 2, y: "bar"},
     {x: 3, y: "baz"},
     {x: 4, y: "qux"},
     {x: 5, y: "corge"},
     {x: 6, y: "grault"}
slice begin=3, stride=2
```

```tql
{x: 4, y: "qux"}
{x: 6, y: "grault"}
```

## Pick fields with `select` and `drop`

Use the [`select`](../../tql2/operators/select.md) operator to
pick fields:

```tql
from {x: 1, y: "foo"},
     {x: 2, y: "bar"},
     {x: 3, y: "baz"}
select x
```

```tql
{x: 1}
{x: 2}
{x: 3}
```

The [`drop`](../../tql2/operators/drop.md) operator is the dual to `select` and
removes the specified fields:

```tql
from {x: 1, y: "foo"},
     {x: 2, y: "bar"},
     {x: 3, y: "baz"}
drop x
```

```tql
{y: "foo"}
{y: "bar"}
{y: "baz"}
```

## Sample schemas with `taste`

The [`taste`](../../tql2/operators/taste.md) operator provides a sample of the
first N events of every unique schemas. For example, to get 3 unique samples:

```tql
from {x: 1, y: "foo"},
     {x: 2, y: "bar"},
     {x: 1},
     {x: 2},
     {y: "foo"}
taste 1
```

```tql
{x: 1, y: "foo"}
{x: 1}
{y: "foo"}
```

## Add and rename fields with `set` assignment

Use the [`set`](../../tql2/operators/set.md) operator to add new fields to the
output.

```tql
from {x: 1},
     {x: 2}
set y = x + 1
```

```tql
{x: 1, y: 2}
{x: 2, y: 3}
```

Rename fields by combining [`set`](../../tql2/operators/set.md) with
[`drop`](../../tql2/operators/set.md):

```tql
from {x: 1},
     {x: 2}
set y=x
drop x
```

```tql
{y: 1}
{y: 2}
```

Similarly, you can rename and project at the same time with
[`select`](../../tql2/operators/select.md):

```tql
from {x: 1, y: "foo"},
    {x: 2, y: "bar"}
select y=x
```

```tql
{y: 1}
{y: 2}
```

## Aggreate events with `summarize`

Use [`summarize`](../../tql2/operators/summarize.md) to group and aggregate
data.

```tql
from {x: 0, y: 0, z: 1},
     {x: 1, y: 1, z: 2},
     {x: 1, y: 1, z: 3}
summarize y, x=sum(x)
```

```tql
{y: 0, x: 0}
{y: 1, x: 2}
```

A variety of [aggregation functions](../../tql2/functions.md#aggregation) make
it possible to combine grouped data.

## Reorder events with `sort`

Use [`sort`](../../tql2/operators/sort.md) to arrange the output records
according to the order of a specific field.

```tql
from {x: 2, y: "bar"},
     {x: 3, y: "baz"},
     {x: 1, y: "foo"}
sort -x
```

```tql
{x: 3, y: "baz"}
{x: 2, y: "bar"}
{x: 1, y: "foo"}
```

Prepending the field with `-` reverses the sort order.
