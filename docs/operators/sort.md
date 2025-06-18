---
title: sort
category: Analyze
example: 'sort name, -abs(transaction)'
---

Sorts events by the given expressions.

```tql
sort [-]expr...
```

## Description

Sorts events by the given expressions, putting all `null` values at the end.

If multiple expressions are specified, the sorting happens lexicographically,
that is: Later expressions are only considered if all previous expressions
evaluate to equal values.

This operator performs a stable sort (preserves relative ordering when all
expressions evaluate to the same value).

:::note[Potentially High Memory Usage]
Use caution when applying this operator to large inputs. It currently buffers
all data in memory. Out-of-core processing is on our roadmap.
:::

### `[-]expr`

An expression that is evaluated for each event. Normally, events are sorted in
ascending order. If the expression starts with `-`, descending order is used
instead. In both cases, `null` is put last.

## Examples

### Sort by a field in ascending order

```tql
sort timestamp
```

### Sort by a field in descending order

```tql
sort -timestamp
```

### Sort by multiple fields

Sort by a field `src_ip` and, in case of matching values, sort by `dest_ip`:

```tql
sort src_ip, dest_ip
```

Sort by the field `src_ip` in ascending order and by the field `dest_ip` in
descending order.

```tql
sort src_ip, -dest_ip
```

## See Also

[`rare`](/reference/operators/rare),
[`reverse`](/reference/operators/reverse),
[`top`](/reference/operators/top)
