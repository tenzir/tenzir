# sort

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

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `[-]expr`

An expression that is evaluated for each event. Normally, events are sorted in ascending order. If the expression starts with `-`, descending order is used instead. In both cases, `null` is put last.

## Examples

Sort by the `timestamp` field in ascending order.

```tql
sort timestamp
```

Sort by the `timestamp` field in descending order.

```tql
sort -timestamp
```

Sort by the field `src_ip` and, in case of matching values, sort by `dest_ip`.

```tql
sort src_ip, dest_ip
```

Sort by the field `src_ip` in ascending order and by the field `dest_ip` in
descending order.

```tql
sort src_ip, -dest_ip
```
