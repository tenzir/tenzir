# cast

Casts the input to the given schema.

:::warning Expert Operator
The `cast` operator is a lower-level building block of other operators and is a
more powerful, but also less flexible way of reshaping events compared to the
[`put`](put.md) operator.
:::

## Synopsis

```
cast <schema>
```

## Description

The `cast` operator casts input events to a known schema.

## Examples

Cast the input to `zeek.conn` as best possible:

```
cast zeek.conn
```
