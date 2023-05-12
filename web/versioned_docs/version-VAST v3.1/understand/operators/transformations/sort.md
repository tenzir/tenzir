# sort

Sort events.

:::caution Not Yet Implemented
The `sort` operator is still under development but will be available shortly.
:::

## Synopsis

```
sort <operand>...
```

## Description

Sorts events by the specified metadata, fields, or computed values.

### `<operand>...`

A list of extractors, selectors, or functions to sort by in ascending order.
Prefix operands with `-` to sort in descending order.

## Examples

Sort by the timestamp field in ascending order.

```
sort timestamp
```

Sort by the timestamp field in descending order.

```
sort -timestamp
```

Sort by schema name in alphabetical, and secondarily by the time events arrived
at VAST's storage engine showing the latest events first.

```
sort #type, -#import_time
```
