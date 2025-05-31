---
title: tail
category: Filter
example: 'tail 20'
---

Limits the input to the last `n` events.

```tql
tail [n:int]
```

## Description

Forwards the last `n` events and discards the rest.

`tail n` is a shorthand notation for [`slice begin=-n`](/reference/operators/slice).

### `n: int (optional)`

The number of events to keep.

Defaults to `10`.

## Examples

### Get the last 10 results

```tql
export
tail
```

### Get the last 5 results

```tql
export
tail 5
```

## See Also

[`head`](/reference/operators/head),
[`slice`](/reference/operators/slice)
