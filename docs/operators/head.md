---
title: head
category: Filter
example: 'head 20'
---

Limits the input to the first `n` events.

```tql
head [n:int]
```

## Description

Forwards the first `n` events and discards the rest.

`head n` is a shorthand notation for [`slice end=n`](/reference/operators/slice).

### `n: int (optional)`

The number of events to keep.

Defaults to `10`.

## Examples

### Get the first 10 events

```tql
head
```

### Get the first 5 events

```tql
head 5
```

## See Also

[`slice`](/reference/operators/slice),
[`tail`](/reference/operators/tail)
