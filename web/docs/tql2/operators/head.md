# head

Limits the input to the first `n` events.

```tql
head [n:uint]
```

## Description

Forwards the first `n` events and discards the rest.

`head n` is a shorthand notation for [`slice end=n`](slice.md).

### `n: uint (optional)`

An unsigned integer denoting how many events to keep.

Defaults to `10`.

## Examples

Get the first ten events:

```tql
head
```

Get the first five events:

```tql
head 5
```
