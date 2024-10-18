# tail

Limits the input to the last `n` events.

```tql
tail [n:uint]
```

## Description

Forwards the last `n` events and discards the rest.

`tail n` is a shorthand notation for [`slice begin=-n`](slice.md).

### `n: uint (optional)`

The number of events to keep.

Defaults to `10`.

## Examples

Get the last ten results:

```tql
export
tail
```

Get the last five results:

```tql
export
tail 5
```
