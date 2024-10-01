# head

Limits the input to the first `N` events.

## Synopsis

```
head [N:uint]
```

## Description

Forwards the first `N` events and discards the rest.

`head N` is a shorthand notation for [`slice end=N`](slice.md).

### `N`

An unsigned integer denoting how many events to keep.

Defaults to 10.

## Examples

Get the first ten events:

```
<event stream> | head
```

Get the first five events:

```
<event stream> | head 5
```
