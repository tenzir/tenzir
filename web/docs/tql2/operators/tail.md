# tail

Limits the input to the last `n` events.

```
tail [n:uint]
```

## Description

Forwards the last `n` events and discards the rest.

`tail n` is a shorthand notation for [`slice begin=-n`](slice.md).

### `n`

An unsigned integer denoting how many events to keep.

Defaults to 10.

## Examples

Get the last ten results:

```
<event stream> | tail
```

Get the last five results:

```
<event stream> | tail 5
```
