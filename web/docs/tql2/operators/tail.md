# tail

Limits the input to the last `N` events.

```
tail [N:uint]
```

## Description

Forwards the last `N` events and discards the rest.

`tail N` is a shorthand notation for [`slice begin=-N`](slice.md).

### `N`

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
