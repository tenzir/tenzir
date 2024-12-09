---
sidebar_custom_props:
  operator:
    transformation: true
---

# slice

Keep a range events within the half-closed interval `[begin, end)`.

## Synopsis

```
slice [<begin>]:[<end>][:<stride>]
```

## Description

The `slice` operator selects a range of events from the input. The semantics of
the operator match Python's array slicing.

### `<begin>`

An signed integer denoting the beginning (inclusive) of the range to keep. Use a
negative number to count from the end.

### `<end>`

An signed integer denoting the end (exclusive) of the range to keep. Use a
negative number to count from the end.

### `<stride>`

An signed integer denoting the number of elements to advance before the next
element. Use a negative number to count from the end, effectively reversing the
stream.

## Examples

Get the second 100 events:

```
slice 100:200
```

Get the last five events:

```
slice -5:
```

Skip the last ten events:

```
slice :-10
```

Return the last 50 events, except for the last 2:

```
slice -50:-2
```

Skip the first and the last event:

```
slice 1:-1
```

Return every second event starting from the tenth:

```
slice 10::2
```

Return all but the last five events in reverse order:

```
slice :-5:-1
```
