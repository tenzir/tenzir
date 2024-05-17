---
sidebar_custom_props:
  operator:
    transformation: true
---

# slice

Keep a range events within the half-closed interval `[begin, end)`.

## Synopsis

```
slice [--begin <begin>] [--end <end>]
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

## Examples

Get the second 100 events:

```
slice --begin 100 --end 200
```

Get the last five events:

```
slice --begin -5
```

Skip the last ten events:

```
slice --end -10
```

Return the last 50 events, except for the last 2:

```
slice --begin -50 --end -2
```

Skip the first and the last event:

```
slice --begin 1 --end -1
```
