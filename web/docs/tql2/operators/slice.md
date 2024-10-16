# slice

Keep a range of events within the interval `[begin, end)` stepping by `stride`.

```tql
slice [begin=int, end=int, stride=int]
```

## Description

The `slice` operator selects a range of events from the input. The semantics of
the operator match Python's array slicing.

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `begin = int (optional)`

The beginning (inclusive) of the range to keep. Use a negative number to count
from the end.

### `end = int (optional)`

The end (exclusive) of the range to keep. Use a negative number to count from
the end.

### `stride = int (optional)`

The number of elements to advance before the next element. Use a negative number
to count from the end, effectively reversing the stream.

## Examples

Get the second 100 events:

```tql
slice begin=100, end=200
```

Get the last five events:

```tql
slice begin=-5
```

Skip the last ten events:

```tql
slice end=-10
```

Return the last 50 events, except for the last 2:

```tql
slice begin=-50, end=-2
```

Skip the first and the last event:

```tql
slice begin=1, end=-1
```

Return every second event starting from the tenth:

```tql
slice begin=9, stride=2
```

Return all but the last five events in reverse order:

```tql
slice end=-5, stride=-1
```
