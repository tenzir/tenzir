# day

Extracts the day component from a timestamp.

```tql
day(x: time) -> int
```

## Description

The `day` function extracts the day component from a timestamp as an integer
(1-31).

### `x: time`

The timestamp from which to extract the day.

## Examples

### Extract the day from a timestamp

```tql
from {
  ts: 2024-06-15T14:30:45.123456,
}
day = ts.day()
```

```tql
{
  ts: 2024-06-15T14:30:45.123456,
  day: 15,
}
```

## See Also

[`year`](year.md), [`month`](month.md), [`hour`](hour.md),
[`minute`](minute.md), [`second`](second.md)
