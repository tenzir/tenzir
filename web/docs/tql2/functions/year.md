# year

Extracts the year component from a timestamp.

```tql
year(x: time) -> int
```

## Description

The `year` function extracts the year component from a timestamp as an integer.

### `x: time`

The timestamp from which to extract the year.

## Examples

### Extract the year from a timestamp

```tql
from {
  ts: 2024-06-15T14:30:45.123456,
}
year = ts.year()
```

```tql
{
  ts: 2024-06-15T14:30:45.123456,
  year: 2024,
}
```

## See Also

[`month`](month.md), [`day`](day.md), [`hour`](hour.md), [`minute`](minute.md),
[`second`](second.md)
