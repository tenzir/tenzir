---
title: year
category: Time & Date
example: 'ts.year()'
---

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

## See also

[`month`](/reference/functions/month),
[`day`](/reference/functions/day),
[`hour`](/reference/functions/hour),
[`minute`](/reference/functions/minute),
[`second`](/reference/functions/second)