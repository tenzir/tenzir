---
title: minute
category: Time & Date
example: 'ts.minute()'
---
Extracts the minute component from a timestamp.

```tql
minute(x: time) -> int
```

## Description

The `minute` function extracts the minute component from a timestamp as an
integer (0-59).

### `x: time`

The timestamp from which to extract the minute.

## Examples

### Extract the minute from a timestamp

```tql
from {
  ts: 2024-06-15T14:30:45.123456,
}
minute = ts.minute()
```

```tql
{
  ts: 2024-06-15T14:30:45.123456,
  minute: 30,
}
```

## See also

[`year`](/reference/functions/year),
[`month`](/reference/functions/month),
[`day`](/reference/functions/day),
[`hour`](/reference/functions/hour),
[`second`](/reference/functions/second)
