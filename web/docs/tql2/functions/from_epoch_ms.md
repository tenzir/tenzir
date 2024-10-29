# from_epoch_ms

Interprets a number as Unix time.

```tql
from_epoch_ms(x:int) -> time
from_epoch_ms(x:uint) -> time
from_epoch_ms(x:float) -> time
```

## Description

The `from_epoch_ms` function interprets a number as [Unix
time](https://en.wikipedia.org/wiki/Unix_time) in milliseconds.

### `x: int|uint|float`

The number of milliseconds since the Unix epoch, i.e., 00:00:00 UTC on January
1970.

## Examples

### Interpret a number as Unix time

```tql
from { x: from_epoch_ms(1730234246123.456) }
```

```tql
{x: 2024-10-29T20:37:26.123456}
```

## See Also

[`now`](now.md), [`since_epoch`](since_epoch.md)
