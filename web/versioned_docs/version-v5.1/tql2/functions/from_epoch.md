# from_epoch

Interprets a duration as Unix time.

```tql
from_epoch(x:duration) -> time
```

## Description

The `from_epoch` function interprets a duration as [Unix
time](https://en.wikipedia.org/wiki/Unix_time).

### `x: duration`

The duration since the Unix epoch, i.e., 00:00:00 UTC on 1 January 1970.

## Examples

### Convert an integral Unix time

```tql
from {time: 1736525429}
time = from_epoch(time * 1s)
```

```tql
{x: 2025-01-10T16:10:29+00:00}
```

### Interpret a duration as Unix time

```tql
from {x: from_epoch(50y + 12w + 20m)}
```

```tql
{x: 2020-03-13T00:20:00.000000}
```

## See Also

[`now`](now.md), [`since_epoch`](since_epoch.md)
