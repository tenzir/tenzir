# from_epoch

Interprets a duration as Unix time.

```tql
from_epoch(x:duration) -> time
```

## Description

The `from_epoch` function interprets a duration as [Unix
time](https://en.wikipedia.org/wiki/Unix_time).

### `x: duration`

The duration since the Unix epoch, i.e., 00:00:00 UTC on 1 January
1970.

## Examples

### Interpret a duration as Unix time

```tql
from {x: from_epoch(50y + 12w + 20m)}
```

```tql
{x: 2020-03-13T00:20:00.000000}
```

## See Also

[`as_secs`](as_secs.md), [`now`](now.md), [`since_epoch`](since_epoch.md)
