# as_secs

Converts a duration into seconds.

```tql
as_secs(x:duration) -> float
```

## Description

The `as_secs` function converts duration value into seconds.

## Examples

### Convert a duration into seconds

```tql
from { x: as_secs(42ms) }
```

```tql
{x: 0.042}
```

## See Also

[`from_epoch_ms`](from_epoch_ms.md), [`now`](now.md),
[`since_epoch`](since_epoch.md)
