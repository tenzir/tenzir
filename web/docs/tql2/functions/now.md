# now

Gets the current wallclock time.

```tql
now() -> time
```

## Description

The `now` function returns the current wallclock time.

## Examples

### Get the time in UTC

```tql
from { x: now() }
```

```tql
{x: 2024-10-28T13:27:33.957987}
```

### Anchor a computation with the time of now

```tql
let $now = now()
from { x: now() }
```

```tql
{x: 2024-10-28T13:27:33.957987}
```

## See Also

[`as_secs`](as_secs.md), [`from_epoch_ms`](from_epoch_ms.md),
[`since_epoch`](since_epoch.md)
