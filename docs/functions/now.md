---
title: now
category: Time & Date
example: 'now()'
---

Gets the current wallclock time.

```tql
now() -> time
```

## Description

The `now` function returns the current wallclock time.

## Examples

### Get the time in UTC

```tql
let $now = now()
from { x: $now }
```

```tql
{x: 2024-10-28T13:27:33.957987}
```

### Compute a field with the current time

```tql
subscribe "my-topic"
select ts=now()
```

```tql
{ts: 2024-10-30T15:03:04.85298}
{ts: 2024-10-30T15:03:06.31878}
{ts: 2024-10-30T15:03:07.59813}
```

## See Also

[`from_epoch`](/reference/functions/from_epoch),
[`since_epoch`](/reference/functions/since_epoch)
