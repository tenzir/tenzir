---
title: drop_nulls
category: Modify
example: 'drop_nulls name, metadata.id'
---

Removes fields containing null values from the event.

```tql
drop_nulls [field...]
```

## Description

The `drop_nulls` operator removes fields that have `null` values from events.
Without arguments, it removes all fields with `null` values from the entire
event. When provided with specific field paths, it only considers those fields
for removal.

## Examples

### Drop all null fields from the input

```tql
from {
  src: 192.168.0.4,
  dst: null,
  role: "admin",
  info: {
    id: null,
    msg: 8411,
  },
}
drop_nulls
```

```tql
{
  src: 192.168.0.4,
  role: "admin",
  info: {
    msg: 8411,
  },
}
```

### Drop specific null fields

```tql
from {
  src: 192.168.0.4,
  dst: null,
  role: null,
  info: {
    id: null,
    msg: 8411,
  },
}
drop_nulls dst, info.id
```

```tql
{
  src: 192.168.0.4,
  role: null,
  info: {
    msg: 8411,
  },
}
```

## See Also

[`drop`](/reference/operators/drop),
[`select`](/reference/operators/select),
[`where`](/reference/operators/where)