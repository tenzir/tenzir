---
title: drop_null_fields
category: Modify
example: 'drop_null_fields name, metadata.id'
---

Removes fields containing null values from the event.

```tql
drop_null_fields [field...]
```

## Description

The `drop_null_fields` operator removes fields that have `null` values from events.
Without arguments, it removes all fields with `null` values from the entire
event. When provided with specific field paths, it only considers those fields
for removal.

:::note[Behavior with Lists]
The `drop_null_fields` operator only removes top-level fields that contain `null`.
It does not remove `null` values from within lists or arrays. A field
containing a list with `null` elements is not considered a null field.
:::

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
drop_null_fields
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
drop_null_fields dst, info.id
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

### Behavior with records inside lists

The `drop_null_fields` operator does not remove `null` values from within lists,
even when those lists contain records with null fields:

```tql
from {
  id: 1,
  items: [{name: "a", value: 1}, {name: "b", value: null}],
  metadata: null,
  tags: ["x", null, "y"]
}
drop_null_fields
```

```tql
{
  id: 1,
  items: [
    {
      name: "a",
      value: 1,
    },
    {
      name: "b",
      value: null,
    },
  ],
  tags: [
    "x",
    null,
    "y",
  ],
}
```

In this example:
- The `metadata` field is removed because it contains `null`
- The `items` field is kept even though it contains records with `null` values
- The `tags` field is kept even though it contains `null` elements
- `null` values within the lists remain unchanged

## See Also

[`drop`](/reference/operators/drop),
[`select`](/reference/operators/select),
[`where`](/reference/operators/where)