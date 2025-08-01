---
title: drop_null_fields
category: Modify
example: "drop_null_fields name, metadata.id"
---

Removes fields containing null values from the event.

```tql
drop_null_fields [field...]
```

## Description

The `drop_null_fields` operator removes fields that have `null` values from events.
Without arguments, it removes all fields with `null` values from the entire
event. When provided with specific field paths, it removes those fields if they
contain null values, and for record fields, it also recursively removes any
null fields within them.

### `field... (optional)`

A comma-separated list of field paths to process. When specified:

- If a field contains `null`, it will be removed
- If a field is a record, all null fields within it will be removed recursively
- Other null fields outside the specified paths will be preserved

:::note[Behavior with Lists]
The `drop_null_fields` operator does not currently support dropping fields from
records that are inside lists.

The operator does not remove `null` values from within lists. A field
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

### Drop null fields within a record field

When specifying a record field, all null fields within it are removed recursively:

```tql
from {
  metadata: {
    created: "2024-01-01",
    updated: null,
    tags: null,
    author: "admin"
  },
  data: {
    value: 42,
    comment: null
  }
}
drop_null_fields metadata
```

```tql
{
  metadata: {
    created: "2024-01-01",
    author: "admin",
  },
  data: {
    value: 42,
    comment: null,
  },
}
```

Note that `data.comment` remains null because only `metadata` was specified.

### Behavior with records inside lists

The `drop_null_fields` operator does not remove fields from records that are
inside lists:

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
- The `items` field is kept with all its internal structure intact
- The `tags` field is kept even though it contains `null` elements
- Fields within records inside lists (like `value`) are not dropped even if they contain `null`

## See Also

[`drop`](/reference/operators/drop),
[`select`](/reference/operators/select),
[`where`](/reference/operators/where)
