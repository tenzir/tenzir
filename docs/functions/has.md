---
title: has
category: Record
example: 'record.has("field")'
---

Checks whether a record has a specified field.

```tql
has(x:record, field:string) -> bool
```

## Description

The `has` function returns `true` if the record contains the specified field and
`false` otherwise.

## Examples

### Check if a record has a specific field

```tql
from {
  x: "foo",
  y: null,
}
this = {
  has_x: this.has("x"),
  has_y: this.has("y"),
  has_z: this.has("z"),
}
```

```tql
{
  has_x: true,
  has_y: true,
  has_z: false,
}
```

## See Also

[`is_empty`](/reference/functions/is_empty),
[`keys`](/reference/functions/keys)
