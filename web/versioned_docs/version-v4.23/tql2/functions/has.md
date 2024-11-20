# has

Checks whether a record has a specified field.

```tql
has(x:string) -> bool
```

## Description

The `has` function returns `true` if the record contains the specified field `x`
and `false` otherwise.

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
