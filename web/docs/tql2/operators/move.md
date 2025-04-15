# move

```tql
move to=from, ...
```

## Description

Moves the values

### `to: field`

The field to move into.

### `from: field`

The field to move from.

## Examples

```tql
from {x: 1, y: 2}
move z=y, w.x=x
```

```tql
{
  z: 2,
  w: {
    x: 1,
  },
}
```
