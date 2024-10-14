# sort

Sorts events by the given expressions.

```tql
sort [-]expr...
```

## Description

Sorts events by the given expressions, putting all `null` values at the end.

This operator performs a stable sort (preserves relative ordering when all
expressions evaluate to the same value).

### `<expr>`

An expression that is evaluated for each event. To sort in descending order
prefix the `expr` by `-`.

## Examples

Sort by the `x` field in ascending order and by the `y` field in
descending order in case two events have an equal `x` field.

```tql
from {}, {x: 1, y: 2ms}, {x: 4}, {x: 1, y: -1ms} | sort x, -y
```

```json title="Output"
// XXX: Add output
```
