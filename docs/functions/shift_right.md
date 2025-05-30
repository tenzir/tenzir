---
title: shift_right
---

Performs a bit-wise right shift.

```tql
shift_right(lhs:number, rhs:number) -> number
```

## Description

The `shift_right` function performs a bit-wise right shift of `lhs` by `rhs` bit
positions. Each right shift divides the number by 2, truncating any fractional
part.

### `lhs: number`

The number to be shifted.

### `rhs: number`

The number of bit positions to shift to the right.

## Examples

### Shift bits to the right

```tql
from {x: shift_right(20, 2)}
```

```tql
{x: 5}
```

## See Also

[`shift_left`](/reference/functions/shift_left),
[`bit_and`](/reference/functions/bit_and),
[`bit_or`](/reference/functions/bit_or),
[`bit_xor`](/reference/functions/bit_xor),
[`bit_not`](/reference/functions/bit_not)
