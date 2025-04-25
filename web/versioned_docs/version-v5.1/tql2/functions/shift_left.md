# shift_left

Performs a bit-wise left shift.

```tql
shift_left(lhs:number, rhs:number) -> number
```

## Description

The `shift_left` function performs a bit-wise left shift of `lhs` by `rhs` bit
positions. Each left shift multiplies the number by 2.

### `lhs: number`

The number to be shifted.

### `rhs: number`

The number of bit positions to shift to the left.

## Examples

### Shift bits to the left

```tql
from {x: shift_left(5, 2)}
```

```tql
{x: 20}
```

## See Also

[`shift_right`](shift_right.md), [`bit_and`](bit_and.md), [`bit_or`](bit_or.md),
[`bit_xor`](bit_xor.md), [`bit_not`](bit_not.md)
