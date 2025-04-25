# bit_or

Computes the bit-wise OR of its arguments.

```tql
bit_or(lhs:number, rhs:number) -> number
```

## Description

The `bit_or` function computes the bit-wise OR of `lhs` and `rhs`. The operation
is performed on each corresponding bit position of the two numbers.

### `lhs: number`

The left-hand side operand.

### `rhs: number`

The right-hand side operand.

## Examples

### Perform bit-wise OR on integers

```tql
from {x: bit_or(5, 3)}
```

```tql
{x: 7}
```

## See Also

[`bit_and`](bit_and.md), [`bit_xor`](bit_xor.md), [`bit_not`](bit_not.md),
[`shift_left`](shift_left.md), [`shift_right`](shift_right.md)
