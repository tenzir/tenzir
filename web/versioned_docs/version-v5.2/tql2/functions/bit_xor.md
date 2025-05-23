# bit_xor

Computes the bit-wise XOR of its arguments.

```tql
bit_xor(lhs:number, rhs:number) -> number
```

## Description

The `bit_xor` function computes the bit-wise XOR (exclusive OR) of `lhs` and
`rhs`. The operation is performed on each corresponding bit position of the two
numbers.

### `lhs: number`

The left-hand side operand.

### `rhs: number`

The right-hand side operand.

## Examples

### Perform bit-wise XOR on integers

```tql
from {x: bit_xor(5, 3)}
```

```tql
{x: 6}
```

## See Also

[`bit_and`](bit_and.md), [`bit_or`](bit_or.md), [`bit_not`](bit_not.md),
[`shift_left`](shift_left.md), [`shift_right`](shift_right.md)
