# length

Retrieves the length of a list.

```tql
length(xs:list) -> int
```

## Description

The `length` function returns the number of elements in the list `xs`.

## Examples

### Get the length of a list

```tql
from {n: [1, 2, 3].length()}
```

```tql
{n: 3}
```

## See Also

[`length_bytes`](length_bytes.md), [`length_chars`](length_chars.md)
