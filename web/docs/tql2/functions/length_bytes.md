# length_bytes

Returns the length of a string in bytes.

```tql
length_bytes(x:string) -> int
```

## Description

The `length_bytes` function returns the byte length of the `x` string.

## Examples

### Get the byte length of a string

For ASCII strings, the byte length is the same as the number of characters:

```tql
from {x: length_bytes("hello")}
```

```tql
{x: 5}
```

For Unicode, this may not be the case:

```tql
from {x: length_bytes("👻")}
```

```tql
{x: 4}
```

## See Also

[`length_chars`](length_chars.md)
