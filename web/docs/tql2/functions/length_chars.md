# length_chars

Returns the length of a string in characters.

```tql
length_chars(x:string) -> int
```

## Description

The `length_chars` function returns the character length of the `x` string.

## Examples

### Get the character length of a string

For ASCII strings, the character length is the same as the number of bytes:

```tql
from {x: length_chars("hello")}
```

```tql
{x: 5}
```

For Unicode, this may not be the case:

```tql
from {x: length_chars("👻")}
```

```tql
{x: 1}
```

## See Also

[`length_bytes`](length_bytes.md)
