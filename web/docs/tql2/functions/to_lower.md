# to_lower

Converts a string to lowercase.

```tql
to_lower(x:string) -> string
```

## Description

The `to_lower` function converts all characters in `x` to lowercase.

## Examples

### Convert a string to lowercase

```tql
from {x: to_lower("HELLO")}
```

```tql
{x: "hello"}
```

## See Also

[`to_upper`](to_upper.md), [`to_title`](to_title.md)
