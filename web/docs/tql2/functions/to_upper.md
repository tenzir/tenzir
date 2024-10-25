# to_upper

Converts a string to uppercase.

```tql
to_upper(x:string) -> string
```

## Description

The `to_upper` function converts all characters in `x` to uppercase.

## Examples

### Convert a string to uppercase

```tql
from {x: to_upper("hello")}
```

```tql
{x: "HELLO"}
```

## See Also

[`to_lower`](to_lower.md), [`to_title`](to_title.md)
