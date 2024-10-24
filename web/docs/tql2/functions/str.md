# str

Casts an expression to a string.

```tql
str(x:any) -> string
```

## Description

The `str` function casts the given value `x` to a string.

## Examples

### Cast an IP address to a string

```tql
from {x: str(1.2.3.4)}
```

```tql
{x: "1.2.3.4"}
```
