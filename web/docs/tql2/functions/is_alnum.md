# is_alnum

Checks if a string is alphanumeric.

```tql
is_alnum(x:string) -> bool
```

## Description

The `is_alnum` function returns `true` if `x` contains only alphanumeric
characters and `false` otherwise.

## Examples

### Check if a string is alphanumeric

```tql
from {x: is_alnum("hello123")}
```

```tql
{x: true}
```

## See Also

[`is_alpha`](is_alpha.md), [`is_numeric`](is_numeric.md)
