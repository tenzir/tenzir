# is_numeric

Checks if a string contains only numeric characters.

```tql
is_numeric(x:string) -> bool
```

## Description

The `is_numeric` function returns `true` if `x` contains only numeric characters
and `false` otherwise.

## Examples

### Check if a string is numeric

```tql
from {x: is_numeric("1234")}
```

```tql
{x: true}
```

## See Also

[`is_alpha`](is_alpha.md), [`is_alnum`](is_alnum.md)
