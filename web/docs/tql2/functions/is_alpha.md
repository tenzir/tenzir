# is_alpha

Checks if a string contains only alphabetic characters.

```tql
is_alpha(x:string) -> bool
```

## Description

The `is_alpha` function returns `true` if `x` contains only alphabetic
characters and `false` otherwise.

## Examples

### Check if a string is alphabetic

```tql
from {x: is_alpha("hello")}
```

```tql
{x: true}
```

## See Also

[`is_alnum`](is_alnum.md), [`is_lower`](is_lower.md), [`is_upper`](is_upper.md)
