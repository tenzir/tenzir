# is_lower

Checks if a string is in lowercase.

```tql
is_lower(x:string) -> bool
```

## Description

The `is_lower` function returns `true` if `x` is entirely in lowercase and
`false` otherwise.

## Examples

### Check if a string is lowercase

```tql
from {x: "hello".is_lower()}
```

```tql
{x: true}
```

## See Also

[`to_lower`](to_lower.md), [`is_upper`](is_upper.md)
