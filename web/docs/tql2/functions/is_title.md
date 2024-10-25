# is_title

Checks if a string follows title case.

```tql
is_title(x:string) -> bool
```

## Description

The `is_title` function returns `true` if `x` is in title case and `false`
otherwise.

## Examples

### Check if a string is in title case

```tql
from {x: is_title("Hello World")}
```

```tql
{x: true}
```

## See Also

[`to_title`](to_title.md)
