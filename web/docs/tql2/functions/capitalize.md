# capitalize

Capitalizes the first character of a string.

```tql
capitalize(x:string) -> string
```

## Description

The `capitalize` function returns the input string with the first character
converted to uppercase and the rest to lowercase.

## Examples

### Capitalize a lowercase string

```tql
from {x: capitalize("hello world")}
```

```tql
{x: "Hello world"}
```

## See Also

[`to_upper`](to_upper.md), [`to_lower`](to_lower.md), [`to_title`](to_title.md)
