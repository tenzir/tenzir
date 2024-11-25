# otherwise

Returns a `fallback` value if `primary` is `null`.

```tql
otherwise(primary:expr, fallback:expr)
```

## Description

The `otherwise` function evaluates its arguments and replaces `primary` with
`fallback` where `primary` would be `null`.

### `primary: expr`

The expression to return if not `null`.

### `fallback: expr`

The expression to return if `primary` evaluated to `null`.

## Examples

### Set a default value for a key

```tql
from [
  {x: 1},
  {x: 2},
  {}
]
x = x.otherwise(-1)
```

```tql
{x:  1 }
{x:  2 }
{x: -1 }
```
