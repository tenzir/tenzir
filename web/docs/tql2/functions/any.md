# any

Computes the disjunction (OR) of all grouped boolean values.

```tql
any(xs:list) -> bool
```

## Description

The `any` function returns `true` if any value in `xs` is `true` and `false`
otherwise.

### `xs: list`

A list of boolean values.

## Examples

### Check if any value is true

```tql
from [
  {x: false},
  {x: false},
  {x: true},
]
summarize result=any(x)
```

```tql
{result: true}
```

## See Also

[`all`](all.md)
