# all

Computes the conjunction (AND) of all grouped boolean values.

```tql
all(xs:list) -> bool
```

## Description

The `all` function returns `true` if all values in `xs` are `true` and `false`
otherwise.

### `xs: list`

A list of boolean values.

## Examples

### Check if all values are true

```tql
from [
  {x: true},
  {x: true},
  {x: false},
]
summarize result=all(x)
```

```tql
{result: false}
```

## See Also

[`any`](any.md)
