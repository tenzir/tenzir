# first

Takes the first non-null grouped value.

```tql
first(xs:list) -> any
```

## Description

The `first` function returns the first non-null value in `xs`.

### `xs: list`

The values to search.

## Examples

### Get the first non-null value

```tql
from [
  {x: null},
  {x: 2},
  {x: 3},
]
summarize first_value=first(x)
```

```tql
{first_value: 2}
```

## See Also

[`last`](last.md)
