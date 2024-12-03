# map

Maps each list element to an expression.

```tql
map(xs:list, capture:field, expression:any) -> list
```

## Description

The `map` function applies an expression to each element within a list,
returning a list of the same length.

### `xs: list`

A list of values.

### `capture: field`

The name of each list element in the mapping expression.

### `expression: any`

The expression applied to each list element.

## Examples

### Check a predicate for all members of a list

```tql
from {
  hosts: [1.2.3.4, 127.0.0.1, 10.0.0.127]
}
hosts = hosts.map(x, x in 10.0.0.0/8)
```

```tql
{
  hosts: [false, false, true]
}
```

### Reshape a record inside a list

```tql
from {
  answers: [
    {
      rdata: 76.76.21.21,
      rrname: "tenzir.com"
    }
  ]
}
answers = answers.map(x, {hostname: x.rrname, ip: x.rdata})
```

```tql
{
  answers: [
    {
      hostname: "tenzir.com",
      ip: "76.76.21.21",
    }
  ]
}
```

## See Also

[`where`](where.md)
