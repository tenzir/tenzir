# all

Maps each list element to an expression.

```tql
map(list:list, capture:field, expression:any) -> list
```

## Description

The `map` function applies an expression to each element within a list,
returning a list of the same length.

### `list : list`

A list of values.

### `capture : field`

The name of each list element in the mapping expression.

### `expression : any`

The expression applied to each list element.

## Examples

### Stringify all elements of list

```tql
from {
  hosts: [1.2.3.4, 127.0.0.1, 10.0.0.127]
}
hosts = hosts.map(host, str(host))
```

```tql
{hosts: ["1.2.3.4", "127.0.0.1", "10.0.0.127"]}
```

### Reshape a record inside a list

```
from {
  answers: [{rdata: 76.76.21.21, rrname: "tenzir.com"}]
}
answers = answers.map(x, {hostname: x.rrname, ip: x.rdata})
```

## See Also

[`where`](where.md)
