---
title: get
category: List, Record
example: 'list.get(index, default)'
---

Gets a field from a record or an element from a list

```tql
get(x:record, field:string, [fallback:any]) -> any
get(x:record|list, index:number, [fallback:any]) -> any
```

## Description

The `get` function returns the record field with the name `field` or the list
element with the index `index`. If `fallback` is provided, the function
gracefully returns the fallback value instead of emitting a warning and
returning `null`.

## Examples

### Get the first element of a list, or a fallback value

```tql
from (
  {xs: [1, 2, 3]},
  {xs: []},
}
select first = xs.get(0, -1)
```

```tql
{first: 1}
{first: -1}
```

### Access a field of a record, or a fallback value

```tql
from (
  {x: 1, y: 2},
  {x: 3},
}
select x = x.get("x", -1), y = y.get("y", -1)
```

```tql
{x: 1, y: 2}
{x: 3, y: -1}
```
## See Also

[`keys`](/reference/functions/keys)
