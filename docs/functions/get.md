---
title: get
category: List, Record
example: 'xs.get(index, fallback)'
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

### `xs: record|list`

A `record` or list you want to access.

### `index: int`/`field: string`

An index or field to access. If the function's subject `xs` is a `list`, `index`
refers to the position in the list. If the subject `xs` is a `record`, `index`
refers to the field index. If the subject is a `record`, you can also use the
fields name as a `string` to refer to it.

If the given `index` or `field` are do not exist in the subject and no `fallback`
was provided, a warning will be raised and the function will return `null`.

### `fallback: any (optional)`

A fallback value to return if the given `index` or `field` do not exist in the
subject. Providing a `fallback` avoids a warning.

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
select x = this.get("x", -1), y = this.get("y", -1)
```

```tql
{x: 1, y: 2}
{x: 3, y: -1}
```
## See Also

[`keys`](/reference/functions/keys)
