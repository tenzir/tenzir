---
title: map
category: List
example: 'xs.map(x => x + 3)'
---

Maps each list element to an expression.

```tql
map(xs:list, function:any => any) -> list
```

## Description

The `map` function applies an expression to each element within a list,
returning a list of the same length.

### `xs: list`

A list of values.

### `function: any => any`

A lambda function that is applied to each list element.

If the lambda evaluates to different but compatible types for the elements of
the list a unification is performed. For example, records are compatible with
other records, and the resulting record will have the keys of both.

If the lambda evaluates to incompatible types for different elements of the
list, the largest possible group of compatible values will be chosen and all
other values will be `null`.

## Examples

### Check a predicate for all members of a list

```tql
from {
  hosts: [1.2.3.4, 127.0.0.1, 10.0.0.127]
}
hosts = hosts.map(x => x in 10.0.0.0/8)
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
answers = answers.map(x => {hostname: x.rrname, ip: x.rdata})
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

### Null values

In the below example, the first entry does not match the given grok pattern,
causing `parse_grok` to emit a `null`.

`map` will promote `null` values to typed null values, allowing you to still
get all valid parts of the list mapped.

```tql
let $pattern = "%{WORD:w} %{NUMBER:n}"

from {
  l: ["hello", "world 42"]
}
l = l.map(str => str.parse_grok($pattern))
```
```tql
{
  l: [
    null,
    { w: "world", n: 42, },
  ],
}
```

### Incompatible types between elements

In the below example the list `l` contains three strings. Two of those are
JSON objects and one is a JSON list. While all three can be parsed as JSON by
`parse_json`, the resulting `record` and `list` are incompatible types.

`map` will resolve this by picking the "largest compatible group", in this case
preferring the two `record`s over one `list`.

```tql
from {
  l: [
    r#"{ "x": 0 }"#,
    r#"{ "y": 0 }"#,
    r#"[ 3 ]"#,
  ]
}
l = l.map(str => str.parse_json())
```
```tql
{
  l: [
    {x: 0, y: null},
    {x: null, y: 0},
    null,
  ]
}
```

## See Also

[`where`](/reference/functions/where),
[`zip`](/reference/functions/zip)
