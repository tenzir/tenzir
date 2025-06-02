---
title: sort
category: List, Record
example: 'xs.sort()'
---

Sorts lists and record fields.

```tql
sort(xs:list|record) -> list|record
```

## Description

The `sort` function takes either a list or record as input, ordering lists by
value and records by their field name.

### `xs: list|record`

The list or record to sort.

## Examples

### Sort values in a list

```tql
from {xs: [1, 3, 2]}
xs = xs.sort()
```

```tql
{xs: [1, 2, 3]}
```

### Sort a record by its field names

```tql
from {a: 1, c: 3, b: {y: true, x: false}}
this = this.sort()
```

```tql
{a: 1, b: {y: true, x: false}, c: 3}
```

Note that nested records are not automatically sorted. Use `b = b.sort()` to sort it
manually.
