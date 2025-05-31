---
title: merge
category: Record
example: 'merge(foo, bar)'
---

Combines two records into a single record by merging their fields.

```tql
merge(x: record, y: record) -> record
```

## Description

The `merge` function takes two records and returns a new record containing all
fields from both records. If both records contain the same field, the value from
the second record takes precedence.

## Examples

### Basic record merging

```tql
from {x: {a: 1, b: 2}, y: {c: 3, d: 4}}
select result = merge(x, y)
```

```tql
{
  result: {
    a: 1,
    b: 2,
    c: 3,
    d: 4
  }
}
```

### Handling overlapping fields

When fields exist in both records, the second record's values take precedence:

```tql
from {
  r1: {name: "Alice", age: 30},
  r2: {name: "Bob", location: "NY"},
}
select result = merge(r1, r2)
```

```tql
{
  result: {
    name: "Bob",
    age: 30,
    location: "NY"
  }
}
```

### Handling null values

If either input is null, the input will be ignored.

```tql
from {x: {a: 1}, y: null}
select result = merge(x, y)
```

```tql
{
  result {
    a: 1
  }
}
```

## See Also

[`concatenate`](/reference/functions/concatenate)
