---
title: type_of
category: Type System/Introspection
example: 'type_of(this)'
---

Retrieves the type definition of an expression.

```tql
type_of(x:any) -> record
```

## Description

The `type_of` function returns the type definition of the given value `x`.

:::warning Subject to change
This function is designed for internal use of the Tenzir Platform and its output
format is subject to change without notice.
:::

## Examples

### Retrieve the type definition of a schema

```tql
from {x: 1, y: "2"}
this = type_of(this)
```

```tql
{
  name: "tenzir.from",
  kind: "record",
  attributes: [],
  state: {
    fields: [
      {
        name: "x",
        type: {
          name: null,
          kind: "int64",
          attributes: [],
          state: null,
        },
      },
      {
        name: "y",
        type: {
          name: null,
          kind: "string",
          attributes: [],
          state: null,
        },
      },
    ],
  },
}
```

## See also

[`type_id`](/reference/functions/type_id)
