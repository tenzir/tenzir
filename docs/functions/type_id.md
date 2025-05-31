---
title: type_id
category: Type System/Introspection
example: 'type_id(1 + 3.2)'
---
Retrieves the type id of an expression.

```tql
type_id(x:any) -> string
```

## Description

The `type_id` function returns the type id of the given value `x`.

## Examples

### Retrieve the type of a numeric expression

```tql
from {x: type_id(1 + 3.2)}
```

```tql
{x: "41615fdb30a38aaf"}
```

## See also

[`type_of`](/reference/functions/type_of)
