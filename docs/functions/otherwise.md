---
title: otherwise
category: Aggregation
example: 'x.otherwise(0)'
---

Returns a `fallback` value if `primary` is `null`.

```tql
otherwise(primary:any, fallback:any) -> any
```

## Description

The `otherwise` function evaluates its arguments and replaces `primary` with
`fallback` where `primary` would be `null`.

### `primary: any`

The expression to return if not `null`.

### `fallback: any`

The expression to return if `primary` evaluates to `null`.

## Examples

### Set a default value for a key

```tql
from {x: 1}, {x: 2}, {}
x = x.otherwise(-1)
```

```tql
{x: 1}
{x: 2}
{x: -1}
```
