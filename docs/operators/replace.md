---
title: replace
category: Modify
example: 'replace what=42, with=null'
---

Replaces all occurrences of a value with another value.

```tql
replace [path:field...], what=any, with=any
```

## Description

The `replace` operator scans all fields of each input event and replaces every
occurrence of a value equal to `what` with the value specified by `with`.

:::note
The operator does not replace values in lists.
:::

### `path: field... (optional)`

An optional set of paths to restrict replacements to.

### `what: any`

The value to search for and replace.

### `with: any`

The value to replace in place of `what`.

## Examples

### Replace all occurrences of 42 with null

```tql
from {
  count: 42,
  data: {value: 42, other: 100},
  list: [42, 24, 42]
}
replace what=42, with=null
```

```tql
{
  count: null,
  data: {value: null, other: 100},
  list: [42, 24, 42]
}
```

### Replace only within specific fields

```tql
from {
  count: 42,
  data: {value: 42, other: 100},
}
replace data, what=42, with=null
```

```tql
{
  count: 42,
  data: {value: null, other: 100},
}
```

### Replace a specific IP address with a redacted value

```tql
from {
  src_ip: 192.168.1.1,
  dst_ip: 10.0.0.1,
  metadata: {source: 192.168.1.1}
}
replace what=192.168.1.1, with="REDACTED"
```

```tql
{
  src_ip: "REDACTED",
  dst_ip: 10.0.0.1,
  metadata: {
    source: "REDACTED",
  },
}
```

## See Also

[`replace`](/reference/functions/replace)
