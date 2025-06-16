---
title: ocsf::category_uid
category: OCSF
example: 'ocsf::category_uid("Findings")'
---

Returns the `category_uid` for a given `category_name`.

```tql
ocsf::category_uid(name:string) -> int
```

## Description

### `name: string`

The `category_name` for which `category_uid` should be returned.

## See Also

[`ocsf::category_name`](/reference/functions/ocsf/category_name)
