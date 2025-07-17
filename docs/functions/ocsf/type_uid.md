---
title: ocsf::type_uid
category: OCSF
example: 'ocsf::type_uid("SSH Activity: Fail")'
---

Returns the `type_uid` for a given `type_name`.

```tql
ocsf::type_uid(name:string) -> int
```

## Description

### `name: string`

The `type_name` for which `type_uid` should be returned.

## See Also

[`ocsf::type_name`](/reference/functions/ocsf/type_name)
