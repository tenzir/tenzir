---
title: remove
---

Uninstalls a package.

```tql
package::remove package_id:string
```

## Description

The `package::remove` operator uninstalls a previously installed package.

### `package_id : string`

The unique ID of the package as in the package definition.

## Examples

### Remove an installed package

```tql
package::remove "suricata-ocsf"
```

## See Also

[`list`](/reference/operators/package/list),
[`package::add`](/reference/operators/package/add)
