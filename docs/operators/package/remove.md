---
title: package::remove
category: Packages
example: 'package::remove "suricata-ocsf"'
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

[`package::add`](/reference/operators/package/add)
