# remove

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

[`package::add`](add.md), [`package::list`](list.md)
