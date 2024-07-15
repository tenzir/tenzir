---
sidebar_custom_props:
  operator:
    source: true
    transformation: false
    sink: true
---

# package

Manages the packages installed on a given node.

Manages a [package](../packages.md).

## Synopsis

```
package add
package remove <package_id>
show packages

// tql2
package_add
package_remove <package_id>
```

## Description

The `packages` operator manages [packages](../packages.md).

- The `add` command adds a new package on the node by
  running all pipelines and contexts defined in the package.

- The `remove` command removes an existing package.

- The `show packages` command generates a list of successfully
  installed packages.

### `<package_id>`

The unique id of the package, as found in the package definition.

## Examples

Add a [package](../packages.md) from a public URL:

```
from https://github.com/tenzir/library/raw/main/feodo/package.yaml read yaml
| package add
```

Add a package with required inputs:

```
// experimental-tql2
load "https://github.com/tenzir/library/raw/main/zeek/package.yaml"
read_yaml
config.inputs.format = "tsv"
config.inputs["log-directory"] = "/opt/tenzir/logs"
package_add
```

Remove the installed package `zeek`:
```
package remove zeek
```
