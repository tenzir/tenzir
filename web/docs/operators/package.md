---
sidebar_custom_props:
  operator:
    source: true
    transformation: false
    sink: true
---

# package

Manages the packages at a node.

A package is a set of pipelines and contexts that can be added to a node
as a single unit. Most packages are installed from a library, a public
repository of packages, and contain a collection of thematically related
pipelines and contexts.

:::note Experimental
This operator is made available in preparation of the upcoming package
management support of Tenzir. All interfaces are subject to change,
and user-facing documentation of the package format is still in progress.
:::

## Synopsis

```
package add
package remove <package_id>
```

## Description

The `packages` operator manages packages.

- The `add` command adds a new package on the node by
  running all pipelines and contexts defined in the package.

- The `remove` command removes an existing package.

To get a list of all successfully installed packages, use the [show](./show.md) operator
by running `show packages`.

### `<package_id>`

The unique id of the package, as found in the package definition.

## Examples

Add a package from a public URL:

```
from https://github.com/tenzir/library/raw/main/feodo/package.yaml
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
