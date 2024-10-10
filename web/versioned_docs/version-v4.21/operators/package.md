---
sidebar_custom_props:
  operator:
    source: false
    transformation: false
    sink: true
---

# package

Manages the packages at a node.

A package is a set of pipelines and contexts that can be added to a node
as a single unit. Most packages are installed from a library, a public
repository of packages, and contain a collection of thematically related
pipelines and contexts.

## Synopsis

```
package add
package remove <package_id>
```

## Description

The `package` operator manages packages.

- The `add` command adds a new package on the node by
  running all pipelines and contexts defined in the package.

- The `remove` command removes an existing package.

### `<package_id>`

The unique id of the package, as found in the package definition.

## Examples

Add a package from a public URL:

```
from https://github.com/tenzir/library/raw/main/feodo/package.yaml
| package add
```

Add a package from the [Community Library](https://github.com/tenzir/library):

```
// tql2
package_add "suricata-ocsf",
```

Add a package with required inputs:

```
// tql2
package_add "https://github.com/tenzir/library/raw/main/zeek/package.yaml",
  inputs={format: "tsv", "log-directory": "/opt/tenzir/logs"}
```

Remove the installed package `zeek`:

```
package remove zeek
```
