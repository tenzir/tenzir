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
packages [--format=<format>]
```

## Description

The `packages` operator show a list of installed packages.

The `package` operator manages packages.

- The `add` command adds a new package on the node by
  running all pipelines and contexts defined in the package.

- The `remove` command removes an existing package.

### `<package_id>`

The unique id of the package, as found in the package definition.

### `<format>`

This option controls the output format of the `packages` operator.
Valid options are `compact` (the default) and `extended`. See the
`Package List Formats` section below for more details.

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

## Package List Formats

The output of the `packages` operator can be controlled by the `--format`
parameter

### Compact Format

The `compact` format consists of one object per installed package
with keys `id`, `name`, `author` and `description`.

### Extended Format

The `extended` format is mainly intended for use by non-human consumers, like shell
scripts or frontend code. It consists of an object with four top-level keys:

 1. `package_definition`: The original package definition object as found in the library.
 2. `resolved_package`: The effective package definition that was produced by applying
    all inputs and overrides from the `config` section and removing all disabled pipelines
    and contexts.
 3. `config`: The user-provided `config` object that was provided when installing the package.
 4. `package_status`: Contains additional information about the package provided by the package
    manager, currently the `install_state` field that can be one of `installing`, `installed` or
    `zombie` and the `from_configuration` boolean that shows whether a package was added
    at runtime via the `package add` operator or from a configuration file on the node.
