---
title: package::add
category: Packages
example: 'package::add "suricata-ocsf"'
---

Installs a package.

```tql
package::add [package_path:string, inputs=record]
```

## Description

The `package::add` operator installs all operators, pipelines, and contexts from a package.

### `package_path : string (optional)`

The path to a package located on the file system.

### `inputs = record (optional)`

A record of optional package inputs that configure the package.

## Examples

### Add a package from the Community Library

```tql
package::add "suricata-ocsf"
```

### Add a local package with inputs

```tql
package::add "/mnt/config/tenzir/library/zeek",
  inputs={format: "tsv", "log-directory": "/opt/tenzir/logs"}
```

## See Also

[`list`](/reference/operators/package/list),
[`remove`](/reference/operators/package/remove)
