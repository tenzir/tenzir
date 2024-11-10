# add

Installs a package.

```tql
package::add [package_id:string, inputs=record]
```

## Description

The `package::add` operator installs all pipelines and contexts from a package.

### `package_id : string (optional)`

The unique ID of the package as in the package definition.

### `inputs = record (optional)`

A record of optional package inputs that configure the package.

## Examples

### Add a package from the Community Library

```tql
package::add "suricata-ocsf"
```

### Add a package from a public URL

```tql
load "https://github.com/tenzir/library/raw/main/feodo/package.yaml"
read_yaml
package::add
```

### Add a package with inputs

```tql
package_add "https://github.com/tenzir/library/raw/main/zeek/package.yaml",
  inputs={format: "tsv", "log-directory": "/opt/tenzir/logs"}
```

## See Also

[`package::remove`](remove.md), [`package::list`](list.md)
