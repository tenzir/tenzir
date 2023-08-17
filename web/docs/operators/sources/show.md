# show

Returns meta information about Tenzir and nodes.

:::caution Experimental
This operator is experimental and subject to change without notice, even in
minor or patch releases.
:::

## Synopsis

```
show <aspect> [options]
```

## Description

The `show` operator offers introspection capabilities to look at various
*aspects* of Tenzir.

### `<aspect>`

Describes the part of Tenzir to look at.

Available aspects:

- `build`: shows compile-time build information.
- `config`: shows all current configuration options.
- `connectors`: shows all available [connectors](../../connectors.md).
- `dependencies`: shows information about build-time dependencies.
- `fields`: shows all fields of existing tables at a remote node.
- `formats`: shows all available [formats](../../formats.md).
- `operators`: shows all available [operators](../../operators.md).
- `partitions`: shows all table partitions of a remote node.
- `pipelines`: shows all managed pipelines of a remote node.
- `plugins`: shows all loaded plugins.
- `tables`: shows schema and metadata of all tables at a remote catalog.
- `types`: shows all known types at a remote node.
- `version`: shows the Tenzir version.

## Examples

Show all available connectors and formats:

```
show connectors
show formats
```

Show all transformations:

```
show operators | where transformation == true
```

Show all fields and partitions at a node:

```
show fields
show partitions
```

Show the version of a remote node:

```
remote show version
```
