# show

Returns meta information about Tenzir and nodes.

:::caution Experimental
This operator is experimental and subject to change without notice, even in
minor or patch releases.
:::

## Synopsis

```
show [<aspect>]
```

## Description

The `show` operator offers introspection capabilities to look at various
*aspects* of Tenzir.

### `<aspect>`

Describes the part of Tenzir to look at.

Available aspects:

- `config`: shows all current configuration options.
- `connectors`: shows all available [connectors](../../connectors.md).
- `fields`: shows all fields of existing tables at a remote node.
- `formats`: shows all available [formats](../../formats.md).
- `nics`: shows all network interfaces for the [`nic`](../../connectors/nic.md)
  loader.
- `operators`: shows all available [operators](../../operators.md).
- `partitions`: shows all table partitions of a remote node.
- `pipelines`: shows all managed pipelines of a remote node.
- `plugins`: shows all loaded plugins.
- `version`: shows the Tenzir version.

We also offer some additional aspects for experts that want to take a deeper
look at what's going on:

- `build`: shows compile-time build information.
- `dependencies`: shows information about build-time dependencies.
- `serves` shows all pipelines with the `serve` sink operator currently
  available from the `/serve` API endpoint.
- `types`: shows all known types at a remote node.

When no aspect is specified, all are shown.

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

Show all aspects of a remote node:

```
show
```
