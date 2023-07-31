# show

Returns meta information about Tenzir and nodes.

:::caution Experimental
This operator is experimental and subject to change without notice, even in
minor or patch releases.
:::

## Synopsis

```
show <aspect>
```

## Description

The `show` operator offers introspection capabilities to look at various
*aspects* of Tenzir.

### `<aspect>`

Describes the part of Tenzir to look at.

Available aspects:

- `connectors`: shows all available [connectors](../../connectors.md).
- `formats`: shows all available [formats](../../formats.md).
- `operators`: shows all available [operators](../../operators.md).
- `partitions`: shows all partitions of a (remote) node.
- `types`: shows all known type definitions.

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

Show all partitions at a remote node:

```
show partitions
```
