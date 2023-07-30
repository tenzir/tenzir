# show

Returns meta information about the system.

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

Show all available connectors, formats, and operators:

```
show connectors
show formats
show operators
```

Show all partitions at a remote node:

```
remote show partitions
```
