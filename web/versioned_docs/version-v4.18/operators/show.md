---
sidebar_custom_props:
  operator:
    source: true
---

# show

Returns information about a Tenzir node.

## Synopsis

```
show [<aspect>]
```

## Description

The `show` operator offers introspection capabilities to look at various
*aspects* of a Tenzir node.

### `<aspect>`

Describes the part of Tenzir to look at.

Available aspects:

- `config`: shows all current configuration options.
- `connectors`: shows all available [connectors](../connectors.md).
- `contexts`: shows all available contexts.
- `formats`: shows all available [formats](../formats.md).
- `operators`: shows all available [operators](../operators.md).
- `pipelines`: shows all managed pipelines of a remote node.
- `plugins`: shows all loaded plugins.

We also offer some additional aspects for experts that want to take a deeper
look at what's going on:

- `build`: shows compile-time build information.
- `dependencies`: shows information about build-time dependencies.
- `fields`: shows all fields of existing tables at a remote node.
- `schemas` shows all schema definitions for which data is stored at the node.
- `serves` shows all pipelines with the `serve` sink operator currently
  available from the `/serve` API endpoint.

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

Show all fields at a node:

```
show fields
```

Show all aspects of a node:

```
show
```
