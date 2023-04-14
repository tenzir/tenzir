# save

The `save` operator acquires raw bytes from a [connector][connectors].

:::warning Expert Operator
The `save` operator is a lower-level building block of the [`to`](to.md) and
[`write`](write.md) operators. Only use this if you need to operate on raw
bytes.
:::

## Synopsis

```
save <connector>
```

## Description

The `save` operator operates on raw bytes.

Notably, it cannot be used after an operator that emits events, but rather only
with operators that emit bytes, e.g., [`print`](../transformations/print.md) or
[`load`](../sources/load.md).

### `<connector>`

The [connector][connectors] used to save bytes.

Some connectors have connector-specific options. Please refer to the
documentation of the individual connectors for more information.

## Examples

Write bytes to stdout:

```
save stdin
```

Write bytes to the file `path/to/eve.json`:

```
save file path/to/eve.json
```

[connectors]: ../../connectors/README.md
