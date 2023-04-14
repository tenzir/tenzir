# dump

The `dump` operator acquires raw bytes from a [connector][connector-docs].

:::warning Expert Operator
The `dump` operator is a lower-level building block of the [`to`](to.md) and
[`write`](write.md) operators. Only use this if you need to operate on the raw
bytes themselves.
:::

## Synopsis

```
dump <connector>
```

## Description

The `dump` operator operates on raw bytes. Notably, it cannot be used after an
opeerator that emits events,  but rather only with operators that emit bytes,
e.g., [`print`](../transformations/print.md) or [`load`](../sources/load.md).

### `<connector>`

The [connector][connector-docs] used to dump bytes.

Some connectors have connector-specific options. Please refer to the
documentation of the individual connectors for more information.

## Examples

Write bytes to stdout

```
dump stdin
```

Write bytes to the file `path/to/eve.json`.

```
dump file path/to/eve.json
```

[connector-docs]: ../../connectors/README.md
