# load

The `load` operator acquires raw bytes from a [connector][connector-docs].

:::warning Expert Operator
The `load` operator is a lower-level building block of the [`from`](from.md) and
[`read`](read.md) operators. Only use this if you need to operate on the raw
bytes themselves.
:::

## Synopsis

```
load <connector>
```

## Description

The `load` operator emits raw bytes. Notably, it cannot be used together with
operators that expects events as an input, but rather only with operators that
expect bytes, e.g., [`parse`](../transformations/parse.md) or
[`dump`](../sinks/dump.md).

### `<connector>`

The [connector][connector-docs] used to load bytes.

Some connectors have connector-specific options. Please refer to the
documentation of the individual connectors for more information.

## Examples

Read bytes from stdin.

```
load stdin
```

Read bytes from the file `path/to/eve.json`.

```
from file path/to/eve.json
```

[connector-docs]: ../../connectors/README.md
