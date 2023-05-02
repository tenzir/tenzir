# load

The `load` operator acquires raw bytes from a [connector][connectors].

:::warning Expert Operator
The `load` operator is a lower-level building block of the [`from`](from.md) and
[`read`](read.md) operators. Only use this if you need to operate on raw bytes.
:::

## Synopsis

```
load <connector>
```

## Description

The `load` operator emits raw bytes.

Notably, it cannot be used together with operators that expect events as input,
but rather only with operators that expect bytes, e.g.,
[`parse`](../transformations/parse.md) or [`save`](../sinks/save.md).

### `<connector>`

The [connector][connectors] used to load bytes.

Some connectors have connector-specific options. Please refer to the
documentation of the individual connectors for more information.

## Examples

Read bytes from stdin:

```
load stdin
```

Read bytes from the file `path/to/eve.json`:

```
from file path/to/eve.json
```

[connectors]: ../../connectors/README.md
