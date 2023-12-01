# load

The `load` operator acquires raw bytes from a [connector](../../connectors.md).

:::warning Expert Operator
The `load` operator is a lower-level building block of the [`from`](from.md)
operator. Only use this if you need to operate on raw bytes.
:::

## Synopsis

```
load <url>
load <path>
load <connector>
```

## Description

The `load` operator emits raw bytes.

Notably, it cannot be used together with operators that expect events as input,
but rather only with operators that expect bytes, e.g.,
[`read`](../transformations/read.md) or [`save`](../sinks/save.md).

### `<connector>`

The [connector](../../connectors.md) used to load bytes.

Some connectors have connector-specific options. Please refer to the
documentation of the individual connectors for more information.

## Examples

Read bytes from stdin:

```
load stdin
```

Read bytes from the URL `https://example.com/file.json`:

```
load https://example.com/file.json
load https example.com/file.json
```

Read bytes from the file `path/to/eve.json`:

```
load path/to/eve.json
load file path/to/eve.json
```
