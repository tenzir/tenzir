---
sidebar_custom_props:
  operator:
    sink: true
---

# save

The `save` operator saves bytes to a [connector](../connectors.md).

## Synopsis

```
save <uri>
save <path>
save <connector>
```

## Description

The `save` operator operates on raw bytes.

Notably, it cannot be used after an operator that emits events, but rather only
with operators that emit bytes, e.g., [`write`](write.md) or [`load`](load.md).

### `<connector>`

The [connector](../connectors.md) used to save bytes.

Some connectors have connector-specific options. Please refer to the
documentation of the individual connectors for more information.

## Examples

Write bytes to stdout:

```
save stdin
```

Write bytes to the file `path/to/eve.json`:

```
save path/to/eve.json
save file path/to/eve.json
```
