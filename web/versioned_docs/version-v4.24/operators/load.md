---
sidebar_custom_props:
  operator:
    source: true
---

# load

The `load` operator acquires raw bytes from a [connector](../connectors.md).

## Synopsis

```
load <url>
load <path>
load <connector>
```

## Description

The `load` operator emits raw bytes.

Notably, it cannot be used together with operators that expect events as input,
but rather only with operators that expect bytes, e.g., [`read`](read.md) or
[`save`](save.md).

### `<connector>`

The [connector](../connectors.md) used to load bytes.

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
