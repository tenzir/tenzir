---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# feather

Reads and writes the [Feather][feather] file format, a thin wrapper around
[Apache Arrow's IPC][arrow-ipc] wire format.

[feather]: https://arrow.apache.org/docs/python/feather.html
[arrow-ipc]: https://arrow.apache.org/docs/python/ipc.html

## Synopsis

```
feather
```

## Description

The `feather` format provides both a parser and a printer for Feather files and
Apache Arrow IPC streams.

:::note Limitation
Tenzir currently assumes that all Feather files and Arrow IPC streams use
metadata recognized by Tenzir. We plan to lift this restriction in the future.
:::

## Examples

Read a Feather file via the [`from`](../operators/from.md) operator:

```
from file --mmap /tmp/data.feather read feather
```
