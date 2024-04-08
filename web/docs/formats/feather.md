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

Parser:
```
feather
```

Printer:
```
feather [—compression-type=<type>] [—compression-level=<level] [—min—space-savings=<rate>]
```

## Description

The `feather` format provides both a parser and a printer for Feather files and
Apache Arrow IPC streams.  

:::note Compression
feather printer offers a more efficent alternative to the [`compress`](../operators/compress.md). The feather printer can more efficent batch the data because it knows the underlying data.
:::

:::note Limitation
Tenzir currently assumes that all Feather files and Arrow IPC streams use
metadata recognized by Tenzir. We plan to lift this restriction in the future.
:::

### `--compression-type` (Printer)

An optional output file type, either LZ4_FRAME or ZSTD. If compression-type is not specified uncompressed Feather is the default output type. The compression-type variable should be lowercase, lz4 or zstd, respectively. A default compression-level and min-space-savings are choosen if not specified.

### `--compression-level` (Printer)

An optional compression level for the corresponding compression-type. If no compression-type was supplied, this option is ignored.

### `--min-space-savings` (Printer)

An optional minimum space savings percentage required for compression. Space savings is calculated as '1.0 - compressed_size / uncompressed_size'. For example, if 'min_space_savings = 0.1', a 100-byte body buffer won’t undergo compression if its expected compressed size exceeds 90 bytes. If this option is unset, compression will be used indiscriminately. If no compression-type was supplied, this option is ignored. Values outside of the range [0,1] are handled as errors.


## Examples

Read a Feather file via the [`from`](../operators/from.md) operator:

```
from file --mmap /tmp/data.feather read feather
```

Write a Feather file via [`to`](../operators/to.md) operator:
```
from file --mmap /tmp/data.json | write feather
```
```
from file tmp/suricata.json | to file tmp/suricata.feather write feather --compression-level -1 --compression-type lz4 --min-space-savings 0.6
```
