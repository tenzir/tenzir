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

:::warning Limitation
Tenzir currently assumes that all Feather files and Arrow IPC streams use
metadata recognized by Tenzir. We plan to lift this restriction in the future.
:::

### `--compression-type` (Printer)

Specifies an optional compression type. Supported options are `zstd` for
[Zstandard][zstd-docs] compression and `lz4` for [LZ4 Frame][lz4-docs]
compression.

[zstd-docs]: http://facebook.github.io/zstd/
[lz4-docs]: https://android.googlesource.com/platform/external/lz4/+/HEAD/doc/lz4_Frame_format.md

:::info Why would I use this over the `compress` operator?
The Feather format offers more efficient compression compared to the
[`compress`](../operators/compress.md) operator. This is because it compresses
the data column-by-column, leaving metadata that needs to be accessed frequently
uncompressed.
:::

### `--compression-level` (Printer)

An optional compression level for the corresponding compression type. This
option is ignored if no compression type is specified.

Defaults to the compression type's default compression level.

### `--min-space-savings` (Printer)

An optional minimum space savings percentage required for compression to be
applied. This option is ignored if no compression is specified. The provided
value must be between 0 and 1 inclusive.

Defaults to 0, i.e., always applying compression.

Space savings are calculated as `1.0 - compressed_size / uncompressed_size`.
E.g., for a minimum space savings rate of 0.1 a 100-byte body buffer will not
be compressed if its expected compressed size exceeds 90 bytes.

## Examples

Read a Feather file via the [`from`](../operators/from.md) operator:

```
from /tmp/data.feather --mmap read feather
```

Write a Zstd-compressed Feather file via [`to`](../operators/to.md) operator:

```
to /tmp/suricata.feather write feather --compression-type zstd
```
