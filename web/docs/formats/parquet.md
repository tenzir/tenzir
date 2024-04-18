---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# parquet

Reads events from a [Parquet][parquet] file. Writes events to a [Parquet][parquet] file.

[parquet]: https://parquet.apache.org/

## Synopsis

Parser:

```
parquet
```

Printer:

```
parquet [—compression-type=<type>] [—compression-level=<level>]
```


## Description

The `parquet` format provides both a parser and a printer for Parquet files.

[Apache Parquet][parquet] is a columnar storage format that a variety of data
tools support.

:::tip MMAP Parsing
When using the parser with the [`file`](../connectors/file.md) connector, we
recommend passing the `--mmap` option to `file` to give the parser full control
over the reads, which leads to better performance and memory usage.
:::

:::note Limitation
Tenzir currently assumes that all Feather files and Arrow IPC streams use
metadata recognized by Tenzir. We plan to lift this restriction in the future.
:::

### `--compression-type` (Printer)

Specifies an optional compression type. Supported options are `zstd` for
[Zstandard][zstd-docs]http://facebook.github.io/zstd/ compression and `lz4` for
[LZ4 Frame][lz4-docs] compression.

[zstd-docs]: http://facebook.github.io/zstd/
[lz4-docs]: https://android.googlesource.com/platform/external/lz4/+/HEAD/doc/lz4_Frame_format.md

:::info Why would I use this over the `compress` operator?
The Parquet format offers more efficient compression for LZ4 and Zstd compared
to the [`compress`](../operators/compress.md) operator. This is because it
compresses the data column-by-column, leaving metadata that needs to be accessed
frequently uncompressed.
:::

### `--compression-level` (Printer)

An optional compression level for the corresponding compression type. This
option is ignored if no compression type is specified.

Defaults to the compression type's default compression level.

[parquet-and-feather-blog]: ../../../../blog/parquet-and-feather-writing-security-telemetry/

## Examples

Read a Parquet file via the [`from`](../operators/from.md) operator:

```
from file --mmap /tmp/data.prq read parquet
```

Write a Zstd-compressed Parquet file via [`to`](../operators/to.md) operator:

```
to /tmp/suricata.parquet write parquet --compression-type zstd
```
