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

:::warning Limitation
Tenzir currently assumes that all Parquet files use metadata recognized by
Tenzir. We plan to lift this restriction in the future.
:::

### `--compression-type` (Printer)

Specifies an optional compression type. Supported options are `zstd` for
[Zstandard][zstd-docs] compression, `brotli` for [brotli][brotli-docs]
compression, `gzip` for [gzip][gzip-docs] compression, and `snappy` for
[snappy][snappy-docs] compression.

[zstd-docs]: http://facebook.github.io/zstd/
[gzip-docs]: https://www.gzip.org
[brotli-docs]: https://www.brotli.org
[snappy-docs]: https://google.github.io/snappy/

:::info Why would I use this over the `compress` operator?
The Parquet format offers more efficient compression compared to the
[`compress`](../operators/compress.md) operator. This is because it compresses
the data column-by-column, leaving metadata that needs to be accessed frequently
uncompressed.
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
