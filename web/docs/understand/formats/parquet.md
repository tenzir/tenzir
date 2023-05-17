# parquet

Reads events from a Parquet file. Writes events to a [Parquet][parquet] file.

[parquet]: https://parquet.apache.org/

## Synopsis

```
parquet
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

VAST writes Parquet files with Zstd compression enables. Our blog has a [post
with an in-depth analysis][parquet-and-feather-blog] about the effect of Zstd
compression.

[parquet-and-feather-blog]: ../../../../blog/parquet-and-feather-writing-security-telemetry/

## Examples

Read a Parquet file via the [`from`](../operators/sources/from.md) operator:

```
from file --mmap /tmp/data.prq read parquet
```
:::caution Limitation
The `parquet` parser currently supports only Parquet files written with VAST. We
will remove this limitation in the future.
:::
