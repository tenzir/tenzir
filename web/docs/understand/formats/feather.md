# feather

Reads and writes the [Feather][feather] file format.

[feather]: https://arrow.apache.org/docs/python/feather.html

## Synopsis

```
feather
```

## Description

The `feather` format provides both a parser and a printer for Feather files.

[Feather][feather] is a thin layer on top of [Arrow
IPC](https://arrow.apache.org/docs/python/ipc.html#ipc), making it
conducive for memory mapping and zero-copy usage scenarios.

:::tip MMAP Parsing
When using the parser with the [`file`](../connectors/file.md) connector, we
recommend passing the `--mmap` option to `file` to give the parser full control
over the reads, which leads to better performance and memory usage.
:::

VAST writes Feather files with Zstd compression enables. Our blog has a [post
with an in-depth analysis][parquet-and-feather-blog] about the effect of Zstd
compression.

[parquet-and-feather-blog]: ../../../../blog/parquet-and-feather-writing-security-telemetry/

## Examples

Read a Feather file via the [`from`](../operators/sources/from.md) operator:

```
from file --mmap /tmp/data.feather read feather
```
