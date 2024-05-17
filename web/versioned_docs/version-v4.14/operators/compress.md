---
sidebar_custom_props:
  operator:
    transformation: true
---

# compress

Compresses a stream of bytes.

## Synopsis

```
compress [--level=<level>] <codec>
```

## Description

The `compress` operator compresses bytes in a pipeline incrementally with a
known codec.

The `compress` operator is invoked automatically as a part of [`to`](to.md)
if the resulting file has a file extension indicating compression.
This behavior can be circumvented by using [`save`](save.md) directly.

:::note Streaming Compression
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming compression.

Besides the supported `brotli`, `bz2`, `gzip`, `lz4`, and `zstd`, Apache Arrow
also ships with codecs for `lzo`, `lz4_raw`, `lz4_hadoop` and `snappy`, which
only support oneshot compression. Support for them is not currently implemented.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

### `--level=<level>`

The compression level to use. The supported values depend on the codec used. If
omitted, the default level for the codec is used.

### `<codec>`

An identifier of the codec to use. Currently supported are `brotli`, `bz2`,
`gzip`, `lz4`, and `zstd`.

## Examples

Export all events in a Gzip-compressed NDJSON file:

```
export
| write json --compact-output
| compress gzip
| save file /tmp/backup.json.gz
```

Recompress a Zstd-compressed file at a higher compression level:

```
load file in.zst
| decompress zstd
| compress --level 18 zstd
| save file out.zst
```
