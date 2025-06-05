---
title: compress
category: Encode & Decode
example: 'compress "zstd"'
---

Compresses a stream of bytes.

```tql
compress codec:string, [level=int]
```

:::warning[Deprecated]
The `compress` operator is deprecated. You should use the
[bespoke operators](/reference/operators#encode--decode) instead.
These operators offer more options for some of the formats.
:::

## Description

The `compress` operator compresses bytes in a pipeline incrementally with a
known codec.

:::note[Streaming Compression]
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming compression.

Besides the supported `brotli`, `bz2`, `gzip`, `lz4`, and `zstd`, Apache Arrow
also ships with codecs for `lzo`, `lz4_raw`, `lz4_hadoop` and `snappy`, which
only support oneshot compression. Support for them is not currently implemented.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

### `codec: string`

An identifier of the codec to use. Currently supported are `brotli`, `bz2`,
`gzip`, `lz4`, and `zstd`.

### `level = int (optional)`

The compression level to use. The supported values depend on the codec used. If
omitted, the default level for the codec is used.

## Examples

### Export all events in a Gzip-compressed NDJSON file

```tql
export
write_ndjson
compress "gzip"
save_file "/tmp/backup.json.gz"
```

###  Recompress a Zstd-compressed file at a higher compression level

```tql
load_file "in.zst"
decompress "zstd"
compress "zstd", level=18
save_file "out.zst"
```
