---
title: decompress
category: Encode & Decode
example: 'decompress "gzip"'
---
Decompresses a stream of bytes.

```tql
decompress codec:string
```

:::warning[Deprecated]
The `decompress` operator is deprecated. You should use the
[bespoke operators](/reference/operators#encode--decode) instead.
:::

## Description

The `decompress` operator decompresses bytes in a pipeline incrementally with a
known codec. The operator supports decompressing multiple concatenated streams
of the same codec transparently.

:::note[Streaming Decompression]
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming decompression.

Besides the supported `brotli`, `bz2`, `gzip`, `lz4`, and `zstd`, Apache Arrow
also ships with codecs for `lzo`, `lz4_raw`, `lz4_hadoop` and `snappy`, which
only support oneshot decompression. Support for them is not currently
implemented.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

### `codec: string`

An identifier of the codec to use. Currently supported are `brotli`, `bz2`,
`gzip`, `lz4`, and `zstd`.

## Examples

### Import Suricata events from a Zstd-compressed file

```tql
load_file "eve.json.zst"
decompress "zstd"
read_suricata
import
```

### Convert a Zstd-compressed file into an LZ4-compressed file

```tql
load_file "in.zst"
decompress "zstd"
compress "lz4"
save_file "out.lz4"
```
