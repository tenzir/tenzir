---
sidebar_custom_props:
  operator:
    transformation: true
---

# decompress

Decompresses a stream of bytes.

## Synopsis

```
decompress <codec>
```

## Description

The `decompress` operator decompresses bytes in a pipeline incrementally with a
known codec. The operator supports decompressing multiple concatenated streams
of the same codec transparently.

The `decompress` operator is invoked automatically as a part of [`from`](from.md)
if the source file has a file extension indicating compression.
This behavior can be circumvented by using [`load`](load.md) directly.

:::note Streaming Decompression
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming decompression.

Besides the supported `brotli`, `bz2`, `gzip`, `lz4`, and `zstd`, Apache Arrow
also ships with codecs for `lzo`, `lz4_raw`, `lz4_hadoop` and `snappy`, which
only support oneshot decompression. Support for them is not currently implemented.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

### `<codec>`

An identifier of the codec to use. Currently supported are `brotli`, `bz2`,
`gzip`, `lz4`, and `zstd`.

## Examples

Import Suricata events from a Zstd-compressed file:

```
from eve.json.zst
| import

load file eve.json.zst
| decompress zstd
| read suricata
| import
```

Convert a Zstd-compressed file into an LZ4-compressed file:

```
from in.zst
| to out.lz4

load file in.zst
| decompress zstd
| compress lz4
| save file out.lz4
```
