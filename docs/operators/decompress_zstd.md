---
title: decompress_zstd
category: Encode & Decode
example: 'decompress_zstd'
---
Decompresses a stream of bytes in the Zstd format.

```tql
decompress_zstd
```

## Description

The `decompress_zstd` operator decompresses bytes in a pipeline incrementally.
The operator supports decompressing multiple concatenated streams
of the same codec transparently.

## Examples

### Import Suricata events from a Zstd-compressed file

```tql
load_file "eve.json.zstd"
decompress_zstd
read_suricata
import
```

## See Also

[`compress_zstd`](/reference/operators/compress_zstd),
[`decompress_brotli`](/reference/operators/decompress_brotli),
[`decompress_bz2`](/reference/operators/decompress_bz2),
[`decompress_gzip`](/reference/operators/decompress_gzip),
[`decompress_lz4`](/reference/operators/decompress_lz4),
[`decompress_zstd`](/reference/operators/decompress_zstd)
