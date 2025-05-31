---
title: decompress_lz4
category: Encode & Decode
example: 'decompress_lz4'
---
Decompresses a stream of bytes in the Lz4 format.

```tql
decompress_lz4
```

## Description

The `decompress_lz4` operator decompresses bytes in a pipeline incrementally.
The operator supports decompressing multiple concatenated streams
of the same codec transparently.

## Examples

### Import Suricata events from a LZ4-compressed file

```tql
load_file "eve.json.lz4"
decompress_lz4
read_suricata
import
```

## See Also

[`compress_lz4`](/reference/operators/compress_lz4),
[`decompress_brotli`](/reference/operators/decompress_brotli),
[`decompress_bz2`](/reference/operators/decompress_bz2),
[`decompress_gzip`](/reference/operators/decompress_gzip),
[`decompress_lz4`](/reference/operators/decompress_lz4),
[`decompress_zstd`](/reference/operators/decompress_zstd)
