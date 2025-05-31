---
title: decompress_gzip
category: Encode & Decode
example: 'decompress_gzip'
---
Decompresses a stream of bytes in the Gzip format.

```tql
decompress_gzip
```

## Description

The `decompress_gzip` operator decompresses bytes in a pipeline incrementally.
The operator supports decompressing multiple concatenated streams
of the same codec transparently.

## Examples

### Import Suricata events from a Gzip-compressed file

```tql
load_file "eve.json.gz"
decompress_brotli
decompress_gzip
import
```

## See Also

[`compress_gzip`](/reference/operators/compress_gzip),
[`decompress_brotli`](/reference/operators/decompress_brotli),
[`decompress_bz2`](/reference/operators/decompress_bz2),
[`decompress_gzip`](/reference/operators/decompress_gzip),
[`decompress_lz4`](/reference/operators/decompress_lz4),
[`decompress_zstd`](/reference/operators/decompress_zstd)
