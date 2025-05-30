---
title: decompress_brotli
---

Decompresses a stream of bytes in the Brotli format.

```tql
decompress_brotli
```

## Description

The `decompress_brotli` operator decompresses bytes in a pipeline incrementally.
The operator supports decompressing multiple concatenated streams
of the same codec transparently.

## Examples

### Import Suricata events from a Brotli-compressed file

```tql
load_file "eve.json.brotli"
decompress_brotli
read_suricata
import
```

## See Also

[`compress_brotli`](/reference/operators/compress_brotli),
[`decompress_brotli`](/reference/operators/decompress_brotli),
[`decompress_bz2`](/reference/operators/decompress_bz2),
[`decompress_gzip`](/reference/operators/decompress_gzip),
[`decompress_lz4`](/reference/operators/decompress_lz4),
[`decompress_zstd`](/reference/operators/decompress_zstd)
