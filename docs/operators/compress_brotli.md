---
title: compress_brotli
---

Compresses a stream of bytes using Brotli compression.

```tql
compress_brotli [level=int, window_bits=int]
```

## Description

The `compress_brotli` operator compresses bytes in a pipeline incrementally.

### `level = int (optional)`

The compression level to use. The supported values depend on the codec used. If
omitted, the default level for the codec is used.

### `window_bits = int (optional)`

A number representing the encoder window bits.

## Examples

### Export all events in a Brotli-compressed NDJSON file

```tql
export
write_ndjson
compress_brotli
save_file "/tmp/backup.json.bt"
```

### Recompress a Brotli-compressed file at a different compression level

```tql
load_file "in.brotli"
decompress_brotli
compress_brotli level=18
save_file "out.brotli"
```

## See Also

[`compress_bz2`](/reference/operators/compress_bz2),
[`compress_gzip`](/reference/operators/compress_gzip),
[`compress_lz4`](/reference/operators/compress_lz4),
[`compress_zstd`](/reference/operators/compress_zstd),
[`decompress_brotli`](/reference/operators/decompress_brotli)
