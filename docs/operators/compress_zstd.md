---
title: compress_zstd
category: Encode & Decode
example: 'compress_zstd, level=6'
---
Compresses a stream of bytes using zstd compression.

```tql
compress_zstd [level=int]
```

## Description

The `compress_zstd` operator compresses bytes in a pipeline incrementally.

### `level = int (optional)`

The compression level to use. The supported values depend on the codec used. If
omitted, the default level for the codec is used.

## Examples

### Export all events in a Zstd-compressed NDJSON file

```tql
export
write_ndjson
compress_zstd
save_file "/tmp/backup.json.zstd"
```

### Recompress a Zstd-compressed file at a different compression level

```tql
load_file "in.zstd"
decompress_zstd
compress_zstd level=18
save_file "out.zstd"
```

## See Also

[`compress_brotli`](/reference/operators/compress_brotli),
[`compress_bz2`](/reference/operators/compress_bz2),
[`compress_gzip`](/reference/operators/compress_gzip),
[`compress_lz4`](/reference/operators/compress_lz4),
[`decompress_zstd`](/reference/operators/decompress_zstd)
