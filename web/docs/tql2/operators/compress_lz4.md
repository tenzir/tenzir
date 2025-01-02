# compress_lz4

Compresses a stream of bytes using lz4 compression.

```tql
compress_lz4 [level=int]
```

## Description

The `compress_lz4` operator compresses bytes in a pipeline incrementally.

### `level = int (optional)`

The compression level to use. The supported values depend on the codec used. If
omitted, the default level for the codec is used.

## Examples

### Export all events in a Lz4-compressed NDJSON file

```tql
export
write_ndjson
compress_lz4
save_file "/tmp/backup.json.lz4"
```

### Recompress a Lz4-compressed file at a different compression level

```tql
load_file "in.lz4"
decompress_lz4
compress_lz4 level=18
save_file "out.lz4"
```
