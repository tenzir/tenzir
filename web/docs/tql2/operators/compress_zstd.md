# compress_zstd

Compresses a stream of bytes using zstd compression.

```tql
compress_zstd [level=int]
```

## Description

The `compress_zstd` operator compresses bytes in a pipeline incrementally.

:::note Streaming Compression
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming compression.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

### `level = int (optional)`

The compression level to use. The supported values depend on the codec used. If
omitted, the default level for the codec is used.

## Examples

### Export all events in a Zstd-compressed NDJSON file

```tql
export
write_ndjson
compress_zstd
save_file "/tmp/backup.json.gz"
```

###  Recompress a Zstd-compressed file at a higher compression level

```tql
load_file "in.zstd"
decompress_zstd
compress_zstd level=18
save_file "out.zstd"
```
