# compress_bz2

Compresses a stream of bytes using bz2 compression.

```tql
compress_bz2 [level=int]
```

## Description

The `compress_bz2` operator compresses bytes in a pipeline incrementally.

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

### Export all events in a Bzip2-compressed NDJSON file

```tql
export
write_ndjson
compress_bz2
save_file "/tmp/backup.json.gz"
```

###  Recompress a Bzip2-compressed file at a higher compression level

```tql
load_file "in.bz2"
decompress_bz2
compress_bz2 level=18
save_file "out.bz2"
```
