# compress_brotli

Compresses a stream of bytes using Brotli compression.

```tql
compress_brotli [level=int, window_bits=int]
```

## Description

The `compress_brotli` operator compresses bytes in a pipeline incrementally.

:::note Streaming Compression
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming compression.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

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
save_file "/tmp/backup.json.gz"
```

###  Recompress a Brotli-compressed file at a higher compression level

```tql
load_file "in.brotli"
decompress_brotli
compress_brotli level=18
save_file "out.brotli"
```
