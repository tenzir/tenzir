# write_parquet

Transforms event stream to a Parquet byte stream.

```tql
write_parquet [compression_level=int, compression_type=str]
```

## Description

[Apache Parquet][parquet] is a columnar storage format that a variety of data
tools support.

[parquet]: https://parquet.apache.org/

### `compression_level = int (optional)`

An optional compression level for the corresponding compression type. This
option is ignored if no compression type is specified.

Defaults to the compression type's default compression level.

### `compression_type = str (optional)`

Specifies an optional compression type. Supported options are `zstd` for
[Zstandard][zstd-docs] compression, `brotli` for [brotli][brotli-docs]
compression, `gzip` for [gzip][gzip-docs] compression, and `snappy` for
[snappy][snappy-docs] compression.

[zstd-docs]: http://facebook.github.io/zstd/
[gzip-docs]: https://www.gzip.org
[brotli-docs]: https://www.brotli.org
[snappy-docs]: https://google.github.io/snappy/

:::tip Why would I use this over the `compress` operator?
The Parquet format offers more efficient compression compared to the
[`compress`](compress.md) operator. This is because it compresses
the data column-by-column, leaving metadata that needs to be accessed frequently
uncompressed.
:::
## Examples

Write a Parquet file:

```tql
load_file "/tmp/data.json"
read_json
write_parquet
```
