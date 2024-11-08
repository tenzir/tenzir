# write_parquet

Transforms event stream to a Parquet byte stream.

```tql
write_parquet [compression_level=int, compression_type=str]
```

## Description

[Apache Parquet][parquet] is a columnar storage format that a variety of data
tools support.

[parquet]: https://parquet.apache.org/

:::warning Limitation
Tenzir currently assumes that all Parquet files use metadata recognized by
Tenzir. We plan to lift this restriction in the future.
:::

### `compression_level = int (optional)`

An optional compression level for the corresponding compression type. This
option is ignored if no compression type is specified.

Defaults to the compression type's default compression level.

### `compression_type = str (optional)`

Supported options are `zstd` for [Zstandard][zstd-docs] compression
and `lz4` for [LZ4 Frame][lz4-docs] compression.

[zstd-docs]: http://facebook.github.io/zstd/
[lz4-docs]: https://android.googlesource.com/platform/external/lz4/+/HEAD/doc/lz4_Frame_format.md

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
