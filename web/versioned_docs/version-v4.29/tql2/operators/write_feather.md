# write_feather

Transforms the input event stream to Feather byte stream.

```tql
write_feather [compression_level=int, compression_type=str, min_space_savings=double]
```

## Description

Transforms the input event stream to [Feather] (a thin wrapper around
[Apache Arrow's IPC][arrow-ipc] wire format) byte stream.

[feather]: https://arrow.apache.org/docs/python/feather.html
[arrow-ipc]: https://arrow.apache.org/docs/python/ipc.html

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
The Feather format offers more efficient compression compared to the
[`compress`](compress.md) operator. This is because it compresses
the data column-by-column, leaving metadata that needs to be accessed frequently
uncompressed.
:::

### `min_space_savings = double (optional)`

Minimum space savings percentage required for compression to be
applied. This option is ignored if no compression is specified. The provided
value must be between 0 and 1 inclusive.

Space savings are calculated as `1.0 - compressed_size / uncompressed_size`.
For example, with a minimum space savings rate of 0.1, a 100-byte body buffer will not
be compressed if its expected compressed size exceeds 90 bytes.

Defaults to `0`, i.e., always applying compression.

## Examples

### Convert a JSON stream into a Feather file

```tql
load_file "input.json"
read_json
write_feather
save_file "output.feather"
```
