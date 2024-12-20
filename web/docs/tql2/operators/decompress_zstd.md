# decompress_zstd

Decompresses a stream of bytes in the Zstd format.

```tql
decompress_zstd
```

## Description

The `decompress_zstd` operator decompresses bytes in a pipeline incrementally.
The operator supports decompressing multiple concatenated streams
of the same codec transparently.

:::note Streaming Decompression
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming decompression.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

## Examples

### Import Suricata events from a Zstd-compressed file

```tql
load_file "eve.json.zstd"
decompress_zstd
read_suricata
import
```
