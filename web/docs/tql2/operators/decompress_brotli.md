# decompress_brotli

Decompresses a stream of bytes in the Bzip2 format.

```tql
decompress_brotli
```

## Description

The `decompress_brotli` operator decompresses bytes in a pipeline incrementally.
The operator supports decompressing multiple concatenated streams
of the same codec transparently.

:::note Streaming Decompression
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming decompression.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

## Examples

### Import Suricata events from a Brotli-compressed file

```tql
load_file "eve.json.brotli"
decompress_brotli
read_suricata
import
```
