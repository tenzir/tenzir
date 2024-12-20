# decompress_bz2

Decompresses a stream of bytes in the Bzip2 format.

```tql
decompress_bz2
```

## Description

The `decompress_bz2` operator decompresses bytes in a pipeline incrementally.
The operator supports decompressing multiple concatenated streams
of the same codec transparently.

:::note Streaming Decompression
The operator uses [Apache Arrow's compression
utilities][apache-arrow-compression] under the hood, and transparently supports
all options that Apache Arrow supports for streaming decompression.
:::

[apache-arrow-compression]: https://arrow.apache.org/docs/cpp/api/utilities.html#compression

## Examples

### Import Suricata events from a Bzip2-compressed file

```tql
load_file "eve.json.bz"
decompress_bz2
read_suricata
import
```
