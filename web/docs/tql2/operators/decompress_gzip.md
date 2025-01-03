# decompress_gzip

Decompresses a stream of bytes in the Gzip format.

```tql
decompress_gzip
```

## Description

The `decompress_gzip` operator decompresses bytes in a pipeline incrementally.
The operator supports decompressing multiple concatenated streams
of the same codec transparently.

## Examples

### Import Suricata events from a Gzip-compressed file

```tql
load_file "eve.json.gz"
decompress_brotli
decompress_gzip
import
```
