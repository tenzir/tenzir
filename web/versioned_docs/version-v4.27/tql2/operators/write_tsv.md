# write_tsv

Transforms event stream to TSV (Tab-Separated Values) byte stream.

```tql
write_tsv [no_header=bool]
```

## Description

The `write_tsv` operator transforms an event stream into a byte stream by writing
the events as TSV.

### `no_header=bool (optional)`

Whether to not print a header line containing the field names.

## Examples

Write an event as TSV.

```tql
from {x:1, y:true, z: "String"}
write_tsv
```
