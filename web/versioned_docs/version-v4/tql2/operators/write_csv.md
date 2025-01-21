# write_csv

Transforms event stream to CSV (Comma-Separated Values) byte stream.

```tql
write_csv [no_header=bool]
```

## Description

The `write_csv` operator transforms an event stream into a byte stream by writing
the events as CSV.

### `no_header=bool (optional)`

Whether to not print a header line containing the field names.

## Examples

Write an event as CSV.

```tql
from {x:1, y:true, z: "String"}
write_csv
```
