# write_csv

Transforms event stream to CSV (Comma-Separated Values) byte stream.

```tql
write_csv [list_sep=str, null_value=str, no_header=bool]
```

## Description

The `write_csv` operator transforms an event stream into a byte stream by writing
the events as CSV.

### `list_sep: str`

The string separating different elements in a list within a single field.

Defaults to `";"`.

### `null_value: str`

The string denoting an absent value.

Defaults to `" "`.

### `no_header=bool (optional)`

Whether to not print a header line containing the field names.

## Examples

Write an event as CSV.

```tql
from {x:1, y:true, z: "String"}
write_csv
```
