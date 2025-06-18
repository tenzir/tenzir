---
title: write_csv
category: Printing
example: 'write_csv'
---

Transforms event stream to CSV (Comma-Separated Values) byte stream.

```tql
write_csv [list_separator=str, null_value=str, no_header=bool]
```

## Description

The `write_csv` operator transforms an event stream into a byte stream by writing
the events as CSV.

### `list_separator = str (optional)`

The string separating different elements in a list within a single field.

Defaults to `";"`.

### `null_value = str (optional)`

The string denoting an absent value.

Defaults to `" "`.

### `no_header = bool (optional)`

Whether to not print a header line containing the field names.

## Examples

Write an event as CSV.

```tql
from {x:1, y:true, z: "String"}
write_csv
```
```
x,y,z
1,true,String
```

## See Also

[`parse_csv`](/reference/functions/parse_csv),
[`print_csv`](/reference/functions/print_csv),
[`read_csv`](/reference/operators/read_csv),
[`write_lines`](/reference/operators/write_lines),
[`write_ssv`](/reference/operators/write_ssv),
[`write_tsv`](/reference/operators/write_tsv),
[`write_xsv`](/reference/operators/write_xsv)
