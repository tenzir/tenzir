---
title: write_tsv
category: Printing
example: 'write_tsv'
---
Transforms event stream to TSV (Tab-Separated Values) byte stream.

```tql
write_tsv [list_separator=str, null_value=str, no_header=bool]
```

## Description

The `write_tsv` operator transforms an event stream into a byte stream by writing
the events as TSV.

### `list_separator = str (optional)`

The string separating different elements in a list within a single field.

Defaults to `","`.

### `null_value = str (optional)`

The string denoting an absent value.

Defaults to `"-"`.

### `no_header = bool (optional)`

Whether to not print a header line containing the field names.

## Examples

Write an event as TSV.

```tql
from {x:1, y:true, z: "String"}
write_tsv
```
```
x	y	z
1	true	String
```

## See Also

[`write_csv`](/reference/operators/write_csv),
[`write_lines`](/reference/operators/write_lines),
[`write_ssv`](/reference/operators/write_ssv),
[`write_xsv`](/reference/operators/write_xsv)
