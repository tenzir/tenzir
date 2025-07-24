---
title: read_delimited
category: Parsing
example: 'read_delimited "|"'
---

Parses an incoming bytes stream into events using a string as delimiter.

```tql
read_delimited separator:string|blob, [binary=bool, include_separator=bool]
```

## Description

The `read_delimited` operator takes its input bytes and splits it using the
provided string as a delimiter. This is useful for parsing data that uses
simple string delimiters instead of regular expressions or standard newlines.

The resulting events have a single field called `data`.

:::note
If the input ends with a separator, no additional empty event will be generated.
For example, splitting `"a|b|"` with delimiter `"|"` will produce two events:
`"a"` and `"b"`, not three events with an empty third one.
:::

### `separator: string|blob (required)`

The string or blob to use as delimiter. The operator will split the input whenever this
exact sequence is matched. When a blob literal is provided (e.g., `b"\x00\x01"`),
the `binary` option defaults to `true`.

### `binary = bool (optional)`

Treat the input as binary data instead of UTF-8 text. When enabled, invalid
UTF-8 sequences will not cause warnings, and the resulting `data` field will be
of type `blob` instead of `string`.

### `include_separator = bool (optional)`

When enabled, includes the matched separator string in the output events. By
default, the separator is excluded from the results.

## Examples

### Split on a simple delimiter

```tql
load_file "data.txt"
read_delimited "||"
```

### Parse CSV-like data with custom delimiter

```tql
load_file "custom.csv"
read_delimited ";;;"
```

### Include the separator in the output

```tql
load_file "data.txt"
read_delimited "||", include_separator=true
```

### Parse binary data with blob delimiters

```tql
load_file "binary.dat"
read_delimited b"\x00\x01"
```

### Use blob separator with include_separator

```tql
load_file "data.txt"
read_delimited b"||", include_separator=true
```

### Parse binary data with string delimiters

```tql
load_file "binary.dat"
read_delimited "\x00\x01", binary=true
```

## See Also

[`read_all`](/reference/operators/read_all),
[`read_delimited_regex`](/reference/operators/read_delimited_regex),
[`read_lines`](/reference/operators/read_lines),
[`read_ssv`](/reference/operators/read_ssv),
[`read_tsv`](/reference/operators/read_tsv),
[`read_xsv`](/reference/operators/read_xsv)
