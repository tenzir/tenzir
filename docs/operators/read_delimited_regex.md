---
title: read_delimited_regex
category: Parsing
example: 'read_delimited_regex r"\s+"'
---

Parses an incoming bytes stream into events using a regular expression as delimiter.

```tql
read_delimited_regex regex:string|blob, [binary=bool, include_separator=bool]
```

## Description

The `read_delimited_regex` operator takes its input bytes and splits it using the
provided regular expression as a delimiter. This is useful for parsing data that
uses custom delimiters or patterns instead of standard newlines.

The regular expression flavor is Perl compatible and documented
[here](https://www.boost.org/doc/libs/1_88_0/libs/regex/doc/html/boost_regex/syntax/perl_syntax.html).

The resulting events have a single field called `data`.

:::note
If the input ends with a separator, no additional empty event will be generated.
For example, splitting `"a|b|"` with delimiter pattern `"|"` will produce two
events: `"a"` and `"b"`, not three events with an empty third one.
:::

### `regex: string|blob (required)`

The regular expression pattern to use as delimiter. This can be provided as a string
or blob literal. The operator will split the input whenever this pattern is matched.
When a blob literal is provided (e.g., `b"\\x00\\x01"`), the `binary` option defaults to `true`.

### `binary = bool (optional)`

Treat the input as binary data instead of UTF-8 text. When enabled, invalid
UTF-8 sequences will not cause warnings.

### `include_separator = bool (optional)`

When enabled, includes the matched separator pattern in the output events. By
default, the separator is excluded from the results.

## Examples

### Split Syslog-like events without newline terminators from a TCP input

```tql
load_tcp "0.0.0.0:514"
read_delimited_regex "(?=<[0-9]+>)"
this = data.parse_syslog()
```

### Parse log entries separated by timestamps

```tql
load_file "application.log"
read_delimited_regex "(?=\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})"
```

### Split on multiple possible delimiters

```tql
load_file "mixed_delimiters.txt"
read_delimited_regex "[;|]"
```

### Include the separator in the output

```tql
load_file "data.txt"
read_delimited_regex "\\|\\|", include_separator=true
```

### Parse binary data with blob patterns

```tql
load_file "binary.dat"
read_delimited_regex b"\\x00\\x01"
```

### Use blob pattern with include_separator for binary delimiters

```tql
load_file "protocol.dat"
read_delimited_regex b"\\xFF\\xFE", include_separator=true
```

## See Also

[`read_delimited`](/reference/operators/read_delimited),
[`read_lines`](/reference/operators/read_lines),
[`read_ssv`](/reference/operators/read_ssv),
[`read_tsv`](/reference/operators/read_tsv),
[`read_xsv`](/reference/operators/read_xsv)
