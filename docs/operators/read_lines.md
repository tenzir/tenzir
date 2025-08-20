---
title: read_lines
category: Parsing
example: "read_lines"
---

Parses an incoming bytes stream into events.

```tql
read_lines [skip_empty=bool, split_at_null=bool, split_at_regex=string]
```

## Description

The `read_lines` operator takes its input bytes and splits it at a newline character.

Newline characters include:

- `\n`
- `\r\n`

The resulting events have a single field called `line`.

### `skip_empty = bool (optional)`

Ignores empty lines in the input.

### `split_at_null = bool (optional)`

:::caution[Deprecated]
This option is deprecated. Use
[`read_delimited_regex`](/reference/operators/read_delimited) instead.
:::

Use null byte (`\0`) as the delimiter instead of newline characters.

### `split_at_regex = string (optional)`

:::caution[Deprecated]
This option is deprecated. Use
[`read_delimited_regex`](/reference/operators/read_delimited_regex) instead.
:::

Use the specified regex as the delimiter instead of newline characters.
The regex flavor is Perl compatible and documented [here](https://www.boost.org/doc/libs/1_88_0/libs/regex/doc/html/boost_regex/syntax/perl_syntax.html).

## Examples

### Reads lines from a file

```tql
load_file "events.log"
read_lines
is_error = line.starts_with("error:")
```

### Split Syslog-like events without newline terminators from a TCP input

:::info
Consider using [`read_delimited_regex`](/reference/operators/read_delimited_regex) for regex-based splitting:

```tql
load_tcp "0.0.0.0:514"
read_delimited_regex "(?=<[0-9]+>)"
this = line.parse_syslog()
```

:::

```tql
load_tcp "0.0.0.0:514"
read_lines split_at_regex="(?=<[0-9]+>)"
this = line.parse_syslog()
```

## See Also

[`read_all`](/reference/operators/read_all),
[`read_ssv`](/reference/operators/read_ssv),
[`read_tsv`](/reference/operators/read_tsv),
[`read_delimited_regex`](/reference/operators/read_delimited_regex),
[`read_xsv`](/reference/operators/read_xsv),
[`write_lines`](/reference/operators/write_lines)
