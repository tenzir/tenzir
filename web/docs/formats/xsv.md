---
sidebar_custom_props:
  format:
    parser: true
    printer: true
---

# xsv

Reads and writes lines with separated values.

## Synopsis

Parser:

```
csv [--allow-comments] [--auto-expand] [--header <header>]
ssv [--allow-comments] [--auto-expand] [--header <header>]
tsv [--allow-comments] [--auto-expand] [--header <header>]
xsv <field-sep> <list-sep> <null-value> [--allow-comments] [--auto-expand] [--header <header>]
```

Printer:

```
csv [--no-header]
ssv [--no-header]
tsv [--no-header]
xsv <field-sep> <list-sep> <null-value> [--no-header]
```

## Description

The `xsv` format is a generalization of [comma-separated values (CSV)][csv] data
in tabular form with a more flexible separator specification supporting tabs,
commas, and spaces. The first line in an XSV file is the header that describes
the field names. The remaining lines contain concrete values. One line
corresponds to one event, minus the header.

The following table juxtaposes the available XSV configurations:

|Format         |Field Separator|List Separator|Null Value|
|---------------|:-------------:|:------------:|:--------:|
|[`csv`](csv.md)|`,`            |`;`           | empty    |
|[`ssv`](ssv.md)|`<space>`      |`,`           |`-`       |
|[`tsv`](tsv.md)|`\t`           |`,`           |`-`       |

[csv]: https://en.wikipedia.org/wiki/Comma-separated_values

Like the [`json`](json.md) parser, the XSV parser infers types automatically.
Consider this piece of CSV data:

```csv
ip,sn,str,rec.a,rec.b
1.2.3.4,10.0.0.0/8,foo bar,-4.2,/foo|bar/
```

Here's the schema that the parser infers from the above data:

```yaml title=test.schema
record:
  - ip: ip
  - sn: subnet
  - str: string
  - record:
    - a: double
    - b: pattern
```

Note that nested records have dot-separated field names.

### `<field-sep>`

Specifies the string that separates fields.

### `<list-sep>`

Specifies the string that separates list elements *within* a field.

### `<null-value>`

Specifies the string that denotes an absent value.

### `--allow-comments` (Parser)

Treat lines beginning with `'#'` as comments.

### `--auto-expand (Parser)

Automatically add fields to the schema when encountering events with too many
values instead of dropping the excess values.

### `--header <header>` (Parser)

Use the manually provided header line instead of treating the first line as the
header.

### `--no-header` (Printer)

Do not print a header line containing the field names.

## Examples

Read CSV from stdin:

```
from stdin read csv
```

Write a multi-schema stream of events to a directory in TSV format, with one
file per unique schema:

```
to directory /tmp/result write tsv
```
