# write_xsv

Transforms event stream to XSV byte stream.

```tql
write_xsv field_sep:str, list_sep:str, null_value:str, [no_header=bool]
```

## Description

The [`xsv`][xsv] format is a generalization of [comma-separated values (CSV)][csv] data
in tabular form with a more flexible separator specification supporting tabs,
commas, and spaces. The first line in an XSV file is the header that describes
the field names. The remaining lines contain concrete values. One line
corresponds to one event, minus the header.

The following table lists existing XSV configurations:

|Format               |Field Separator|List Separator|Null Value|
|---------------------|:-------------:|:------------:|:--------:|
|[`csv`](write_csv.md)|`,`            |`;`           | empty    |
|[`ssv`](write_ssv.md)|`<space>`      |`,`           |`-`       |
|[`tsv`](write_tsv.md)|`\t`           |`,`           |`-`       |

[csv]: https://en.wikipedia.org/wiki/Comma-separated_values
[xsv]: https://en.wikipedia.org/wiki/Delimiter-separated_values

Note that nested records have dot-separated field names.

### `field_sep: str`

The string separating different fields.

### `list_sep: str`

The string separating different elements in a list within a single field.

### `null_value: str`

The string denoting an absent value.

### `no_header=bool (optional)`

Whether to not print a header line containing the field names.

## Examples
