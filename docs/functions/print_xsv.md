---
title: print_xsv
---

Prints a record as a delimited sequence of values.

```tql
print_xsv(input:record, field_separator=str, list_separator=str, null_value=str) -> string
```

## Description

The `parse_xsv` function prints a record's values as delimiter separated string.

The following table lists existing XSV configurations:

|Format               |Field Separator|List Separator|Null Value|
|---------------------|:-------------:|:------------:|:--------:|
|[`csv`](/reference/functions/print_csv)   |`,`            |`;`           | empty    |
|[`ssv`](/reference/functions/print_ssv)   |`<space>`      |`,`           |`-`       |
|[`tsv`](/reference/functions/print_tsv)   |`\t`           |`,`           |`-`       |

### `field_separator = str`

The string separating different fields.

### `list_separator = str`

The string separating different elements in a list within a single field.

### `null_value = str`

The string denoting an absent value.

## Examples

```tql
from {
  x:1,
  y:true,
  z: "String",
}
output = this.print_xsv(
  field_separator=",",
  list_selarator=";",
  null_value="null")
```

```tql
{
  x: 1,
  y: true,
  z: "String",
  output: "1,true,String",
}
```

## See Also

[`parse_xsv`](/reference/functions/parse_xsv),
[`write_xsv`](/reference/operators/write_xsv)
