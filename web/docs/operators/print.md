---
sidebar_custom_props:
  operator:
    transformation: true
---

# print

Applies a print to the record stored in a given field.

## Synopsis

```
print <input> <printer> <args>...
```

## Description

The `print` operator prints a given `<input>` field of type `record` using
`<printer>` and replaces this field with the result. `<printer>` can be one of the
printers in [formats](../formats.md) that support UTF8 printing.

## Examples

Print [CSV](../formats/csv.md) from the Syslog messages stored in `test.log`,
returning only the result from CSV printer.

```
from test.log read syslog | print content csv | yield content
```
