---
sidebar_custom_props:
  operator:
    transformation: true
---

# parse

Applies a parser to the string stored in a given field.

## Synopsis

```
parse <input> <parser> [<args...>]
```

## Description

The `parse` operator parses a given `<input>` field using `<parser>` and
replaces this field with the result.

### `<input>`

Specifies the field of interest. The field must be of type `string`.

### `<parser> [<args...>]`

Specifies the parser format and the corresponding arguments specific to each
parser. `<parser>` can be one of the parsers in [formats](../formats.md).

## Examples

Parse [CEF](../formats/cef.md) from the Syslog messages stored in `test.log`,
returning only the result from CEF parser.

```
from test.log read syslog | parse content cef | yield content
```
