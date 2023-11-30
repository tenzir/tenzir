# parse

Applies a parser to the string stored in a given field.

:::caution Experimental
This operator is experimental and may change at any time. There are known issues
which will be addressed in an upcoming release. Stay tuned!
:::

## Synopsis

```
parse <input> <parser> <args>...
```

## Description

The `parse` operator parses a given `<input>` field of type `string` and
replaces this field with the result.

## Examples

Parse [CEF](../../formats/cef.md) from the Syslog messages stored in `test.log`,
returning only the result from CEF parser.

```
from test.log read syslog | parse content cef | yield content
```
