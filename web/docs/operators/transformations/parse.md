# parse

Applies a parser to the string stored in the given field.

:::caution Experimental
This operator might not behave optimally in some cases. For example, when using
the `json` parser, the string is expected to be a valid record. Strings that
represent non-record types are not yet supported with this format.
:::

## Synopsis

```
parse <input> [--into <output>] <parser> <args>...
```

## Description

The `parse` operator parses a given `<input>` field of type `string` and adds
the result to the event. If `--into <output>` is given, the result is stored in
the given field. Otherwise, the result must be a record, and its fields are
stored directly in the event.

## Examples

Parse [CEF](../../formats/cef.md) from the Syslog messages stored in `test.log`,
returning only the result from CEF parser.

```
from test.log read syslog | parse content --into result cef | yield result
```
