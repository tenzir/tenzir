---
title: print_kv
---

Prints records in a key-value format.

```tql
print_kv( input:record, [field_separator=str, value_separator=str,
                         list_separator=str, flatten_separator=str,
                         null_value=str] ) -> str
```

## Description

Prints records in a Key-Value format. Nested data will be flattend, keys or
values containing the given separators will be quoted and the special characters
`\n`, `\r`, `\` and `"` will be escaped.

### `input: record`

The record to print as a string.

### `field_separator = str (optional)`

A string that shall separate the key-value pairs.

Must not be an empty string.

Defaults to `" "`.

### `value_separator = str (optional)`

A string that shall separate key and value within key-value pair.

Must not be an empty string.

Defaults to `"="`.

### `list_separator = str (optional)`

Must not be an empty string.

Defaults to `","`.

### `flatten_separator = str (optional)`

A string to join the keys of nested records with. For example,
given `flatten="."`

Defaults to `"."`.

### `null_value = str (optional)`

A string to represent null values.

Defaults to the empty string.

## Examples

### Format a record as key-value pair

```tql
from {
  input: {key: "value"}
}
output = input.print_kv()
```

```tql
{
  input: {
    key: "value",
  },
  output: "key=value",
}
```

## See Also

[`parse_kv`](/reference/functions/parse_kv),
[`write_kv`](/reference/operators/read_kv),
[`write_kv`](/reference/operators/write_kv)
