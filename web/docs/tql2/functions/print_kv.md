# print_kv

Prints records in a Key-Value format.

```tql
print_kv( input:record, [field_sep=str, value_sep=str, list_sep=str, flatten=str, null=str] )
```

## Description

Prints records in a Key-Value format. Nested data will be flattend, keys or
values containing the given separators will be quoted and
the special characters `\n`, `\r`, `\` and `"` will be escaped.

### `input: record`

The record to print as a string.

### `field_sep=str (optional)`

A string that shall separate the key-value pairs.

Must not be an empty string.

Defaults to `" "`.

### `value_sep=str (optional)`

A string that shall separate key and value within key-value pair.

Must not be an empty string.

Defaults to `"="`.

### `list_sep=str (optional)`

Must not be an empty string.

Defaults to `","`.

### `flatten=str (optional)`

A string to join the keys of nested records with. For example,
given `flatten="."`

Defaults to `"."`.

### `null=str (optional)`

A string to represent null values.

Defaults to the empty string.

## Examples

### Conditionally quoted strings

```tql
from { input: { key:"value" } }
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

[`write_kv`](../operators/read_kv.mdx), [`parse_kv`](parse_kv.mdx)
