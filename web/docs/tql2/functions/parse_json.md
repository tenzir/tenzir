# parse_json

Parses a string as a JSON record

```tql
parse_json(input:string) -> record
```

## Description

The `parse_json` function parses a string as a JSON record.

### `input: string`

The string to parse.

## Examples

```tql
from { input = R#"{ a = 42, b = "text"}"# }
output = input.parse_json()
```
```tql
{
  input: "{ key: value}",
  output: {
    a: 42,
    b: "text"
  }
}
```
