# parse_leef

Parses a string as a LEEF message

```tql
parse_leef(input:string) -> record
```

## Description

The `parse_leef` function parses a string as a LEEF message

### `input: string`

The string to parse.

## Examples

```tql
from { x = "LEEF:1.0|Vendor|Product|Version|EventID|key=value" }
y = x.parse_leef()
```
```tql
{
  x: "LEEF:1.0|Vendor|Product|Version|EventID|key=value",
  y: {
    "leef_version": "1.0",
    "vendor": "Microsoft",
    "product_name": "MSExchange",
    "product_version": "2016",
    "attributes": {
      "key": "value",
    }
  }
}
```
