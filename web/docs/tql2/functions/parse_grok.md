# parse_grok

Parses a string according to a grok pattern.

```tql
parse_grok(input:string, pattern:string) -> record
```

## Description

The `parse_grok` function parses a string according to a grok pattern.

### `input: string`

The string to parse.

### `pattern: string`

The pattern to use for parsing.

## Examples

```tql
let $pattern = "%{IP:client} %{WORD} %{URIPATHPARAM:req} %{NUMBER:bytes} %{NUMBER:dur}"
from { input = "Input: 55.3.244.1 GET /index.html 15824 0.043" }
output = output.parse_grok(pattern)
```
```tql
{
  x: "Input: 55.3.244.1 GET /index.html 15824 0.043",
  y: {
    client: 55.3.244.1,
    req: "/index.html",
    bytes: 15824,
    dur: 0.043
  }
}
```
