# parse_grok

Parses a string according to a grok pattern.

```tql
parse_grok(input:string, pattern:string, [pattern_definitions=record|string]) -> record
```

## Description

The `parse_grok` function parses a string according to a grok pattern.

### `input: string`

The string to parse.

### `pattern: string`

The `grok` pattern used for matching. Must match the input in its entirety.

### `pattern_definitions = record|string (optional)`

New pattern definitions to use. This may be a record of the form

```tql
{
  pattern_name: "pattern"
}
```

For example, the built-in pattern `INT` would be defined as

```tql
{ INT: "(?:[+-]?(?:[0-9]+))" }
```

Alternatively, this may be a user-defined newline-delimited list of patterns,
where a line starts with the pattern name, followed by a space, and the
`grok`-pattern for that pattern. For example, the built-in pattern `INT` is
defined as follows:

```
INT (?:[+-]?(?:[0-9]+))
```

## Examples

```tql
let $pattern = "%{IP:client} %{WORD} %{URIPATHPARAM:req} %{NUMBER:bytes} %{NUMBER:dur}"
from { input: "55.3.244.1 GET /index.html 15824 0.043" }
output = input.parse_grok($pattern)
output.dur = output.dur * 1s
```
```tql
{
  input: "Input: 55.3.244.1 GET /index.html 15824 0.043",
  output: {
    client: 55.3.244.1,
    req: "/index.html",
    bytes: 15824,
    dur: 43.0ms
  }
}
```
