# match_regex

Checks if a string partially matches a regular expression.

```tql
match_regex(input:string,regeinput:string) -> bool
```

## Description

The `match_regex` function returns `true` if `x` partially matches a regular
expression.

To check whether the full string matches, you can use `^` and `$` to signify
start and end of the string.

## Examples

### Check contains a matching substring

```tql
from {input: "Hello There World"},
  {input: "hi there!"},
  {input: "Good Morning" }
output = input.match_regex("[T|t]here")
```
```tql
{input: "Hello There World", output: true}
{input: "hi there!", output: true}
{input: "Good Morning", output: false}
```

### Check if a string matches fully

```tql
from {input: "example"},
  {input: "Example!"},
  {input: "example?" }
output = input.match_regex("^[E|e]xample[!]?$")
```
```tql
{input: "example", output: true}
{input: "example!", output: true}
{input: "example?", output: false}
```

## See Also

[String Operations](../language/expressions.md#string-operations)
