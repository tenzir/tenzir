# Functions

Functions appear in [expressions](../language/expressions.md) and take
positional and/or named arguments, producing a value as a result of their
computation.

Function signatures have the following notation:

```tql
f(arg1:<type>, arg2=<type>, [arg3=type]) -> <type>
```

- `arg:<type>`: postional argument
- `arg=<type>`: named argument
- `[arg=type]`: optional (named) argument
- `-> <type>`: function return type

TQL features the [uniform function call syntax
(UFCS)](https://en.wikipedia.org/wiki/Uniform_Function_Call_Syntax), which
allows you to interchangeably call a function with more than argument either as
*free function* or *method*. For example, `length(str)` and `str.length()`
resolve to the identical function call. The latter syntax is particularly
suitable for function chaining, e.g., `x.f().g().h()` reads left-to-right as
"start with `x`, apply `f()`, then `g()` and then `h()`," compared to
`h(g(f(x)))`, which reads "inside out."

Throughout our documentation, we use the free function syntax.

## String

Function | Description | Example
:--------|:-------------|:-------
[`capitalize`](functions/capitalize.md) | Capitalizes the first character of a string | `capitalize("hello")`
[`ends_with`](functions/ends_with.md) | Checks if a string ends with a substring | `ends_with("hello", "lo")`
[`is_alnum`](functions/is_alnum.md) | Checks if a string is alphanumeric | `is_alnum("hello123")`
[`is_alpha`](functions/is_alpha.md) | Checks if a string contains only letters | `is_alpha("hello")`
[`is_lower`](functions/is_lower.md) | Checks if a string is in lowercase | `is_lower("hello")`
[`is_numeric`](functions/is_numeric.md) | Checks if a string contains only numbers | `is_numeric("1234")`
[`is_printable`](functions/is_printable.md) | Checks if a string contains only printable characters | `is_printable("hello")`
[`is_title`](functions/is_title.md) | Checks if a string follows title case | `is_title("Hello World")`
[`is_upper`](functions/is_upper.md) | Checks if a string is in uppercase | `is_upper("HELLO")`
[`length_bytes`](functions/length_bytes.md) | Returns the length of a string in bytes | `length_bytes("hello")`
[`length_chars`](functions/length_chars.md) | Returns the length of a string in characters | `length_chars("hello")`
[`starts_with`](functions/starts_with.md) | Checks if a string starts with a substring | `starts_with("hello", "he")`
[`to_lower`](functions/to_lower.md) | Converts a string to lowercase | `to_lower("HELLO")`
[`to_title`](functions/to_title.md) | Converts a string to title case | `to_title("hello world")`
[`to_upper`](functions/to_upper.md) | Converts a string to uppercase | `to_upper("hello")`
[`trim`](functions/trim.md) | Trims whitespace from both ends of a string | `trim(" hello ")`
[`trim_end`](functions/trim_end.md) | Trims whitespace from the end of a string | `trim_end("hello ")`
[`trim_start`](functions/trim_start.md) | Trims whitespace from the start of a string | `trim_start(" hello")`

Function | Description | Example
:--------|:-------------|:-------
[`file_name`](functions/file_name.md) | Extracts the file name from a file path | `file_name("/path/to/log.json")`
[`parent_dir`](functions/parent_dir.md) | Extracts the parent directory from a file path | `parent_dir("/path/to/log.json")`

## Math

Function | Description | Example
:--------|:-------------|:-------
[`ceil`](functions/ceil.md) | Takes the ceiling | `ceil(4.2)`, `ceil(3.2s, 1m)`
[`floor`](functions/floor.md) | Takes the floor | `floor(4.2)`, `floor(32h, 1d)`
[`random`](functions/random.md) | Generates a random number | `random()`
[`round`](functions/round.md) | Rounds a value | `round(4.2)`, `round(31m, 1h)`
[`sqrt`](functions/sqrt.md) | Calculates the square root | `sqrt(49)`

## Networking

Function | Description | Example
:--------|:-------------|:-------
[`community_id`](functions/community_id.md) | Computes a Community ID | `community_id(src_ip=1.2.3.4, dst_ip=4.5.6.7, proto="tcp")`
[`decapsulate`](functions/decapsulate.md) | Decapsulates PCAP packets | `decapsulate(this)`
[`is_v4`](functions/is_v4.md) | Checks if an IP is IPv4 | `is_v4(1.2.3.4)`
[`is_v6`](functions/is_v6.md) | Checks if an IP is IPv6 | `is_v6(::1)`

<!--

## OCSF
- `ocsf::category_name`
- `ocsf::category_uid`
- `ocsf::class_name`
- `ocsf::class_uid`

-->

## Type System

Function | Description | Example
:--------|:-------------|:-------
[`int`](functions/int.md) | Casts an expression to a signed integer | `int(-4.2)`
[`uint`](functions/uint.md) | Casts an expression to an unsigned integer | `uint(4.2)`
[`float`](functions/float.md) | Casts an expression to a float | `float(42)`
[`str`](functions/str.md) | Casts an expression to string | `str(1.2.3.4)`
[`ip`](functions/ip.md) | Casts an expression to an IP | `ip("1.2.3.4")`
[`time`](functions/time.md) | Casts an expression to a time value | `time("2020-03-15")`
[`type_id`](functions/type_id.md) | Retrieves the type of an expression | `type_id(1 + 3.2)`

<!--

## Aggregation (?)
- `count`
- `quantile`
- `sum`

## ???
- `env`
- `secret`

## Time and Duration
- `now`
- `as_secs`
- `since_epoch`

## Lists
- `length`

## Records
- `has`

## List and String ?
- `reverse`

## TODO
- `grok`
- `parse_cef`
- `parse_json`
-->
