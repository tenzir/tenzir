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
[`is_v4`](functions/is_v4.md) | Checks if an IP is IPv4 | `is_v4(1.2.3.4)`
[`is_v6`](functions/is_v6.md) | Checks if an IP is IPv6 | `is_v6(::1)`

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

## OCSF
- `ocsf::category_name`
- `ocsf::category_uid`
- `ocsf::class_name`
- `ocsf::class_uid`

## Paths
- `file_name`
- `parent_dir`

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

## Strings
- `capitalize`
- `ends_with`
- `is_alnum`
- `is_alpha`
- `is_lower`
- `is_numeric`
- `is_printable`
- `is_title`
- `is_upper`
- `length_bytes`
- `length_chars`
- `starts_with`
- `to_lower`
- `to_title`
- `to_upper`
- `trim`
- `trim_end`
- `trim_start`

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
