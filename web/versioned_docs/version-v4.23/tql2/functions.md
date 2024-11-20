# Functions

<!--

TODO: the following functions still need to be documented:

- `format`
- `parse_*`
- `print_*`
- `grok`

## OCSF

- `ocsf::category_name`
- `ocsf::category_uid`
- `ocsf::class_name`
- `ocsf::class_uid`

-->

Functions appear in [expressions](../language/expressions.md) and take
positional and/or named arguments, producing a value as a result of their
computation.

Function signatures have the following notation:

```tql
f(arg1:<type>, arg2=<type>, [arg3=type]) -> <type>
```

- `arg:<type>`: positional argument
- `arg=<type>`: named argument
- `[arg=type]`: optional (named) argument
- `-> <type>`: function return type

TQL features the [uniform function call syntax
(UFCS)](https://en.wikipedia.org/wiki/Uniform_Function_Call_Syntax), which
allows you to interchangeably call a function with at least one argument either
as *free function* or *method*. For example, `length(str)` and `str.length()`
resolve to the identical function call. The latter syntax is particularly
suitable for function chaining, e.g., `x.f().g().h()` reads left-to-right as
"start with `x`, apply `f()`, then `g()` and then `h()`," compared to
`h(g(f(x)))`, which reads "inside out."

Throughout our documentation, we use the free function style in the synopsis
but often resort to the method style when it is more idiomatic.

## Aggregation

Function | Description | Example
:--------|:------------|:-------
[`all`](functions/all.md) | Computes the conjunction (AND) of all boolean values | `all([true,true,false])`
[`any`](functions/any.md) | Computes the disjunction (OR) of all boolean values | `any([true,false,true])`
[`collect`](functions/collect.md) | Creates a list of all non-null values, preserving duplicates | `collect([1,2,2,3])`
[`count`](functions/count.md) | Counts the events or non-null values | `count([1,2,null])`
[`count_distinct`](functions/count_distinct.md) | Counts all distinct non-null values | `count_distinct([1,2,2,3])`
[`distinct`](functions/distinct.md) | Creates a sorted list without duplicates of non-null values | `distinct([1,2,2,3])`
[`first`](functions/first.md) | Takes the first non-null value | `first([null,2,3])`
[`last`](functions/last.md) | Takes the last non-null value | `last([1,2,null])`
[`max`](functions/max.md) | Computes the maximum of all values | `max([1,2,3])`
[`mean`](functions/mean.md) | Computes the mean of all values | `mean([1,2,3])`
[`median`](functions/median.md) | Computes the approximate median with a t-digest algorithm | `median([1,2,3,4])`
[`min`](functions/min.md) | Computes the minimum of all values | `min([1,2,3])`
[`mode`](functions/mode.md) | Takes the most common non-null value | `mode([1,1,2,3])`
[`quantile`](functions/quantile.md) | Computes the specified quantile `q` of values | `quantile([1,2,3,4], q=0.5)`
[`stddev`](functions/stddev.md) | Computes the standard deviation of all values | `stddev([1,2,3])`
[`sum`](functions/sum.md) | Computes the sum of all values | `sum([1,2,3])`
[`value_counts`](functions/value_counts.md) | Returns a list of values with their frequency | `value_counts([1,2,2,3])`
[`variance`](functions/variance.md) | Computes the variance of all values | `variance([1,2,3])`

## String

### Inspection

Function | Description | Example
:--------|:-------------|:-------
[`length_bytes`](functions/length_bytes.md) | Returns the length of a string in bytes | `"hello".length_bytes()`
[`length_chars`](functions/length_chars.md) | Returns the length of a string in characters | `"hello".length_chars()`
[`starts_with`](functions/starts_with.md) | Checks if a string starts with a substring | `"hello".starts_with("he")`
[`ends_with`](functions/ends_with.md) | Checks if a string ends with a substring | `"hello".ends_with("lo")`
[`is_alnum`](functions/is_alnum.md) | Checks if a string is alphanumeric | `"hello123".is_alnum()`
[`is_alpha`](functions/is_alpha.md) | Checks if a string contains only letters | `"hello".is_alpha()`
[`is_lower`](functions/is_lower.md) | Checks if a string is in lowercase | `"hello".is_lower()`
[`is_numeric`](functions/is_numeric.md) | Checks if a string contains only numbers | `"1234".is_numeric()`
[`is_printable`](functions/is_printable.md) | Checks if a string contains only printable characters | `"hello".is_printable()`
[`is_title`](functions/is_title.md) | Checks if a string follows title case | `"Hello World".is_title()`
[`is_upper`](functions/is_upper.md) | Checks if a string is in uppercase | `"HELLO".is_upper()`

### Transformation

Function | Description | Example
:--------|:-------------|:-------
[`trim`](functions/trim.md) | Trims whitespace from both ends of a string | `" hello ".trim()`
[`trim_start`](functions/trim_start.md) | Trims whitespace from the start of a string | `" hello".trim_start()`
[`trim_end`](functions/trim_end.md) | Trims whitespace from the end of a string | `"hello ".trim_end()`
[`capitalize`](functions/capitalize.md) | Capitalizes the first character of a string | `"hello".capitalize()`
[`replace`](functions/replace.md) | Replaces characters within a string | `"hello".replace("o", "a")`
[`replace_regex`](functions/replace_regex.md) | Reverses the characters of a string | `"hello".replace("l+o", "y")`
[`reverse`](functions/reverse.md) | Reverses the characters of a string | `"hello".reverse()`
[`to_lower`](functions/to_lower.md) | Converts a string to lowercase | `"HELLO".to_lower()`
[`to_title`](functions/to_title.md) | Converts a string to title case | `"hello world".to_title()`
[`to_upper`](functions/to_upper.md) | Converts a string to uppercase | `"hello".to_upper()`

### File Paths

Function | Description | Example
:--------|:-------------|:-------
[`file_name`](functions/file_name.md) | Extracts the file name from a file path | `file_name("/path/to/log.json")`
[`parent_dir`](functions/parent_dir.md) | Extracts the parent directory from a file path | `parent_dir("/path/to/log.json")`

## Time & Date

Function | Description | Example
:--------|:-------------|:-------
[`as_secs`](functions/as_secs.md) | Converts a duration into seconds | `as_secs(42ms)`
[`from_epoch_ms`](functions/from_epoch_ms.md) | Interprets a number as Unix time | `from_epoch_ms(1730234246123.456)`
[`now`](functions/now.md) | Gets the current wallclock time | `now()`
[`since_epoch`](functions/since_epoch.md) | Turns a time value into a duration since the Unix epoch | `since_epoch(2021-02-24)`

<!--

- `year`
- `month`
- `week` (?)
- `day`
- `hour`
- `minute`
- `second`
- `tz` (?)

-->

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
[`encrypt_cryptopan`](functions/encrypt_cryptopan.md) | Encrypts IPs via Crypto-PAn | `encrypt_cryptopan(1.2.3.4)`
[`is_v4`](functions/is_v4.md) | Checks if an IP is IPv4 | `is_v4(1.2.3.4)`
[`is_v6`](functions/is_v6.md) | Checks if an IP is IPv6 | `is_v6(::1)`

## Hashing

Function | Description | Example
:--------|:-------------|:-------
[`hash_md5`](functions/hash_md5.md) | Computes a MD5 hash digest | `hash_md5("foo")`
[`hash_sha1`](functions/hash_sha1.md) | Computes a SHA1 hash digest | `hash_sha1("foo")`
[`hash_sha224`](functions/hash_sha224.md) | Computes a SHA224 hash digest | `hash_sha224("foo")`
[`hash_sha256`](functions/hash_sha256.md) | Computes a SHA256 hash digest | `hash_sha256("foo")`
[`hash_sha384`](functions/hash_sha384.md) | Computes a SHA384 hash digest | `hash_sha384("foo")`
[`hash_sha512`](functions/hash_sha512.md) | Computes a SHA512 hash digest | `hash_sha512("foo")`
[`hash_xxh3`](functions/hash_xxh3.md) | Computes a XXH3 hash digest | `hash_xxh3("foo")`

## Type System

### Introspection

Function | Description | Example
:--------|:-------------|:-------
[`type_id`](functions/type_id.md) | Retrieves the type of an expression | `type_id(1 + 3.2)`
[`has`](functions/has.md) | Checks whether a record has a field | `record.has("field")`
[`length`](functions/length.md) | Retrieves the length of a list | `[1,2,3].length()`

### Conversion

Function | Description | Example
:--------|:-------------|:-------
[`int`](functions/int.md) | Casts an expression to a signed integer | `int(-4.2)`
[`uint`](functions/uint.md) | Casts an expression to an unsigned integer | `uint(4.2)`
[`float`](functions/float.md) | Casts an expression to a float | `float(42)`
[`time`](functions/time.md) | Casts an expression to a time value | `time("2020-03-15")`
[`str`](functions/str.md) | Casts an expression to string | `str(1.2.3.4)`
[`ip`](functions/ip.md) | Casts an expression to an IP | `ip("1.2.3.4")`

### Transposition

Function | Description | Example
:--------|:-------------|:-------
[`flatten`](functions/flatten.md) | Flattens nested data | `flatten(this)`
[`unflatten`](functions/unflatten.md) | Unflattens nested structures | `unflatten(this)`

## Runtime

Function | Description | Example
:--------|:-------------|:-------
[`env`](functions/env.md) | Reads an environment variable | `env("PATH")`
[`secret`](functions/secret.md) | Reads a secret from a store | `secret("PATH")`
