# Functions

<!--

TODO: the following functions still need to be documented:

## OCSF

- `ocsf::category_name`
- `ocsf::category_uid`
- `ocsf::class_name`
- `ocsf::class_uid`

-->

Functions appear in [expressions](../tql2/language/expressions.md) and take
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
as _free function_ or _method_. For example, `length(str)` and `str.length()`
resolve to the identical function call. The latter syntax is particularly
suitable for function chaining, e.g., `x.f().g().h()` reads left-to-right as
"start with `x`, apply `f()`, then `g()` and then `h()`," compared to
`h(g(f(x)))`, which reads "inside out."

Throughout our documentation, we use the free function style in the synopsis
but often resort to the method style when it is more idiomatic.

## Aggregation

| Function                                        | Description                                                  | Example                      |
| :---------------------------------------------- | :----------------------------------------------------------- | :--------------------------- |
| [`all`](functions/all.md)                       | Computes the conjunction (AND) of all boolean values         | `all([true,true,false])`     |
| [`any`](functions/any.md)                       | Computes the disjunction (OR) of all boolean values          | `any([true,false,true])`     |
| [`collect`](functions/collect.md)               | Creates a list of all non-null values, preserving duplicates | `collect([1,2,2,3])`         |
| [`count`](functions/count.md)                   | Counts the events or non-null values                         | `count([1,2,null])`          |
| [`count_distinct`](functions/count_distinct.md) | Counts all distinct non-null values                          | `count_distinct([1,2,2,3])`  |
| [`distinct`](functions/distinct.md)             | Creates a sorted list without duplicates of non-null values  | `distinct([1,2,2,3])`        |
| [`first`](functions/first.md)                   | Takes the first non-null value                               | `first([null,2,3])`          |
| [`last`](functions/last.md)                     | Takes the last non-null value                                | `last([1,2,null])`           |
| [`max`](functions/max.md)                       | Computes the maximum of all values                           | `max([1,2,3])`               |
| [`mean`](functions/mean.md)                     | Computes the mean of all values                              | `mean([1,2,3])`              |
| [`median`](functions/median.md)                 | Computes the approximate median with a t-digest algorithm    | `median([1,2,3,4])`          |
| [`min`](functions/min.md)                       | Computes the minimum of all values                           | `min([1,2,3])`               |
| [`mode`](functions/mode.md)                     | Takes the most common non-null value                         | `mode([1,1,2,3])`            |
| [`quantile`](functions/quantile.md)             | Computes the specified quantile `q` of values                | `quantile([1,2,3,4], q=0.5)` |
| [`stddev`](functions/stddev.md)                 | Computes the standard deviation of all values                | `stddev([1,2,3])`            |
| [`sum`](functions/sum.md)                       | Computes the sum of all values                               | `sum([1,2,3])`               |
| [`value_counts`](functions/value_counts.md)     | Returns a list of values with their frequency                | `value_counts([1,2,2,3])`    |
| [`variance`](functions/variance.md)             | Computes the variance of all values                          | `variance([1,2,3])`          |

## Record

| Function                      | Description                         | Example                                    |
| :---------------------------- | :---------------------------------- | :----------------------------------------- |
| [`get`](functions/get.md)     | Accesses a field of a record        | `record.get("field with spaces", default)` |
| [`has`](functions/has.md)     | Checks whether a record has a field | `record.has("field")`                      |
| [`merge`](functions/merge.md) | Merges two records                  | `merge(foo, bar)`                          |
| [`sort`](functions/sort.md)   | Sorts a record by field names       | `xs.sort()`                                |

## List

| Function                                  | Description                                | Example                    |
| :---------------------------------------- | :----------------------------------------- | :------------------------- |
| [`append`](functions/append.md)           | Inserts an element at the back of a list   | `xs.append(y)`             |
| [`prepend`](functions/prepend.md)         | Inserts an element at the front of a list  | `xs.prepend(y)`            |
| [`concatenate`](functions/concatenate.md) | Merges two lists                           | `concatenate(xs, ys)`      |
| [`get`](functions/get.md)                 | Accesses an element of a list              | `list.get(index, default)` |
| [`length`](functions/length.md)           | Retrieves the length of a list             | `[1,2,3].length()`         |
| [`map`](functions/map.md)                 | Maps each list element to an expression    | `xs.map(x, x + 3)`         |
| [`sort`](functions/sort.md)               | Sorts a list by its values.                | `xs.sort()`                |
| [`where`](functions/where.md)             | Filters list elements based on a predicate | `xs.where(x, x > 5)`       |
| [`zip`](functions/zip.md)                 | Combines two lists into a list of pairs    | `zip(xs, ys)`              |

## Subnet

| Function                          | Description                               | Example                |
| :-------------------------------- | :---------------------------------------- | :--------------------- |
| [`network`](functions/network.md) | Retrieves the network address of a subnet | `10.0.0.0/8.network()` |

## String

### Inspection

| Function                                    | Description                                               | Example                         |
| :------------------------------------------ | :-------------------------------------------------------- | :------------------------------ |
| [`length_bytes`](functions/length_bytes.md) | Returns the length of a string in bytes                   | `"hello".length_bytes()`        |
| [`length_chars`](functions/length_chars.md) | Returns the length of a string in characters              | `"hello".length_chars()`        |
| [`starts_with`](functions/starts_with.md)   | Checks if a string starts with a substring                | `"hello".starts_with("he")`     |
| [`ends_with`](functions/ends_with.md)       | Checks if a string ends with a substring                  | `"hello".ends_with("lo")`       |
| [`is_alnum`](functions/is_alnum.md)         | Checks if a string is alphanumeric                        | `"hello123".is_alnum()`         |
| [`is_alpha`](functions/is_alpha.md)         | Checks if a string contains only letters                  | `"hello".is_alpha()`            |
| [`is_lower`](functions/is_lower.md)         | Checks if a string is in lowercase                        | `"hello".is_lower()`            |
| [`is_numeric`](functions/is_numeric.md)     | Checks if a string contains only numbers                  | `"1234".is_numeric()`           |
| [`is_printable`](functions/is_printable.md) | Checks if a string contains only printable characters     | `"hello".is_printable()`        |
| [`is_title`](functions/is_title.md)         | Checks if a string follows title case                     | `"Hello World".is_title()`      |
| [`is_upper`](functions/is_upper.md)         | Checks if a string is in uppercase                        | `"HELLO".is_upper()`            |
| [`match_regex`](functions/match_regex.md)   | Checks if a string partially matches a regular expression | `"Hi".match_regex("[Hh]i")`     |
| [`slice`](functions/slice.md)               | Slices a string with offsets and strides                  | `"Hi".slice(begin=2, stride=4)` |

### Transformation

| Function                                      | Description                                  | Example                       |
| :-------------------------------------------- | :------------------------------------------- | :---------------------------- |
| [`trim`](functions/trim.md)                   | Trims whitespace from both ends of a string  | `" hello ".trim()`            |
| [`trim_start`](functions/trim_start.md)       | Trims whitespace from the start of a string  | `" hello".trim_start()`       |
| [`trim_end`](functions/trim_end.md)           | Trims whitespace from the end of a string    | `"hello ".trim_end()`         |
| [`capitalize`](functions/capitalize.md)       | Capitalizes the first character of a string  | `"hello".capitalize()`        |
| [`replace`](functions/replace.md)             | Replaces characters within a string          | `"hello".replace("o", "a")`   |
| [`replace_regex`](functions/replace_regex.md) | Reverses the characters of a string          | `"hello".replace("l+o", "y")` |
| [`reverse`](functions/reverse.md)             | Reverses the characters of a string          | `"hello".reverse()`           |
| [`to_lower`](functions/to_lower.md)           | Converts a string to lowercase               | `"HELLO".to_lower()`          |
| [`to_title`](functions/to_title.md)           | Converts a string to title case              | `"hello world".to_title()`    |
| [`to_upper`](functions/to_upper.md)           | Converts a string to uppercase               | `"hello".to_upper()`          |
| [`split`](functions/split.md)                 | Splits a string into substrings              | `split("a,b,c", ",")`         |
| [`split_regex`](functions/split_regex.md)     | Splits a string into substrings with a regex | `split_regex("a1b2c", r"\d")` |
| [`join`](functions/join.md)                   | Joins a list of strings into a single string | `join(["a", "b", "c"], ",")`  |

### Filesystem

| Function                                | Description                                    | Example                           |
| :-------------------------------------- | :--------------------------------------------- | :-------------------------------- |
| [`file_contents`](functions/file_contents.md)   | Reads a file's contents      | `file_contents("/path/to/file")`  |
| [`file_name`](functions/file_name.md)   | Extracts the file name from a file path        | `file_name("/path/to/log.json")`  |
| [`parent_dir`](functions/parent_dir.md) | Extracts the parent directory from a file path | `parent_dir("/path/to/log.json")` |

## Parsing

| Function                                     | Description                              | Example                                            |
| :------------------------------------------- | :--------------------------------------- | :------------------------------------------------- |
| [`parse_cef`](functions/parse_cef.mdx)       | Parses a string as a CEF message         | `string.parse_cef()`                               |
| [`parse_csv`](functions/parse_csv.mdx)       | Parses a string as CSV                   | `string.parse_csv(header=["a","b"])`               |
| [`parse_grok`](functions/parse_grok.mdx)     | Parses a string following a grok pattern | `string.parse_grok("%{IP:client} …")`              |
| [`parse_json`](functions/parse_json.mdx)     | Parses a string as a JSON value          | `string.parse_json()`                              |
| [`parse_kv`](functions/parse_kv.mdx)         | Parses a string as Key-Value paris       | `string.parse_kv()`                                |
| [`parse_leef`](functions/parse_leef.mdx)     | Parses a string as a LEEF message        | `string.parse_leef()`                              |
| [`parse_ssv`](functions/parse_ssv.mdx)       | Parses a string as SSV                   | `string.parse_ssv(header=["a","b"])`               |
| [`parse_syslog`](functions/parse_syslog.mdx) | Parses a string as a Syslog message      | `string.parse_syslog()`                            |
| [`parse_tsv`](functions/parse_tsv.mdx)       | Parses a string as TSV                   | `string.parse_tsv(header=["a","b"])`               |
| [`parse_xsv`](functions/parse_xsv.mdx)       | Parses a string as XSV                   | `string.parse_xsv(",", ";", "", header=["a","b"])` |
| [`parse_yaml`](functions/parse_yaml.mdx)     | Parses a string as YAML                  | `string.parse_yaml()`                              |

## Printing

| Function                                     | Description                                   | Example                 |
| :------------------------------------------- | :-------------------------------------------- | :---------------------- |
| [`print_csv`](functions/print_csv.mdx)       | Prints a record as comma separated values     | `record.print_csv()`    |
| [`print_kv`](functions/print_kv.md)          | Prints a record as Key-Value pairs            | `record.print_kv()`     |
| [`print_json`](functions/print_json.mdx)     | Prints a record as a JSON string              | `record.print_json()`   |
| [`print_ndjson`](functions/print_ndjson.mdx) | Prints a record as NDJSON string              | `record.print_ndjson()` |
| [`print_ssv`](functions/print_ssv.mdx)       | Prints a record as space separated values     | `record.print_ssv()`    |
| [`print_tsv`](functions/print_tsv.mdx)       | Prints a record as tab separated values       | `record.print_tsv()`    |
| [`print_xsv`](functions/print_xsv.md)        | Prints a record as delimited separated values | `record.print_tsv()`    |
| [`print_yaml`](functions/print_yaml.md)      | Prints a value as a YAML document             | `record.print_yaml()`   |

## Time & Date

| Function                                                 | Description                                       | Example                               |
| :------------------------------------------------------- | :------------------------------------------------ | :------------------------------------ |
| [`as_secs`](functions/as_secs.md)                        | Converts a duration into seconds                  | `as_secs(42ms)`                       |
| [`from_epoch`](functions/from_epoch.md)                  | Interprets a duration as Unix time                | `from_epoch(time_ms * 1ms)`           |
| [`now`](functions/now.md)                                | Gets the current wallclock time                   | `now()`                               |
| [`since_epoch`](functions/since_epoch.md)                | Turns a time into a duration since the Unix epoch | `since_epoch(2021-02-24)`             |
| [`parse_time`](functions/parse_time.md)                  | Parses a timestamp following a given format       | `"10/11/2012".parse_time("%d/%m/%Y")` |
| [`years`](functions/years.mdx)                           | Converts a number to equivalent years             | `years(100)`                          |
| [`months`](functions/months.mdx)                         | Converts a number to equivalent months            | `months(100)`                         |
| [`weeks`](functions/weeks.mdx)                           | Converts a number to equivalent weeks             | `weeks(100)`                          |
| [`days`](functions/days.mdx)                             | Converts a number to equivalent days              | `days(100)`                           |
| [`hours`](functions/hours.mdx)                           | Converts a number to equivalent hours             | `hours(100)`                          |
| [`minutes`](functions/minutes.mdx)                       | Converts a number to equivalent minutes           | `minutes(100)`                        |
| [`seconds`](functions/seconds.mdx)                       | Converts a number to equivalent seconds           | `seconds(100)`                        |
| [`milliseconds`](functions/milliseconds.mdx)             | Converts a number to equivalent milliseconds      | `milliseconds(100)`                   |
| [`microseconds`](functions/microseconds.mdx)             | Converts a number to equivalent microseconds      | `microseconds(100)`                   |
| [`nanoseconds`](functions/nanoseconds.mdx)               | Converts a number to equivalent nanoseconds       | `nanoseconds(100)`                    |
| [`count_years`](functions/count_years.mdx)               | Counts the number of years in a duration          | `count_years(100d)`                   |
| [`count_months`](functions/count_months.mdx)             | Counts the number of months in a duration         | `count_months(100d)`                  |
| [`count_weeks`](functions/count_weeks.mdx)               | Counts the number of weeks in a duration          | `count_weeks(100d)`                   |
| [`count_days`](functions/count_days.mdx)                 | Counts the number of days in a duration           | `count_days(100d)`                    |
| [`count_hours`](functions/count_hours.mdx)               | Counts the number of hours in a duration          | `count_hours(100d)`                   |
| [`count_minutes`](functions/count_minutes.mdx)           | Counts the number of minutes in a duration        | `count_minutes(100d)`                 |
| [`count_seconds`](functions/count_seconds.mdx)           | Counts the number of seconds in a duration        | `count_seconds(100d)`                 |
| [`count_milliseconds`](functions/count_milliseconds.mdx) | Counts the number of milliseconds in a duration   | `count_milliseconds(100d)`            |
| [`count_microseconds`](functions/count_microseconds.mdx) | Counts the number of microseconds in a duration   | `count_microseconds(100d)`            |
| [`count_nanoseconds`](functions/count_nanoseconds.mdx)   | Counts the number of nanoseconds in a duration    | `count_nanoseconds(100d)`             |

<!--
This is hidden because there is an issue with the timezone DB.
[`format_time`](functions/format_time.md) | Format a timestamp following a given format | `2012-11-10.format_time("%d/%m/%Y")`
-->

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

| Function                        | Description                | Example                        |
| :------------------------------ | :------------------------- | :----------------------------- |
| [`ceil`](functions/ceil.md)     | Takes the ceiling          | `ceil(4.2)`, `ceil(3.2s, 1m)`  |
| [`floor`](functions/floor.md)   | Takes the floor            | `floor(4.2)`, `floor(32h, 1d)` |
| [`random`](functions/random.md) | Generates a random number  | `random()`                     |
| [`round`](functions/round.md)   | Rounds a value             | `round(4.2)`, `round(31m, 1h)` |
| [`sqrt`](functions/sqrt.md)     | Calculates the square root | `sqrt(49)`                     |

## Networking

| Function                                              | Description                 | Example                                                     |
| :---------------------------------------------------- | :-------------------------- | :---------------------------------------------------------- |
| [`community_id`](functions/community_id.md)           | Computes a Community ID     | `community_id(src_ip=1.2.3.4, dst_ip=4.5.6.7, proto="tcp")` |
| [`decapsulate`](functions/decapsulate.md)             | Decapsulates PCAP packets   | `decapsulate(this)`                                         |
| [`encrypt_cryptopan`](functions/encrypt_cryptopan.md) | Encrypts IPs via Crypto-PAn | `encrypt_cryptopan(1.2.3.4)`                                |
| [`is_v4`](functions/is_v4.md)                         | Checks if an IP is IPv4     | `is_v4(1.2.3.4)`                                            |
| [`is_v6`](functions/is_v6.md)                         | Checks if an IP is IPv6     | `is_v6(::1)`                                                |

## Hashing

| Function                                  | Description                   | Example              |
| :---------------------------------------- | :---------------------------- | :------------------- |
| [`hash_md5`](functions/hash_md5.md)       | Computes a MD5 hash digest    | `hash_md5("foo")`    |
| [`hash_sha1`](functions/hash_sha1.md)     | Computes a SHA1 hash digest   | `hash_sha1("foo")`   |
| [`hash_sha224`](functions/hash_sha224.md) | Computes a SHA224 hash digest | `hash_sha224("foo")` |
| [`hash_sha256`](functions/hash_sha256.md) | Computes a SHA256 hash digest | `hash_sha256("foo")` |
| [`hash_sha384`](functions/hash_sha384.md) | Computes a SHA384 hash digest | `hash_sha384("foo")` |
| [`hash_sha512`](functions/hash_sha512.md) | Computes a SHA512 hash digest | `hash_sha512("foo")` |
| [`hash_xxh3`](functions/hash_xxh3.md)     | Computes a XXH3 hash digest   | `hash_xxh3("foo")`   |

## Encoding

| function                                      | description                                       | example                   |
| :-------------------------------------------- | :------------------------------------------------ | :------------------------ |
| [`encode_base64`](functions/encode_base64.md) | Encodes bytes as Base64                           | `encode_base64("Tenzir")` |
| [`encode_hex`](functions/encode_hex.md)       | Encodes bytes as their hexadecimal representation | `encode_hex("Tenzir")`    |

## Decoding

| function                                      | description                                         | example                     |
| :-------------------------------------------- | :-------------------------------------------------- | :-------------------------- |
| [`decode_base64`](functions/decode_base64.md) | Decodes bytes as Base64                             | `decode_base64("VGVuemly")` |
| [`decode_hex`](functions/decode_hex.md)       | Decodes bytes from their hexadecimal representation | `decode_hex("4e6f6E6365")`  |

## Type System

### Introspection

| Function                          | Description                                    | Example            |
| :-------------------------------- | :--------------------------------------------- | :----------------- |
| [`type_id`](functions/type_id.md) | Retrieves the type id of an expression         | `type_id(1 + 3.2)` |
| [`type_of`](functions/type_id.md) | Retrieves the type definition of an expression | `type_id(this)`    |

### Conversion

| Function                            | Description                                | Example                |
| :---------------------------------- | :----------------------------------------- | :--------------------- |
| [`int`](functions/int.md)           | Casts an expression to a signed integer    | `int(-4.2)`            |
| [`uint`](functions/uint.md)         | Casts an expression to an unsigned integer | `uint(4.2)`            |
| [`float`](functions/float.md)       | Casts an expression to a float             | `float(42)`            |
| [`string`](functions/string.md)     | Casts an expression to string              | `string(1.2.3.4)`      |
| [`ip`](functions/ip.md)             | Casts an expression to an IP               | `ip("1.2.3.4")`        |
| [`subnet`](functions/subnet.md)     | Casts an expression to a subnet            | `subnet("1.2.3.4/16")` |
| [`time`](functions/time.md)         | Casts an expression to a time value        | `time("2020-03-15")`   |
| [`duration`](functions/duration.md) | Casts an expression to a duration value    | `duration("1.34w")`    |

### Transposition

| Function                              | Description                  | Example           |
| :------------------------------------ | :--------------------------- | :---------------- |
| [`flatten`](functions/flatten.md)     | Flattens nested data         | `flatten(this)`   |
| [`unflatten`](functions/unflatten.md) | Unflattens nested structures | `unflatten(this)` |

## Runtime

| Function                        | Description                   | Example          |
| :------------------------------ | :---------------------------- | :--------------- |
| [`config`](functions/config.md) | Reads the configuration file  | `config()`       |
| [`env`](functions/env.md)       | Reads an environment variable | `env("PATH")`    |
| [`secret`](functions/secret.md) | Reads a secret from a store   | `secret("PATH")` |
