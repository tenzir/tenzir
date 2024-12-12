---
sidebar_position: 2
---

# Expressions

This page outlines the expressions available in the Tenzir Programming Language.

## Literals

Literals serve as the foundational building blocks for constructing data. They
are simple, self-contained constants.

```tql
true
false
null
42
123.45
2.5k
2s
"Hello!"
r"C:\tmp"
2024-10-03
2001-02-03T04:05:06Z
192.168.0.1
::ab12:253
192.0.0.0/8
```

Literals such as `42`, `123.45`, `2.5k` and `2s` are called scalars.

### Numeric Suffixes

Numeric scalars may have magnitude suffixes:

* **Power-of-ten suffixes**: `k` (=1,000), `M` (=1,000,000),
`G`, `T`, `P` and `E`.
* **Power-of-two suffixes**: `Ki` (=1,024), `Mi` (=1,048,576), `Gi`, `Ti`, `Pi` and `Ei`, may also be used. For example, `2k` is
equivalent to `2000`.

### Duration Literals

Duration scalars use unit suffixes `ns`, `us`, `ms`, `s`,
`min`, `h`, `d`, `w` and `y`.

### Date Literals

Date literals follow the [ISO 8601 standard](https://en.wikipedia.org/wiki/ISO_8601).

### IP Literals

IP literals can be written using the IPv4 or IPv6 notation. Subnet literals are
IP literals followed by a slash and the number of active bits.

### String Literals

String literals support escape sequences. For instance, `"\n"` is a single
newline character. To opt out of this behavior, you can use raw strings: `r"\n"`
is a backslash followed by the letter "n". Raw strings can also be enclosed with
the `#` symbol. This is helpful if you want to include quotes in your string:
`r#"They said "hello"."#`




## Fields

A single identifier can be used to refer to a top-level field. To reference a
field that is not on the top-level, use `.<name>` on an expression that returns
a record.

```tql
from { my_field: 42, top_level: { nested: 0 } }
my_field = top_level.nested
```
```tql
{ my_field: 0, top_level: { nested: 0 } }
```

## `this`

The `this` keyword allows you to reference the entire top-level event. For
instance, `from {x: 1, y: 2} | z = this` has the output
`{x: 1, y: 2, z: {x: 1, y: 2}}`. This keyword can also be used to overwrite the
whole event, as demonstrated by `this = {a: x, y: b}`.

## Metadata

Events carry not only data, but also metadata. To refer to the metadata, use the
`@` prefix. For example, `@name` carries the name of the event. Currently, the
set of metadata fields is limited to just  `@name`, `@import_time` and
`@internal`, but will potentially be expanded later to allow arbitrary
user-defined metadata fields.

## Unary Expression

The unary expression operators are `+`, `-` and `not`. `+` and `-` expect a
number or duration. `not` expects a boolean value.

## Binary Expression

The binary expressions operators are `+`, `-`, `*`, `/`, `==`, `!=`, `>`, `>=`,
`<`, `<=`, `and`, `or`, and `in`.

### Arithmetic Operations

The arithmetic operators `+`, `-`, `*` and `/` can be used to perform arithmetic
on some types.

#### Numeric Values

The numeric types, `int64`, `uint64` and `double` support all arithmetic operations.
If the types of the left- and right-hand side are different, the return type will
be the type able to hold most values:

Operation | Result
|:--- |: ---
`int64` + `int64` | `int64`
`int64` + `uint64` | `int64`
`int64` + `double` | `double`

The same applies to the other arithmetic operators, `-`, `*` and `/`.

If the resulting value would be outside of the value range of the result type,
it will be `null`. Conversely, there is no overflow/wrapping behavior.

Integer division by zero yields `null`.

#### Time & Duration

The `time` and `duration` types only support a select set of operations.

Operation | Result
|:--- |: ---
`time + duration` | `time`
`time - duration` | `time`
`time - time` | `duration`
`duration + duration` | `duration`
`duration / duration` | `double`
`duration * number` | `duration`
`duration / number` | `duration`

### String Operations

Strings can be concatenated using the `+` operator:

```tql
result = "Hello " + "World!"
```
```tql
{ result: "Hello World!" }
```

`in` can be used to check whether a string contains a substring:

```tql
a = "World" in "Hello World"
b = "Planet" in "Hello World"
```
```tql
{ a: true, b: false }
```

### Relational Operations

#### Equality

All types are equality comparable with themselves. Additionally, number types
are equality comparable with each other. All types can be compared with `null`
to check if they are null.

#### Ordering

For numeric types, operators `<`, `<=`, `>` and `>=` compare their magnitude.
For the `string` type, the comparison is done lexicographically.
`ip`s and `subnet`s are ordered by their IP-v6 bit pattern.

### Logical Operations

The logical operators `and` and `or` can be used to join multiple boolean
expressions, allowing you to check multiple conditions in a single expression.

```tql
where timestamp > now() - 1d and severity == "alert"
```

### Range Operations

The `in` operator can be used to check whether a value is in a list/range.

* `T in list<T>` checks whether a list contains a value.
* `ip in subnet` checks whether an IP is in a given subnet.
* `subnet in subnet` checks whether a subnet is a subset of a given subnet.

`Value not in Range` is an alternative way to use `not (Value in Range)`.


## Indexing/Element access

### Lists

You can access the elements of a list using an integral index, where `0` refers
to the first element:

```tql
let $my_list = ["Hello", "World"]
result = my_list[0]
```
```tql
{ result: "Hello" }
```

### Records

Most fields in a record can just be accessed using `record.fieldname`. If your
fieldname is not a valid identifier (for example if it contains spaces), or if
it depends on an event value, you can used an indexing expression:

```tql title="Accessing a fieldname with a space"
let $answers = { "the ultimate question": 42 }
result = $answers["the ultimate question"]
```
```tql
{ result: 42 }
```

```tql title="Accessing a field based on a runtime value"
let $severity_to_level = { "ERROR": 1, "WARNING": 2, "INFO": 3 }
from { severity: "ERROR" }
level = $severity_to_level[severity]
```
```tql
{
  severity: "ERROR",
  level: 1
}
```

## Records

Records are created with a pair of braces. `{}` denotes the empty record. Fields
are normally specified  by using simple identifiers, followed by a colon and
then an expression, for example: `{foo: 1, bar: 2}`. If the field name would not
be valid identifier, use a string literal instead:
`{"not a valid identifier!": 3}`. The individual fields are separated with
commas. The final field may have a trailing comma: `{foo: 42,}`.

```tql title="Creating a record"
let $my_record = {
  name: "Tom",
  age: 42,
  friends: ["Jerry", "Brutus"],
  "detailed summary": "Jerry is a cat."
}
```

Records can be expanded into other records by using `...`:

For example, if `foo` is
`{a: 1, b: 2}`, then `{...foo, c: 3}` is `{a: 1, b: 2, c: 3}`. As fields must be
unique, having the same fields multiple times will only keep the last value.

```tql title="Lifting nested fields"
from { nested: { severity: 4, type: "source" } }
```
```tql
{
  nested: { severity: 4, type: "source" },
  severity: 4,
  type: "source
}
```

## Lists

Lists are created with a pair of brackets. `[]` denotes the empty list. The
items of the list are specified with a comma-delimited list of expressions, such
as `[1, 2+3, foo()]`. As with records, the final item may have a trailing comma:
`[foo, bar,]`. Lists can be expanded into other lists by using `...`. For
example, if `foo` is `[1, 2]`, then `[...foo, 3]` is `[1, 2, 3]`.

## Functions and Methods

Functions are invoked by following name with parenthesis and a comma-delimited
sequence of arguments, for example: `now()`, `sqrt(42)`, `round(391s, 1min)`.
Methods are like functions, but also have additional method subject followed by
a dot, such as `expr.trim()`. The final argument may have a trailing comma.

## Pipeline Expression

Some operators expect a pipeline expression as an argument. Pipeline expressions
are written with a pair of braces, for example: `{ head 5 }`. If the final
argument to an operator is a pipeline expression, then the preceding comma may
be omitted, as in `every 10s { head 5 }`. The braces can contain multiple
statements. The same statement separation rules apply as usual. For example,
newlines can be used to separate statements.

## Let Substitution

A previously defined `let` binding can be referenced in an expression by using
the same `$`-prefixed name:

```tql
let $pi = 3
from { radius = 1 }
area = radius * radius * pi
```
```tql
{ radius: 1, area: 3 }
```

## `if` Expression

:::note
This functionality is not implemented yet.
:::

The `if` keyword can also be used in an expression context. For example:
`if foo == 42 { "yes" } else { "no" }`.

## `match` Expression

:::note
This functionality is not implemented yet.
:::

The `match` keyword can also be used in an expression context to perform pattern
matching. For example, the expression
`match num { 1 => "one", 2 => "two", _ => "neither one nor two"}` inspects the
value of `num` and returns the corresponding description. `_` can be used as a
final catch-all. Without a `_` case, it can happen that there is no match for
the value. In that case, the `match` expression will evaluate to `null` and a
warning will be emitted.


## Operator Precedence

Expressions such as `1 - 2 * 3 + 4` follow additional precedence and
associativity rules, making the previous expression equivalent to
`(1 - (2 * 3)) + 4`. The following table details the disambiguation process,
ordered from highest to lowest precedence.

Expression | Associativity
-----------|-----
method call |
field access |
`[]`-indexing |
unary `+` `-`  |
`*` `/` | left
binary `+` `-` | left
`==` `!=` `>` `>=` `<` `<=` `in` | left (will be changed to none)
`not` |
`and` | left
`or` | left

## Constant Expressions

A constant expression is an expression that can be evaluated to a constant when
the pipeline that contains it is started. Many pipeline operators require certain
arguments to be constants. For example, `head 5` is valid because the integer
literal is constant. On the other hand, `head x` is invalid, because the value
of the field `x` depends on the events flowing through the `head` operator.
Functions such as `now()` and `random()` can also be constant evaluated, even
though their results vary. In such cases, the function call is evaluated once
when the pipeline starts, and the resulting value is treated as a constant.
