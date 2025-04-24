---
sidebar_position: 2
---

# Expressions

This page describes the expressions available in the Tenzir Query Language (TQL).

## Literals

You use literals as the foundational building blocks to construct data. They are
simple, self-contained constants.

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

Literals such as `42`, `123.45`, `2.5k`, and `2s` are called scalars.

### Numeric Suffixes

Numeric scalars can have magnitude suffixes:

- **Power-of-ten suffixes**: `k` (=1,000), `M` (=1,000,000), `G`, `T`, `P`, and
  `E`.
- **Power-of-two suffixes**: `Ki` (=1,024), `Mi` (=1,048,576), `Gi`, `Ti`, `Pi`,
  and `Ei`. For example, `2k` is equivalent to `2000`.

### Duration Literals

Use unit suffixes like `ns`, `us`, `ms`, `s`, `min`, `h`, `d`, `w`, `mo`, or `y`
to create duration scalars.

### Date Literals

Write date literals using the
[ISO 8601 standard](https://en.wikipedia.org/wiki/ISO_8601).

### IP Literals

Write IP literals using either IPv4 or IPv6 notation. Subnet literals are IP
literals followed by a slash and the number of active bits.

### String Literals

Write string literals with escape sequences. For example, `"\n"` represents a
newline character. Use raw strings like `r"\n"` to prevent escape sequence
behavior. Enclose raw strings with the `#` symbol to include quotes in your
string, such as `r#"They said "hello"."#`.

## Fields

Use a single identifier to refer to a top-level field. To access a nested field,
append `.<name>` to an expression that returns a record.

```tql
from {
  my_field: 42,
  top_level: {
    nested: 0
  }
}
my_field = top_level.nested
```

```tql
{my_field: 0, top_level: {nested: 0}}
```

To avoid a warning when the nested field does not exist, use `<name>?`.

```tql
from (
  {foo: 1},
  {bar: 2},
)
select foo = foo?
```

```tql
{foo: 1}
{foo: null}
```

To access a field with special characters in its name or based on its index, use
an [Index Expression](#indexingelement-access).

## `this`

Use the `this` keyword to reference the entire top-level event. For example,
`from {x: 1, y: 2} | z = this` produces `{x: 1, y: 2, z: {x: 1, y: 2}}`. You can
also use `this` to overwrite the entire event, as in `this = {a: x, y: b}`.

## Moving Fields

Use `move` keyword before a field to move fields as part of an assigment:

```tql
from {foo: 1, bar: 2}
qux = move bar + 2
```

```tql
{foo: 1, qux: 4}
```

Notice how the field `bar` does not exist anymore in the output.

:::tip Bulk-move fields with the `move` operator

When moving many fields, the `move` operator is a convenient alternative:

```tql
x = move foo
y = move bar
z = move baz
```

Can also just be spelled as:

```
move x=foo, y=bar, z=baz
```

:::

## Metadata

Events carry both data and metadata. Access metadata fields using the `@`
prefix. For instance, `@name` holds the name of the event. Currently, available
metadata fields include `@name`, `@import_time`, and `@internal`. Future updates
may allow defining custom metadata fields.

## Unary Expression

Use the unary operators `+`, `-`, and `not`. The `+` and `-` operators expect a
number or duration, while `not` expects a boolean value.

## Binary Expression

The binary expression operators include `+`, `-`, `*`, `/`, `==`, `!=`, `>`,
`>=`, `<`, `<=`, `and`, `or`, and `in`.

### Arithmetic Operations

Use the arithmetic operators `+`, `-`, `*`, and `/` to perform arithmetic on
specific types.

#### Numeric Values

The numeric types `int64`, `uint64`, and `double` support all arithmetic
operations. If the types of the left- and right-hand side differ, the return
type will be the one capable of holding the most values.

| Operation          | Result   |
| :----------------- | :------- |
| `int64` + `int64`  | `int64`  |
| `int64` + `uint64` | `int64`  |
| `int64` + `double` | `double` |

The same applies to the other arithmetic operators: `-`, `*`, and `/`.

If the resulting value exceeds the range of the result type, it evaluates to
`null`. There is no overflow or wrapping behavior. Division by zero also
produces `null`.

#### Time & Duration

The `time` and `duration` types support specific operations:

| Operation             | Result     |
| :-------------------- | :--------- |
| `time + duration`     | `time`     |
| `time - duration`     | `time`     |
| `time - time`         | `duration` |
| `duration + duration` | `duration` |
| `duration / duration` | `double`   |
| `duration * number`   | `duration` |
| `duration / number`   | `duration` |

### String Operations

Concatenate strings using the `+` operator:

```tql
result = "Hello " + "World!"
```

```tql
{ result: "Hello World!" }
```

Check if a string contains a substring using `in`:

```tql
a = "World" in "Hello World"
b = "Planet" in "Hello World"
```

```tql
{ a: true, b: false }
```

### Relational Operations

#### Equality

All types can compare equality with themselves. Numeric types can compare
equality across different numeric types. All types can also compare equality
with `null`.

#### Ordering

For numeric types, operators `<`, `<=`, `>`, and `>=` compare their magnitude.
For `string`, comparisons are lexicographic. The `ip` and `subnet` types are
ordered by their IPv6 bit pattern.

### Logical Operations

Join multiple boolean expressions using the `and` and `or` operators to check
multiple conditions.

```tql
where timestamp > now() - 1d and severity == "alert"
```

### Range Operations

Use the `in` operator to check if a value is within a list or range.

- `T in list<T>` checks if a list contains a value.
- `ip in subnet` checks if an IP is in a given subnet.
- `subnet in subnet` checks if one subnet is a subset of another.

To negate, use `not (Value in Range)` or `Value not in Range`.

## Indexing/Element Access

### Lists

Access list elements using an integral index, starting with `0` for the first
element.

```tql
let $my_list = ["Hello", "World"]
result = my_list[0]
```

```tql
{result: "Hello"}
```

To suppress warnings when the list index is out of bounds, use the
[`get`](../functions/get.md) function with a fallback value:

```tql
let $my_list = ["Hello", "World"]
result = get(my_list[2], "default")
```

```tql
{result: "default"}
```

### Records

Access fields in a record using `record.fieldname`. If the field name contains
spaces or depends on a runtime value, use an indexing expression:

```tql title="Accessing a fieldname with a space"
let $answers = {"the ultimate question": 42}
result = $answers["the ultimate question"]
```

```tql
{result: 42}
```

```tql title="Accessing a field based on a runtime value"
let $severity_to_level = {"ERROR": 1, "WARNING": 2, "INFO": 3}
from {severity: "ERROR"}
level = $severity_to_level[severity]
```

```tql
{
  severity: "ERROR",
  level: 1
}
```

To suppress warnings when the record field is missing, use the `?` operator:

```tql
from {foo: 1, bar: 2}
result = baz?
```

```tql
{result: null}
```

Indexing expressions support numeric indices to access record fields:

```tql title="Accessing a field by index"
from {foo: "Hello", bar: "World"}
select first_field = this[0]
```

```tql
{first_field: "Hello"}
```

## Records

Create records using a pair of braces. `{}` denotes the empty record. Specify
fields using simple identifiers followed by a colon and an expression, e.g.,
`{foo: 1, bar: 2}`. For invalid identifiers, use a string literal, e.g.,
`{"not valid!": 3}`. Separate fields with commas. The final field can have a
trailing comma, e.g., `{foo: 42,}`.

```tql title="Creating a record"
let $my_record = {
  name: "Tom",
  age: 42,
  friends: ["Jerry", "Brutus"],
  "detailed summary": "Jerry is a cat."
}
```

Expand records into other records using `...`. For example, if `foo` is
`{a: 1, b: 2}`, then `{...foo, c: 3}` is `{a: 1, b: 2, c: 3}`. Fields must be
unique, and later values overwrite earlier ones.

```tql title="Lifting nested fields"
from { nested: { severity: 4, type: "source" } }
```

```tql
{
  nested: { severity: 4, type: "source" },
  severity: 4,
  type: "source"
}
```

## Lists

Create lists using a pair of brackets. `[]` denotes the empty list. Specify list
items with a comma-delimited sequence of expressions, e.g., `[1, 2+3, foo()]`.
The final item can have a trailing comma, e.g., `[foo, bar,]`. Expand lists into
other lists using `...`. For example, if `foo` is `[1, 2]`, then `[...foo, 3]`
is `[1, 2, 3]`.

## Functions and Methods

Invoke functions by following the name with parentheses and a comma-delimited
sequence of arguments, e.g., `now()`, `sqrt(42)`, `round(391s, 1min)`. Methods
are similar to functions but include a method subject followed by a dot, e.g.,
`expr.trim()`. The final argument can have a trailing comma.

## Pipeline Expression

Some operators expect a pipeline expression as an argument. Write pipeline
expressions using a pair of braces, e.g., `{ head 5 }`. If the final argument to
an operator is a pipeline expression, omit the preceding comma, e.g.,
`every 10s { head 5 }`. Braces can contain multiple statements. Separate
statements using newlines or other delimiters.

## Let Substitution

Reference a previously defined `let` binding in an expression using the same
`$`-prefixed name:

```tql
let $pi = 3
from { radius = 1 }
area = radius * radius * pi
```

```tql
{ radius: 1, area: 3 }
```

## `if` and `else` Expressions

The `if` keyword can also be used in an expression context, e.g.,
`"yes" if foo == 42 else "no"`.

The `else` keyword may be omitted, returning `null` instead if the predicate is
false, e.g., `"yes" if foo == 42`.

The `else` keyword may also be used standalone to provide a fallback value,
e.g., `foo else "fallback"` returns `"fallback"` if `foo == null`, and returns
`foo` otherwise.

## `match` Expression

:::note
This functionality is not implemented yet.
:::

Use the `match` keyword in an expression context to perform pattern matching,
e.g., `match num { 1 => "one", 2 => "two", _ => "neither one nor two" }`. The
`_` can be used as a catch-all case. If no match exists and no `_` is provided,
the `match` expression evaluates to `null`.

## Operator Precedence

Expressions like `1 - 2 * 3 + 4` follow precedence and associativity rules. The
expression evaluates as `(1 - (2 * 3)) + 4`. The following table lists
precedence, ordered from highest to lowest.

| Expression                             | Associativity                  |
| -------------------------------------- | ------------------------------ |
| method call                            |
| field access                           |
| `[]`-indexing                          |
| `move`                                 |
| unary `+`, `-`                         |
| `*`, `/`                               | left                           |
| binary `+`, `-`                        | left                           |
| `==`, `!=`, `>`, `<`, `>=`, `<=`, `in` | left (will be changed to none) |
| `not`                                  |
| `and`                                  | left                           |
| `or`                                   | left                           |
| `if`                                   | right                          |
| `else`                                 | left                           |

## Constant Expressions

A constant expression evaluates to a constant when the pipeline containing it
starts. Many pipeline operators require constant arguments. For example,
`head 5` is valid because the integer literal is constant. However, `head x` is
invalid because the value of `x` depends on events flowing through the operator.
Functions like `now()` and `random()` can also be constant evaluated; they are
evaluated once at pipeline start, and the result is treated as a constant.
