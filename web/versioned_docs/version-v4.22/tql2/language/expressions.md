---
sidebar_position: 2
---

# Expressions

This page outlines the expressions available in the Tenzir Programming Language.

## Expression Kinds

### Literals

Literals serve as the foundational building blocks for constructing data. They
are simple, self-contained constants.

```
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

Literals such as `42`, `123.45`, `2.5k` and `2s` are called scalars. Numeric
scalars may have power-of-ten suffixes, such as `k` (=1,000), `M` (=1,000,000),
`G`, `T`, `P` and `E`. Power-of-two suffixes, such as `Ki` (=1,024), `Mi`
(=1,048,576), `Gi`, `Ti`, `Pi` and `Ei`, may also be used. For example, `2k` is
equivalent to `2000`. Duration scalars use `ns`, `us`, `ms`, `s`,
`min`, `h`, `d`, `w` and `y`.

String literals support escape sequences. For instance, `"\n"` is a single
newline character. To opt out of this behavior, you can use raw strings: `r"\n"`
is a backslash followed by the letter "n". Raw strings can also be enclosed with
the `#` symbol. This is helpful if you want to include quotes in your string:
`r#"They said "hello"."#`

Date literals follow the ISO 8601 standard. IP literals can be written using the
IPv4 or IPv6 notation. Subnet literals are IP literals followed by a slash and
the number of active bits.

### Fields

A single identifier can be used to refer to a top-level field. For example,
`my_field` references the top-level field of that name. To reference a field
that is not on the top-level, use `.<name>` on an expression that returns a
record. For example, `my_field.my_subfield` references the field `my_subfield`
in `my_field`, assuming `my_field` is a record.

### `this`

The `this` keyword allows you to reference the entire top-level event. For
instance, `from {x: 1, y: 2} | z = this` has the output
`{x: 1, y: 2, z: {x: 1, y: 2}}`. This keyword can also be used to overwrite the
whole event, as demonstrated by `this = {a: x, y: b}`.

### Metadata

Events carry not only data, but also metadata. To refer to the metadata, use the
`@` prefix. For example, `@name` carries the name of the event. Currently, the
set of metadata fields is limited to just  `@name`, `@import_time` and
`@internal`, but will potentially be expanded later to allow arbitrary
user-defined metadata fields.

### Unary Expression

The unary expression operators are `+`, `-` and `not`. `+` and `-` expect a
number or duration. `not` expects a boolean value.

### Binary Expression

The binary expressions operators are `+`, `-`, `*`, `/`, `==`, `!=`, `>`, `>=`,
`<`, `<=`, `and`, `or`, and `in`.

### Indexing

The syntax `expr[index]` can be used to access items of both lists and records.
If `expr` a list, then `index` must be an integer, where `0` refers to the first
element of the list. If `expr` is a record, then `index` must be a string which
is interpreted as the name of a field. This can be used to refer to fields which
are not valid identifiers, for example `foo["not a valid identifier!"]`. At the
moment, the string is required to be a constant expression.

### Records

Records are created with a pair of braces. `{}` denotes the empty record. Fields
are normally specified  by using simple identifiers, followed by a colon and
then an expression, for example: `{foo: 1, bar: 2}`. If the field name would not
be valid identifier, use a string literal instead:
`{"not a valid identifier!": 3}`. The individual fields are separated with
commas. The final field may have a trailing comma: `{foo: 42,}`. Records can be
expanded into other records by using `...`. For example, if `foo` is
`{a: 1, b: 2}`, then `{...foo, c: 3}` is `{a: 1, b: 2, c: 3}`. As fields must be
unique, having the same fields multiple times will only keep the last value.

### Lists

Lists are created with a pair of brackets. `[]` denotes the empty list. The
items of the list are specified with a comma-delimited list of expressions, such
as `[1, 2+3, foo()]`. As with records, the final item may have a trailing comma:
`[foo, bar,]`. Lists can be expanded into other lists by using `...`. For
example, if `foo` is `[1, 2]`, then `[...foo, 3]` is `[1, 2, 3]`.

### Functions and Methods

Functions are invoked by following name with parenthesis and a comma-delimited
sequence of arguments, for example: `now()`, `sqrt(42)`, `round(391s, 1min)`.
Methods are like functions, but also have additional method subject followed by
a dot, such as `expr.trim()`. The final argument may have a trailing comma.

### Pipeline Expression

Some operators expect a pipeline expression as an argument. Pipeline expressions
are written with a pair of braces, for example: `{ head 5 }`. If the final
argument to an operator is a pipeline expression, then the preceding comma may
be omitted, as in `every 10s { head 5 }`. The braces can contain multiple
statements. The same statement separation rules apply as usual. For example,
newlines can be used to separate statements.

### Let Substitution

A previously defined `let` binding can be referenced in an expression by using
the same `$`-prefixed name. For example, if `let $foo = 42` is defined, then
`where some_field == $foo` is equivalent to `where some_field == 42`.

### `if` Expression

:::note
This functionality is not implemented yet.
:::

The `if` keyword can also be used in an expression context. For example:
`if foo == 42 { "yes" } else { "no" }`.

### `match` Expression

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

## Precedence

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
