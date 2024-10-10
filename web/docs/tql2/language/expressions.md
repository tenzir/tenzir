---
sidebar_position: 3
---

# Expressions

:::warning
This page still needs to be written. You'll just find some experimentation below.
:::

## Expression Kinds

### Literals

Literals are the basic building blocks for constructing data. They are
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

Literals such as `42`, `123.45`, `2.5k` and `2s` are called scalars. Numeric
scalars can have the power-of-ten suffixes `k` (=1,000), `M` (=1,000,000), `G`,
`T`, `P` and `E`. The power-of-two suffixes `Ki` (=1,024), `Mi` (=1,048,576),
`Gi`, `Ti`, `Pi` and `Ei` may also be used. For example, `2k == 2000`. Duration
scalars can have the suffixes `ns`, `us`, `ms`, `s`, `min`, `h`, `d`, `w` and
`y`.

Strings normally provide escape sequences. For example, `"\n"` is a single
newline character. To opt out of this behavior, you can use raw strings: `r"\n"`
is a backslash followed by the letter "n". Raw strings can also be surrounded by
the `#` symbol. This is helpful if you want to include quotes in your string:
`r#"They said "hello"."#`

Date literals follow ISO 8601.

IP literals can be written using IPv4 or IPv6 notation.

Subnet literals are IP literals followed by the number of active bits.

### Field Name

A single identifier can be used to refer to a top-level field. For example,
`my_field` references the top-level field of that name. To reference a field
that is not on the top-level, use `.<name>`. For example, `my_field.my_subfield`
references the field "my_subfield" in "my_field", assuming "my_field" is a
record.

### `this`

To reference the whole top-level event, you can use the `this` keyword.

### `meta`

Events do not only carry data, but also metadata. To refer to metadata, use
the `meta` keyword. For example, `meta.name` carries the name of the event.

### Binary and Unary

Unary:
- `+x`
- `-x`
- `not x`

Binary:
- `x + y`
- `x - y`
- `x * y`
- `x / y`
- `x == y`
- `x != y`
- `x > y`
- `x >= y`
- `x < y`
- `x <= y`
- `x and y`
- `x or y`
- `x in y`

### Indexing with `[]`

### Records

### Lists

### Functions and Methods

### Pipeline Expression

### Let Substitution



## Precedence

Expression | Associativity
-----------|-----
method call |
field access |
`[]`-indexing |
unary `+` `-`  |
`*` `/`| left
`+` `-`| left
`==` `!=` `>` `>=` `<` `<=` `in` | left (should be none)
`not` |
`and` | left
`or` | left


## What is a selector?

Maybe?


## Expressions

- `42`: literal
- `my_field_name`: field
- `<expr>.foo`
- `this`
- `meta` / `@`
- binary: `foo + bar`
- unary: `-foo`
- `foo[bar]`
- `foo()` and `test.foo()`
- `{ foo: 42, bar: test() }` + `...`
- `[42, $okay]` + `...`
- `{ <pipeline> }`
- `$identifier`
- `if foo { 1 } else { 2 }`
- assignment
