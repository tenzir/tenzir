---
sidebar_custom_props:
  format:
    parser: true
---

# kv

Reads key-value pairs by splitting strings based on regular expressions.

## Synopsis

```
kv <field_split> <value_split>
```

## Description

The `kv` parser is usually used with the [`parse`](../operators/parse.md)
operator to extract key-value pairs from a given string, in particular if the
keys are not known before.

Incoming strings are first split into fields according to `<field_split>`. This
can be a regular expression. For example, the input `foo: bar, baz: 42` can be
split into `foo: bar` and `baz: 42` with the `",\s*"` (a comma, followed by any
amount of whitespace) as the field splitter. Note that the matched separators
are removed when splitting a string.

Afterwards, the extracted fields are split into their key and value by
`<value_split>`, which can again be a regular expression. In our example,
`":\s*"` could be used to split `foo: bar` into the key `foo` and its value
`bar`, and similarly `baz: 42` into `baz` and `42`. The result would thus be
`{"foo": "bar", "baz": 42}`. If the regex matches multiple substrings, only the
first match is used.

The supported regular expression syntax is
[RE2](https://github.com/google/re2/wiki/Syntax). In particular, this means that
lookahead `(?=...)` and lookbehind `(?<=...)` are not supported by `kv` at
the moment. However, if the regular expression has a capture group, it is assumed
that only the content of the capture group shall be used as the separator. This
means that unsupported regular expressions such as `(?=foo)bar(?<=baz)` can be
effectively expressed as `foo(bar)baz` instead.

### Quoted Values

The parser is aware of double-quotes (`"`). If the `<field_split>` or
`<value_split>` are found within enclosing quotes, they are not considered matches.

This means that both the key and value may be enclosed in double-quotes.

### `<field_split>`

The regular expression used to separate individual fields.

### `<value_split>`

The regular expression used to separate a key from its value.

## Examples

Extract comma-separated key-value pairs from `foo:1, bar:2,baz:3 , qux:4`:

```
kv "\s*,\s*" ":"
```

Extract key-value pairs from strings such as `FOO: C:\foo BAR_BAZ: hello world`.
This requires lookahead because the fields are separated by whitespace, but not
every whitespace acts as a field separator. Instead, we only want to split if
the whitespace is followed by `[A-Z][A-Z_]+:`, i.e., at least two uppercase
characters followed by a colon. We can express this as `"(\s+)[A-Z][A-Z_]+:"`,
which yields `FOO: C:\foo` and `BAR_BAZ: hello world`. We then split the key
from its value with `":\s*"` (only the first match is used to split them). The
final result is thus `{"FOO": "C:\foo", "BAR_BAZ": "hello world"}`.

```
kv "(\s+)[A-Z][A-Z_]+:" ":\s*"
```
