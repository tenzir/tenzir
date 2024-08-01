---
sidebar_custom_props:
  format:
    parser: true
---

# grok

Parses a string using a `grok`-pattern, backed by regular expressions.

## Synopsis

```
grok [--raw] [--include-unnamed] [--indexed-captures]
     [--pattern-definitions <additional_patterns>]
     <input_pattern>
```

## Description

`grok` uses a regular expression based parser similar to the
[Logstash `grok` plugin](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html)
in Elasticsearch. Tenzir ships with the same built-in patterns as Elasticsearch,
found [here](https://github.com/logstash-plugins/logstash-patterns-core/tree/main/patterns/ecs-v1).

In short, `<input_pattern>` consists of replacement fields, that look like
`%{SYNTAX[:SEMANTIC[:CONVERSION]]}`. `SYNTAX` is a reference to a pattern,
either built-in or user-defined through `--pattern-defintions`.
`SEMANTIC` is an identifier that names the field in the parsed record.
`CONVERSION` is either `infer` (default), `string` (default with `--raw`),
`int`, or `float`.

The supported regular expression syntax is the only supported by
[Boost.Regex](https://www.boost.org/doc/libs/1_81_0/libs/regex/doc/html/boost_regex/syntax/perl_syntax.html),
which is effectively Perl-compatible.

### `<input_pattern>`

The `grok` pattern used for matching. Must match the input in its entirety.

### `--raw`

By default, `grok` attempts to do type inference to the parsed fields.
This behavior can be accessed explicitly by setting the `CONVERSION` option
in a replacement field to `infer`.

To disable type inference, use `--raw`.

### `--include-unnamed`

By default, only fields that were given a name with `SEMANTIC`, or with
the regular expression named capture syntax `(?<name>...)` are included
in the resulting record.

With `--include-unnamed`, replacement fields without a `SEMANTIC` are included
in the output, using their `SYNTAX` value as the record field name.

### `--indexed-captures`

All subexpression captures are included in the output, with the `SEMANTIC` used
as the field name if possible, and the capture index otherwise.

### `--pattern-definitions <additional_patterns>`

`<additional_patterns>` can contain a user-defined newline-delimited list
of patterns, where a line starts with the pattern name, followed by a space,
and the `grok`-pattern for that pattern. For example, the built-in pattern
`INT` is defined as follows:

```
INT (?:[+-]?(?:[0-9]+))
```

## Examples

Parse a fictional HTTP request log:

```
# Example input:
# 55.3.244.1 GET /index.html 15824 0.043
grok "%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}"
```
