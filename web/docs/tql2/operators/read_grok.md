# read_grok

```tql
read_grok pattern:str, [pattern_definitions=str, indexed_captures=bool,
          include_unnamed=bool, schema=str, selector=str,
          schema_only=bool, merge=bool, raw=bool, unflatten=str]
```

## Description

`read_grok` uses a regular expression based parser similar to the
[Logstash `grok` plugin](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html)
in Elasticsearch. Tenzir ships with the same built-in patterns as Elasticsearch,
found [here](https://github.com/logstash-plugins/logstash-patterns-core/tree/main/patterns/ecs-v1).

In short, `pattern` consists of replacement fields, that look like
`%{SYNTAX[:SEMANTIC[:CONVERSION]]}`, where:
- `SYNTAX` is a reference to a pattern, either built-in or user-defined 
    through the `pattern_defintions` option.
- `SEMANTIC` is an identifier that names the field in the parsed record.
- `CONVERSION` is either `infer` (default), `string` (default with
    `raw=true`), `int`, or `float`.

The supported regular expression syntax is the one supported by
[Boost.Regex](https://www.boost.org/doc/libs/1_81_0/libs/regex/doc/html/boost_regex/syntax/perl_syntax.html),
which is effectively Perl-compatible.

### `pattern: str`

The `grok` pattern used for matching. Must match the input in its entirety.

### `pattern_definitions = str (optional)`

A user-defined newline-delimited list of patterns, where a line starts 
with the pattern name, followed by a space, and the `grok`-pattern for that
pattern. For example, the built-in pattern `INT` is defined as follows:

```
INT (?:[+-]?(?:[0-9]+))
```

### `indexed_captures = bool (optional)`

All subexpression captures are included in the output, with the `SEMANTIC` used
as the field name if possible, and the capture index otherwise.

### `include_unnamed = bool (optional)`

By default, only fields that were given a name with `SEMANTIC`, or with
the regular expression named capture syntax `(?<name>...)` are included
in the resulting record.

With `include_unnamed=true`, replacement fields without a `SEMANTIC` are included
in the output, using their `SYNTAX` value as the record field name.

### `merge = bool (optional)`

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

This option can not be combined with `raw=true, schema=<schema>`.

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen schema will still be parsed according to the schema.

For example, the JSON format has no notion of an IP address, so this will cause all IP addresses
to be parsed as strings, unless the field is specified to be an IP address by the schema.
JSON however has numeric types, so those would be parsed.

Use with caution.

This option can not be combined with `merge=true, schema=<schema>`.

### `schema = str (optional)`

Provide the name of a [schema](../../data-model/schemas.md) to be used by the
parser. If the schema uses the `blob` type, then the JSON parser expects
base64-encoded strings.

The `schema` option is incompatible with the `selector` option.

### `selector = str (optional)`

Designates a field value as schema name with an optional dot-separated prefix.

For example, the Suricata EVE JSON format includes a field
`event_type` that contains the event type. Setting the selector to
`event_type:suricata` causes an event with the value `flow` for the field
`event_type` to map onto the schema `suricata.flow`.

The `selector` option is incompatible with the `schema` option.

### `schema_only = bool (optional)`

When working with an existing schema, this option will ensure that the output
schema has *only* the fields from that schema. If the schema name is obtained via a `selector`
and it does not exist, this has no effect.

This option requires either `schema` or `selector` to be set.

### `unflatten = str (optional)`

A delimiter that, if present in keys, causes values to be treated as values of
nested records.

A popular example of this is the [Zeek JSON](read_zeek_json.md) format. It includes
the fields `id.orig_h`, `id.orig_p`, `id.resp_h`, and `id.resp_p` at the
top-level. The data is best modeled as an `id` record with four nested fields
`orig_h`, `orig_p`, `resp_h`, and `resp_p`.

Without an unflatten separator, the data looks like this:

```json
{
  "id.orig_h" : "1.1.1.1",
  "id.orig_p" : 10,
  "id.resp_h" : "1.1.1.2",
  "id.resp_p" : 5
}
```

With the unflatten separator set to `.`, Tenzir reads the events like this:

```json
{
  "id" : {
    "orig_h" : "1.1.1.1",
    "orig_p" : 10,
    "resp_h" : "1.1.1.2",
    "resp_p" : 5
  }
}
```

## Examples

Parse a fictional HTTP request log:

```tql
// Example input:
// 55.3.244.1 GET /index.html 15824 0.043
read_grok "%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}"
```

```json title="Output"
{
  "client": "55.3.244.1",
  "method": "GET",
  "request": "/index.html",
  "bytes": 15824,
  "duration": 0.043
}
```
