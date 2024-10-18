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

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen `schema` will still be parsed according to the schema.

Since grok is just textual parsing, this means that no parsing of data takes place at all
and every value remains a string, unless the field is in the `schema`.

### `schema = str (optional)`

Provide the name of a [schema](../../data-model/schemas.md) to be used by the
parser.

If a schema with a matching name is installed, the result will always have
all fields from that schema.
* Fields that are specified in the schema, but did not appear in the input will be null.
* Fields that appear in the input, but not in the schema will also be kept. `schema_only=true`
can be used to reject fields that are not in the schema.

If the given schema does not exist, this option instead assigns the output schema name only.

The `schema` option is incompatible with the `selector` option.

### `selector = str (optional)`

Designates a field value as [schema](../../data-model/schemas.md) name with an
optional dot-separated prefix.

The string is parsed as `<filename>[:<prefix>]`. The `prefix` is optional and
will be prepended to the field value to generate the schema name.

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

```json title="Without unflattening"
{
  "id.orig_h": "1.1.1.1",
  "id.orig_p": 10,
  "id.resp_h": "1.1.1.2",
  "id.resp_p": 5
}
```

With the unflatten separator set to `.`, Tenzir reads the events like this:

```json title="With 'unflatten'"
{
  "id": {
    "orig_h": "1.1.1.1",
    "orig_p": 10,
    "resp_h": "1.1.1.2",
    "resp_p": 5
  }
}
```

## Examples

Parse a fictional HTTP request log:

```tql
// Input: 55.3.244.1 GET /index.html 15824 0.043
read_grok "%{IP:client} %{WORD} %{URIPATHPARAM:req} %{NUMBER:bytes} %{NUMBER:dur}"
――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
{
  client: 55.3.244.1,
  req: "/index.html",
  bytes: 15824,
  dur: 0.043
}
```
