# from_fluent_bit

Receives events via [Fluent Bit](https://docs.fluentbit.io/).

## Synopsis

```tql
from_fluent_bit plugin:string, [options=record, fluent_bit_options=record,
                schema=string, selector=string, schema_only=bool, merge=bool, raw=bool,
                unflatten=string, arrays_of_objects=bool]
```

## Description

The `from_fluent_bit` operator acts as a bridge into the Fluent Bit ecosystem,
making it possible to acquire events from a Fluent Bit [input plugin][inputs].

[inputs]: https://docs.fluentbit.io/manual/pipeline/inputs

An invocation of the `fluent-bit` commandline utility

```bash
fluent-bit -o plugin -p key1=value1 -p key2=value2 -p…
```

translates to our `from_fluent_bit` operator as follows:

```tql
from_fluent_bit "plugin", options{key1: value1, key2:value2, …}
```

:::tip Output to Fluent Bit
You can output events to Fluent Bit using the [`to_fluent_bit` operator](to_fluent_bit.md).
:::

### `plugin: string`

The name of the Fluent Bit plugin.

Run `fluent-bit -h` and look under the **Inputs** section of the
help text for available plugin names. The web documentation often comes with an
example invocation near the bottom of the page, which also provides a good idea
how you could use the operator.

### `options = record (optional)`

Sets plugin configuration properties.

The key-value pairs in this record are equivalent to `-p key=value` for the
`fluent-bit` executable.

### `fluent_bit_options = record (optional)`

Sets global properties of the Fluent Bit service., e.g., `fluent_bit_options={flush:1, grace:3}`.

Consult the list of available [key-value pairs][service-properties] to configure
Fluent Bit according to your needs.

[service-properties]: https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/classic-mode/configuration-file#config_section

We recommend factoring these options into the plugin-specific `fluent-bit.yaml`
so that they are independent of the `fluent-bit` operator arguments.

### `merge = bool (optional)`

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can
lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a type
specified in the chosen `schema` will still be parsed according to the schema.

This means that JSON numbers will be parsed as numbers,
but every JSON string remains a string, unless the field is in the `schema`.

### `schema = string (optional)`

Provide the name of a schema to be used by the parser.

If a schema with a matching name is installed, the result will always have
all fields from that schema.
* Fields that are specified in the schema, but did not appear in the input will be null.
* Fields that appear in the input, but not in the schema will also be kept. `schema_only=true`
can be used to reject fields that are not in the schema.

If the given schema does not exist, this option instead assigns the output schema name only.

The `schema` option is incompatible with the `selector` option.

### `selector = string (optional)`

Designates a field value as schema name with an optional dot-separated prefix.

The string is parsed as `<fieldname>[:<prefix>]`. The `prefix` is optional and
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

### `unflatten = string (optional)`

A delimiter that, if present in keys, causes values to be treated as values of
nested records.

A popular example of this is the [Zeek JSON](read_zeek_json.md) format. It includes
the fields `id.orig_h`, `id.orig_p`, `id.resp_h`, and `id.resp_p` at the
top-level. The data is best modeled as an `id` record with four nested fields
`orig_h`, `orig_p`, `resp_h`, and `resp_p`.

## URI support & integration with `from`

The `from_fluent_bit` operator can also be used from the [`from`](from.md)
operator. For this, the `fluentbit://` scheme can be used. The URI is then translated:

```tql
from "fluentbit://plugin"
```
```tql
from_fluent_bit "plugin"
```

## Examples

### OpenTelemetry

Ingest [OpenTelemetry](https://docs.fluentbit.io/manual/pipeline/inputs/slack)
logs, metrics, and traces:

```tql
from_fluent_bit "opentelemetry"
```

You can then send JSON-encoded log data to a freshly created API endpoint:

```bash
curl \
  --header "Content-Type: application/json" \
  --request POST \
  --data '{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"1660296023390371588","body":{"stringValue":"{\"message\":\"dummy\"}"},"traceId":"","spanId":""}]}]}]}' \
  http://0.0.0.0:4318/v1/logs
```

### Splunk

Handle [Splunk](https://docs.fluentbit.io/manual/pipeline/inputs/splunk) HTTP HEC requests:

```tql
from_fluent_bit "splunk", options={port: 8088}
```

### ElasticSearch & OpenSearch

Handle [ElasticSearch & OpenSearch](https://docs.fluentbit.io/manual/pipeline/inputs/elasticsearch)
Bulk API requests or ingest from beats (e.g., Filebeat, Metricbeat, Winlogbeat):

```tql
from_fluent_bit "elasticsearch", options={port: 9200}
```
