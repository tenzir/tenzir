# fluentbit

Sends and receives events via [Fluent Bit](https://docs.fluentbit.io/).

```tql
fluentbit plugin:string, [options=record, fluent_bit_options=record, schema=string, selector=string, schema_only=bool, merge=bool, raw=bool, unflatten=string]
```

## Description

The `fluentbit` operator acts as a bridge into the Fluent Bit ecosystem,
making it possible to acquire events from a Fluent Bit [input plugin][inputs]
and process events with a Fluent Bit [output plugin][outputs].

[inputs]: https://docs.fluentbit.io/manual/pipeline/inputs
[outputs]: https://docs.fluentbit.io/manual/pipeline/outputs

### `plugin: string`

The name of the Fluent Bit plugin.

Run `fluent-bit -h` on the command line and look under the **Inputs** and
**Outputs** section of the help text for available plugin names. The web
documentation often comes with an example invocation near the bottom of the
page, which also provides a good idea how you could use the operator.

### `fluent_bit_options = record (optional)`

A record of the global properties of the Fluent Bit service.

Consult the list of available [key-value pairs][service-properties] to configure
Fluent Bit according to your needs.

[service-properties]: https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/classic-mode/configuration-file#config_section

We recommend factoring these options into the plugin-specific `fluent-bit.yaml`
so that they are independent of the operator arguments.

### `options = record (optional)`

A record of the plugin configuration properties. Equivalent to setting each
property with `-p key=value` on the command line.

### `merge = bool (optional)`

Merges all incoming events into a single schema[^1] that converges over time.
This option is usually the fastest *for reading* highly heterogeneous data, but
can lead to huge schemas filled with nulls and imprecise results. Use with
caution.

[^1]: In selector mode, only events with the same selector are merged.

This option cannot be combined with `raw=true, schema=<schema>`.

### `raw = bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a
type specified in the chosen schema will still be parsed according to the
schema.

For example, the JSON format has no notion of an IP address, so this will cause
all IP addresses to be parsed as strings, unless the field is specified to be an
IP address by the schema. JSON however has numeric types, so those would be
parsed.

Use with caution.

This option cannot be combined with `merge=true, schema=<schema>`.

### `schema = string (optional)`

Provide the name of a schema to be used by the parser. If the schema uses the
`blob` type, then the JSON parser expects base64-encoded strings.

The `schema` option is incompatible with the `selector` option.

### `selector = string (optional)`

Designates a field value as schema name with an optional dot-separated prefix.

For example, the Suricata EVE JSON format includes a field
`event_type` that contains the event type. Setting the selector to
`event_type:suricata` causes an event with the value `flow` for the field
`event_type` to map onto the schema `suricata.flow`.

The `selector` option is incompatible with the `schema` option.

### `schema_only = bool (optional)`

When working with an existing schema, this option will ensure that the output
schema has *only* the fields from that schema. If the schema name is obtained
via a `selector` and it does not exist, this has no effect.

This option requires either `schema` or `selector` to be set.

### `unflatten = string (optional)`

A delimiter that, if present in keys, causes values to be treated as values of
nested records.

A popular example of this is the [Zeek JSON](read_zeek_json.md) format. It
includes the fields `id.orig_h`, `id.orig_p`, `id.resp_h`, and `id.resp_p` at
the top-level. The data is best modeled as an `id` record with four nested
fields `orig_h`, `orig_p`, `resp_h`, and `resp_p`.

Without an unflatten separator, the data looks like this:

```tql
{
  id.orig_h: 1.1.1.1,
  id.orig_p: 10,
  id.resp_h: 1.1.1.2,
  id.resp_p: 5,
}
```

With the unflatten separator set to `.`, Tenzir reads the events like this:

```json
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

### Imitate a Splunk HEC endpoint

```tql
fluentbit "splunk", options = { port: 8088 }
```

### Imitate an ElasticSearch & OpenSearch Bulk API endpoint

This allows you to ingest from beats (e.g., Filebeat, Metricbeat, Winlogbeat).

```tql
fluentbit "elasticsearch", options = { port: 9200 }
```

### Send to ElasticSearch

```tql
fluentbit "es", options = { host: 192.168.2.3, port: 9200, index: "my_index", type: "my_type" }
```
